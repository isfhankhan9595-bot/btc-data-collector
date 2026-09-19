"""OKX v5 public WebSocket raw capture.

Purpose
-------
D11 records that :class:`~collector.collector.adapters.okx.OKXAdapter`
declares seven channels and implements one. The six unimplemented channels
are pure field-mapping parsers, and their push-data field names could not be
obtained from official documentation (``okx.com/docs-v5`` is a single-page
application; fetching it returns the document flattened and truncated before
the WebSocket public-channel push-data tables). Guessing those names yields a
parser that runs, passes any test written against the same guess, and emits
silently wrong numbers into the research record.

This module is the alternative: capture the real frames, then read the
schemas off captured data instead of off documentation. It deliberately
implements **only the connection protocol** -- which *is* documented and
verified -- and never inspects the contents of ``data[]``.

Verified protocol facts
-----------------------
Source: OKX v5 API documentation, "WebSocket" overview and "Subscribe" /
"Unsubscribe" / "Notification" sections, read 2026-09-19.

* Public endpoint: ``wss://ws.okx.com:8443/ws/v5/public``. No authentication
  is required for public channels.
* Subscribe request: ``{"id": <optional>, "op": "subscribe",
  "args": [{"channel": <name>, "instId": <id>}]}``. The total length of
  multiple channels must not exceed 64 KB.
* Subscribe response: ``{"id": ..., "event": "subscribe",
  "arg": {"channel": ..., "instId": ...}, "connId": ...}``.
* Error response: ``{"event": "error", "code": ..., "msg": ...,
  "connId": ...}``.
* Notice: ``{"event": "notice", "code": "64008", "msg": ...}`` is sent 60
  seconds before a service upgrade closes the connection.
* Heartbeat: the connection breaks automatically if no subscription is
  established or no data has been pushed for more than 30 seconds. The
  documented client obligation is to set a timer of N seconds (N < 30) from
  the last received message, send the string ``ping``, and expect ``pong``.
* Connection limit: 3 connect requests per second, by IP.
* Request limit: 480 ``subscribe``/``unsubscribe``/``login`` requests per
  connection per hour.

Push-data envelope fields used here -- ``arg.channel`` and ``arg.instId`` --
are documented envelope fields, not payload fields. Nothing inside ``data``
is read, so there is nothing to guess.

What this module does NOT do
----------------------------
It does not normalise, does not build canonical events, and does not write
any derived stream. Capturing a frame is lossless; interpreting one is not,
and interpretation is blocked until the schemas are verified.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable, Iterable, Mapping, Optional

from .raw_capture import RawWireRecord
from .websocket_client import Keepalive, WebSocketClient

#: Official public endpoint. Demo trading uses ``wspap.okx.com``; that is a
#: different venue's data and is deliberately not offered here.
OKX_PUBLIC_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"

#: Documented heartbeat. The venue disconnects after 30s of silence, so the
#: timer must be strictly below that; 20s leaves room for one lost ping.
OKX_IDLE_DISCONNECT_S = 30.0
OKX_PING_PAYLOAD = "ping"
OKX_PONG_PAYLOAD = "pong"
OKX_KEEPALIVE = Keepalive(
    payload=OKX_PING_PAYLOAD,
    interval_s=20.0,
    expect=OKX_PONG_PAYLOAD,
    timeout_s=10.0,
)

#: Documented connect pacing, by IP.
OKX_MAX_CONNECTS_PER_SECOND = 3
#: Documented subscribe/unsubscribe/login budget, per connection per hour.
OKX_MAX_SUBSCRIBE_REQUESTS_PER_HOUR = 480

#: BTC linear perpetual on OKX.
OKX_BTC_SWAP_INST_ID = "BTC-USDT-SWAP"

#: Guard against an oversized subscribe request. The venue caps the combined
#: args at 64 KB; this is a local sanity bound, not a protocol claim.
OKX_SUBSCRIBE_MAX_BYTES = 64 * 1024


class FrameKind(str, Enum):
    """How an inbound frame was classified, for observability only."""

    CONTROL = "control"          # literal pong
    SUBSCRIBE_ACK = "subscribe_ack"
    UNSUBSCRIBE_ACK = "unsubscribe_ack"
    ERROR = "error"
    NOTICE = "notice"
    CONN_COUNT = "conn_count"
    DATA_PUSH = "data_push"
    UNKNOWN = "unknown"


#: Default underlying-index instId, used by ``index-tickers``. This is OKX's
#: spot-style pair for BTC's index -- distinct from the SWAP instId every
#: other channel here subscribes with. Unverified against a live connection
#: (docs/OKX_D11_CHANNEL_SCHEMAS.md, open question #3); a caller who has
#: confirmed the correct identifier can override it via
#: ``okx_subscribe_message``'s ``index_inst_id`` parameter rather than this
#: module silently assuming it is right.
OKX_BTC_INDEX_INST_ID = "BTC-USDT"

#: OKX channel that subscribes by instType, not instId (confirmed in
#: docs/OKX_D11_CHANNEL_SCHEMAS.md, §F -- two independent sources). A single
#: subscribe args builder that applied ``instId`` to every channel uniformly
#: would send this channel the wrong argument shape entirely; that was the
#: bug before this set existed.
OKX_INST_TYPE_SCOPED_CHANNELS = frozenset({"liquidation-orders"})
OKX_LIQUIDATION_INST_TYPE = "SWAP"


def okx_subscribe_message(
    channels: Iterable[str],
    inst_id: str = OKX_BTC_SWAP_INST_ID,
    *,
    request_id: Optional[str] = None,
    index_inst_id: str = OKX_BTC_INDEX_INST_ID,
) -> dict[str, Any]:
    """Build the documented subscribe request.

    ``id`` is optional in the protocol but is set when supplied, because it is
    echoed back in the acknowledgement and is the only way to tie an ack to
    the request that caused it.

    Argument shape is channel-aware, not uniform: ``liquidation-orders``
    subscribes by ``instType`` (never ``instId``), and ``index-tickers``
    subscribes by its own index instId, not the SWAP instId every other
    channel here uses. See docs/OKX_D11_CHANNEL_SCHEMAS.md for the sourcing
    on both.
    """
    args = []
    for channel in channels:
        if channel in OKX_INST_TYPE_SCOPED_CHANNELS:
            args.append({"channel": channel, "instType": OKX_LIQUIDATION_INST_TYPE})
        elif channel == "index-tickers":
            args.append({"channel": channel, "instId": index_inst_id})
        else:
            args.append({"channel": channel, "instId": inst_id})
    if not args:
        raise ValueError("subscribe requires at least one channel")
    message: dict[str, Any] = {"op": "subscribe", "args": args}
    if request_id is not None:
        message["id"] = request_id
    return message


def classify_frame(parsed: Any) -> FrameKind:
    """Classify a decoded OKX frame by its envelope alone.

    Only envelope keys are examined. A frame that matches nothing known is
    ``UNKNOWN`` rather than being silently ignored -- an unrecognised envelope
    is information about the venue or about this code being out of date, and
    either way it must reach the quality record.
    """
    if not isinstance(parsed, Mapping):
        return FrameKind.UNKNOWN
    event = parsed.get("event")
    if event is not None:
        return {
            "subscribe": FrameKind.SUBSCRIBE_ACK,
            "unsubscribe": FrameKind.UNSUBSCRIBE_ACK,
            "error": FrameKind.ERROR,
            "notice": FrameKind.NOTICE,
            "channel-conn-count": FrameKind.CONN_COUNT,
            "channel-conn-count-error": FrameKind.CONN_COUNT,
        }.get(str(event), FrameKind.UNKNOWN)
    if "arg" in parsed and "data" in parsed:
        return FrameKind.DATA_PUSH
    return FrameKind.UNKNOWN


@dataclass
class SubscriptionLedger:
    """Requested vs acknowledged vs rejected channels.

    Without this, subscribing to seven channels and having three rejected is
    indistinguishable from a quiet market: the socket is open, frames arrive
    for the four that worked, and nothing anywhere says the other three are
    dead. The ledger makes "we are not subscribed" a first-class observable.
    """

    requested: set[str] = field(default_factory=set)
    acknowledged: set[str] = field(default_factory=set)
    rejected: dict[str, str] = field(default_factory=dict)

    def request(self, channels: Iterable[str]) -> None:
        self.requested.update(channels)

    def acknowledge(self, channel: Optional[str]) -> None:
        if channel:
            self.acknowledged.add(channel)
            self.rejected.pop(channel, None)

    def reject(self, channel: Optional[str], reason: str) -> None:
        # A rejection with no channel in the envelope is still a rejection;
        # file it under a reserved key rather than dropping it.
        self.rejected[channel or "<unattributed>"] = reason

    def reset_for_new_connection(self) -> None:
        """Acks belong to a connection, not to the process.

        After a reconnect nothing is subscribed until the venue says so
        again, so carrying old acks forward would claim coverage the new
        connection does not have.
        """
        self.acknowledged.clear()
        self.rejected.clear()

    @property
    def pending(self) -> set[str]:
        return self.requested - self.acknowledged - set(self.rejected)

    @property
    def fully_subscribed(self) -> bool:
        return bool(self.requested) and self.acknowledged >= self.requested

    def summary(self) -> dict[str, Any]:
        return {
            "requested": sorted(self.requested),
            "acknowledged": sorted(self.acknowledged),
            "rejected": dict(sorted(self.rejected.items())),
            "pending": sorted(self.pending),
        }


class OKXPublicCapture:
    """Connect, subscribe, and persist raw public frames with lineage.

    ``raw_capture`` is any object exposing ``capture_wire(RawWireRecord)``
    (in production, :class:`~collector.collector.raw_capture.RawCapture`).
    ``quality_sink`` receives dicts in the shape ``run_collector`` already
    persists, so OKX quality events land in the same durable stream as
    Binance's rather than in a second parallel mechanism.
    """

    VENUE = "OKX"
    MARKET_TYPE = "linear_perpetual"

    def __init__(
        self,
        channels: Iterable[str],
        *,
        inst_id: str = OKX_BTC_SWAP_INST_ID,
        raw_capture: Any = None,
        quality_sink: Optional[Callable[[dict], None]] = None,
        url: str = OKX_PUBLIC_WS_URL,
        client_factory: Optional[Callable[..., WebSocketClient]] = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.channels = tuple(dict.fromkeys(channels))
        if not self.channels:
            raise ValueError("OKXPublicCapture requires at least one channel")
        self.inst_id = inst_id
        self.raw_capture = raw_capture
        self.quality_sink = quality_sink
        self.url = url
        self._clock = clock
        self.ledger = SubscriptionLedger()
        self.frame_counts: dict[str, int] = {kind.value: 0 for kind in FrameKind}
        self.channel_frame_counts: dict[str, int] = {}
        self.last_frame_ts: Optional[int] = None
        self.subscribe_requests_sent = 0

        message = okx_subscribe_message(self.channels, self.inst_id, request_id="sub-1")
        encoded = json.dumps(message)
        if len(encoded.encode("utf-8")) > OKX_SUBSCRIBE_MAX_BYTES:
            raise ValueError(
                "subscribe request exceeds the venue's 64 KB args limit; "
                "split the channels across connections"
            )
        self._subscribe_message = message

        factory = client_factory or WebSocketClient
        self.client = factory(
            url=self.url,
            on_message=self._on_message,
            on_reconnect=self._on_reconnect,
            on_quality_event=self._on_quality_event,
            stream_group="okx_public",
            on_raw_frame=self._on_raw_frame,
            on_open=self._on_open,
            keepalive=OKX_KEEPALIVE,
            control_frames=frozenset({OKX_PONG_PAYLOAD}),
        )

    # -- lifecycle --------------------------------------------------------

    async def _on_open(self, send: Callable[[Any], Awaitable[None]]) -> None:
        await send(self._subscribe_message)
        self.subscribe_requests_sent += 1
        self.ledger.request(self.channels)
        self._emit("CONNECT", f"okx_subscribe_sent:{len(self.channels)}")

    def _on_reconnect(self) -> None:
        self.ledger.reset_for_new_connection()

    def _on_quality_event(self, event_type, reason, connection_id=None,
                          stream_group="okx_public") -> None:
        self._emit(event_type, reason, connection_id=connection_id,
                   stream=stream_group)

    def _emit(self, event_type: Any, reason: str, *, connection_id: Optional[str] = None,
              stream: str = "okx_public", rows_lost: Optional[int] = None) -> None:
        if self.quality_sink is None:
            return
        now = int(self._clock() * 1000)
        try:
            self.quality_sink({
                "exchange": self.VENUE,
                "stream": stream,
                "event_type": event_type,
                "reason": reason,
                "rows_lost": rows_lost,
                "connection_id": connection_id or getattr(self.client, "connection_id", None),
                "local_receive_ts": now,
                "local_ts": now,
            })
        except Exception:  # noqa: BLE001 - the sink must never break ingest
            pass

    # -- capture ----------------------------------------------------------

    def _on_raw_frame(self, payload, *, local_receive_ts, connection_id,
                      connection_generation, decode_ok, decode_error,
                      parsed=None, control_frame=False) -> None:
        """Persist the frame before anything interprets it.

        Channel and instrument are taken from the ``arg`` envelope when
        present so a captured frame is self-describing; they are never
        inferred from ``data``.
        """
        channel = None
        symbol = None
        if isinstance(parsed, Mapping):
            arg = parsed.get("arg")
            if isinstance(arg, Mapping):
                channel = arg.get("channel")
                symbol = arg.get("instId")
        if control_frame:
            channel = "__control__"

        record = RawWireRecord(
            local_receive_ts=local_receive_ts,
            payload=payload if isinstance(payload, str) else str(payload),
            venue=self.VENUE,
            connection_id=connection_id,
            connection_generation=connection_generation,
            channel=channel,
            stream="okx_public",
            symbol=symbol if isinstance(symbol, str) else None,
            market_type=self.MARKET_TYPE,
            decode_ok=bool(decode_ok),
            decode_error=decode_error,
        )
        if self.raw_capture is not None:
            self.raw_capture.capture_wire(record)

    async def _on_message(self, parsed, local_receive_ts, connection_id=None) -> None:
        """Classify the envelope and update the ledger. No payload parsing."""
        self.last_frame_ts = local_receive_ts
        kind = classify_frame(parsed)
        self.frame_counts[kind.value] = self.frame_counts.get(kind.value, 0) + 1

        arg = parsed.get("arg") if isinstance(parsed, Mapping) else None
        channel = arg.get("channel") if isinstance(arg, Mapping) else None

        if kind is FrameKind.SUBSCRIBE_ACK:
            self.ledger.acknowledge(channel)
            self._emit("CONNECT", f"okx_subscribed:{channel}", connection_id=connection_id)
            return

        if kind is FrameKind.ERROR:
            code = parsed.get("code")
            message = str(parsed.get("msg", ""))[:200]
            self.ledger.reject(channel, f"{code}:{message}")
            # Durable, not log-only: a rejected subscription means a channel
            # produces nothing for the life of this connection.
            self._emit("ERROR", f"okx_subscribe_error:{code}:{message}",
                       connection_id=connection_id, rows_lost=None)
            return

        if kind is FrameKind.NOTICE:
            self._emit("DISCONNECT",
                       f"okx_notice:{parsed.get('code')}:{str(parsed.get('msg',''))[:120]}",
                       connection_id=connection_id)
            return

        if kind is FrameKind.DATA_PUSH:
            if channel:
                self.channel_frame_counts[channel] = (
                    self.channel_frame_counts.get(channel, 0) + 1)
            return

        if kind is FrameKind.CONN_COUNT:
            self._emit("CONNECT", f"okx_conn_count:{parsed.get('connCount')}",
                       connection_id=connection_id)
            return

        # UNKNOWN / UNSUBSCRIBE_ACK
        keys = ",".join(sorted(str(k) for k in parsed.keys())) if isinstance(parsed, Mapping) else type(parsed).__name__
        self._emit("DATA_DROP", f"okx_unclassified_frame:keys={keys}",
                   connection_id=connection_id, rows_lost=1)

    # -- operations -------------------------------------------------------

    async def run(self) -> None:
        await self.client.start()

    def stop(self) -> None:
        self.client.stop()

    def status(self) -> dict[str, Any]:
        """Operational snapshot. Answers 'is anything actually arriving?'."""
        return {
            "venue": self.VENUE,
            "url": self.url,
            "inst_id": self.inst_id,
            "connected": getattr(self.client, "connected", False),
            "connection_id": getattr(self.client, "connection_id", None),
            "subscriptions": self.ledger.summary(),
            "fully_subscribed": self.ledger.fully_subscribed,
            "frame_counts": dict(self.frame_counts),
            "channel_frame_counts": dict(sorted(self.channel_frame_counts.items())),
            "last_frame_ts": self.last_frame_ts,
            "control_frames": getattr(self.client, "control_frames_received", 0),
            "malformed_frames": getattr(self.client, "malformed_frames", 0),
            "keepalive_timeouts": getattr(self.client, "keepalive_timeouts", 0),
            "raw_captured": getattr(self.raw_capture, "wire_captured", None),
            "capture_failures": getattr(self.raw_capture, "capture_failures", None),
        }
