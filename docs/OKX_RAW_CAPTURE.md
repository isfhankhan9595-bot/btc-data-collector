# OKX v5 public raw capture (D11 partial unblock)

## Why this exists

`OKXAdapter` declares seven channels and implements one. The six others —
`trades`, `mark-price`, `index-tickers`, `open-interest`, `funding-rate`,
`liquidation-orders` — are pure field-mapping parsers, and their push-data
field names could not be read from official documentation:
`okx.com/docs-v5` is a single-page application, and fetching it returns the
document flattened and truncated in the REST/account sections, before the
WebSocket public-channel push-data tables.

Writing those parsers from memory would produce code that runs, passes any
test written against the same assumption, and emits **silently wrong numbers**
into the research record. That is worse than the current state, where the six
channels are named in `unimplemented_channels` and every routed message
raises `CHANNEL_NOT_IMPLEMENTED` through `unhandled()`.

So instead of guessing the schemas, this layer captures the real frames and
lets them be read off the wire.

## What is verified, and what is not

**Verified from official documentation** (OKX v5 API, "WebSocket" overview /
"Subscribe" / "Unsubscribe" / "Notification", read 2026-09-19) — and therefore
implemented here:

| Fact | Value |
|---|---|
| Public endpoint | `wss://ws.okx.com:8443/ws/v5/public`, no auth for public channels |
| Subscribe request | `{"id": <optional>, "op": "subscribe", "args": [{"channel": …, "instId": …}]}` |
| Args size limit | combined channels must not exceed 64 KB |
| Subscribe response | `{"event": "subscribe", "arg": {…}, "connId": …}` |
| Error response | `{"event": "error", "code": …, "msg": …, "connId": …}` |
| Notice | `{"event": "notice", "code": "64008", …}`, 60s before an upgrade closes the socket |
| Idle disconnect | connection breaks if nothing pushed for >30s |
| Heartbeat | timer of N seconds (N < 30) from last message → send literal `ping` → expect literal `pong` |
| Connect limit | 3 requests/second, by IP |
| Subscribe budget | 480 `subscribe`/`unsubscribe`/`login` per connection per hour |

**Not verified, and therefore not implemented anywhere:** the field names,
units, sign conventions and side encodings inside `data[]` for any channel.

The only payload fields this module reads are `arg.channel` and `arg.instId`,
which are documented **envelope** fields, not payload fields.

## Components

| File | Role |
|---|---|
| `collector/collector/websocket_client.py` | extended with three optional venue hooks: `on_open`, `keepalive`, `control_frames` |
| `collector/collector/okx_capture.py` | `OKXPublicCapture` — connect, subscribe, classify envelopes, persist raw frames with lineage |
| `collector/run_okx_capture.py` | standalone bounded entrypoint; writes `okx_raw_wire` + `okx_quality_events` only (its own namespace, not Binance's `raw_wire`/`quality_events`; captures made before the storage-namespace phase are in the unprefixed directories — see `docs/STORAGE_NAMESPACES.md`) |
| `collector/scripts/okx_schema_report.py` | reads captured frames back (`okx_raw_wire`, plus legacy `raw_wire` filtered to OKX rows) and reports observed field structure per channel |

### Why the WS client needed extending rather than duplicating

Binance carries its streams in the URL and needs no application heartbeat —
the server drives protocol pings and the `websockets` library answers them.
OKX must subscribe over the socket and must heartbeat itself. All three hooks
default to off, so Binance behaviour is byte-for-byte unchanged (pinned by
`test_binance_style_client_starts_no_keepalive_task`).

`control_frames` matters more than it looks. OKX's `pong` is the bare string
`pong`, not JSON. Without an explicit control-frame allowlist it reaches
`json.loads`, fails, and writes a durable `ERROR` quality event — one per
heartbeat, forever, on a completely healthy connection. That is a false
data-quality signal of the same class as D18.

### Subscription ledger

`SubscriptionLedger` tracks requested vs acknowledged vs rejected channels.
Without it, subscribing to seven channels and having three rejected is
indistinguishable from a quiet market: the socket is open, frames arrive for
the four that worked, and nothing in the stored data says the other three are
dead. Acknowledgements are cleared on reconnect, because an ack belongs to a
connection — carrying them forward would claim coverage the new connection has
not been granted.

### Capture subscribes to channels the parser refuses

This is the point. `run_okx_capture` defaults to every channel the adapter
*declares*, including all six it does not implement, because those are exactly
the schemas that need observing. `OKXAdapter.normalize` still refuses them;
capture and parsing are deliberately decoupled.

## Usage

```bash
# Capture (requires outbound access to ws.okx.com:8443)
python -m collector.run_okx_capture --duration 600 --data-dir data

# Read the schemas off what was captured
python -m collector.scripts.okx_schema_report --data-dir data
```

## What the schema report does and does not establish

It reports, per channel: which keys appeared inside `data[]`, how often
(presence ratio, so a partially-present field does not look mandatory), the
observed value types, and one truncated example.

Value typing distinguishes `str` from `str(numeric)`, including nested —
order-book levels report as `list[list[str(numeric)]]`. OKX sends numbers as
strings, and knowing that is what prevents float-coercion bugs.

**It establishes field NAMES and TYPES. It does not establish MEANINGS.**
Whether `fundingRate` is a period rate or an annualised one, what a
liquidation `side` value refers to, and what units `oi` versus `oiCcy` carry
are semantic questions that observation cannot answer. The rendered report
says so explicitly, and a test asserts that caveat is present — because a
report listing field names without it invites exactly the mistake D11 exists
to prevent.

## Status

| Item | Status |
|---|---|
| OKX connection protocol | **COMPLETE** (verified against official docs) |
| OKX raw frame capture + lineage | **COMPLETE** (39 tests) |
| Subscription observability | **COMPLETE** |
| Schema observation tooling | **COMPLETE** |
| Live captured OKX frames | **NOT OBTAINED** — see below |
| OKX six-channel parsers (D11) | **BLOCKED** — needs semantics, not just names |
| OKX live ingestion into canonical events | **NOT STARTED** |

### Environment blocker

This code has **never been run against the live venue**. The container it was
written in denies egress to `ws.okx.com` (`x-deny-reason: host_not_allowed`;
only an allowlist of package/registry hosts is reachable). Every test here
drives the real client loop through a fake socket, which verifies the logic
and the protocol *shape* but cannot verify that OKX accepts the subscribe
request or that the heartbeat satisfies it.

Until it runs somewhere with network access, treat "OKX capture works" as
**UNVERIFIED against the venue**.
