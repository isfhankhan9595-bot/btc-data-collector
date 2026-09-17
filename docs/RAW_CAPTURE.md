# Raw wire capture and the no-silent-discard contract

Implemented in `collector/collector/raw_capture.py` and
`collector/collector/adapters/base.py`; wired in `collector/run_collector.py`
and `collector/collector/websocket_client.py`.

## Why

Deterministic replay is only possible if enough original information was
persisted to reproduce ingestion. Before this phase the collector stored
*derived* data:

- `binance_orderbook_raw` holds normalised levels produced **after**
  reconstruction — not the wire.
- REST snapshot bodies were parsed and discarded once applied.
- Websocket frame text was thrown away immediately after `json.loads`.
- There was no connection id on any stored record.

Replaying from that storage would require contacting the live exchange for
the missing history. That is not replay.

## Two record kinds

### `raw_wire`

One websocket frame, exactly as received, written **before** any lossy
transformation.

| Field | Meaning |
|---|---|
| `payload` | the frame text, verbatim |
| `payload_bytes` | true byte length (of the original, even when truncated) |
| `truncated` | payload exceeded the cap and was clipped |
| `decode_ok` / `decode_error` | whether `json.loads` succeeded, and why not |
| `local_receive_ts` | captured before decoding |
| `local_capture_ts` | when the capture row was built |
| `connection_id`, `connection_generation` | which socket, which connection |
| `venue`, `market_type`, `symbol`, `channel`, `stream` | routing lineage |
| `exchange_event_ts`, `update_id`, `first_update_id`, `previous_update_id` | venue-native identifiers, copied verbatim, never derived |

### `raw_rest`

One REST request/response exchange.

| Field | Meaning |
|---|---|
| `request_ts` | when the request left |
| `response_receive_ts` | when the body arrived (`NULL` if it never did) |
| `local_process_ts` | when the collector acted on it |
| `endpoint`, `method`, `request_params`, `purpose` | what was asked for |
| `http_status`, `ok`, `error` | outcome |
| `payload` | the response body, verbatim |

Recording the body is what allows replay to bridge the order book from
recorded data rather than the live exchange. Verified end to end: a captured
`depth` snapshot round-trips off disk with its `lastUpdateId` intact.

A **failed** REST exchange is recorded too, ordered by `request_ts` since no
response time exists. A recovery attempt that produced no bridge is lineage:
its absence would look identical to never having tried.

## Design rules

- **Capture precedes parsing.** A frame that fails to decode is still a frame
  that arrived; it is stored with `decode_ok=False`.
- **Never fabricate.** Fields the wire did not carry stay `NULL`. The capture
  layer does not parse, enrich or repair.
- **Capture cannot stop ingestion.** A capture failure becomes a durable
  `DATA_DROP` quality event and the frame still flows. Losing research
  fidelity is bad; losing the live feed is worse.
- **Bounded.** Payloads above `DEFAULT_MAX_PAYLOAD_BYTES` (4 MB) are stored
  truncated with the original length recorded, and truncation itself raises a
  quality event, because a clipped payload is partial raw data.

## The no-silent-discard contract

Seven paths previously turned received data into nothing with no durable
record. Each now leaves one.

| Path | Before | After |
|---|---|---|
| `handle_message` non-envelope frame | bare `return` | `malformed_envelope` counter + `DATA_DROP` with frame keys |
| unrouted stream | counter + log line | durable `DATA_DROP` naming the stream |
| websocket `JSONDecodeError` | log line, text discarded | raw row with `decode_ok=False` + durable `ERROR` |
| adapter `normalize()` fallthrough | bare `return []` | `UnhandledMessage` → durable `DATA_DROP` |
| REST snapshot body | parsed, discarded | `raw_rest` row, success **and** failure |
| OI poll body | parsed, discarded | `raw_rest` row with request/response times kept distinct |
| OI poll exception | log line | `raw_rest` failure row + durable `ERROR` |

### Adapter classification

`normalize()` still returns `list`, so callers are unchanged. Every non-event
outcome now routes through `ExchangeAdapter.unhandled()`, which classifies it:

| Reason | Meaning |
|---|---|
| `NO_ROUTE` | frame matched no channel this adapter routes |
| `CHANNEL_NOT_IMPLEMENTED` | channel is declared but `normalize()` does not handle it |
| `MALFORMED_PAYLOAD` | routed, but structurally unusable |
| `CONTROL_FRAME` | subscribe ack, pong, error envelope |
| `EMPTY_DATA` | routed and well formed, empty data array |

`CONTROL_FRAME` is deliberately distinct from the others: a subscription ack
carrying no market data is not data loss, and conflating the two would make
the drop counter useless.

The unhandled buffer is bounded (256). Eviction increments
`unhandled_dropped`, so losing an unhandled record is itself not silent.

### D11 made explicit

`OKXAdapter.unimplemented_channels` now names the six declared-but-unbuilt
channels in code:

```
trades, mark-price, index-tickers, open-interest, funding-rate, liquidation-orders
```

`implemented_channels()` returns `{"books"}`. The gap is asserted in tests
rather than hidden behind an early return. **This does not implement those
channels** — it stops them from pretending to work.

## Known limitations

- Raw capture is wired for **Binance only**, because only Binance is
  connected in `run_collector`. Bybit and OKX adapters classify unhandled
  messages but are not yet driven by a live client.
- A replay *engine* does not exist yet. This phase makes replay **possible**
  by persisting sufficient input; D9 remains open.
- `binance_orderbook_raw` still stores post-reconstruction levels. It is now
  redundant with `raw_wire` for replay purposes but is retained because
  downstream tooling reads it. Consolidation is deferred.
- Payload truncation at 4 MB is a policy choice, not a measured limit.
