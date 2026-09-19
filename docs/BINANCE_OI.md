# Binance open-interest replayability (G2)

Implemented in `collector/collector/binance_oi.py`, consumed by
`collector/run_collector.py` (live) and `collector/collector/replay.py`
(replay).

## The defect

Live OI polling parsed the REST body directly in `_poll_openinterest`,
calling `compute_openinterest_features()`, which used **wall-clock time**
(`int(time.time() * 1000)`) for both the observation's `timestamp` and
`local_timestamp` -- discarding the request/response timestamps raw capture
already recorded alongside it.

`ReplaySource.from_records` kept only REST rows whose `purpose` was
`"orderbook_snapshot"`. Every recorded OI observation (`purpose ==
"open_interest"`) was excluded **before it ever became a `ReplayFrame`** --
not misprocessed, simply never reached the engine. Live OI and replayed OI
were not the same pipeline; replay could not reproduce OI at all.

## The fix

One normalizer, two call sites:

```
LIVE REST RESPONSE  ─┐
                      ├─> normalize_binance_oi() → CanonicalOIEvent → storage / non_book_events
RECORDED RAW REST    ─┘
```

`normalize_binance_oi(body, response_receive_ts, ...)` parses the response
body and returns a `CanonicalOIEvent`. `run_collector._poll_openinterest`
and `ReplayEngine._handle_rest_oi` both call it; neither has its own
parsing logic. A structural test (`test_live_poll_uses_the_shared_normalizer_not_a_second_parser`)
asserts the legacy parser is gone from the live path, not just that a new
one exists alongside it.

## Causality

The event's `local_receive_ts` is the **response's** receive timestamp --
not the request timestamp, and not the exchange's own `time` field. A
response that took 5 seconds to arrive must not claim availability 5
seconds earlier than it actually was known; that would erase real latency
and let replay see information before the collector actually held it.

A request that never returned (`response_receive_ts is None`) was never
available live, so it is excluded from the replay frame stream entirely --
the same rule the Binance depth-snapshot replay already followed.

## New frame kind

`FrameKind.REST_OI` is distinct from `FrameKind.REST_SNAPSHOT`: an OI
reading drives no order-book state and never bridges a gap. It is routed
straight into `ReplayResult.non_book_events`, the same list trades,
funding, and liquidations already flow through -- there is no OI-specific
storage path in the replay result.

`normalize_binance_oi` is Binance-specific. Replaying a recorded OI row
under a different venue (`ReplayEngine(venue=...)`) is refused with a
`DATA_DROP` quality event, not silently skipped -- that would be a
configuration mistake worth surfacing, not a normal empty case.

## Unit

Binance USD-M's `/fapi/v1/openInterest` response does not state the
physical unit of `openInterest` against documentation this project has
verified. Per the OI-unit contract (`OIUnit`), an unstated unit is
`OIUnit.UNKNOWN` -- never guessed as `CONTRACTS` or `BASE_COIN`.
`assert_comparable_oi()` continues to refuse any cross-venue comparison
involving this event.

## Known limitations

- Only OI is wired through this pattern. Binance mark price, funding, and
  liquidations flow through the websocket adapter's `normalize()` already
  (that path was correct before this phase); OI was the one REST-polled
  stream still bypassing the canonical layer.
- The unit remains genuinely `UNKNOWN`. Resolving it requires reading
  current official Binance USD-M documentation closely enough to state the
  physical quantity with confidence -- not yet done.
