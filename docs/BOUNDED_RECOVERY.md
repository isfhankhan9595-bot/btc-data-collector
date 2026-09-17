# Bounded recovery, backoff and rate limiting

Implemented in `collector/collector/backoff.py` and
`collector/collector/recovery_control.py`; wired into
`collector/collector/websocket_client.py` and `collector/run_collector.py`.

## The defect (D13)

Two unbounded loops.

**Reconnect.** The websocket loop doubled a bare delay (`retry_delay * 2`,
capped at 60s) with **no jitter and no attempt cap**. Every client that lost
its connection at the same moment retried at the same moment, forever.

**Recovery.** A sequence gap scheduled a REST snapshot, guarded only by
"is the previous task still running". A burst of gaps arriving faster than a
snapshot completes, or a flapping connection, could drive repeated snapshot
requests with no minimum spacing, no ceiling per minute, and no response to
a venue rate limit. One gap could become hundreds of REST calls.

## Four independent limits on recovery

| Limit | Purpose |
|---|---|
| **Deduplication** | at most one recovery in flight; further requests are suppressed, not queued — a second snapshot taken during the first adds nothing |
| **Cooldown** | minimum interval between the end of one attempt and the start of the next |
| **Window ceiling** | hard cap on attempts per rolling window, so even perfectly spaced attempts cannot exceed a known request rate |
| **Backoff** | after consecutive failures, wait longer with jitter, up to an attempt budget |

A venue rate-limit response overrides all four with the penalty the venue
asked for.

The controller is the **single dedup authority**. An earlier draft checked
the asyncio task handle first, which short-circuited before the suppression
could be counted — a gap burst would then be invisible in exactly the
counters the controller exists to produce. The order is now: ask the
controller, then create the task, then mark in-flight **synchronously**,
because between `create_task` and the coroutine's first line there is a
window in which further gaps would otherwise slip through.

## Jitter

Full jitter — `delay = uniform(0, cap)` — where only the *cap* grows
exponentially. The "multiply the delay itself" variant is not used: one
unlucky short sleep would permanently depress the series.

A consequence worth stating plainly: **successive delays are not
monotonically increasing.** That is the point. An existing test asserted
`sleep_calls[1] > sleep_calls[0]`, which is incompatible with jitter and was
a latent flake; it now asserts the correct contract — each delay lies within
its own growing cap.

## Rate limits

`rate_limit_penalty()` handles 429 and 418. A venue-supplied `Retry-After`
always wins; the local defaults (30s for 429, 300s for 418) are fallbacks
used only when no header is present. Binance USD-M returns 429 as a limit
warning and 418 when an IP has been auto-banned for continuing past 429,
which is why the ban penalty is an order of magnitude larger.

Only the delta-seconds form of `Retry-After` is honoured. The HTTP-date form
returns `None` and falls back to the documented default rather than
mis-parsing a date into a wrong delay.

## Observability without a second storm

Every suppression is counted, broken out by reason. Quality events are
emitted on **transitions** — the first suppression of an episode, and the
resumption — never one per suppressed request. Emitting an event per
suppressed gap would replace a REST storm with a quality-event storm, which
is the same bug wearing a different hat. The resumption event carries the
full magnitude of what was suppressed.

## Known limitations

- The window ceiling is a rolling count, not a token bucket; a burst at the
  start of a window is permitted so long as the total stays under the cap.
- Backoff parameters are policy defaults, not tuned against measured venue
  behaviour.
- The reconnect budget (64 attempts) stops a permanently broken endpoint and
  records it, but does not attempt a slower "cold" retry afterwards. The
  process must be restarted. That is deliberate for now: silently retrying
  forever at a low rate is how an outage becomes invisible.
- Rate-limit handling is wired for the Binance depth snapshot only, since
  that is the only REST call under recovery pressure. The OI poll records
  failures but does not yet consult the controller.
