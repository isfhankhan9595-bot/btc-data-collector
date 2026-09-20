"""Deterministic local order-book reconstruction for venue diff streams."""
from __future__ import annotations
from dataclasses import dataclass, replace
from decimal import Decimal
from threading import RLock
from .canonical import CanonicalOrderBookEvent
from .quality_events import BookQuality, BookQualityStateMachine, QualityEvent, QualityEventType
from .sequence import BinanceSequenceComparator, BybitSequenceComparator, OKXSequenceComparator, SpotSequenceComparator, binance_snapshot_bridge, binance_spot_snapshot_bridge

#: Venues whose local book buffers every event until a REST snapshot bridges
#: it (the "buffer diffs, then bridge with a snapshot" flow both Binance
#: products document, as opposed to Bybit/OKX's plain websocket-snapshot
#: bridge via `is_snapshot`). Binance Spot follows the identical flow --
#: same buffering philosophy, different discard/bridge arithmetic (see
#: sequence.py) -- so it belongs in this set, not a new code path.
_BUFFER_UNTIL_BRIDGED_VENUES = frozenset({"BINANCE", "BINANCE_SPOT"})

#: Per-venue snapshot-bridge predicate for `_attempt_bridge`. Only venues in
#: `_BUFFER_UNTIL_BRIDGED_VENUES` ever reach `_attempt_bridge`, so this needs
#: no entry for Bybit/OKX.
_SNAPSHOT_BRIDGE_FNS = {"BINANCE": binance_snapshot_bridge, "BINANCE_SPOT": binance_spot_snapshot_bridge}

#: Sources that may never mutate an authoritative reconstructed book.
#: ``<symbol>@depth10@100ms`` is a top-N *snapshot* pushed periodically, not a
#: diff: it never names levels that left the top N, so applying it through the
#: diff path silently freezes stale depth below the visible window. The
#: adapter already labels it ``PARTIAL_DEPTH`` and ``run_collector`` refuses it
#: at the routing layer, but ``LocalBook`` is the component that owns book
#: authority and must not depend on a caller for that guarantee -- the same
#: adapter output is reachable from replay, tests and any future runner.
NON_AUTHORITATIVE_BOOK_SOURCES = frozenset({"PARTIAL_DEPTH"})


@dataclass(frozen=True)
class BookTransition:
    previous_state: BookQuality
    new_state: BookQuality
    event: CanonicalOrderBookEvent | None
    reason: str = ""
    expected_previous_update_id: int | None = None

class LocalBook:
    """Authoritative in-memory book; non-VALID Binance diffs are buffered."""
    def __init__(self, venue: str, max_buffer_events: int = 10_000):
        if max_buffer_events <= 0: raise ValueError("max_buffer_events must be positive")
        self.venue=venue; self.bids={}; self.asks={}; self.previous=None; self.buffer=[]; self.max_buffer_events=max_buffer_events
        self.buffer_overflow_count=0; self.buffer_overflowed=False; self.state=BookQualityStateMachine()
        self.last_reason=""; self.duplicate_count=0; self.last_transition=None; self.recovery_generation=0
        self.committed_recovery_events=[]; self.quality_events=[]
        self.stale_count=0; self.non_authoritative_count=0
        # A snapshot that arrived ahead of every buffered diff. See
        # `binance_snapshot` for why it is retained rather than discarded.
        self._pending_snapshot=None
        self.pending_snapshot_bridges=0
        self._lock=RLock()
        self.comparator={"BINANCE":BinanceSequenceComparator(),"BYBIT":BybitSequenceComparator(),"OKX":OKXSequenceComparator(),"BINANCE_SPOT":SpotSequenceComparator()}[venue]

    @staticmethod
    def _valid_levels(levels):
        try:
            return all(LocalBook._decimal(price) > 0 and LocalBook._decimal(quantity) >= 0 and LocalBook._decimal(price).is_finite() and LocalBook._decimal(quantity).is_finite() for price, quantity in levels)
        except (TypeError, ValueError, AttributeError): return False

    @staticmethod
    def _decimal(value):
        """Keep Binance decimal text exact; Decimal inputs need no conversion."""
        return value if isinstance(value, Decimal) else Decimal(str(value))

    def _validated_maps(self, event, bids=None, asks=None):
        if not self._valid_levels(event.bids) or not self._valid_levels(event.asks): return None
        bids=dict(self.bids if bids is None else bids); asks=dict(self.asks if asks is None else asks)
        for levels, target in ((event.bids,bids),(event.asks,asks)):
            for price, quantity in levels:
                price, quantity = self._decimal(price), self._decimal(quantity)
                if quantity == 0: target.pop(price, None)
                else: target[price]=quantity
        if bids and asks and max(bids) >= min(asks): return None
        return bids, asks

    def _apply(self,event, *, maps=None):
        maps=self._validated_maps(event) if maps is None else maps
        if maps is None:
            old=self.state.state; self.last_reason="invalid_book"; self.state.gap()
            self.last_transition=BookTransition(old,self.state.state,event,self.last_reason,getattr(self.previous,"update_id",None)); return None
        self.bids, self.asks=maps; self.previous=event
        return replace(event,bids=tuple(sorted(self.bids.items(),reverse=True)),asks=tuple(sorted(self.asks.items())),quality_state=self.state.state.value)

    def _buffer_event(self, event):
        """Buffer only causally usable post-overflow events.

        Once capacity is exhausted the old chain is irrecoverable. The event
        that triggered overflow is discarded, and the book remains untrusted
        until a fresh REST snapshot bridges a new post-overflow chain.
        """
        if self.buffer_overflowed:
            if len(self.buffer) < self.max_buffer_events:
                self.buffer.append(event)
            return False
        if len(self.buffer) >= self.max_buffer_events:
            self.buffer.clear(); self.buffer_overflow_count += 1; self.buffer_overflowed=True
            old=self.state.state; self.state.gap(); self.last_reason="buffer_overflow"
            self.last_transition=BookTransition(old,self.state.state,event,self.last_reason,getattr(self.previous,"update_id",None))
            self.quality_events.append(QualityEvent(exchange=self.venue, stream="orderbook", event_type=QualityEventType.BUFFER_OVERFLOW, reason="buffer_overflow", rows_lost=self.max_buffer_events, quality_state=self.state.state.value))
            return False
        self.buffer.append(event)
        return True

    def snapshot(self,event):
        with self._lock:
            maps=self._validated_maps(event, {}, {})
            if maps is None: self.last_reason="invalid_snapshot"; return False
            self.bids, self.asks=maps; self.previous=event; self.buffer=[]; self.buffer_overflowed=False
            self._pending_snapshot=None; return True

    def binance_snapshot(self,last_update_id,event):
        """Prove a snapshot plus ordered buffered chain before atomically committing it.

        A snapshot can fail to bridge for two causally opposite reasons, and
        the previous implementation collapsed both into one discard:

        *The snapshot is ahead of the buffer* -- every buffered diff has
        ``u < lastUpdateId`` (documented step 4 drops them all). Nothing is
        wrong: the next diff to arrive will straddle ``lastUpdateId`` and
        bridge this snapshot. Discarding it meant the collector fetched
        another snapshot, which was also likely to land ahead of the buffer,
        looping until a diff happened to straddle -- spending the bounded
        recovery budget (5 per 60s) on a problem that resolves itself in
        milliseconds, and holding the book un-bridged for up to a minute.
        Such a snapshot is now retained as pending; see
        :meth:`retry_pending_snapshot`.

        *The snapshot is behind a hole in the buffer* -- the earliest
        surviving diff has ``U > lastUpdateId``, so the diffs between the
        snapshot and the buffer were never received. This snapshot can never
        bridge, no future diff changes that, and a newer one is genuinely
        required. It is discarded, as before.
        """
        with self._lock:
            return self._attempt_bridge(last_update_id, event, retain_if_ahead=True)

    def retry_pending_snapshot(self):
        """Re-attempt a retained snapshot against the current buffer.

        Returns ``True`` only when the bridge committed, in which case
        ``committed_recovery_events`` holds the reconstructed chain and the
        caller is responsible for persisting it -- identical to the contract
        of a successful :meth:`binance_snapshot`.
        """
        with self._lock:
            pending=self._pending_snapshot
            if pending is None: return False
            last_update_id, event = pending
            if self._attempt_bridge(last_update_id, event, retain_if_ahead=True):
                self.pending_snapshot_bridges += 1
                return True
            return False

    def _attempt_bridge(self,last_update_id,event,*,retain_if_ahead):
        with self._lock:
            # Step 4 (discard rule) differs by one token between the two
            # Binance products: futures keeps `u >= lastUpdateId` (discards
            # strictly `<`); Spot keeps `u > lastUpdateId` (discards `<=`).
            # See sequence.py's SpotSequenceComparator/binance_spot_snapshot_bridge
            # docstrings for the sourcing.
            keep_equal = self.venue != "BINANCE_SPOT"
            bridge_fn = _SNAPSHOT_BRIDGE_FNS[self.venue]
            original=list(self.buffer); candidates=[]
            for diff in original:
                if not isinstance(diff.update_id, int) or not isinstance(diff.first_update_id, int):
                    self.buffer=original; self.last_reason="malformed_update_ids"; return False
                if (diff.update_id >= last_update_id) if keep_equal else (diff.update_id > last_update_id):
                    candidates.append(diff)
            if not candidates:
                self.buffer=original; self.state.resync()
                if retain_if_ahead:
                    self._pending_snapshot=(last_update_id, event); self.last_reason="snapshot_ahead_of_buffer"
                else:
                    self._pending_snapshot=None; self.last_reason="snapshot_bridge_not_found"
                return False
            bridge=candidates[0]
            if not bridge_fn(bridge,last_update_id):
                self.buffer=original; self._pending_snapshot=None
                self.last_reason="snapshot_bridge_not_found"; self.state.resync(); return False
            maps=self._validated_maps(event, {}, {})
            if maps is None:
                self.buffer=original; self.last_reason="invalid_snapshot"; self.state.gap(); return False
            candidate_bids, candidate_asks=maps; previous=None; candidate_duplicates=0
            generation=self.recovery_generation + 1; committed=[]
            for index, diff in enumerate(candidates):
                if index:
                    result=self.comparator.check(diff,previous)
                    if result.is_stale: candidate_duplicates += 1; continue
                    if result.is_gap or result.is_resync_signal:
                        self.buffer=candidates; self._pending_snapshot=None
                        self.last_reason=result.reason; self.state.gap(); return False
                maps=self._validated_maps(diff,candidate_bids,candidate_asks)
                if maps is None:
                    self.buffer=candidates; self._pending_snapshot=None
                    self.last_reason="invalid_book"; self.state.gap(); return False
                candidate_bids,candidate_asks=maps; previous=diff
                committed.append((replace(diff, bids=tuple(sorted(candidate_bids.items(), reverse=True)), asks=tuple(sorted(candidate_asks.items())), quality_state=BookQuality.VALID.value), "RECOVERY_BRIDGE" if index == 0 else "RECOVERY_INCREMENTAL", generation))
            self.bids,self.asks,self.previous=candidate_bids,candidate_asks,previous
            self.buffer=[]; self.buffer_overflowed=False; self._pending_snapshot=None; self.state.recovered(); self.recovery_generation = generation; self.duplicate_count += candidate_duplicates; self.committed_recovery_events=committed; self.last_reason=""
            self.last_transition=BookTransition(BookQuality.RECOVERING,BookQuality.VALID,previous)
            return True

    def drain_quality_events(self):
        events=self.quality_events; self.quality_events=[]; return events

    def invalidate(self, reason="reconnect"):
        with self._lock:
            old=self.state.state; self.state.resync(); self.previous=None; self.buffer=[]; self.buffer_overflowed=False
            self._pending_snapshot=None; self.last_reason=reason
            self.last_transition=BookTransition(old,self.state.state,None,reason)

    def apply(self,event):
        with self._lock:
            if getattr(event, "book_source", None) in NON_AUTHORITATIVE_BOOK_SOURCES:
                # Never mutates the book and never changes quality state: a
                # partial-depth push is not evidence about the diff chain,
                # so it must neither corrupt the book nor invalidate it.
                self.non_authoritative_count += 1
                self.last_reason=f"non_authoritative_book_source:{event.book_source}"
                return None
            if self.venue in _BUFFER_UNTIL_BRIDGED_VENUES and (self.state.state != BookQuality.VALID or self.previous is None):
                self._buffer_event(event); return None
            if event.is_snapshot:
                if self.snapshot(event): self.state.recovered(); return self._apply(event)
                self.state.gap(); return None
            expected=getattr(self.previous,"update_id",None)
            result=self.comparator.check(event,self.previous)
            if result.is_resync_signal:
                old=self.state.state; self.last_reason=result.reason; self.state.resync(); self._buffer_event(event)
                self.last_transition=BookTransition(old,self.state.state,event,result.reason,expected); return None
            if result.is_gap:
                old=self.state.state; self.last_reason=result.reason; self.state.gap(); self._buffer_event(event)
                self.last_transition=BookTransition(old,self.state.state,event,result.reason,expected); return None
            if result.is_stale:
                # u did not advance: absolute-quantity semantics mean this
                # event holds nothing the book lacks. No state transition,
                # no buffering, no recovery.
                self.duplicate_count += 1; self.stale_count += 1
                self.last_reason=result.reason; return None
            return self._apply(event)
