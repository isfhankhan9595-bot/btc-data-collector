"""Deterministic local order-book reconstruction for venue diff streams."""
from __future__ import annotations
from dataclasses import dataclass, replace
from decimal import Decimal
from threading import RLock
from .canonical import CanonicalOrderBookEvent
from .quality_events import BookQuality, BookQualityStateMachine
from .sequence import BinanceSequenceComparator, BybitSequenceComparator, OKXSequenceComparator, binance_snapshot_bridge

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
        self.committed_recovery_events=[]
        self._lock=RLock()
        self.comparator={"BINANCE":BinanceSequenceComparator(),"BYBIT":BybitSequenceComparator(),"OKX":OKXSequenceComparator()}[venue]

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

        Once capacity is exhausted the old chain is irrecoverable.  The event
        that triggered overflow is therefore discarded, and the book remains
        untrusted until a fresh REST snapshot bridges a new post-overflow chain.
        """
        if self.buffer_overflowed:
            if len(self.buffer) < self.max_buffer_events:
                self.buffer.append(event)
            return False
        if len(self.buffer) >= self.max_buffer_events:
            self.buffer.clear(); self.buffer_overflow_count += 1; self.buffer_overflowed=True
            old=self.state.state; self.state.gap(); self.last_reason="buffer_overflow"
            self.last_transition=BookTransition(old,self.state.state,event,self.last_reason,getattr(self.previous,"update_id",None))
            return False
        self.buffer.append(event)
        return True

    def snapshot(self,event):
        with self._lock:
            maps=self._validated_maps(event, {}, {})
            if maps is None: self.last_reason="invalid_snapshot"; return False
            self.bids, self.asks=maps; self.previous=event; self.buffer=[]; self.buffer_overflowed=False; return True

    def binance_snapshot(self,last_update_id,event):
        """Prove a snapshot plus ordered buffered chain before atomically committing it."""
        with self._lock:
            original=list(self.buffer)
            candidates=[]
            for diff in original:
                if not isinstance(diff.update_id, int) or not isinstance(diff.first_update_id, int):
                    self.buffer=original; self.last_reason="malformed_update_ids"; return False
                if diff.update_id >= last_update_id: candidates.append(diff)
            if not candidates:
                self.buffer=original; self.last_reason="snapshot_bridge_not_found"; self.state.resync(); return False
            bridge=candidates[0]
            if not binance_snapshot_bridge(bridge,last_update_id):
                self.buffer=original; self.last_reason="snapshot_bridge_not_found"; self.state.resync(); return False
            maps=self._validated_maps(event, {}, {})
            if maps is None:
                self.buffer=original; self.last_reason="invalid_snapshot"; self.state.gap(); return False
            candidate_bids, candidate_asks=maps; previous=None; candidate_duplicates=0
            generation=self.recovery_generation + 1
            committed=[]
            for index, diff in enumerate(candidates):
                if index:
                    result=self.comparator.check(diff,previous)
                    if result.reason == "duplicate_update": candidate_duplicates += 1; continue
                    if result.is_gap or result.is_resync_signal:
                        self.buffer=candidates; self.last_reason=result.reason; self.state.gap(); return False
                maps=self._validated_maps(diff,candidate_bids,candidate_asks)
                if maps is None:
                    self.buffer=candidates; self.last_reason="invalid_book"; self.state.gap(); return False
                candidate_bids,candidate_asks=maps; previous=diff
                committed.append((replace(diff, bids=tuple(sorted(candidate_bids.items(), reverse=True)), asks=tuple(sorted(candidate_asks.items())), quality_state=BookQuality.VALID.value), "RECOVERY_BRIDGE" if index == 0 else "RECOVERY_INCREMENTAL", generation))
            self.bids,self.asks,self.previous=candidate_bids,candidate_asks,previous
            self.buffer=[]; self.buffer_overflowed=False; self.state.recovered(); self.recovery_generation = generation; self.duplicate_count += candidate_duplicates; self.committed_recovery_events=committed; self.last_reason=""
            self.last_transition=BookTransition(BookQuality.RECOVERING,BookQuality.VALID,previous)
            return True

    def invalidate(self, reason="reconnect"):
        with self._lock:
            old=self.state.state; self.state.resync(); self.previous=None; self.buffer=[]; self.buffer_overflowed=False; self.last_reason=reason
            self.last_transition=BookTransition(old,self.state.state,None,reason)

    def apply(self,event):
        with self._lock:
            if self.venue=="BINANCE" and (self.state.state != BookQuality.VALID or self.previous is None):
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
            if result.reason=="duplicate_update": self.duplicate_count += 1; self.last_reason=result.reason; return None
            return self._apply(event)
