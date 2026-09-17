from __future__ import annotations
from dataclasses import asdict, dataclass
from enum import Enum
import time
from typing import Optional, Union

class QualityEventType(str, Enum):
    CONNECT="CONNECT"; DISCONNECT="DISCONNECT"; RESYNC="RESYNC"; SEQUENCE_GAP="SEQUENCE_GAP"; RATE_LIMIT="RATE_LIMIT"; ERROR="ERROR"; DATA_DROP="DATA_DROP"; BUFFER_OVERFLOW="BUFFER_OVERFLOW"; BOOK_INVALID="BOOK_INVALID"; BOOK_STALE="BOOK_STALE"; DISK_FULL="DISK_FULL"; RECOVERY="RECOVERY"; DUPLICATE="DUPLICATE"; CLOCK_ANOMALY="CLOCK_ANOMALY"
class BookQuality(str, Enum): VALID="VALID"; SEQUENCE_GAP="SEQUENCE_GAP"; RECOVERING="RECOVERING"
@dataclass(frozen=True)
class QualityEvent:
    exchange: str; stream: str; event_type: QualityEventType; reason: str
    gap_size_ms: Optional[int] = None; rows_lost: Optional[Union[int, str]] = None; local_ts: int = 0
    quality_state: str = "VALID"; connection_id: Optional[str] = None
    def record(self): return asdict(self)
class BookQualityStateMachine:
    """Starts RECOVERING, not VALID.

    A book that has never been bridged by a snapshot holds no authoritative
    state, so reporting VALID was a false claim: quality events and raw
    records written before the first bridge were labelled VALID even though
    no snapshot had ever been applied. RECOVERING is the truthful initial
    state -- awaiting a bridge -- and it makes run_collector request the
    startup snapshot through the normal recovery path.
    """
    def __init__(self): self.state = BookQuality.RECOVERING
    def gap(self): self.state = BookQuality.SEQUENCE_GAP; return self.state
    def resync(self): self.state = BookQuality.RECOVERING; return self.state
    def recovered(self): self.state = BookQuality.VALID; return self.state
    def event(self, exchange, stream, kind, reason): return QualityEvent(exchange, stream, kind, reason, local_ts=int(time.time()*1000))
