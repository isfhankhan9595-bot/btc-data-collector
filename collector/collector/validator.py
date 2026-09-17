"""Structural validation for collected market data.

Validation is deliberately limited to data-integrity invariants.  Extreme but
real market states (wide spreads, fast price moves, unusual funding, or clock
offsets) must remain in the raw dataset so downstream research can measure
those regimes rather than having the collector censor them.
"""
from __future__ import annotations

import math
import time
from typing import Any, Dict, Optional, Tuple

from .quality_events import QualityEvent, QualityEventType
from .utils import logger


class Validator:
    CLOCK_DRIFT_THRESHOLD_MS = 5_000

    def __init__(self):
        self.last_timestamps: Dict[str, int] = {
            "orderbook": 0,
            "trades": 0,
            "markprice": 0,
            "liquidation": 0,
        }
        self.last_trade_id = -1
        self.last_mid_price: Optional[float] = None
        self.failures_in_window = 0
        self.total_in_window = 0
        self.window_start = time.time()
        self.quality_events: list[QualityEvent] = []

    @staticmethod
    def _finite(value: Any) -> bool:
        try:
            return math.isfinite(float(value))
        except (TypeError, ValueError, OverflowError):
            return False

    def check_failure_rate(self) -> bool:
        now = time.time()
        if now - self.window_start > 60:
            failures = self.failures_in_window
            total = self.total_in_window
            self.failures_in_window = 0
            self.total_in_window = 0
            self.window_start = now
            if total == 0:
                return False
            return failures / total > 0.001
        return False

    def validate_timestamp(
        self, stream_name: str, record: Dict[str, Any], allow_equal: bool = False
    ) -> Tuple[bool, str]:
        ts = record.get("timestamp")
        if not isinstance(ts, int) or ts <= 0:
            return False, "Invalid timestamp"

        exchange_ts = record.get("exchange_timestamp")
        if exchange_ts is not None and (
            not isinstance(exchange_ts, int) or exchange_ts <= 0
        ):
            return False, "Invalid exchange timestamp"

        if exchange_ts is not None:
            drift_ms = abs(exchange_ts - ts)
            if drift_ms > self.CLOCK_DRIFT_THRESHOLD_MS:
                self.quality_events.append(
                    QualityEvent(
                        exchange=str(record.get("exchange", "UNKNOWN")),
                        stream=stream_name,
                        event_type=QualityEventType.CLOCK_ANOMALY,
                        reason="clock_drift",
                        gap_size_ms=drift_ms,
                        local_ts=ts,
                        quality_state="VALID",
                        connection_id=record.get("connection_id"),
                    )
                )
                logger.warning(
                    "Clock drift observed; preserving event",
                    stream=stream_name,
                    exchange=record.get("exchange", "UNKNOWN"),
                    drift_ms=drift_ms,
                )

        previous = self.last_timestamps[stream_name]
        if ts < previous or (ts == previous and not allow_equal):
            return False, "Timestamp regression"

        return True, ""

    def validate_orderbook(self, record: Dict[str, Any]) -> Tuple[bool, str]:
        self.total_in_window += 1

        valid_ts, reason = self.validate_timestamp("orderbook", record)
        if not valid_ts:
            self._handle_failure("orderbook", reason, record)
            return False, reason

        if len(record.get("bids_price", [])) < 1 or len(record.get("asks_price", [])) < 1:
            reason = "Empty book"
            self._handle_failure("orderbook", reason, record)
            return False, reason

        best_bid = record.get("best_bid", 0)
        best_ask = record.get("best_ask", 0)
        if not self._finite(best_bid) or not self._finite(best_ask) or best_bid <= 0 or best_ask <= 0:
            reason = "Invalid best bid/ask"
            self._handle_failure("orderbook", reason, record)
            return False, reason

        if best_ask <= best_bid:
            reason = "Crossed book"
            self._handle_failure("orderbook", reason, record)
            return False, reason

        total_bid_qty = record.get("total_bid_qty", 0)
        total_ask_qty = record.get("total_ask_qty", 0)
        if (
            not self._finite(total_bid_qty)
            or not self._finite(total_ask_qty)
            or total_bid_qty <= 0
            or total_ask_qty <= 0
        ):
            reason = "Invalid total qty"
            self._handle_failure("orderbook", reason, record)
            return False, reason

        obi = record.get("obi", -2)
        if not self._finite(obi) or obi < -1.0 or obi > 1.0:
            reason = "OBI out of bounds"
            self._handle_failure("orderbook", reason, record)
            return False, reason

        self.last_timestamps["orderbook"] = record["timestamp"]
        self.last_mid_price = record.get("mid_price")
        return True, ""

    def validate_trade(self, record: Dict[str, Any]) -> Tuple[bool, str]:
        self.total_in_window += 1

        valid_ts, reason = self.validate_timestamp("trades", record, allow_equal=True)
        if not valid_ts:
            self._handle_failure("trades", reason, record)
            return False, reason

        price = record.get("price", 0)
        quantity = record.get("quantity", 0)
        if not self._finite(price) or price <= 0:
            reason = "Invalid price"
            self._handle_failure("trades", reason, record)
            return False, reason

        if not self._finite(quantity) or quantity <= 0:
            reason = "Invalid quantity"
            self._handle_failure("trades", reason, record)
            return False, reason

        trade_id = record.get("trade_id", -1)
        if self.last_trade_id != -1 and trade_id <= self.last_trade_id:
            reason = "Duplicate/Regressive trade_id"
            self._handle_failure("trades", reason, record)
            return False, reason

        self.last_timestamps["trades"] = record["timestamp"]
        self.last_trade_id = trade_id
        return True, ""

    def validate_liquidation(self, record: Dict[str, Any]) -> Tuple[bool, str]:
        self.total_in_window += 1

        valid_ts, reason = self.validate_timestamp("liquidation", record, allow_equal=True)
        if not valid_ts:
            self._handle_failure("liquidation", reason, record)
            return False, reason

        price = record.get("price", 0)
        quantity = record.get("quantity", 0)
        if not self._finite(price) or price <= 0:
            reason = "Invalid price"
            self._handle_failure("liquidation", reason, record)
            return False, reason

        if not self._finite(quantity) or quantity <= 0:
            reason = "Invalid quantity"
            self._handle_failure("liquidation", reason, record)
            return False, reason

        self.last_timestamps["liquidation"] = record["timestamp"]
        return True, ""

    def validate_markprice(self, record: Dict[str, Any]) -> Tuple[bool, str]:
        self.total_in_window += 1

        valid_ts, reason = self.validate_timestamp("markprice", record)
        if not valid_ts:
            self._handle_failure("markprice", reason, record)
            return False, reason

        mark_price = record.get("mark_price", 0)
        if not self._finite(mark_price) or mark_price <= 0:
            reason = "Invalid mark price"
            self._handle_failure("markprice", reason, record)
            return False, reason

        funding_rate = record.get("funding_rate")
        if funding_rate is not None and not self._finite(funding_rate):
            reason = "Invalid funding rate"
            self._handle_failure("markprice", reason, record)
            return False, reason

        next_funding_time = record.get("next_funding_time")
        exchange_ts = record.get("exchange_timestamp")
        if next_funding_time is not None and (
            not isinstance(next_funding_time, int) or next_funding_time <= 0
        ):
            reason = "Invalid next funding time"
            self._handle_failure("markprice", reason, record)
            return False, reason
        if exchange_ts is not None and next_funding_time is not None and next_funding_time <= exchange_ts:
            reason = "Invalid next funding time"
            self._handle_failure("markprice", reason, record)
            return False, reason

        self.last_timestamps["markprice"] = record["timestamp"]
        return True, ""

    def drain_quality_events(self) -> list[QualityEvent]:
        events = self.quality_events
        self.quality_events = []
        return events

    def _handle_failure(self, stream_name: str, reason: str, record: Dict[str, Any]):
        self.failures_in_window += 1
        logger.error(
            "Validation failed",
            stream=stream_name,
            reason=reason,
            timestamp=record.get("timestamp"),
            record=record,
        )

    def reset_stream(self, stream_name: str):
        if stream_name in self.last_timestamps:
            self.last_timestamps[stream_name] = 0
        if stream_name == "trades":
            self.last_trade_id = -1
        if stream_name == "orderbook":
            self.last_mid_price = None

    def reset(self):
        self.last_timestamps = {
            "orderbook": 0,
            "trades": 0,
            "markprice": 0,
            "liquidation": 0,
        }
        self.last_trade_id = -1
        self.last_mid_price = None
        self.failures_in_window = 0
        self.total_in_window = 0
        self.window_start = time.time()
        self.quality_events = []
