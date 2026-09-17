from typing import Dict, Any, Optional
from .config import OI_STALE_MS
from .utils import logger, send_telegram_alert

class GapDetector:
    def __init__(self):
        self.last_seen: Dict[str, int] = {
            "orderbook": 0,
            "trades": 0,
            "markprice": 0,
            "openinterest": 0
        }
        self.thresholds = {
            "orderbook": 500,
            "trades": 5000,   # was 30000; 19s gaps are definitive WebSocket drops on BTC perp
            "markprice": 5000,
            "openinterest": OI_STALE_MS,
        }

    def check_gap(self, stream_name: str, current_ts: int, threshold_ms: Optional[int] = None):
        last_ts = self.last_seen.get(stream_name, 0)
        threshold = self.thresholds.get(stream_name, threshold_ms)
        if threshold_ms is not None:
            threshold = threshold_ms
        if threshold is None:
            raise ValueError(f"No gap threshold configured for stream: {stream_name}")

        if last_ts > 0 and current_ts < last_ts:
            logger.warning("Clock regression detected", stream=stream_name, last_ts=last_ts, current_ts=current_ts)
            return "clock_regression"
        alert_message = None
        if last_ts > 0:
            gap_duration = current_ts - last_ts
            if gap_duration > threshold:
                logger.warning("Gap detected",
                               stream=stream_name,
                               gap_start=last_ts,
                               gap_end=current_ts,
                               duration_ms=gap_duration)
                if gap_duration > 2000:
                    alert_message = f"Gap > 2s detected in {stream_name}: {gap_duration}ms"

        # Commit hot-path state BEFORE notifying. Notification is an
        # operational convenience; a backend that raises must never cost us
        # gap-tracking state, which would silently corrupt every later gap
        # measurement for this stream.
        self.last_seen[stream_name] = current_ts

        if alert_message is not None:
            try:
                send_telegram_alert(alert_message)
            except Exception as exc:  # noqa: BLE001 - alerting fails open
                logger.warning("Gap alert failed open",
                               stream=stream_name, error=str(exc))

    def reset_stream(self, stream_name: str):
        if stream_name in self.last_seen:
            self.last_seen[stream_name] = 0

    def reset(self):
        self.last_seen = {"orderbook": 0, "trades": 0, "markprice": 0, "openinterest": 0}
