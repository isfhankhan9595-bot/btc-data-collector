import os
import structlog
import logging
import logging.handlers

from .notifications import send_alert, validate_startup

def setup_logging(log_dir="logs"):
    os.makedirs(log_dir, exist_ok=True)

    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = logging.Formatter("%(message)s")

    # File handler with daily rotation, keep 7 days
    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename=os.path.join(log_dir, "collector.log"),
        when="midnight",
        interval=1,
        backupCount=7
    )
    file_handler.setFormatter(formatter)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    return structlog.get_logger()

logger = setup_logging()

def send_telegram_alert(message: str) -> bool:
    """Best-effort operator alert.

    Retained under its historical name so existing call sites keep working.
    Delivery is delegated to the optional notification backend, which fails
    open: ingestion correctness never depends on an alert being delivered.
    """
    return send_alert(message)


def validate_telegram_startup():
    """Best-effort startup probe of the optional notification backend."""
    return validate_startup()
