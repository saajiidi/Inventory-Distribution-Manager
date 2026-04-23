"""
Structured Logging Infrastructure for Automation-Pivot

Provides consistent logging across the application with different log levels
and automatic log rotation.
"""

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Optional
from functools import wraps

from BackEnd.core.paths import LOGS_DIR

def log_error(error_msg: any, context: str = "General", details: dict = None):
    """Bridge to the centralized error handler."""
    from FrontEnd.utils.error_handler import log_error as core_log
    return core_log(error_msg, context, details)

# Configure log directory
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Log file paths
APP_LOG_FILE = LOGS_DIR / "app.log"
ERROR_LOG_FILE = LOGS_DIR / "errors.json"
AUDIT_LOG_FILE = LOGS_DIR / "audit.json"
PERFORMANCE_LOG_FILE = LOGS_DIR / "performance.json"


class StructuredLogFormatter(logging.Formatter):
    """Custom formatter for structured JSON logging."""

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add extra fields if present
        if hasattr(record, "context"):
            log_data["context"] = record.context
        if hasattr(record, "user_id"):
            log_data["user_id"] = record.user_id
        if hasattr(record, "session_id"):
            log_data["session_id"] = record.session_id
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data, default=str)


def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Set up a logger with both console and file handlers."""
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid adding handlers if they already exist
    if logger.handlers:
        return logger

    # Console handler with simple format
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_format = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)

    # File handler with structured JSON format
    file_handler = logging.FileHandler(APP_LOG_FILE, mode="a")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(StructuredLogFormatter())
    logger.addHandler(file_handler)

    return logger


def get_logger(name: str) -> logging.Logger:
    """Get or create a logger instance."""
    return (
        logging.getLogger(name) if name in logging.Logger.manager.loggerDict else setup_logger(name)
    )


def log_structured(
    level: str, message: str, context: Optional[dict] = None, logger_name: str = "automation_pivot"
) -> None:
    """Log a structured message with optional context."""
    logger = get_logger(logger_name)

    extra = {"context": context or {}}

    if level.upper() == "DEBUG":
        logger.debug(message, extra=extra)
    elif level.upper() == "INFO":
        logger.info(message, extra=extra)
    elif level.upper() == "WARNING":
        logger.warning(message, extra=extra)
    elif level.upper() == "ERROR":
        logger.error(message, extra=extra)
    elif level.upper() == "CRITICAL":
        logger.critical(message, extra=extra)


def log_audit(
    action: str,
    entity_type: str,
    entity_id: Optional[str] = None,
    user_id: Optional[str] = None,
    details: Optional[dict] = None,
) -> None:
    """Log an audit event for data mutation tracking."""
    audit_entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "action": action,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "user_id": user_id,
        "details": details or {},
    }

    try:
        # Append to audit log
        audit_logs = []
        if AUDIT_LOG_FILE.exists():
            with open(AUDIT_LOG_FILE, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if content:
                    audit_logs = [json.loads(line) for line in content.split("\n") if line.strip()]

        audit_logs.append(audit_entry)

        # Keep only last 10000 entries to manage file size
        audit_logs = audit_logs[-10000:]

        with open(AUDIT_LOG_FILE, "w", encoding="utf-8") as f:
            for entry in audit_logs:
                f.write(json.dumps(entry, default=str) + "\n")
    except Exception as e:
        get_logger("audit").error(f"Failed to write audit log: {e}")


def log_performance(
    operation: str, duration_ms: float, success: bool = True, metadata: Optional[dict] = None
) -> None:
    """Log performance metrics."""
    perf_entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "operation": operation,
        "duration_ms": duration_ms,
        "success": success,
        "metadata": metadata or {},
    }

    try:
        with open(PERFORMANCE_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(perf_entry, default=str) + "\n")
    except Exception as e:
        get_logger("performance").error(f"Failed to write performance log: {e}")


def get_audit_logs(entity_type: Optional[str] = None, limit: int = 100) -> list[dict]:
    """Retrieve audit logs with optional filtering."""
    if not AUDIT_LOG_FILE.exists():
        return []

    try:
        with open(AUDIT_LOG_FILE, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if not content:
                return []
            logs = [json.loads(line) for line in content.split("\n") if line.strip()]

        if entity_type:
            logs = [log for log in logs if log.get("entity_type") == entity_type]

        return logs[-limit:]
    except Exception as e:
        get_logger("audit").error(f"Failed to read audit logs: {e}")
        return []


def timed(operation_name: str):
    """Decorator to log function execution time."""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            import time

            start_time = time.perf_counter()
            try:
                result = func(*args, **kwargs)
                duration_ms = (time.perf_counter() - start_time) * 1000
                log_performance(operation_name, duration_ms, success=True)
                return result
            except Exception as e:
                duration_ms = (time.perf_counter() - start_time) * 1000
                log_performance(
                    operation_name, duration_ms, success=False, metadata={"error": str(e)}
                )
                raise

        return wrapper

    return decorator
