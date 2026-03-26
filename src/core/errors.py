import json
import datetime
import os
import traceback
import tempfile
import logging

from src.core.paths import DATA_DIR, ERROR_LOG_FILE, prepare_data_dirs

prepare_data_dirs()


def log_error(error_msg, context="General", details=None):
    """
    Logs an error to a local JSON file for future analysis.
    """
    try:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = {
            "timestamp": timestamp,
            "context": context,
            "error": str(error_msg),
            "traceback": traceback.format_exc(),
            "details": details or {},
        }

        logs = []
        if os.path.exists(ERROR_LOG_FILE):
            try:
                with open(ERROR_LOG_FILE, "r", encoding="utf-8") as f:
                    logs = json.load(f)
            except json.JSONDecodeError:
                logging.getLogger(__name__).warning(
                    "Error JSON decode error, resetting log file."
                )
                logs = []
            except FileNotFoundError:
                logs = []
            except Exception as e:
                logging.getLogger(__name__).warning(f"Error reading log file: {e}")
                logs = []

        logs.append(log_entry)

        # Keep only last 100 logs to prevent file bloat
        logs = logs[-100:]

        # Atomic write
        try:
            fd, temp_path = tempfile.mkstemp(dir=str(DATA_DIR))
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(logs, f, indent=4)
            os.replace(temp_path, ERROR_LOG_FILE)
        except Exception as file_e:
            logging.getLogger(__name__).error(f"Failed to atomic write logs: {file_e}")
            try:
                os.unlink(temp_path)
            except OSError:
                pass

    except Exception as e:
        print(f"Error logging failed: {e}")


def get_logs():
    """Returns the list of logged errors."""
    if os.path.exists(ERROR_LOG_FILE):
        try:
            with open(ERROR_LOG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return []
    return []
