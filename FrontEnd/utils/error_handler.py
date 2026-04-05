from __future__ import annotations

import datetime
import json
import logging
import os
import platform
import tempfile
import traceback
from pathlib import Path
from typing import Any

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
PROMPT_DIR = DATA_DIR / "error_prompts"
DATA_DIR.mkdir(exist_ok=True)
PROMPT_DIR.mkdir(exist_ok=True)
ERROR_LOG_FILE = DATA_DIR / "error_logs.json"
LATEST_PROMPT_FILE = PROMPT_DIR / "latest_error_prompt.md"



def _safe_jsonable(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, dict):
        return {str(k): _safe_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_safe_jsonable(v) for v in value]
    return str(value)



def build_fix_prompt(entry: dict[str, Any]) -> str:
    details = json.dumps(entry.get("details", {}), indent=2, ensure_ascii=False)
    environment = json.dumps(entry.get("environment", {}), indent=2, ensure_ascii=False)
    return f"""### SYSTEM ERROR DETECTED FOR FIXING

Context: {entry.get('context', 'General')}
Error Type: {entry.get('error_type', 'Unknown')}
Error: {entry.get('error', '')}
Timestamp: {entry.get('timestamp', '')}

Environment:
```json
{environment}
```

Additional Details:
```json
{details}
```

Traceback:
```python
{entry.get('traceback', '')}
```

Task:
1. Explain the likely root cause.
2. Identify the safest code change.
3. Suggest tests to prevent regression.
4. Mention any schema mismatch, missing secret, or data-quality issue involved.
"""



def log_error(error_msg: Any, context: str = "General", details: dict[str, Any] | None = None) -> dict[str, Any] | None:
    """Persist a structured application error plus an AI-ready prompt payload."""
    try:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        traceback_text = traceback.format_exc()
        if traceback_text.strip() == "NoneType: None":
            traceback_text = ""

        entry = {
            "timestamp": timestamp,
            "context": context,
            "error": str(error_msg),
            "error_type": getattr(error_msg, "__class__", type(error_msg)).__name__,
            "traceback": traceback_text,
            "details": _safe_jsonable(details or {}),
            "environment": {
                "python": platform.python_version(),
                "platform": platform.platform(),
                "cwd": os.getcwd(),
            },
        }
        entry["fix_prompt"] = build_fix_prompt(entry)

        logs: list[dict[str, Any]] = []
        if ERROR_LOG_FILE.exists():
            try:
                logs = json.loads(ERROR_LOG_FILE.read_text(encoding="utf-8"))
            except Exception:
                logs = []

        logs.append(entry)
        logs = logs[-200:]

        fd, temp_path = tempfile.mkstemp(dir=str(DATA_DIR), suffix=".json")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(logs, handle, indent=2, ensure_ascii=False)
            os.replace(temp_path, ERROR_LOG_FILE)
        finally:
            if os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except OSError:
                    pass

        timestamp_slug = timestamp.replace(":", "-").replace(" ", "_")
        prompt_path = PROMPT_DIR / f"error_prompt_{timestamp_slug}.md"
        prompt_path.write_text(entry["fix_prompt"], encoding="utf-8")
        LATEST_PROMPT_FILE.write_text(entry["fix_prompt"], encoding="utf-8")
        entry["prompt_file"] = str(prompt_path)
        return entry
    except Exception as exc:
        logging.getLogger(__name__).error("Error logging failed: %s", exc)
        return None



def get_logs() -> list[dict[str, Any]]:
    if ERROR_LOG_FILE.exists():
        try:
            return json.loads(ERROR_LOG_FILE.read_text(encoding="utf-8"))
        except Exception:
            return []
    return []
