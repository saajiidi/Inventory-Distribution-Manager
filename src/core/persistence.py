import os
import streamlit as st
import pandas as pd
import tempfile
import logging
import json

from src.core.paths import DATA_DIR, STATE_FILE, prepare_data_dirs

prepare_data_dirs()


def save_state():
    """Saves relevant session state keys to a local file."""
    state_to_save = {}
    keys_to_persist = [
        "inv_res_data",
        "inv_active_l",
        "inv_t_col",
        "pathao_res_df",
        "low_stock_threshold",
    ]

    for key in keys_to_persist:
        if key in st.session_state and st.session_state[key] is not None:
            val = st.session_state[key]
            if isinstance(val, pd.DataFrame):
                state_to_save[f"{key}_serial"] = val.to_dict("records")
            else:
                state_to_save[key] = val

    try:
        fd, temp_path = tempfile.mkstemp(dir=str(DATA_DIR))
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(state_to_save, f, indent=4)
        os.replace(temp_path, STATE_FILE)
    except Exception as e:
        default_logger = logging.getLogger(__name__)
        default_logger.error(f"Failed to save state: {e}")
        try:
            os.unlink(temp_path)
        except OSError:
            pass


def load_state():
    """Loads session state from local file."""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                for k, v in data.items():
                    if k.endswith("_serial"):
                        orig_key = k.replace("_serial", "")
                        st.session_state[orig_key] = pd.DataFrame(v)
                    else:
                        st.session_state[k] = v
        except FileNotFoundError:
            pass
        except json.JSONDecodeError as e:
            default_logger = logging.getLogger(__name__)
            default_logger.error(f"Failed to parse state file: {e}")
        except Exception as e:
            default_logger = logging.getLogger(__name__)
            default_logger.error(f"Unexpected error loading state: {e}")


def init_state():
    """Initialize defaults if not present."""
    if "low_stock_threshold" not in st.session_state:
        st.session_state.low_stock_threshold = 5
    load_state()


def clear_state_keys(keys):
    """Clear selected session state keys and persist."""
    for key in keys:
        if key in st.session_state:
            del st.session_state[key]
    save_state()
