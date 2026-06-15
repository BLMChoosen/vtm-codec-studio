"""
Session Manager
===============
Crash detection via lock file and queue state persistence.

On startup, checks whether a lock file exists (indicating the previous
instance did not exit cleanly). If so, loads the saved queue state and
offers the user an opportunity to restart interrupted jobs.
"""

import json
from dataclasses import asdict
from pathlib import Path
from typing import Optional

from utils.config import CONFIG_DIR

LOCK_FILE = CONFIG_DIR / "app.lock"
SESSION_FILE = CONFIG_DIR / "session.json"


# ---------------------------------------------------------------------------
# Lock-file helpers
# ---------------------------------------------------------------------------

def acquire_lock() -> None:
    """Create the lock file that marks the app as running."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    try:
        LOCK_FILE.write_text("running", encoding="utf-8")
    except OSError as exc:
        print(f"[Session] Failed to write lock file: {exc}")


def release_lock() -> None:
    """Remove the lock file on a clean exit."""
    try:
        LOCK_FILE.unlink(missing_ok=True)
    except OSError:
        pass


def was_crashed() -> bool:
    """Return True when a lock file is present, indicating a previous crash."""
    return LOCK_FILE.exists()


# ---------------------------------------------------------------------------
# Session state helpers
# ---------------------------------------------------------------------------

def save_tab_session(
    tab_name: str,
    queue: list,
    in_progress_indices: list[int],
    next_index: int,
) -> None:
    """Persist queue state for *tab_name* so it can be recovered after a crash."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    data = _load_raw()
    data[tab_name] = {
        "queue": [asdict(job) for job in queue],
        "in_progress_indices": sorted(set(in_progress_indices)),
        "next_index": next_index,
    }
    _save_raw(data)


def clear_tab_session(tab_name: str) -> None:
    """Remove session data for *tab_name* after a normal queue finish."""
    data = _load_raw()
    if tab_name in data:
        del data[tab_name]
        _save_raw(data)


def load_tab_session(tab_name: str) -> Optional[dict]:
    """Return the raw session dict for *tab_name*, or None if absent."""
    return _load_raw().get(tab_name)


def clear_all_sessions() -> None:
    """Delete the entire session file."""
    try:
        SESSION_FILE.unlink(missing_ok=True)
    except OSError:
        pass


def recovery_job_count(session: dict) -> int:
    """Return the number of jobs that would need to be re-run for *session*."""
    queue = session.get("queue", [])
    in_progress = session.get("in_progress_indices", [])
    next_idx = session.get("next_index", 0)
    return len(in_progress) + max(0, len(queue) - next_idx)


# ---------------------------------------------------------------------------
# Internal I/O
# ---------------------------------------------------------------------------

def _load_raw() -> dict:
    if SESSION_FILE.exists():
        try:
            return json.loads(SESSION_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def _save_raw(data: dict) -> None:
    try:
        SESSION_FILE.write_text(
            json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8"
        )
    except OSError as exc:
        print(f"[Session] Failed to save session: {exc}")
