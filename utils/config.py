"""
Configuration Manager
=====================
Handles persistent application settings stored as JSON.
Manages VTM paths, recent files, and user preferences.
"""

import json
import os
from pathlib import Path
from typing import Any, Optional


# Default config stored in user's home directory
CONFIG_DIR = Path.home() / ".vtm_codec_studio"
CONFIG_FILE = CONFIG_DIR / "settings.json"

# Default settings template
DEFAULT_SETTINGS = {
    "vtm_root_folder": "",
    "cfg_folder": "",
    "encoder_executable": "",
    "decoder_executable": "",
    "yuview_executable": "",
    "ffmpeg_executable": "",
    "recent_input_files": [],
    "recent_output_files": [],
    "last_encoder_config": "encoder_intra_vtm.cfg",
    "last_qp": "32",
    "last_frames": "100",
    "encoder_output_dir": "",
    "encoder_name_custom_enabled": False,
    "encoder_name_custom_text": "",
    "encoder_name_include_q": True,
    "encoder_name_include_frames": True,
    "encoder_name_include_yuv": True,
    "encoder_artifacts_dir": "",
    "metrics_csv_enabled": False,
    "metrics_csv_path": "",
    "encoder_tracefiles_enabled": False,
    "encoder_parallel_jobs": 2,
    "decoder_parallel_jobs": 2,
    "converter_parallel_jobs": 2,
    "window_geometry": None,
    "max_recent_files": 10,
}


class ConfigManager:
    """
    Singleton configuration manager that reads/writes settings
    to a local JSON file, providing thread-safe access to all
    application preferences.
    """

    _instance: Optional["ConfigManager"] = None
    _settings: dict

    def __new__(cls) -> "ConfigManager":
        """Ensure only one instance exists (singleton pattern)."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._settings = {}
            cls._instance._load()
        return cls._instance

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get(self, key: str, default: Any = None) -> Any:
        """Retrieve a setting by key, with an optional fallback."""
        return self._settings.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """Set a single value and persist immediately."""
        self._settings[key] = value
        self._save()

    def update(self, data: dict) -> None:
        """Bulk-update settings and persist."""
        self._settings.update(data)
        self._save()

    def add_recent_file(self, category: str, filepath: str) -> None:
        """
        Append *filepath* to the recent-files list identified by *category*.
        Duplicates are moved to the front; the list is trimmed to max size.
        """
        key = f"recent_{category}_files"
        recent = self._settings.get(key, [])

        # Remove duplicate if present, then prepend
        if filepath in recent:
            recent.remove(filepath)
        recent.insert(0, filepath)

        # Trim to configured max
        max_items = self._settings.get("max_recent_files", 10)
        self._settings[key] = recent[:max_items]
        self._save()

    def get_recent_files(self, category: str) -> list[str]:
        """Return the recent-files list for *category*."""
        return self._settings.get(f"recent_{category}_files", [])

    def get_all(self) -> dict:
        """Return a shallow copy of all settings."""
        return dict(self._settings)

    def reset(self) -> None:
        """Restore factory defaults and persist."""
        self._settings = dict(DEFAULT_SETTINGS)
        self._save()

    # ------------------------------------------------------------------
    # Path helpers (convenience)
    # ------------------------------------------------------------------

    def encoder_path(self) -> str:
        """Full path to EncoderAppStatic.exe."""
        return self._settings.get("encoder_executable", "")

    def decoder_path(self) -> str:
        """Full path to DecoderAppStatic.exe."""
        return self._settings.get("decoder_executable", "")

    def yuview_path(self) -> str:
        """Full path to YUView executable."""
        return self._settings.get("yuview_executable", "")

    def ffmpeg_path(self) -> str:
        """Full path to FFmpeg executable."""
        return self._settings.get("ffmpeg_executable", "")

    def cfg_folder(self) -> str:
        """Path to the VTM cfg directory."""
        return self._settings.get("cfg_folder", "")

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _load(self) -> None:
        """Load settings from disk, falling back to defaults."""
        if CONFIG_FILE.exists():
            try:
                with open(CONFIG_FILE, "r", encoding="utf-8") as fh:
                    data = json.load(fh)
                # Merge with defaults so new keys are always present
                merged = dict(DEFAULT_SETTINGS)
                merged.update(data)
                self._settings = merged
            except (json.JSONDecodeError, OSError):
                self._settings = dict(DEFAULT_SETTINGS)
        else:
            self._settings = dict(DEFAULT_SETTINGS)

    def _save(self) -> None:
        """Persist current settings to disk."""
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        try:
            with open(CONFIG_FILE, "w", encoding="utf-8") as fh:
                json.dump(self._settings, fh, indent=2, ensure_ascii=False)
        except OSError as exc:
            print(f"[ConfigManager] Failed to save settings: {exc}")
