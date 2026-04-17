"""
Preset Manager
==============
Save and load named encoder/decoder presets to disk.
Each preset is a JSON file stored under ~/.vtm_codec_studio/presets/.
"""

import json
from pathlib import Path
from typing import Optional

PRESETS_DIR = Path.home() / ".vtm_codec_studio" / "presets"
COMPRESSION_PROFILES_DIR = Path.home() / ".vtm_codec_studio" / "compression_profiles"


def _ensure_dir() -> None:
    """Create the presets directory if it doesn't exist."""
    PRESETS_DIR.mkdir(parents=True, exist_ok=True)


def _ensure_profiles_dir() -> None:
    """Create the compression-profiles directory if it doesn't exist."""
    COMPRESSION_PROFILES_DIR.mkdir(parents=True, exist_ok=True)


def list_presets() -> list[str]:
    """Return sorted list of available preset names (without .json extension)."""
    _ensure_dir()
    return sorted(p.stem for p in PRESETS_DIR.glob("*.json"))


def save_preset(name: str, data: dict) -> Path:
    """
    Save *data* as a named preset. Returns the path to the saved file.
    Overwrites if a preset with that name already exists.
    """
    _ensure_dir()
    filepath = PRESETS_DIR / f"{name}.json"
    with open(filepath, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)
    return filepath


def load_preset(name: str) -> Optional[dict]:
    """Load and return the preset dict, or ``None`` if not found / corrupt."""
    filepath = PRESETS_DIR / f"{name}.json"
    if not filepath.is_file():
        return None
    try:
        with open(filepath, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (json.JSONDecodeError, OSError):
        return None


def delete_preset(name: str) -> bool:
    """Delete a preset by name. Returns True if removed, False if not found."""
    filepath = PRESETS_DIR / f"{name}.json"
    if filepath.is_file():
        filepath.unlink()
        return True
    return False


def list_compression_profiles() -> list[str]:
    """Return sorted list of available compression-profile names."""
    _ensure_profiles_dir()
    return sorted(p.stem for p in COMPRESSION_PROFILES_DIR.glob("*.json"))


def save_compression_profile(name: str, data: dict) -> Path:
    """
    Save *data* as a named compression profile.
    Overwrites if an existing profile has the same name.
    """
    _ensure_profiles_dir()
    filepath = COMPRESSION_PROFILES_DIR / f"{name}.json"
    with open(filepath, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)
    return filepath


def load_compression_profile(name: str) -> Optional[dict]:
    """Load and return a compression profile dict, or ``None`` on failure."""
    filepath = COMPRESSION_PROFILES_DIR / f"{name}.json"
    if not filepath.is_file():
        return None
    try:
        with open(filepath, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (json.JSONDecodeError, OSError):
        return None


def delete_compression_profile(name: str) -> bool:
    """Delete a compression profile by name."""
    filepath = COMPRESSION_PROFILES_DIR / f"{name}.json"
    if filepath.is_file():
        filepath.unlink()
        return True
    return False
