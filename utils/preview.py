"""
Preview Helpers
===============
Launch media assets in YUView.
"""

import subprocess
from pathlib import Path


def launch_yuview(yuview_exe: str, media_path: str) -> tuple[bool, str]:
    """
    Open *media_path* in YUView.

    Returns
    -------
    tuple[bool, str]
        (ok, message)
    """
    if not yuview_exe or not yuview_exe.strip():
        return False, "YUView executable path is not set. Configure it in Settings."

    exe = Path(yuview_exe)
    media = Path(media_path)

    if not exe.is_file():
        return False, f"YUView executable not found:\n{exe}"
    if not media_path or not media_path.strip():
        return False, "No media file selected for preview."
    if not media.is_file():
        return False, f"Media file not found:\n{media}"

    try:
        creationflags = (
            subprocess.CREATE_NO_WINDOW
            if hasattr(subprocess, "CREATE_NO_WINDOW")
            else 0
        )
        subprocess.Popen([str(exe), str(media)], creationflags=creationflags)
        return True, f"Opened in YUView: {media.name}"
    except Exception as exc:
        return False, f"Failed to open YUView:\n{exc}"
