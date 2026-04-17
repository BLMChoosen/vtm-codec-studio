"""
Validators
==========
Input-validation helpers used across the encoder / decoder UI.
Each function returns (ok: bool, message: str).
"""

import os
from pathlib import Path


def validate_file_exists(filepath: str, label: str = "File") -> tuple[bool, str]:
    """Check that *filepath* is a non-empty string pointing to an existing file."""
    if not filepath or not filepath.strip():
        return False, f"{label} path is empty."
    if not Path(filepath).is_file():
        return False, f"{label} not found:\n{filepath}"
    return True, ""


def validate_extension(filepath: str, expected: str, label: str = "File") -> tuple[bool, str]:
    """
    Verify that *filepath* ends with *expected* extension
    (case-insensitive, dot included, e.g. '.yuv').
    """
    if not filepath:
        return False, f"{label} path is empty."
    if not filepath.lower().endswith(expected.lower()):
        return False, f"{label} must have '{expected}' extension.\nGot: {filepath}"
    return True, ""


def validate_positive_int(value: str, label: str = "Value") -> tuple[bool, str]:
    """Ensure *value* is a string representation of a positive integer."""
    try:
        n = int(value)
        if n <= 0:
            return False, f"{label} must be a positive integer. Got: {n}"
        return True, ""
    except ValueError:
        return False, f"{label} is not a valid integer: '{value}'"


def validate_qp(value: str) -> tuple[bool, str]:
    """QP must be an integer in 0..63."""
    try:
        qp = int(value)
        if not 0 <= qp <= 63:
            return False, f"QP must be between 0 and 63. Got: {qp}"
        return True, ""
    except ValueError:
        return False, f"QP is not a valid integer: '{value}'"


def validate_executable(filepath: str, label: str = "Executable") -> tuple[bool, str]:
    """Check that the executable exists and has .exe extension (Windows)."""
    ok, msg = validate_file_exists(filepath, label)
    if not ok:
        return ok, msg
    if os.name == "nt" and not filepath.lower().endswith(".exe"):
        return False, f"{label} must be an .exe file on Windows."
    return True, ""


def validate_directory(dirpath: str, label: str = "Directory") -> tuple[bool, str]:
    """Check that *dirpath* points to an existing directory."""
    if not dirpath or not dirpath.strip():
        return False, f"{label} path is empty."
    if not Path(dirpath).is_dir():
        return False, f"{label} directory not found:\n{dirpath}"
    return True, ""


def validate_output_path(filepath: str, expected_ext: str, label: str = "Output") -> tuple[bool, str]:
    """
    Validate an output path: parent directory must exist and extension must match.
    The file itself doesn't need to exist yet.
    """
    if not filepath or not filepath.strip():
        return False, f"{label} path is empty."
    p = Path(filepath)
    if not p.parent.is_dir():
        return False, f"{label} directory does not exist:\n{p.parent}"
    if not filepath.lower().endswith(expected_ext.lower()):
        return False, f"{label} must have '{expected_ext}' extension."
    return True, ""
