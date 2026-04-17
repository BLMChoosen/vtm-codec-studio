"""
Y4M Helpers
===========
Utilities to extract metadata from .y4m files and build VTM sequence config text.
"""

from dataclasses import dataclass
from pathlib import Path
import re


@dataclass(frozen=True)
class Y4MMetadata:
    """Parsed metadata from a Y4M stream header."""

    width: int
    height: int
    frame_rate_num: int
    frame_rate_den: int
    frame_rate: int
    input_bit_depth: int
    input_chroma_format: str
    ffmpeg_pix_fmt: str


def parse_y4m_metadata(y4m_path: str) -> Y4MMetadata:
    """Parse the Y4M stream header and return normalized metadata."""
    path = Path(y4m_path)
    if not path.is_file():
        raise ValueError(f"Input Y4M file not found: {y4m_path}")

    with path.open("rb") as fh:
        header_bytes = fh.readline()

    if not header_bytes:
        raise ValueError("Input Y4M file is empty.")

    header = header_bytes.decode("ascii", errors="ignore").strip()
    if not header.startswith("YUV4MPEG2"):
        raise ValueError("Input file is not a valid Y4M stream (missing YUV4MPEG2 header).")

    width = None
    height = None
    frame_rate_num = None
    frame_rate_den = None
    chroma_token = "420"

    for token in header.split()[1:]:
        if token.startswith("W"):
            width = _parse_positive_int(token[1:], "Y4M width")
        elif token.startswith("H"):
            height = _parse_positive_int(token[1:], "Y4M height")
        elif token.startswith("F"):
            frame_rate_num, frame_rate_den = _parse_frame_rate(token[1:])
        elif token.startswith("C"):
            chroma_token = token[1:]

    if width is None or height is None:
        raise ValueError("Y4M header is missing width and/or height.")

    if frame_rate_num is None or frame_rate_den is None:
        raise ValueError("Y4M header is missing frame rate (F<num:den>).")

    chroma_format, bit_depth, pix_fmt = _parse_chroma(chroma_token)
    frame_rate = max(1, round(frame_rate_num / frame_rate_den))

    return Y4MMetadata(
        width=width,
        height=height,
        frame_rate_num=frame_rate_num,
        frame_rate_den=frame_rate_den,
        frame_rate=frame_rate,
        input_bit_depth=bit_depth,
        input_chroma_format=chroma_format,
        ffmpeg_pix_fmt=pix_fmt,
    )


def frame_size_bytes(metadata: Y4MMetadata) -> int:
    """Return one frame size in bytes for a raw YUV stream with given metadata."""
    factors = {
        "400": (1, 1),
        "420": (3, 2),
        "422": (2, 1),
        "444": (3, 1),
    }

    if metadata.input_chroma_format not in factors:
        raise ValueError(f"Unsupported chroma format: {metadata.input_chroma_format}")

    mul_num, mul_den = factors[metadata.input_chroma_format]
    pixels = metadata.width * metadata.height
    sample_num = pixels * mul_num
    if sample_num % mul_den != 0:
        raise ValueError(
            "Frame dimensions are incompatible with the selected chroma format."
        )

    samples_per_frame = sample_num // mul_den
    bytes_per_sample = 1 if metadata.input_bit_depth <= 8 else 2
    return samples_per_frame * bytes_per_sample


def count_frames_in_raw_yuv(yuv_path: str, one_frame_bytes: int) -> int:
    """Count total frames from output .yuv size and a known one-frame byte size."""
    if one_frame_bytes <= 0:
        raise ValueError("Invalid frame size for frame counting.")

    path = Path(yuv_path)
    if not path.is_file():
        raise ValueError(f"Output YUV file not found: {yuv_path}")

    file_size = path.stat().st_size
    if file_size <= 0:
        raise ValueError("Output YUV file is empty.")

    if file_size % one_frame_bytes != 0:
        raise ValueError(
            "Output YUV size is not aligned with one frame size "
            f"({file_size} bytes vs frame {one_frame_bytes} bytes)."
        )

    frames = file_size // one_frame_bytes
    if frames <= 0:
        raise ValueError("Could not infer frame count from output YUV file.")

    return frames


def build_sequence_cfg_text(
    metadata: Y4MMetadata,
    input_file: str,
    frames_to_encode: int,
    level: str = "4.1",
    frame_skip: int = 0,
) -> str:
    """Build sequence configuration text compatible with VTM."""
    level_value = (level or "4.1").strip()
    if not level_value:
        level_value = "4.1"

    return "\n".join([
        "#======== File I/O ===============",
        f"InputFile                     : {input_file}",
        f"InputBitDepth                 : {metadata.input_bit_depth}           # Input bitdepth",
        f"InputChromaFormat             : {metadata.input_chroma_format}         # Ratio of luminance to chrominance samples",
        f"FrameRate                     : {metadata.frame_rate}          # Frame Rate per second",
        f"FrameSkip                     : {frame_skip}           # Number of frames to be skipped in input",
        f"SourceWidth                   : {metadata.width}        # Input  frame width",
        f"SourceHeight                  : {metadata.height}        # Input  frame height",
        f"FramesToBeEncoded             : {frames_to_encode}         # Number of frames to be coded",
        "",
        f"Level                         : {level_value}",
        "",
    ])


def _parse_positive_int(raw_value: str, label: str) -> int:
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"Invalid {label}: {raw_value}") from exc
    if value <= 0:
        raise ValueError(f"Invalid {label}: {raw_value}")
    return value


def _parse_frame_rate(raw_value: str) -> tuple[int, int]:
    if ":" in raw_value:
        num_str, den_str = raw_value.split(":", 1)
    else:
        num_str, den_str = raw_value, "1"

    num = _parse_positive_int(num_str, "frame-rate numerator")
    den = _parse_positive_int(den_str, "frame-rate denominator")
    return num, den


def _parse_chroma(chroma_token: str) -> tuple[str, int, str]:
    token = (chroma_token or "420").lower()

    if token.startswith("mono") or token.startswith("400"):
        chroma = "400"
    elif token.startswith("420"):
        chroma = "420"
    elif token.startswith("422"):
        chroma = "422"
    elif token.startswith("444"):
        chroma = "444"
    else:
        raise ValueError(f"Unsupported Y4M chroma format token: C{chroma_token}")

    bit_depth_match = re.search(r"p(\d+)", token)
    bit_depth = int(bit_depth_match.group(1)) if bit_depth_match else 8

    if chroma == "400":
        pix_fmt = "gray" if bit_depth <= 8 else f"gray{bit_depth}le"
    else:
        pix_fmt = f"yuv{chroma}p" if bit_depth <= 8 else f"yuv{chroma}p{bit_depth}le"

    return chroma, bit_depth, pix_fmt
