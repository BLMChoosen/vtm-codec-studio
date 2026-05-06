"""
Variance Maps Worker
====================
QThread worker that computes per-block variance maps from original
and two decoded YUV files (LD and RA) and writes the results to a CSV.

Depths:  0 → 128×128 blocks
         1 →  64×64 blocks
         2 →  32×32 blocks
         3 →  16×16 blocks

Frame 0 is skipped.
diff_variance_LD  uses frames from the LD-decoded file, indexed by PREVIOUS_FRAME_ORDER["LD"]
diff_variance_RA  uses frames from the RA-decoded file, indexed by PREVIOUS_FRAME_ORDER["RA"]
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from PySide6.QtCore import QThread

from core.process_runner import ProcessSignals

# Reference-frame lookup tables (33-entry lists; index 0 → None = skip)
PREVIOUS_FRAME_ORDER: dict[str, list] = {
    "LD": [
        None, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
        15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
    ],
    "RA": [
        None, 2, 4, 2, 8, 6, 4, 6, 16, 10, 8, 10, 8, 14, 12, 14,
        32, 18, 20, 18, 24, 22, 20, 22, 16, 26, 28, 26, 24, 30, 28, 30, 0,
    ],
}

BLOCK_SIZES = [128, 64, 32, 16]
DEPTH_MAP   = {128: 0, 64: 1, 32: 2, 16: 3}
MAX_FRAMES  = 33


# ─────────────────────────────────────────────────────────────────────────────
# Job dataclass
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class VarianceJob:
    original_yuv:  str
    decoded_yuv_ld: str   # decoded with Low Delay config
    decoded_yuv_ra: str   # decoded with Random Access config
    width:         int
    height:        int
    bitdepth:      int    # original bit depth (8 or 10)
    frames:        int    # total frames to read (including frame 0)
    output_csv:    str


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _read_y_frames(
    path: str, width: int, height: int, bitdepth: int, num_frames: int
) -> list[np.ndarray]:
    uv_w = width  // 2
    uv_h = height // 2
    frames: list[np.ndarray] = []

    with open(path, "rb") as fh:
        for _ in range(num_frames):
            if bitdepth == 8:
                raw = fh.read(width * height)
                if len(raw) < width * height:
                    break
                y = np.frombuffer(raw, dtype=np.uint8).reshape(height, width).astype(np.uint16)
                fh.read(uv_w * uv_h * 2)          # skip U + V
            else:
                raw = fh.read(2 * width * height)
                if len(raw) < 2 * width * height:
                    break
                y = (np.frombuffer(raw, dtype=np.uint16) >> 2).reshape(height, width)
                fh.read(2 * uv_w * uv_h * 2)      # skip U + V (2 bytes each)
            frames.append(y)

    return frames


def _variance_rows(
    curr:        np.ndarray,
    ref_ld:      np.ndarray,
    ref_ra:      np.ndarray,
    block_size:  int,
    frame_index: int,
    width:       int,
    height:      int,
) -> list[list]:
    depth = DEPTH_MAP[block_size]
    rows: list[list] = []

    for by in range(block_size, height + 1, block_size):
        for bx in range(block_size, width + 1, block_size):
            sx, sy = bx - block_size, by - block_size
            c   = curr[sy:by, sx:bx]
            dld = np.abs(ref_ld[sy:by, sx:bx].astype(np.int32) - c.astype(np.int32))
            dra = np.abs(ref_ra[sy:by, sx:bx].astype(np.int32) - c.astype(np.int32))
            rows.append([
                frame_index, sx, sy, f"QT_{depth}",
                float(np.var(c)),
                float(np.var(dra)),
                float(np.var(dld)),
            ])

    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Worker
# ─────────────────────────────────────────────────────────────────────────────

class VarianceMapsWorker(QThread):
    """Compute variance maps for one VarianceJob in a background thread."""

    def __init__(self, job: VarianceJob, parent=None):
        super().__init__(parent)
        self.signals    = ProcessSignals()
        self._job       = job
        self._cancelled = False

    def run(self) -> None:
        import pandas as pd

        job   = self._job
        start = time.time()

        try:
            self.signals.started.emit()

            self.signals.log_line.emit(f"Reading original YUV: {job.original_yuv}")
            orig = _read_y_frames(job.original_yuv, job.width, job.height, job.bitdepth, job.frames)
            if self._cancelled:
                self.signals.finished.emit(False, "Cancelled")
                return
            self.signals.log_line.emit(f"  Loaded {len(orig)} original frame(s)")

            self.signals.log_line.emit(f"Reading decoded LD:   {job.decoded_yuv_ld}")
            dec_ld = _read_y_frames(job.decoded_yuv_ld, job.width, job.height, 10, job.frames)
            if self._cancelled:
                self.signals.finished.emit(False, "Cancelled")
                return
            self.signals.log_line.emit(f"  Loaded {len(dec_ld)} LD decoded frame(s)")

            self.signals.log_line.emit(f"Reading decoded RA:   {job.decoded_yuv_ra}")
            dec_ra = _read_y_frames(job.decoded_yuv_ra, job.width, job.height, 10, job.frames)
            if self._cancelled:
                self.signals.finished.emit(False, "Cancelled")
                return
            self.signals.log_line.emit(f"  Loaded {len(dec_ra)} RA decoded frame(s)")

            n_process = min(len(orig), len(dec_ld), len(dec_ra), job.frames) - 1
            if n_process <= 0:
                self.signals.finished.emit(False, "Not enough frames to process")
                return

            all_rows: list[list] = []

            for step, frame_idx in enumerate(range(1, n_process + 1)):
                if self._cancelled:
                    self.signals.finished.emit(False, "Cancelled")
                    return

                if frame_idx >= len(PREVIOUS_FRAME_ORDER["LD"]) or \
                   frame_idx >= len(PREVIOUS_FRAME_ORDER["RA"]):
                    self.signals.log_line.emit(f"  Frame {frame_idx}: no reference entry — stopping.")
                    break

                ref_ld_idx = PREVIOUS_FRAME_ORDER["LD"][frame_idx]
                ref_ra_idx = PREVIOUS_FRAME_ORDER["RA"][frame_idx]

                if ref_ld_idx is None or ref_ra_idx is None:
                    continue
                if ref_ld_idx >= len(dec_ld) or ref_ra_idx >= len(dec_ra):
                    self.signals.log_line.emit(f"  Frame {frame_idx}: decoded sequence too short — skipping.")
                    continue

                curr   = orig[frame_idx]
                ref_ld = dec_ld[ref_ld_idx]
                ref_ra = dec_ra[ref_ra_idx]

                for bs in BLOCK_SIZES:
                    all_rows.extend(
                        _variance_rows(curr, ref_ld, ref_ra, bs, frame_idx, job.width, job.height)
                    )

                self.signals.progress.emit(int((step + 1) / n_process * 95))
                self.signals.log_line.emit(f"  Frame {frame_idx}/{n_process} done")

            self.signals.log_line.emit(f"Writing CSV: {job.output_csv}")
            Path(job.output_csv).parent.mkdir(parents=True, exist_ok=True)
            df = pd.DataFrame(
                all_rows,
                columns=["Frame", "xCU", "yCU", "depth",
                         "block_variance", "diff_variance_RA", "diff_variance_LD"],
            )
            df.to_csv(job.output_csv, index=False)

            elapsed = time.time() - start
            self.signals.progress.emit(100)
            self.signals.log_line.emit(
                f"\n✅ Done in {elapsed:.1f}s — {len(all_rows):,} rows → {job.output_csv}\n"
            )
            self.signals.finished.emit(True, f"Done in {elapsed:.1f}s")

        except Exception as exc:
            self.signals.log_line.emit(f"\n❌ Error: {exc}\n")
            self.signals.finished.emit(False, str(exc))

    def cancel(self) -> None:
        self._cancelled = True
