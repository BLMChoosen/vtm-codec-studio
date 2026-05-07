"""
Dataset Builder Worker
======================
QThread worker that generates VTM quadtree split-decision datasets
from trace files and variance-map CSVs.

For each (video, config, qp) experiment it appends rows to four
shared output CSVs (one per QT depth 0-3):

  dataset_depth_0.csv — 128×128 block decisions
  dataset_depth_1.csv —  64×64 block decisions
  dataset_depth_2.csv —  32×32 block decisions
  dataset_depth_3.csv —  16×16 block decisions

Row format: video;config;height;qp;blockVar;diffVar;prevSplit;decisionSplit
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import IO

import numpy as np
from PySide6.QtCore import QThread

from core.process_runner import ProcessSignals


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

DEPTH_TO_SIZE: dict[int, int] = {0: 128, 1: 64, 2: 32, 3: 16}
MAX_QT_DEPTH  = 3
CTU_SIZE      = 128
DM_RES        = 4   # pixels per depth-map cell

DATASET_HEADER = 'video;config;height;qp;blockVar;diffVar;prevSplit;decisionSplit\n'

REFERENCE_FRAME_ORDER: dict[str, list] = {
    'LD': [
        None, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
        15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
    ],
    'RA': [
        None, 2, 4, 2, 8, 6, 4, 6, 16, 10, 8, 10, 8, 14, 12, 14,
        32, 18, 20, 18, 24, 22, 20, 22, 16, 26, 28, 26, 24, 30, 28, 30, 0,
    ],
}

FRAMES_TO_SKIP: dict[str, set] = {
    'RA': {0, 32},
    'LD': {0},
}


# ─────────────────────────────────────────────────────────────────────────────
# Sequence CFG parser
# ─────────────────────────────────────────────────────────────────────────────

def parse_sequence_cfg(cfg_path: str) -> tuple[int, int, int]:
    """
    Parse a VTM per-sequence .cfg file and return (width, height, bit_depth).
    Reads SourceWidth, SourceHeight and InputBitDepth keys.
    """
    keys = {'SourceWidth': None, 'SourceHeight': None, 'InputBitDepth': None}
    with open(cfg_path, encoding='utf-8', errors='ignore') as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if ':' not in line:
                continue
            key, _, rest = line.partition(':')
            key = key.strip()
            if key not in keys:
                continue
            value = rest.split('#')[0].strip()
            try:
                keys[key] = int(value)
            except ValueError:
                pass

    missing = [k for k, v in keys.items() if v is None]
    if missing:
        raise ValueError(
            f"Missing key(s) in '{cfg_path}': {', '.join(missing)}"
        )

    return keys['SourceWidth'], keys['SourceHeight'], keys['InputBitDepth']


def scan_cfg_folder(folder: str) -> list[str]:
    """Return video names (stems) found as *.cfg files in *folder*, sorted."""
    return sorted(p.stem for p in Path(folder).glob('*.cfg'))


# ─────────────────────────────────────────────────────────────────────────────
# Job dataclass
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class DatasetJob:
    trace_files_path:   str
    variance_maps_path: str
    sequence_cfgs_path: str
    output_path:        str
    videos:             list[str]
    qps:                list[str]
    configs:            list[str]
    frames:             int  = 33
    append_mode:        bool = False


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _build_depth_map(trace_path: str, w: int, h: int) -> dict[int, np.ndarray]:
    """Parse a VTM trace CSV → {framePoc: 2-D int8 array of QT depths}."""
    gw, gh = w // DM_RES, h // DM_RES
    dm: dict[int, np.ndarray] = {}

    with open(trace_path, newline='') as fh:
        for line in fh:
            tok = line.rstrip('\n').split(';')
            if len(tok) != 8 or tok[6] != 'QT_Depth':
                continue
            fp  = int(tok[1])
            xcu = int(tok[2])
            ycu = int(tok[3])
            wcu = int(tok[4])
            hcu = int(tok[5])
            dv  = int(tok[7])

            if fp not in dm:
                dm[fp] = np.full((gh, gw), -1, dtype=np.int8)

            dm[fp][
                ycu // DM_RES : ycu // DM_RES + hcu // DM_RES,
                xcu // DM_RES : xcu // DM_RES + wcu // DM_RES,
            ] = dv

    return dm


def _parse_variance_map(var_path: str, num_frames: int) -> dict[int, dict[int, dict]]:
    """Parse a variance CSV → {framePoc: {qtDepth: {(xCU, yCU): (bVar, dRA, dLD)}}}."""
    vm: dict[int, dict[int, dict]] = {
        f: {d: {} for d in range(4)} for f in range(num_frames)
    }
    with open(var_path, newline='') as fh:
        next(fh)  # skip header
        for line in fh:
            parts = line.rstrip('\n').split(',')
            if len(parts) < 7:
                continue
            fp       = int(parts[0])
            xcu      = int(parts[1])
            ycu      = int(parts[2])
            qt_depth = int(parts[3].split('_')[1])
            bvar     = float(parts[4])
            dra      = float(parts[5])
            dld      = float(parts[6])
            if 0 <= fp < num_frames:
                vm[fp][qt_depth][(xcu, ycu)] = (bvar, dra, dld)
    return vm


def _traverse_cu(
    video: str, config: str, qp: str,
    frame_poc: int, ref_frame: int,
    xCU: int, yCU: int, trav_depth: int,
    dm: dict[int, np.ndarray],
    vm: dict[int, dict[int, dict]],
    w: int, h: int,
    rows: list[list[str]],
) -> None:
    if trav_depth > MAX_QT_DEPTH:
        return

    size     = DEPTH_TO_SIZE[trav_depth]
    in_bound = (xCU + size <= w) and (yCU + size <= h)

    if in_bound:
        var_entry = vm[frame_poc][trav_depth].get((xCU, yCU))
        if var_entry is None:
            return  # no variance data for this block → prune subtree

        gx = xCU // DM_RES
        gy = yCU // DM_RES
        curr_d = int(dm[frame_poc][gy, gx])
        ref_d  = int(dm[ref_frame][gy, gx])

        if curr_d < trav_depth:
            return  # encoder pruned this subtree

        bvar, dra, dld = var_entry
        prev_split     = ref_d  > trav_depth
        decision_split = curr_d > trav_depth
        diff_var       = dld if config == 'LD' else dra
        rows[trav_depth].append(
            f'{video};{config};{h};{qp};{bvar};{diff_var};{prev_split};{decision_split}\n'
        )

    half = size // 2
    _traverse_cu(video, config, qp, frame_poc, ref_frame, xCU,        yCU,        trav_depth + 1, dm, vm, w, h, rows)
    _traverse_cu(video, config, qp, frame_poc, ref_frame, xCU + half, yCU,        trav_depth + 1, dm, vm, w, h, rows)
    _traverse_cu(video, config, qp, frame_poc, ref_frame, xCU,        yCU + half, trav_depth + 1, dm, vm, w, h, rows)
    _traverse_cu(video, config, qp, frame_poc, ref_frame, xCU + half, yCU + half, trav_depth + 1, dm, vm, w, h, rows)


# ─────────────────────────────────────────────────────────────────────────────
# Worker
# ─────────────────────────────────────────────────────────────────────────────

class DatasetBuilderWorker(QThread):
    """Build dataset CSVs for a DatasetJob in a background thread."""

    def __init__(self, job: DatasetJob, parent=None):
        super().__init__(parent)
        self.signals    = ProcessSignals()
        self._job       = job
        self._cancelled = False

    def run(self) -> None:
        job   = self._job
        start = time.time()
        log   = self.signals.log_line.emit
        prog  = self.signals.progress.emit

        try:
            self.signals.started.emit()

            out_dir = Path(job.output_path)
            out_dir.mkdir(parents=True, exist_ok=True)

            file_mode = 'a' if job.append_mode else 'w'
            out_fps: list[IO[str]] = []
            for d in range(4):
                fp = open(out_dir / f'dataset_depth_{d}.csv', file_mode, encoding='utf-8')
                if not job.append_mode:
                    fp.write(DATASET_HEADER)
                out_fps.append(fp)

            total_exp = len(job.videos) * len(job.configs) * len(job.qps)
            exp_done  = 0

            for video in job.videos:
                if self._cancelled:
                    break

                cfg_path = Path(job.sequence_cfgs_path) / f'{video}.cfg'
                if not cfg_path.exists():
                    log(f'  CFG not found for "{video}": {cfg_path} — skipping')
                    exp_done += len(job.configs) * len(job.qps)
                    prog(int(exp_done / total_exp * 99))
                    continue

                try:
                    w, h, _ = parse_sequence_cfg(str(cfg_path))
                except Exception as e:
                    log(f'  Failed to parse CFG for "{video}": {e} — skipping')
                    exp_done += len(job.configs) * len(job.qps)
                    prog(int(exp_done / total_exp * 99))
                    continue

                log(f'\n{video}: {w}×{h} (from {cfg_path.name})')

                for config in job.configs:
                    if self._cancelled:
                        break
                    skip_set = FRAMES_TO_SKIP.get(config, set())
                    ref_order = REFERENCE_FRAME_ORDER[config]

                    for qp in job.qps:
                        if self._cancelled:
                            break

                        exp_id     = f'{video}_{qp}_{config}'
                        trace_path = Path(job.trace_files_path)   / f'{exp_id}.csv'
                        var_path   = Path(job.variance_maps_path) / f'{exp_id}-data.csv'

                        log(f'\n── {exp_id} ──')

                        if not trace_path.exists():
                            log(f'  Trace file not found: {trace_path}')
                            exp_done += 1
                            prog(int(exp_done / total_exp * 99))
                            continue
                        if not var_path.exists():
                            log(f'  Variance map not found: {var_path}')
                            exp_done += 1
                            prog(int(exp_done / total_exp * 99))
                            continue

                        log(f'  Building depth map…')
                        dm = _build_depth_map(str(trace_path), w, h)
                        log(f'  Parsing variance map…')
                        vm = _parse_variance_map(str(var_path), job.frames)

                        rows: list[list[str]] = [[] for _ in range(4)]
                        frames_processed = 0

                        for frame_poc in range(job.frames):
                            if self._cancelled:
                                break
                            if frame_poc in skip_set:
                                continue
                            if frame_poc not in dm:
                                continue
                            if frame_poc >= len(ref_order):
                                continue

                            ref_idx = ref_order[frame_poc]
                            if ref_idx is None or ref_idx not in dm:
                                continue

                            for yCTU in range(0, h, CTU_SIZE):
                                for xCTU in range(0, w, CTU_SIZE):
                                    _traverse_cu(
                                        video, config, qp,
                                        frame_poc, ref_idx,
                                        xCTU, yCTU, 0,
                                        dm, vm, w, h, rows,
                                    )

                            frames_processed += 1
                            log(f'  Frame {frame_poc} done')

                        if not self._cancelled:
                            for d in range(4):
                                out_fps[d].writelines(rows[d])
                            total_rows = sum(len(r) for r in rows)
                            log(f'  {exp_id}: {frames_processed} frame(s), {total_rows:,} rows written')

                        exp_done += 1
                        prog(int(exp_done / total_exp * 99))

            for fp in out_fps:
                fp.close()

            if self._cancelled:
                self.signals.finished.emit(False, 'Cancelled')
                return

            elapsed = time.time() - start
            prog(100)
            log(f'\nDataset build complete in {elapsed:.1f}s → {job.output_path}')
            self.signals.finished.emit(True, f'Done in {elapsed:.1f}s')

        except Exception as exc:
            try:
                for fp in out_fps:
                    fp.close()
            except Exception:
                pass
            log(f'\nError: {exc}\n')
            self.signals.finished.emit(False, str(exc))

    def cancel(self) -> None:
        self._cancelled = True
