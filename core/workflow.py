"""
Complete Workflow Orchestrator
==============================
Background QThread that runs the full pipeline end-to-end:

    Convert (.y4m -> .yuv + .cfg)
        -> Encode (LD / RA, multiple QPs)
            -> Decode
                -> Variance Maps (LD + RA pair)
                    -> Build Dataset (consolidated dataset.csv + metadata.json)

Each stage is optional and the output of every stage feeds automatically into
the next. Outputs are organised under a single root folder following a
deterministic structure (see CompleteWorkflowTab docs).
"""

from __future__ import annotations

import itertools
import json
import math
import re
import shutil
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

import numpy as np
import pandas as pd

from PySide6.QtCore import QObject, QThread, Signal

from core.dataset_builder import (
    CTU_SIZE,
    DATASET_HEADER,
    FRAMES_TO_SKIP,
    REFERENCE_FRAME_ORDER,
    _build_depth_map,
    _parse_variance_map,
    _traverse_cu,
    parse_sequence_cfg,
)
from core.variance_maps import (
    BLOCK_SIZES,
    PREVIOUS_FRAME_ORDER,
    _read_y_frames,
    _variance_rows,
)
from utils.parser import parse_vtm_log
from utils.y4m import (
    Y4MMetadata,
    build_sequence_cfg_text,
    count_frames_in_raw_yuv,
    frame_size_bytes,
    parse_y4m_metadata,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _run_safely(runner: Callable[[int, object], bool], idx: int, item: object) -> bool:
    """Wrap a runner so the executor surfaces booleans (and never None)."""
    try:
        return bool(runner(idx, item))
    except Exception:
        # Re-raise so the orchestrator can log and treat it as failure.
        raise


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

TRACE_RULE_DEFAULT = "D_BLOCK_STATISTICS_CODED:poc>=0"

# Mapping of internal mode codes to encoder cfg + folder names + dataset config codes.
MODE_INFO: dict[str, dict[str, str]] = {
    "LD": {
        "label": "Lowdelay",
        "cfg_file": "encoder_lowdelay_vtm.cfg",
        "folder": "lowdelay",
    },
    "RA": {
        "label": "Random Access",
        "cfg_file": "encoder_randomaccess_vtm.cfg",
        "folder": "random_access",
    },
}

# Maximum frames the variance / dataset stages can process (frame 0 skipped).
MAX_VARIANCE_FRAMES = 33


# ─────────────────────────────────────────────────────────────────────────────
# Data classes
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class InputItem:
    """One input file plus its (existing or to-be-generated) per-sequence cfg."""

    path: str                          # original file path (.y4m or .yuv)
    per_sequence_cfg: str = ""         # required for .yuv inputs; auto-filled for .y4m
    name: str = ""                     # video stem (sanitised, used in dataset rows)

    @property
    def is_y4m(self) -> bool:
        return self.path.lower().endswith(".y4m")

    @property
    def is_yuv(self) -> bool:
        return self.path.lower().endswith(".yuv")


@dataclass
class WorkflowSteps:
    """Which stages of the pipeline to execute."""

    converter:     bool = True
    encode:        bool = True
    decode:        bool = True
    variance_maps: bool = True
    dataset:       bool = True


@dataclass
class WorkflowConfig:
    """Complete description of one workflow run."""

    inputs: list[InputItem]
    output_root: str
    steps: WorkflowSteps

    encoder_exe: str
    decoder_exe: str
    ffmpeg_exe: str
    cfg_folder: str = ""

    # Converter
    converter_max_frames: Optional[int] = None   # None = convert everything
    converter_level: str = "4.1"

    # Encode parameters
    encode_qps: list[int] = field(default_factory=lambda: [22, 27, 32, 37])
    encode_modes: list[str] = field(default_factory=lambda: ["LD", "RA"])
    encode_frames: int = 33

    # Variance / Dataset
    variance_frames: int = 33

    # Parallelism — number of independent stage tasks that run at the same time.
    # The Dataset stage is always sequential and waits for the others to finish.
    parallel_jobs: int = 2

    def selected_stage_count(self) -> int:
        return sum([
            self.steps.converter,
            self.steps.encode,
            self.steps.decode,
            self.steps.variance_maps,
            self.steps.dataset,
        ])


# ─────────────────────────────────────────────────────────────────────────────
# Signals
# ─────────────────────────────────────────────────────────────────────────────

class WorkflowSignals(QObject):
    """Signals emitted by the orchestrator."""

    log_line          = Signal(str)
    progress_overall  = Signal(int)         # 0..100 across the whole run
    progress_step     = Signal(int)         # 0..100 for the current sub-step
    stage_started     = Signal(str)         # "Converter", "Encode", ...
    stage_finished    = Signal(str, bool)   # (stage_name, success)
    finished_workflow = Signal(bool, str)   # (success, message)


# ─────────────────────────────────────────────────────────────────────────────
# Orchestrator
# ─────────────────────────────────────────────────────────────────────────────

class WorkflowOrchestrator(QThread):
    """Run the configured workflow in a background thread."""

    def __init__(self, cfg: WorkflowConfig, parent: Optional[QObject] = None):
        super().__init__(parent)
        self.signals = WorkflowSignals()
        self._cfg = cfg
        self._cancelled = False

        # Active subprocesses — keyed by a unique id so cancel() can terminate
        # every in-flight worker, not just the most recent one.
        self._processes: dict[int, subprocess.Popen] = {}
        self._processes_lock = threading.Lock()
        self._proc_seq = itertools.count(1)

        # Progress accounting — number of "atomic operations" planned and done.
        self._total_units = 0
        self._done_units  = 0
        self._units_lock  = threading.Lock()

        # Per-stage task progress (used to drive the stage progress bar).
        self._stage_lock = threading.Lock()
        self._stage_task_progress: dict[str, int] = {}
        self._stage_task_total: int = 0

        # Shared workflow state
        self._executions: list[dict] = []         # populated as we go (used for metadata)
        self._sequence_meta: dict[str, dict] = {}  # video name -> metadata

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def cancel(self) -> None:
        self._cancelled = True
        with self._processes_lock:
            in_flight = list(self._processes.values())
        for proc in in_flight:
            if proc.poll() is None:
                try:
                    proc.terminate()
                except OSError:
                    pass

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run(self) -> None:  # noqa: C901 - end-to-end flow is naturally long
        cfg = self._cfg
        start = time.time()

        try:
            root = Path(cfg.output_root)
            root.mkdir(parents=True, exist_ok=True)

            # Pre-compute progress units and execution_NNN counters so logs stay
            # readable even when stages are skipped.
            self._total_units, exec_count = self._count_planned_units()
            self._done_units = 0
            self._emit_overall(0)

            self._log(
                f"Workflow start — {len(cfg.inputs)} input(s), "
                f"{len(cfg.encode_qps)} QP(s), modes={','.join(cfg.encode_modes)}. "
                f"Stages: {self._stage_summary()}."
            )
            self._log(f"Output root: {root}")
            self._log(f"Planned (input × QP) executions: {exec_count}")

            # ── Stage 1: Converter ────────────────────────────────────
            yuv_inputs = self._stage_converter(root)
            if yuv_inputs is None:
                self._finish(False, "Cancelled" if self._cancelled else "Converter stage failed")
                return

            # ── Stage 2: Encode ───────────────────────────────────────
            executions = self._build_execution_plan(yuv_inputs)
            if cfg.steps.encode:
                if not self._stage_encode(root, executions):
                    self._finish(False, "Cancelled" if self._cancelled else "Encode stage failed")
                    return
            else:
                # Encode disabled: rely on existing artefacts under root for downstream stages.
                # This is unusual but allowed. The bin paths are filled in for any present file.
                for ex in executions:
                    for code in ex["modes"]:
                        m = ex["modes"][code]
                        bin_path = Path(root) / "encoder" / MODE_INFO[code]["folder"] / ex["id"] / "result.bin"
                        if bin_path.is_file():
                            m["bin"] = str(bin_path)

            # ── Stage 3: Decode ───────────────────────────────────────
            if cfg.steps.decode:
                if not self._stage_decode(root, executions):
                    self._finish(False, "Cancelled" if self._cancelled else "Decode stage failed")
                    return
            else:
                for ex in executions:
                    for code in ex["modes"]:
                        m = ex["modes"][code]
                        rec_path = Path(root) / "decode" / MODE_INFO[code]["folder"] / ex["id"] / "reconstructed.yuv"
                        if rec_path.is_file():
                            m["reconstructed"] = str(rec_path)

            # ── Stage 4: Variance Maps ────────────────────────────────
            if cfg.steps.variance_maps:
                if not self._stage_variance(root, executions):
                    self._finish(False, "Cancelled" if self._cancelled else "Variance Maps stage failed")
                    return

            # ── Stage 5: Dataset ──────────────────────────────────────
            if cfg.steps.dataset:
                if not self._stage_dataset(root, executions):
                    self._finish(False, "Cancelled" if self._cancelled else "Dataset stage failed")
                    return

            elapsed = time.time() - start
            self._emit_overall(100)
            self._finish(True, f"Workflow finished in {elapsed:.1f}s")

        except Exception as exc:  # pylint: disable=broad-except
            self._log(f"❌ Unexpected error: {exc}")
            self._finish(False, str(exc))

    # ==================================================================
    # Stage 1 — Converter
    # ==================================================================

    def _stage_converter(self, root: Path) -> Optional[dict[str, dict]]:
        """
        Run the converter stage on all .y4m inputs in parallel.

        Returns a dict mapping input-name → {yuv_path, cfg_path, metadata}
        for every input (whether converted in this run, copied from the
        converter folder, or supplied directly by the user). Returns None
        on cancellation / failure.
        """
        cfg = self._cfg
        results: dict[str, dict] = {}
        results_lock = threading.Lock()

        # Step 1 — handle .yuv inputs (no work needed) and pre-create the
        # converter/ folder when the stage is enabled.
        if cfg.steps.converter:
            self._stage_started("Converter")
            (root / "converter").mkdir(parents=True, exist_ok=True)

        y4m_items: list[InputItem] = []
        for item in cfg.inputs:
            if item.is_yuv:
                results[item.name] = {
                    "yuv": item.path,
                    "cfg": item.per_sequence_cfg,
                    "frames_converted": None,
                    "metadata": None,
                }
            else:
                y4m_items.append(item)

        # Step 2 — for .y4m inputs, either run conversion in parallel or
        # discover previously converted files.
        if not cfg.steps.converter:
            for item in y4m_items:
                target_yuv = root / "converter" / f"{item.name}.yuv"
                target_cfg = root / "converter" / f"{item.name}_per-sequence.cfg"
                if not target_yuv.is_file() or not target_cfg.is_file():
                    self._log(
                        f"❌ Converter is disabled but no pre-converted files found "
                        f"for {item.name} (expected {target_yuv} and {target_cfg})."
                    )
                    return None
                results[item.name] = {
                    "yuv": str(target_yuv),
                    "cfg": str(target_cfg),
                    "frames_converted": "existing",
                    "metadata": None,
                }
        elif y4m_items:
            self._stage_begin(len(y4m_items))

            def runner(idx: int, item: InputItem) -> bool:
                if self._cancelled:
                    return False
                target_yuv = root / "converter" / f"{item.name}.yuv"
                target_cfg = root / "converter" / f"{item.name}_per-sequence.cfg"
                target_yuv.parent.mkdir(parents=True, exist_ok=True)
                task_id = f"conv-{idx:03d}"

                self._log(f"\n── Converter [{idx + 1}/{len(y4m_items)}]: {item.name} ──")
                self._log(f"  Source : {item.path}")
                self._log(f"  Output : {target_yuv}")

                ok = self._convert_y4m_to_yuv(
                    src_y4m=item.path,
                    out_yuv=str(target_yuv),
                    out_cfg=str(target_cfg),
                    max_frames=cfg.converter_max_frames,
                    task_id=task_id,
                )
                if not ok:
                    if not self._cancelled:
                        self._log(f"❌ Converter failed for {item.name}")
                    return False

                with results_lock:
                    results[item.name] = {
                        "yuv": str(target_yuv),
                        "cfg": str(target_cfg),
                        "frames_converted": cfg.converter_max_frames or "all",
                        "metadata": None,
                    }
                self._advance_unit()
                self._stage_task_complete(task_id)
                return True

            success, _ = self._run_parallel(y4m_items, runner)
            if not success:
                self._stage_finished("Converter", False)
                return None

        if cfg.steps.converter:
            self._stage_finished("Converter", True)

        # Cache cfg-derived metadata once (used by later stages).
        for video, info in results.items():
            try:
                w, h, bd = parse_sequence_cfg(info["cfg"])
                info["metadata"] = {"width": w, "height": h, "bitdepth": bd}
                self._sequence_meta[video] = info["metadata"]
            except Exception as exc:  # pylint: disable=broad-except
                self._log(f"❌ Failed to parse cfg for {video}: {exc}")
                return None
        return results

    def _convert_y4m_to_yuv(
        self,
        src_y4m: str,
        out_yuv: str,
        out_cfg: str,
        max_frames: Optional[int],
        task_id: str,
    ) -> bool:
        cfg = self._cfg

        try:
            metadata = parse_y4m_metadata(src_y4m)
        except Exception as exc:  # pylint: disable=broad-except
            self._log(f"❌ Failed to read Y4M header for {Path(src_y4m).name}: {exc}")
            return False

        self._log(
            f"  Header : {metadata.width}×{metadata.height}, "
            f"{metadata.frame_rate} fps, "
            f"{metadata.input_bit_depth}-bit {metadata.input_chroma_format}"
        )
        if max_frames is not None and max_frames > 0:
            self._log(f"  Limit  : first {max_frames} frame(s)")

        ffmpeg_cmd: list[str] = [cfg.ffmpeg_exe, "-y", "-i", src_y4m]
        if metadata.ffmpeg_pix_fmt:
            ffmpeg_cmd += ["-pix_fmt", metadata.ffmpeg_pix_fmt]
        if max_frames is not None and max_frames > 0:
            ffmpeg_cmd += ["-frames:v", str(max_frames)]
        ffmpeg_cmd += ["-f", "rawvideo", out_yuv]

        ok = self._run_command(
            ffmpeg_cmd,
            label=f"ffmpeg {Path(src_y4m).stem}",
            parse_progress=self._ffmpeg_progress,
            on_progress=lambda pct, _id=task_id: self._stage_set_task_progress(_id, pct),
        )
        if not ok:
            return False
        if self._cancelled:
            return False

        try:
            one_frame = frame_size_bytes(metadata)
            frames = count_frames_in_raw_yuv(out_yuv, one_frame)
        except Exception as exc:  # pylint: disable=broad-except
            self._log(f"❌ Could not count converted frames: {exc}")
            return False

        cfg_text = build_sequence_cfg_text(
            metadata=metadata,
            input_file=Path(out_yuv).name,
            frames_to_encode=frames,
            level=cfg.converter_level,
        )
        try:
            Path(out_cfg).write_text(cfg_text, encoding="utf-8")
        except OSError as exc:
            self._log(f"❌ Failed to write per-sequence cfg: {exc}")
            return False

        self._log(f"  ✅ {frames} frame(s) written → {Path(out_yuv).name}")
        self._log(f"  📝 Sequence cfg → {Path(out_cfg).name}")
        return True

    # ==================================================================
    # Stage 2 — Encode
    # ==================================================================

    def _stage_encode(self, root: Path, executions: list[dict]) -> bool:
        cfg = self._cfg
        self._stage_started("Encode")

        # Validate every selected mode has a usable cfg before queuing tasks.
        for mode_code in cfg.encode_modes:
            main_cfg = self._resolve_main_cfg(MODE_INFO[mode_code]["cfg_file"])
            if not Path(main_cfg).is_file():
                self._log(
                    f"❌ Encoder config not found: {main_cfg}\n"
                    f"   Set the cfg folder under Settings."
                )
                self._stage_finished("Encode", False)
                return False

        # Build the (execution × mode) task list.
        tasks: list[tuple[dict, str]] = [
            (ex, mode_code)
            for ex in executions
            for mode_code in cfg.encode_modes
        ]
        if not tasks:
            self._stage_finished("Encode", True)
            return True

        self._stage_begin(len(tasks))
        self._log(
            f"  Running {len(tasks)} encode task(s) "
            f"with up to {self._max_parallel()} parallel job(s)."
        )

        def runner(idx: int, task: tuple[dict, str]) -> bool:
            if self._cancelled:
                return False
            ex, mode_code = task
            return self._encode_one(root, ex, mode_code, task_index=idx, task_count=len(tasks))

        success, _ = self._run_parallel(tasks, runner)
        if not success:
            if not self._cancelled:
                self._log("❌ Encode stage finished with failures.")
            self._stage_finished("Encode", False)
            return False

        self._stage_finished("Encode", True)
        return True

    def _encode_one(
        self,
        root: Path,
        ex: dict,
        mode_code: str,
        task_index: int,
        task_count: int,
    ) -> bool:
        cfg = self._cfg
        mode = MODE_INFO[mode_code]
        exec_dir = root / "encoder" / mode["folder"] / ex["id"]
        artifacts_dir = exec_dir / "artifacts"
        artifacts_dir.mkdir(parents=True, exist_ok=True)

        bin_path = exec_dir / "result.bin"
        trace_path = artifacts_dir / "trace.csv"
        report_path = artifacts_dir / "report.txt"
        metrics_path = artifacts_dir / "metrics.csv"
        command_path = artifacts_dir / "command.txt"
        cfg_copy = artifacts_dir / "per-sequence.cfg"

        main_cfg = self._resolve_main_cfg(mode["cfg_file"])

        task_id = f"enc-{task_index:03d}"
        prefix = f"[{task_index + 1}/{task_count} enc-{mode_code} {ex['id']}]"
        self._log(
            f"\n── Encode {prefix} {ex['video']} qp={ex['qp']} ──"
        )

        # Copy the per-sequence cfg into the artifacts folder for traceability.
        try:
            shutil.copyfile(ex["cfg"], cfg_copy)
        except OSError as exc:
            self._log(f"⚠ {prefix} Could not copy per-sequence cfg into artifacts: {exc}")

        command = [
            cfg.encoder_exe,
            "-c", main_cfg,
            "-c", ex["cfg"],
            "-i", ex["yuv"],
            "-f", str(cfg.encode_frames),
            "-q", str(ex["qp"]),
            "-b", str(bin_path),
            f"--TraceFile={trace_path}",
            f"--TraceRule={TRACE_RULE_DEFAULT}",
        ]

        try:
            command_path.write_text(subprocess.list2cmdline(command), encoding="utf-8")
        except OSError:
            pass

        seen_pocs: set[int] = set()
        run_logs: list[str] = []

        def _on_line(line: str) -> None:
            run_logs.append(line)

        def _progress(line: str) -> Optional[int]:
            match = re.match(r"\s*POC\s+(\d+)", line)
            if match and cfg.encode_frames > 0:
                seen_pocs.add(int(match.group(1)))
                encoded = min(len(seen_pocs), cfg.encode_frames)
                return int(encoded / cfg.encode_frames * 100)
            return None

        ok = self._run_command(
            command,
            label=f"enc-{mode_code} {ex['id']}",
            parse_progress=_progress,
            line_capture=_on_line,
            on_progress=lambda pct, _id=task_id: self._stage_set_task_progress(_id, pct),
        )
        if not ok:
            if not self._cancelled:
                self._log(f"❌ {prefix} encode failed.")
            return False
        if self._cancelled:
            return False

        metrics = parse_vtm_log("\n".join(run_logs), str(bin_path))
        self._write_metrics_csv(metrics_path, metrics)
        self._write_report_txt(
            report_path=report_path,
            execution_id=ex["id"],
            mode_code=mode_code,
            video=ex["video"],
            qp=ex["qp"],
            main_cfg=main_cfg,
            sequence_cfg=ex["cfg"],
            input_yuv=ex["yuv"],
            output_bin=str(bin_path),
            frames=cfg.encode_frames,
            command=command,
            metrics=metrics,
            log_lines=run_logs,
        )

        ex["modes"][mode_code]["bin"] = str(bin_path)
        ex["modes"][mode_code]["trace"] = str(trace_path)
        ex["modes"][mode_code]["report"] = str(report_path)
        ex["modes"][mode_code]["metrics_csv"] = str(metrics_path)
        ex["modes"][mode_code]["metrics"] = metrics

        self._advance_unit()
        self._stage_task_complete(task_id)
        self._log(f"  ✅ {prefix} {bin_path.name} ({metrics.get('size', '-')})")
        return True

    # ==================================================================
    # Stage 3 — Decode
    # ==================================================================

    def _stage_decode(self, root: Path, executions: list[dict]) -> bool:
        cfg = self._cfg
        self._stage_started("Decode")

        tasks: list[tuple[dict, str]] = [
            (ex, mode_code)
            for ex in executions
            for mode_code in cfg.encode_modes
        ]
        if not tasks:
            self._stage_finished("Decode", True)
            return True

        # Verify every input bitstream exists before submitting tasks.
        for ex, mode_code in tasks:
            bin_path = ex["modes"][mode_code].get("bin")
            if not bin_path or not Path(bin_path).is_file():
                self._log(
                    f"❌ No bitstream available for decode of {ex['id']} [{mode_code}]"
                )
                self._stage_finished("Decode", False)
                return False

        self._stage_begin(len(tasks))
        self._log(
            f"  Running {len(tasks)} decode task(s) "
            f"with up to {self._max_parallel()} parallel job(s)."
        )

        def runner(idx: int, task: tuple[dict, str]) -> bool:
            if self._cancelled:
                return False
            ex, mode_code = task
            return self._decode_one(root, ex, mode_code, task_index=idx, task_count=len(tasks))

        success, _ = self._run_parallel(tasks, runner)
        if not success:
            if not self._cancelled:
                self._log("❌ Decode stage finished with failures.")
            self._stage_finished("Decode", False)
            return False

        self._stage_finished("Decode", True)
        return True

    def _decode_one(
        self,
        root: Path,
        ex: dict,
        mode_code: str,
        task_index: int,
        task_count: int,
    ) -> bool:
        cfg = self._cfg
        mode = MODE_INFO[mode_code]
        bin_path = ex["modes"][mode_code].get("bin")

        exec_dir = root / "decode" / mode["folder"] / ex["id"]
        exec_dir.mkdir(parents=True, exist_ok=True)
        rec_path = exec_dir / "reconstructed.yuv"
        metrics_path = exec_dir / "metrics.csv"

        task_id = f"dec-{task_index:03d}"
        prefix = f"[{task_index + 1}/{task_count} dec-{mode_code} {ex['id']}]"
        self._log(
            f"\n── Decode {prefix} {ex['video']} qp={ex['qp']} ──"
        )

        command = [cfg.decoder_exe, "-b", bin_path, "-o", str(rec_path)]

        run_logs: list[str] = []

        def _on_line(line: str) -> None:
            run_logs.append(line)

        max_poc = [0]

        def _progress(line: str) -> Optional[int]:
            match = re.match(r"POC\s+(\d+)", line)
            if match:
                max_poc[0] = max(max_poc[0], int(match.group(1)) + 1)
                return min(int(math.log2(max_poc[0] + 1) * 10), 95)
            return None

        ok = self._run_command(
            command,
            label=f"dec-{mode_code} {ex['id']}",
            parse_progress=_progress,
            line_capture=_on_line,
            on_progress=lambda pct, _id=task_id: self._stage_set_task_progress(_id, pct),
        )
        if not ok:
            if not self._cancelled:
                self._log(f"❌ {prefix} decode failed.")
            return False
        if self._cancelled:
            return False

        metrics = parse_vtm_log("\n".join(run_logs), str(rec_path))
        self._write_metrics_csv(metrics_path, metrics)

        ex["modes"][mode_code]["reconstructed"] = str(rec_path)
        ex["modes"][mode_code]["decode_metrics"] = str(metrics_path)
        ex["modes"][mode_code]["decode_metrics_data"] = metrics

        self._advance_unit()
        self._stage_task_complete(task_id)
        self._log(f"  ✅ {prefix} {rec_path.name}")
        return True

    # ==================================================================
    # Stage 4 — Variance Maps
    # ==================================================================

    def _stage_variance(self, root: Path, executions: list[dict]) -> bool:
        cfg = self._cfg
        if "LD" not in cfg.encode_modes or "RA" not in cfg.encode_modes:
            self._log(
                "⚠ Variance Maps stage requires both LD and RA modes — skipping."
            )
            return True

        self._stage_started("Variance Maps")
        n_frames = max(2, min(cfg.variance_frames, MAX_VARIANCE_FRAMES))

        # Validate prerequisites and metadata up-front.
        for ex in executions:
            if not ex["modes"]["LD"].get("reconstructed") or not ex["modes"]["RA"].get("reconstructed"):
                self._log(
                    f"❌ Cannot compute variance for {ex['id']}: missing decoded YUV."
                )
                self._stage_finished("Variance Maps", False)
                return False
            if not self._sequence_meta.get(ex["video"]):
                self._log(
                    f"❌ Missing sequence metadata for {ex['video']} (resolution/bitdepth)."
                )
                self._stage_finished("Variance Maps", False)
                return False

        if not executions:
            self._stage_finished("Variance Maps", True)
            return True

        self._stage_begin(len(executions))
        self._log(
            f"  Running {len(executions)} variance task(s) "
            f"with up to {self._max_parallel()} parallel job(s)."
        )

        def runner(idx: int, ex: dict) -> bool:
            if self._cancelled:
                return False
            return self._variance_one(
                root, ex, n_frames=n_frames,
                task_index=idx, task_count=len(executions),
            )

        success, _ = self._run_parallel(executions, runner)
        if not success:
            if not self._cancelled:
                self._log("❌ Variance Maps stage finished with failures.")
            self._stage_finished("Variance Maps", False)
            return False

        self._stage_finished("Variance Maps", True)
        return True

    def _variance_one(
        self,
        root: Path,
        ex: dict,
        n_frames: int,
        task_index: int,
        task_count: int,
    ) -> bool:
        meta = self._sequence_meta[ex["video"]]
        task_id = f"var-{task_index:03d}"
        prefix = f"[{task_index + 1}/{task_count} var {ex['id']}]"
        self._log(
            f"\n── Variance Maps {prefix} {ex['video']} qp={ex['qp']} "
            f"({meta['width']}×{meta['height']}, frames={n_frames}) ──"
        )

        try:
            rows = self._compute_variance_rows(
                task_id=task_id,
                original_yuv=ex["yuv"],
                decoded_ld=ex["modes"]["LD"]["reconstructed"],
                decoded_ra=ex["modes"]["RA"]["reconstructed"],
                width=meta["width"],
                height=meta["height"],
                bitdepth=meta["bitdepth"],
                frames=n_frames,
            )
        except Exception as exc:  # pylint: disable=broad-except
            self._log(f"❌ {prefix} variance computation failed: {exc}")
            return False

        if self._cancelled:
            return False

        df = pd.DataFrame(
            rows,
            columns=[
                "Frame", "xCU", "yCU", "depth",
                "block_variance", "diff_variance_RA", "diff_variance_LD",
            ],
        )

        for mode_code in ("LD", "RA"):
            mode = MODE_INFO[mode_code]
            target_dir = root / "variance_maps" / mode["folder"] / ex["id"]
            target_dir.mkdir(parents=True, exist_ok=True)
            target_csv = target_dir / "variance.csv"
            df.to_csv(target_csv, index=False)
            ex["modes"][mode_code]["variance_csv"] = str(target_csv)

        self._advance_unit()
        self._stage_task_complete(task_id)
        self._log(f"  ✅ {prefix} {len(rows):,} rows × 2 mode folders")
        return True

    def _compute_variance_rows(
        self,
        task_id: str,
        original_yuv: str,
        decoded_ld: str,
        decoded_ra: str,
        width: int,
        height: int,
        bitdepth: int,
        frames: int,
    ) -> list[list]:
        orig = _read_y_frames(original_yuv, width, height, bitdepth, frames)
        if self._cancelled:
            return []
        dec_ld = _read_y_frames(decoded_ld, width, height, 10, frames)
        if self._cancelled:
            return []
        dec_ra = _read_y_frames(decoded_ra, width, height, 10, frames)
        if self._cancelled:
            return []

        n_process = min(len(orig), len(dec_ld), len(dec_ra), frames) - 1
        if n_process <= 0:
            return []

        all_rows: list[list] = []
        for step, frame_idx in enumerate(range(1, n_process + 1)):
            if self._cancelled:
                break

            if (frame_idx >= len(PREVIOUS_FRAME_ORDER["LD"])
                    or frame_idx >= len(PREVIOUS_FRAME_ORDER["RA"])):
                break

            ref_ld_idx = PREVIOUS_FRAME_ORDER["LD"][frame_idx]
            ref_ra_idx = PREVIOUS_FRAME_ORDER["RA"][frame_idx]
            if ref_ld_idx is None or ref_ra_idx is None:
                continue
            if ref_ld_idx >= len(dec_ld) or ref_ra_idx >= len(dec_ra):
                continue

            curr = orig[frame_idx]
            ref_ld = dec_ld[ref_ld_idx]
            ref_ra = dec_ra[ref_ra_idx]

            for bs in BLOCK_SIZES:
                all_rows.extend(_variance_rows(curr, ref_ld, ref_ra, bs, frame_idx, width, height))

            self._stage_set_task_progress(task_id, int((step + 1) / n_process * 100))

        return all_rows

    # ==================================================================
    # Stage 5 — Dataset
    # ==================================================================

    def _stage_dataset(self, root: Path, executions: list[dict]) -> bool:
        cfg = self._cfg
        self._stage_started("Create Dataset")

        dataset_dir = root / "dataset" / "final_dataset"
        artifacts_dir = dataset_dir / "artifacts"
        artifacts_dir.mkdir(parents=True, exist_ok=True)

        # Per-depth CSV files (kept inside artifacts/).
        depth_paths = [artifacts_dir / f"dataset_depth_{d}.csv" for d in range(4)]
        depth_files = []
        for path in depth_paths:
            fh = open(path, "w", encoding="utf-8")
            fh.write(DATASET_HEADER)
            depth_files.append(fh)

        try:
            total_rows_per_depth = [0, 0, 0, 0]
            total_exp = max(1, len(executions) * len(cfg.encode_modes))
            done_exp = 0

            for ex in executions:
                if self._cancelled:
                    break

                meta = self._sequence_meta.get(ex["video"])
                if not meta:
                    self._log(f"  Missing metadata for {ex['video']} — skipping.")
                    done_exp += len(cfg.encode_modes)
                    continue

                w, h = meta["width"], meta["height"]

                for mode_code in cfg.encode_modes:
                    if self._cancelled:
                        break

                    mode_data = ex["modes"][mode_code]
                    trace_path = mode_data.get("trace")
                    var_path = mode_data.get("variance_csv")

                    if not trace_path or not Path(trace_path).is_file():
                        self._log(
                            f"  ⚠ Trace file missing for {ex['id']} [{mode_code}]: "
                            f"{trace_path or '(none)'} — skipping."
                        )
                        done_exp += 1
                        self.signals.progress_step.emit(int(done_exp / total_exp * 100))
                        continue
                    if not var_path or not Path(var_path).is_file():
                        self._log(
                            f"  ⚠ Variance file missing for {ex['id']} [{mode_code}]: "
                            f"{var_path or '(none)'} — skipping."
                        )
                        done_exp += 1
                        self.signals.progress_step.emit(int(done_exp / total_exp * 100))
                        continue

                    self._log(
                        f"\n  → {ex['id']} [{mode_code}] {ex['video']} qp={ex['qp']}"
                    )

                    try:
                        dm = _build_depth_map(trace_path, w, h)
                        vm = _parse_variance_map(var_path, cfg.variance_frames)
                    except Exception as exc:  # pylint: disable=broad-except
                        self._log(f"    Failed to read trace/variance: {exc}")
                        done_exp += 1
                        self.signals.progress_step.emit(int(done_exp / total_exp * 100))
                        continue

                    rows: list[list[str]] = [[], [], [], []]
                    skip_set = FRAMES_TO_SKIP.get(mode_code, set())
                    ref_order = REFERENCE_FRAME_ORDER[mode_code]

                    for frame_poc in range(cfg.variance_frames):
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
                                    ex["video"], mode_code, str(ex["qp"]),
                                    frame_poc, ref_idx,
                                    xCTU, yCTU, 0,
                                    dm, vm, w, h, rows,
                                )

                    for d in range(4):
                        depth_files[d].writelines(rows[d])
                        total_rows_per_depth[d] += len(rows[d])

                    self._log(
                        f"    Wrote {sum(len(r) for r in rows):,} rows "
                        f"({rows[0].__len__()}+{rows[1].__len__()}"
                        f"+{rows[2].__len__()}+{rows[3].__len__()})"
                    )

                    done_exp += 1
                    self.signals.progress_step.emit(int(done_exp / total_exp * 100))

            for fh in depth_files:
                fh.close()

            if self._cancelled:
                return False

            # Build the consolidated dataset.csv (depth column added).
            consolidated_path = dataset_dir / "dataset.csv"
            self._log(f"\n  Combining depth files → {consolidated_path}")
            self._merge_depth_csvs(depth_paths, consolidated_path)

            # Copy per-sequence cfg files into the artifacts folder for archival.
            cfg_copy_dir = artifacts_dir / "sequence_cfgs"
            cfg_copy_dir.mkdir(parents=True, exist_ok=True)
            for video, info in self._sequence_meta.items():
                # Locate the cfg path back through executions
                for ex in executions:
                    if ex["video"] == video and ex["cfg"]:
                        try:
                            shutil.copyfile(ex["cfg"], cfg_copy_dir / f"{video}.cfg")
                        except OSError:
                            pass
                        break

            metadata = self._build_metadata(executions, total_rows_per_depth)
            (dataset_dir / "metadata.json").write_text(
                json.dumps(metadata, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )

            self._advance_unit()
            self._stage_finished("Create Dataset", True)
            return True

        except Exception as exc:  # pylint: disable=broad-except
            for fh in depth_files:
                try:
                    fh.close()
                except Exception:  # pylint: disable=broad-except
                    pass
            self._log(f"❌ Dataset stage failed: {exc}")
            self._stage_finished("Create Dataset", False)
            return False

    def _merge_depth_csvs(self, depth_paths: list[Path], output_path: Path) -> None:
        """Concatenate the four depth CSVs into one with an extra `depth` column."""
        with open(output_path, "w", encoding="utf-8") as out_fh:
            out_fh.write("video;config;height;qp;depth;blockVar;diffVar;prevSplit;decisionSplit\n")
            for d, path in enumerate(depth_paths):
                if not path.is_file():
                    continue
                with open(path, "r", encoding="utf-8") as fh:
                    header_skipped = False
                    for line in fh:
                        if not header_skipped:
                            header_skipped = True
                            continue
                        if not line.strip():
                            continue
                        # Insert the depth column after qp (4th field).
                        parts = line.rstrip("\n").split(";")
                        if len(parts) < 8:
                            continue
                        new_row = ";".join(parts[:4] + [str(d)] + parts[4:])
                        out_fh.write(new_row + "\n")

    def _build_metadata(self, executions: list[dict], rows_per_depth: list[int]) -> dict:
        cfg = self._cfg

        def _exec_summary(ex: dict) -> dict:
            modes_dump = {}
            for code, data in ex["modes"].items():
                modes_dump[code] = {
                    "bin":          data.get("bin"),
                    "reconstructed": data.get("reconstructed"),
                    "trace":        data.get("trace"),
                    "metrics_csv":  data.get("metrics_csv"),
                    "decode_metrics_csv": data.get("decode_metrics"),
                    "variance_csv": data.get("variance_csv"),
                    "metrics":      data.get("metrics"),
                    "decode_metrics": data.get("decode_metrics_data"),
                }
            return {
                "id":    ex["id"],
                "video": ex["video"],
                "qp":    ex["qp"],
                "yuv":   ex["yuv"],
                "cfg":   ex["cfg"],
                "modes": modes_dump,
            }

        return {
            "version":   "1.0",
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "tool":      "VTM Codec Studio — Complete Workflow",
            "settings": {
                "qps":               cfg.encode_qps,
                "modes":             cfg.encode_modes,
                "encode_frames":     cfg.encode_frames,
                "variance_frames":   cfg.variance_frames,
                "converter_max_frames": cfg.converter_max_frames,
                "stages": {
                    "converter":     cfg.steps.converter,
                    "encode":        cfg.steps.encode,
                    "decode":        cfg.steps.decode,
                    "variance_maps": cfg.steps.variance_maps,
                    "dataset":       cfg.steps.dataset,
                },
            },
            "inputs": [
                {
                    "name":             item.name,
                    "path":             item.path,
                    "type":             "y4m" if item.is_y4m else "yuv",
                    "per_sequence_cfg": item.per_sequence_cfg,
                    "metadata":         self._sequence_meta.get(item.name),
                }
                for item in cfg.inputs
            ],
            "executions": [_exec_summary(ex) for ex in executions],
            "dataset": {
                "rows_per_depth": rows_per_depth,
                "total_rows":     sum(rows_per_depth),
                "files": {
                    "consolidated": "dataset.csv",
                    "depth_csvs":   [f"artifacts/dataset_depth_{d}.csv" for d in range(4)],
                },
            },
        }

    # ==================================================================
    # Helpers — execution plan, command runner, logging, progress
    # ==================================================================

    def _build_execution_plan(self, yuv_inputs: dict[str, dict]) -> list[dict]:
        """
        Build the (input × QP) execution list. Each entry holds the resolved
        YUV/CFG paths and an empty dict for each mode, populated as stages run.
        """
        cfg = self._cfg
        executions: list[dict] = []
        idx = 1
        for item in cfg.inputs:
            info = yuv_inputs[item.name]
            for qp in cfg.encode_qps:
                exec_id = f"execution_{idx:03d}"
                executions.append({
                    "id":    exec_id,
                    "video": item.name,
                    "yuv":   info["yuv"],
                    "cfg":   info["cfg"],
                    "qp":    qp,
                    "modes": {code: {} for code in cfg.encode_modes},
                })
                idx += 1
        return executions

    def _count_planned_units(self) -> tuple[int, int]:
        cfg = self._cfg
        total = 0
        n_y4m = sum(1 for i in cfg.inputs if i.is_y4m)
        n_inputs = len(cfg.inputs)
        n_qps = max(1, len(cfg.encode_qps))
        n_modes = max(1, len(cfg.encode_modes))
        n_exec = n_inputs * n_qps

        if cfg.steps.converter:
            total += n_y4m
        if cfg.steps.encode:
            total += n_exec * n_modes
        if cfg.steps.decode:
            total += n_exec * n_modes
        if cfg.steps.variance_maps and ("LD" in cfg.encode_modes and "RA" in cfg.encode_modes):
            total += n_exec
        if cfg.steps.dataset:
            total += 1
        return max(1, total), n_exec

    def _stage_summary(self) -> str:
        cfg = self._cfg
        labels = []
        if cfg.steps.converter:     labels.append("Converter")
        if cfg.steps.encode:        labels.append("Encode")
        if cfg.steps.decode:        labels.append("Decode")
        if cfg.steps.variance_maps: labels.append("Variance Maps")
        if cfg.steps.dataset:       labels.append("Create Dataset")
        return ", ".join(labels) or "(none)"

    def _resolve_main_cfg(self, cfg_filename: str) -> str:
        if self._cfg.cfg_folder:
            return str(Path(self._cfg.cfg_folder) / cfg_filename)
        return cfg_filename

    def _stage_started(self, name: str) -> None:
        self._log(f"\n========== Stage: {name} ==========")
        self.signals.stage_started.emit(name)
        self._stage_begin(0)

    def _stage_finished(self, name: str, ok: bool) -> None:
        self.signals.stage_finished.emit(name, ok)
        self.signals.progress_step.emit(100 if ok else 0)
        self._log(f"========== Stage finished: {name} ({'OK' if ok else 'FAIL'}) ==========")

    def _advance_unit(self) -> None:
        with self._units_lock:
            self._done_units = min(self._done_units + 1, self._total_units)
            done = self._done_units
            total = max(1, self._total_units)
        self._emit_overall(int(done / total * 100))

    def _emit_overall(self, value: int) -> None:
        self.signals.progress_overall.emit(max(0, min(100, value)))

    def _log(self, line: str) -> None:
        # QObject signal emissions are inherently thread-safe across QThreads.
        self.signals.log_line.emit(line)

    # ------------------------------------------------------------------
    # Stage-level progress + parallel runner
    # ------------------------------------------------------------------

    def _stage_begin(self, total_tasks: int) -> None:
        with self._stage_lock:
            self._stage_task_progress.clear()
            self._stage_task_total = max(0, total_tasks)
        self.signals.progress_step.emit(0)

    def _stage_set_task_progress(self, task_id: str, value: int) -> None:
        value = max(0, min(100, int(value)))
        with self._stage_lock:
            self._stage_task_progress[task_id] = value
            total_tasks = max(1, self._stage_task_total)
            stage_pct = sum(self._stage_task_progress.values()) / total_tasks
        self.signals.progress_step.emit(int(min(100, stage_pct)))

    def _stage_task_complete(self, task_id: str) -> None:
        # Pin the task at 100 so it stays in the average.
        self._stage_set_task_progress(task_id, 100)

    def _max_parallel(self) -> int:
        return max(1, int(self._cfg.parallel_jobs))

    def _run_parallel(
        self,
        items: list,
        runner: Callable[[int, object], bool],
    ) -> tuple[bool, list[bool]]:
        """
        Execute ``runner(idx, item)`` for every item using a thread pool of
        size ``parallel_jobs``. Returns (all_succeeded, per_item_success).
        Cancellation is honoured: pending futures are cancelled, in-flight
        ones notice via terminated subprocesses or cancelled flag.
        """
        if not items:
            return True, []

        max_workers = self._max_parallel()
        results: list[bool] = [False] * len(items)
        succeeded = True

        executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="vtm-wf")
        try:
            futures = {
                executor.submit(_run_safely, runner, idx, item): idx
                for idx, item in enumerate(items)
            }
            for fut in as_completed(futures):
                idx = futures[fut]
                try:
                    ok = fut.result()
                except Exception as exc:  # pylint: disable=broad-except
                    self._log(f"❌ Worker raised an exception: {exc}")
                    ok = False
                results[idx] = ok
                if not ok:
                    succeeded = False
        finally:
            # Cancel pending futures; running workers exit on their own once
            # their subprocesses are terminated by ``cancel()``.
            executor.shutdown(wait=True, cancel_futures=True)

        if self._cancelled:
            succeeded = False
        return succeeded, results

    def _run_command(
        self,
        cmd: list[str],
        label: str,
        parse_progress: Optional[Callable[[str], Optional[int]]] = None,
        line_capture: Optional[Callable[[str], None]] = None,
        on_progress: Optional[Callable[[int], None]] = None,
    ) -> bool:
        """
        Run a command synchronously, streaming stdout to the log.

        Designed to be safe for concurrent use — every invocation registers
        its Popen in ``self._processes`` so ``cancel()`` can terminate every
        in-flight worker. ``on_progress`` (when provided) receives 0..100
        values parsed by ``parse_progress`` so the caller can aggregate
        per-task progress across parallel workers.
        """
        self._log(f"▶ [{label}] {subprocess.list2cmdline(cmd)}")

        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        except FileNotFoundError as exc:
            self._log(f"❌ Executable not found: {exc}")
            return False
        except PermissionError as exc:
            self._log(f"❌ Permission denied: {exc}")
            return False
        except OSError as exc:
            self._log(f"❌ Could not start process: {exc}")
            return False

        proc_id = next(self._proc_seq)
        with self._processes_lock:
            self._processes[proc_id] = process

        try:
            # If cancellation arrived before we registered the process,
            # terminate immediately.
            if self._cancelled:
                try:
                    process.terminate()
                except OSError:
                    pass

            for raw in iter(process.stdout.readline, ""):
                if self._cancelled:
                    try:
                        process.terminate()
                    except OSError:
                        pass
                    self._log(f"⛔ [{label}] cancelled by user.")
                    break

                line = raw.rstrip("\r\n")
                if line_capture is not None:
                    line_capture(line)
                self._log(f"[{label}] {line}")

                if parse_progress is not None:
                    pct = parse_progress(line)
                    if pct is not None and on_progress is not None:
                        on_progress(min(100, max(0, pct)))
        finally:
            try:
                process.stdout.close()
            except Exception:  # pylint: disable=broad-except
                pass
            try:
                return_code = process.wait()
            except Exception:  # pylint: disable=broad-except
                return_code = 1
            with self._processes_lock:
                self._processes.pop(proc_id, None)

        if self._cancelled:
            return False
        if return_code != 0:
            self._log(f"❌ [{label}] exited with code {return_code}")
            return False
        return True

    def _ffmpeg_progress(self, line: str) -> Optional[int]:
        match = re.search(r"frame=\s*(\d+)", line)
        if match:
            frame = int(match.group(1))
            return min(int(math.log2(frame + 1) * 10), 95)
        return None

    def _write_metrics_csv(self, path: Path, metrics: dict) -> None:
        from utils.csv_export import write_metrics_csv
        try:
            write_metrics_csv(str(path), metrics)
        except OSError as exc:
            self._log(f"⚠ Failed to write metrics CSV ({path.name}): {exc}")

    def _write_report_txt(
        self,
        report_path: Path,
        execution_id: str,
        mode_code: str,
        video: str,
        qp: int,
        main_cfg: str,
        sequence_cfg: str,
        input_yuv: str,
        output_bin: str,
        frames: int,
        command: list[str],
        metrics: dict,
        log_lines: list[str],
    ) -> None:
        lines = [
            "VTM Codec Studio — Workflow Encode Report",
            f"timestamp:    {datetime.now().isoformat(timespec='seconds')}",
            f"execution_id: {execution_id}",
            f"mode:         {mode_code} ({MODE_INFO[mode_code]['label']})",
            f"video:        {video}",
            f"qp:           {qp}",
            f"frames:       {frames}",
            f"input_yuv:    {input_yuv}",
            f"main_cfg:     {main_cfg}",
            f"sequence_cfg: {sequence_cfg}",
            f"output_bin:   {output_bin}",
            f"command:      {subprocess.list2cmdline(command)}",
            "",
            "metrics:",
            f"  time:     {metrics.get('time', '-')}",
            f"  psnr_y:   {metrics.get('psnr_y', '-')}",
            f"  psnr_u:   {metrics.get('psnr_u', '-')}",
            f"  psnr_v:   {metrics.get('psnr_v', '-')}",
            f"  psnr_yuv: {metrics.get('psnr_yuv', '-')}",
            f"  bitrate:  {metrics.get('bitrate', '-')}",
            f"  ssim:     {metrics.get('ssim', '-')}",
            f"  entropy:  {metrics.get('entropy', '-')}",
            f"  size:     {metrics.get('size', '-')}",
            "",
            "process_log:",
        ]
        lines.extend(log_lines)
        try:
            report_path.write_text("\n".join(lines), encoding="utf-8")
        except OSError as exc:
            self._log(f"⚠ Failed to write report.txt: {exc}")

    def _finish(self, success: bool, message: str) -> None:
        self.signals.finished_workflow.emit(success, message)
