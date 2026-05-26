"""
Comparison Orchestrator
=======================
Runs the same encode experiment with two encoder executables
(baseline = optimization OFF, optimized = optimization ON) across

    video × executable × config (LD/RA) × QP × repetition

then parses each run's appended Time Profile + Total Time line and writes
per-(video, executable, config, QP) averages across repetitions.

Folder layout (video at the top):

    <root>/<video>/<baseline|optimized>/<Low_Delay|Random_Access>/QP<qp>/
        rep_0/    -> <stem>.bin + <stem>.report
        rep_1/
        ...
        Average/  -> average.csv

A consolidated <root>/comparison_summary.csv pairs baseline vs optimized
(ENCODER time, speed-up, % reduction) for every (video, config, QP).
"""

from __future__ import annotations

import csv
import itertools
import re
import shutil
import subprocess
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

from PySide6.QtCore import QObject, QThread, Signal

from core.dataset_builder import parse_sequence_cfg
from utils.parser import parse_time_profile


# Mapping of config codes to encoder cfg file + on-disk folder name + short tag.
CONFIG_INFO: dict[str, dict[str, str]] = {
    "LD": {"cfg_file": "encoder_lowdelay_vtm.cfg", "folder": "Low_Delay", "short": "LD"},
    "RA": {"cfg_file": "encoder_randomaccess_vtm.cfg", "folder": "Random_Access", "short": "RA"},
}

# The two executables compared, in folder-name order.
EXE_LABELS = ("baseline", "optimized")


# ─────────────────────────────────────────────────────────────────────────────
# Data classes
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ComparisonVideo:
    """One input video plus its per-sequence cfg."""

    yuv: str
    sequence_cfg: str = ""
    name: str = ""


@dataclass
class ComparisonConfig:
    """Complete description of one comparison run."""

    videos: list[ComparisonVideo]
    output_root: str
    baseline_exe: str
    optimized_exe: str
    cfg_folder: str
    qps: list[int]
    configs: list[str]              # subset of ["LD", "RA"]
    frames: int
    repetitions: int
    parallel_jobs: int = 1

    def exe_for(self, label: str) -> str:
        return self.baseline_exe if label == "baseline" else self.optimized_exe


@dataclass
class _Task:
    """One encode unit."""

    video: ComparisonVideo
    exe_label: str
    config: str
    qp: int
    rep: int

    @property
    def key(self) -> tuple:
        return (self.video.name, self.exe_label, self.config, self.qp, self.rep)


# ─────────────────────────────────────────────────────────────────────────────
# Signals
# ─────────────────────────────────────────────────────────────────────────────

class ComparisonSignals(QObject):
    log_line = Signal(str)
    progress = Signal(int)          # 0..100 across the whole run
    finished = Signal(bool, str)    # (success, message)


# ─────────────────────────────────────────────────────────────────────────────
# Orchestrator
# ─────────────────────────────────────────────────────────────────────────────

class ComparisonOrchestrator(QThread):
    """Run the configured comparison in a background thread."""

    def __init__(self, cfg: ComparisonConfig, parent: Optional[QObject] = None):
        super().__init__(parent)
        self.signals = ComparisonSignals()
        self._cfg = cfg
        self._cancelled = False

        # Active subprocesses keyed by id so cancel() can terminate them all.
        self._processes: dict[int, subprocess.Popen] = {}
        self._processes_lock = threading.Lock()
        self._proc_seq = itertools.count(1)

        # Parsed Time Profile per task key.
        self._results: dict[tuple, dict] = {}
        self._results_lock = threading.Lock()

        # Group averages (video, exe, config, qp) -> {"stages": {...}, totals}.
        self._group_avg: dict[tuple, dict] = {}

        # Per-task progress for a smooth overall bar.
        self._task_progress: dict[str, int] = {}
        self._progress_lock = threading.Lock()
        self._total_tasks = 1

        # Frame area per video (drives the lightest→heaviest ordering).
        self._video_area: dict[str, float] = {}
        # Modified main cfgs for the optimized encoder (inject EncoderConfig : LD/RA).
        self._opt_main_cfg: dict[str, str] = {}
        self._tmp_cfg_dir: Optional[Path] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def cancel(self) -> None:
        self._cancelled = True
        with self._processes_lock:
            procs = list(self._processes.values())
        for proc in procs:
            if proc.poll() is None:
                try:
                    proc.terminate()
                except OSError:
                    pass

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run(self) -> None:
        cfg = self._cfg
        start = time.time()
        try:
            root = Path(cfg.output_root)
            root.mkdir(parents=True, exist_ok=True)

            # Resolution per video drives the lightest→heaviest ordering.
            for video in cfg.videos:
                self._video_area[video.name] = self._compute_area(video)

            # The optimized encoder needs 'EncoderConfig : LD/RA' injected into its
            # main config. Work on a throwaway copy so the original cfg is untouched.
            self._tmp_cfg_dir = Path(tempfile.mkdtemp(prefix="vtm_cmp_cfg_"))
            for code in cfg.configs:
                original = self._resolve_main_cfg(CONFIG_INFO[code]["cfg_file"])
                self._opt_main_cfg[code] = self._make_optimized_cfg(
                    original, CONFIG_INFO[code]["short"], self._tmp_cfg_dir
                )

            tasks = self._build_tasks()
            self._total_tasks = max(1, len(tasks))

            self._log(
                f"Comparison start — {len(cfg.videos)} video(s) × 2 executables × "
                f"{len(cfg.configs)} config(s) × {len(cfg.qps)} QP(s) × "
                f"{cfg.repetitions} repetition(s) = {len(tasks)} encode(s)."
            )
            self._log(f"Output root: {root}")
            mode = "sequential" if cfg.parallel_jobs == 1 else "PARALLEL — measured times may be skewed by CPU contention"
            self._log(f"Parallel jobs: {cfg.parallel_jobs} ({mode})")
            self._log("Encode order: lightest → heaviest (resolution asc, then QP desc).")
            for code in cfg.configs:
                self._log(f"  Optimized [{code}] uses: {self._opt_main_cfg[code]}")

            if not self._run_all(tasks):
                self._finish(False, "Cancelled" if self._cancelled else "One or more encodes failed")
                return

            self._log("\nComputing per-QP averages…")
            self._write_all_averages(root)
            summary = self._write_summary(root)
            if summary:
                self._log(f"📝 Comparison summary → {summary}")

            elapsed = time.time() - start
            self.signals.progress.emit(100)
            self._finish(True, f"Comparison finished in {elapsed:.1f}s")

        except Exception as exc:  # pylint: disable=broad-except
            self._log(f"❌ Unexpected error: {exc}")
            self._finish(False, str(exc))
        finally:
            self._cleanup_tmp()

    # ------------------------------------------------------------------
    # Task planning + execution
    # ------------------------------------------------------------------

    def _build_tasks(self) -> list[_Task]:
        cfg = self._cfg
        tasks: list[_Task] = []
        for video in cfg.videos:
            for exe_label in EXE_LABELS:
                for config in cfg.configs:
                    for qp in cfg.qps:
                        for rep in range(cfg.repetitions):
                            tasks.append(_Task(video, exe_label, config, qp, rep))

        # Always encode lightest first: lower resolution, then higher QP.
        # Remaining keys keep the order deterministic regardless of queue order.
        tasks.sort(key=lambda t: (
            self._video_area.get(t.video.name, float("inf")),
            -t.qp,
            t.video.name, t.exe_label, t.config, t.rep,
        ))
        return tasks

    def _run_all(self, tasks: list[_Task]) -> bool:
        if not tasks:
            return True

        max_workers = max(1, int(self._cfg.parallel_jobs))
        succeeded = True
        executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="vtm-cmp")
        try:
            futures = {executor.submit(self._encode_one, i, t): i for i, t in enumerate(tasks)}
            for fut in as_completed(futures):
                try:
                    ok = fut.result()
                except Exception as exc:  # pylint: disable=broad-except
                    self._log(f"❌ Worker raised an exception: {exc}")
                    ok = False
                if not ok:
                    succeeded = False
        finally:
            executor.shutdown(wait=True, cancel_futures=True)

        if self._cancelled:
            return False
        return succeeded

    def _encode_one(self, idx: int, task: _Task) -> bool:
        if self._cancelled:
            return False

        cfg = self._cfg
        info = CONFIG_INFO[task.config]
        rep_dir = (
            Path(cfg.output_root) / task.video.name / task.exe_label
            / info["folder"] / f"QP{task.qp}" / f"rep_{task.rep}"
        )
        rep_dir.mkdir(parents=True, exist_ok=True)

        stem = f"{task.video.name}_{task.qp}_{info['short']}_{task.exe_label}_{task.rep}"
        bin_path = rep_dir / f"{stem}.bin"
        report_path = rep_dir / f"{stem}.report"

        if task.exe_label == "optimized" and task.config in self._opt_main_cfg:
            main_cfg = self._opt_main_cfg[task.config]
        else:
            main_cfg = self._resolve_main_cfg(info["cfg_file"])
        exe = cfg.exe_for(task.exe_label)

        command = [exe, "-c", main_cfg]
        if task.video.sequence_cfg:
            command += ["-c", task.video.sequence_cfg]
        command += [
            "-i", task.video.yuv,
            "-f", str(cfg.frames),
            "-q", str(task.qp),
            "-b", str(bin_path),
        ]

        task_id = f"t{idx:05d}"
        prefix = (
            f"[{idx + 1}/{self._total_tasks} {task.exe_label}/{info['short']} "
            f"{task.video.name} qp{task.qp} rep{task.rep}]"
        )
        self._log(f"\n── Encode {prefix} ──")

        seen_pocs: set[int] = set()
        run_logs: list[str] = []

        def _capture(line: str) -> None:
            run_logs.append(line)

        def _progress(line: str) -> Optional[int]:
            m = re.match(r"\s*POC\s+(\d+)", line)
            if m and cfg.frames > 0:
                seen_pocs.add(int(m.group(1)))
                return int(min(len(seen_pocs), cfg.frames) / cfg.frames * 100)
            return None

        ok = self._run_command(
            command,
            label=prefix,
            parse_progress=_progress,
            line_capture=_capture,
            on_progress=lambda pct, _id=task_id: self._set_task_progress(_id, pct),
        )
        if not ok:
            if not self._cancelled:
                self._log(f"❌ {prefix} encode failed.")
            return False
        if self._cancelled:
            return False

        # Persist the full stdout as the .report file (carries the Time Profile).
        try:
            report_path.write_text("\n".join(run_logs), encoding="utf-8")
        except OSError as exc:
            self._log(f"⚠ {prefix} could not write report: {exc}")

        parsed = parse_time_profile("\n".join(run_logs))
        with self._results_lock:
            self._results[task.key] = parsed

        self._set_task_progress(task_id, 100)

        enc = parsed["stages"].get("ENCODER")
        if enc is not None:
            summary = f"ENCODER {enc / 1000:.1f}s"
        elif parsed["total_elapsed_s"] is not None:
            summary = f"elapsed {parsed['total_elapsed_s']:.1f}s"
        else:
            summary = "done (no Time Profile found)"
        self._log(f"  ✅ {prefix} {summary}")
        return True

    # ------------------------------------------------------------------
    # Averaging + summary
    # ------------------------------------------------------------------

    def _write_all_averages(self, root: Path) -> None:
        cfg = self._cfg
        for video in cfg.videos:
            for exe_label in EXE_LABELS:
                for config in cfg.configs:
                    info = CONFIG_INFO[config]
                    for qp in cfg.qps:
                        reps: list[tuple[int, dict]] = []
                        for rep in range(cfg.repetitions):
                            res = self._results.get((video.name, exe_label, config, qp, rep))
                            if res is not None:
                                reps.append((rep, res))
                        if not reps:
                            continue

                        avg_dir = (
                            root / video.name / exe_label / info["folder"]
                            / f"QP{qp}" / "Average"
                        )
                        avg_dir.mkdir(parents=True, exist_ok=True)
                        self._write_average_csv(avg_dir / "average.csv", reps)
                        self._store_group_avg(video.name, exe_label, config, qp, reps)

    def _write_average_csv(self, path: Path, reps: list[tuple[int, dict]]) -> None:
        # Stage order: first appearance across the repetitions.
        order: list[str] = []
        seen: set[str] = set()
        for _, res in reps:
            for stage in res["stages"].keys():
                if stage not in seen:
                    seen.add(stage)
                    order.append(stage)

        rep_indices = [r for r, _ in reps]
        try:
            with open(path, "w", newline="", encoding="utf-8") as fh:
                writer = csv.writer(fh, delimiter=";")
                writer.writerow(["Stage"] + [f"rep_{r}" for r in rep_indices] + ["Average"])

                # Per-stage rows (milliseconds).
                for stage in order:
                    vals = [res["stages"].get(stage) for _, res in reps]
                    writer.writerow(
                        [stage] + [self._fmt(v) for v in vals] + [self._fmt(self._mean(vals))]
                    )

                # Total Time rows (seconds).
                for label, key in (
                    ("TOTAL_TIME_user(s)", "total_user_s"),
                    ("TOTAL_TIME_elapsed(s)", "total_elapsed_s"),
                ):
                    vals = [res.get(key) for _, res in reps]
                    writer.writerow(
                        [label] + [self._fmt(v) for v in vals] + [self._fmt(self._mean(vals))]
                    )
        except OSError as exc:
            self._log(f"⚠ Failed to write {path}: {exc}")

    def _store_group_avg(
        self, video: str, exe_label: str, config: str, qp: int, reps: list[tuple[int, dict]]
    ) -> None:
        stage_names: list[str] = []
        seen: set[str] = set()
        for _, res in reps:
            for stage in res["stages"].keys():
                if stage not in seen:
                    seen.add(stage)
                    stage_names.append(stage)

        stages_avg = {
            stage: self._mean([res["stages"].get(stage) for _, res in reps])
            for stage in stage_names
        }
        self._group_avg[(video, exe_label, config, qp)] = {
            "stages": stages_avg,
            "total_user_s": self._mean([res.get("total_user_s") for _, res in reps]),
            "total_elapsed_s": self._mean([res.get("total_elapsed_s") for _, res in reps]),
        }

    def _write_summary(self, root: Path) -> Optional[Path]:
        cfg = self._cfg
        rows: list[list] = []
        for video in cfg.videos:
            for config in cfg.configs:
                short = CONFIG_INFO[config]["short"]
                for qp in cfg.qps:
                    base = self._group_avg.get((video.name, "baseline", config, qp))
                    opt = self._group_avg.get((video.name, "optimized", config, qp))
                    if not base or not opt:
                        continue

                    b_enc = base["stages"].get("ENCODER")
                    o_enc = opt["stages"].get("ENCODER")
                    speedup = (b_enc / o_enc) if (b_enc and o_enc) else None
                    reduction = ((b_enc - o_enc) / b_enc * 100) if (b_enc and o_enc) else None

                    rows.append([
                        video.name, short, qp,
                        self._fmt(b_enc), self._fmt(o_enc),
                        self._fmt(speedup), self._fmt(reduction),
                        self._fmt(base["total_elapsed_s"]), self._fmt(opt["total_elapsed_s"]),
                    ])

        if not rows:
            return None

        path = root / "comparison_summary.csv"
        try:
            with open(path, "w", newline="", encoding="utf-8") as fh:
                writer = csv.writer(fh, delimiter=";")
                writer.writerow([
                    "video", "config", "qp",
                    "baseline_ENCODER_ms", "optimized_ENCODER_ms",
                    "speedup_x", "reduction_pct",
                    "baseline_elapsed_s", "optimized_elapsed_s",
                ])
                writer.writerows(rows)
        except OSError as exc:
            self._log(f"⚠ Failed to write summary CSV: {exc}")
            return None
        return path

    # ------------------------------------------------------------------
    # Numeric helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _mean(values: list) -> Optional[float]:
        nums = [v for v in values if v is not None]
        if not nums:
            return None
        return sum(nums) / len(nums)

    @staticmethod
    def _fmt(value: Optional[float]) -> str:
        if value is None:
            return "-"
        return "%.10g" % value

    # ------------------------------------------------------------------
    # Process runner + small helpers
    # ------------------------------------------------------------------

    def _resolve_main_cfg(self, cfg_filename: str) -> str:
        if self._cfg.cfg_folder:
            return str(Path(self._cfg.cfg_folder) / cfg_filename)
        return cfg_filename

    def _compute_area(self, video: ComparisonVideo) -> float:
        """Return the video's frame area (w*h); inf when it cannot be read."""
        if video.sequence_cfg:
            try:
                w, h, _ = parse_sequence_cfg(video.sequence_cfg)
                return float(w * h)
            except Exception as exc:  # pylint: disable=broad-except
                self._log(f"⚠ Could not read resolution for {video.name}: {exc}")
        return float("inf")

    def _make_optimized_cfg(self, original_cfg: str, tag: str, dest_dir: Path) -> str:
        """
        Copy *original_cfg* and inject 'EncoderConfig : <tag>' (LD or RA).

        The injected line is never the last one — the file always ends with an
        empty line, as required by the optimized encoder.
        """
        src = Path(original_cfg)
        try:
            text = src.read_text(encoding="utf-8", errors="ignore")
        except OSError as exc:
            self._log(f"⚠ Could not read {src} for the optimized copy: {exc}")
            text = ""

        lines = text.splitlines()
        while lines and not lines[-1].strip():
            lines.pop()

        new_line = f"EncoderConfig{' ' * 17}: {tag}"
        key_re = re.compile(r"^\s*EncoderConfig\s*:", re.IGNORECASE)
        for i, line in enumerate(lines):
            if key_re.match(line):
                lines[i] = new_line
                break
        else:
            lines.append(new_line)

        # Always finish with an empty last line.
        content = "\n".join(lines) + "\n\n"

        dest = dest_dir / f"opt_{tag}_{src.name or 'config.cfg'}"
        try:
            dest.write_text(content, encoding="utf-8")
        except OSError as exc:
            self._log(f"⚠ Could not write optimized cfg {dest}: {exc}")
            return original_cfg  # fall back to the unmodified config
        return str(dest)

    def _cleanup_tmp(self) -> None:
        if self._tmp_cfg_dir and self._tmp_cfg_dir.exists():
            shutil.rmtree(self._tmp_cfg_dir, ignore_errors=True)
        self._tmp_cfg_dir = None

    def _set_task_progress(self, task_id: str, value: int) -> None:
        value = max(0, min(100, int(value)))
        with self._progress_lock:
            self._task_progress[task_id] = value
            overall = sum(self._task_progress.values()) / max(1, self._total_tasks)
        self.signals.progress.emit(int(min(100, overall)))

    def _log(self, line: str) -> None:
        self.signals.log_line.emit(line)

    def _finish(self, success: bool, message: str) -> None:
        self.signals.finished.emit(success, message)

    def _run_command(
        self,
        cmd: list[str],
        label: str,
        parse_progress: Optional[Callable[[str], Optional[int]]] = None,
        line_capture: Optional[Callable[[str], None]] = None,
        on_progress: Optional[Callable[[int], None]] = None,
    ) -> bool:
        """Run a command synchronously, streaming stdout to the log."""
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
