"""
Catch-Up Orchestrator
=====================
Scans an existing Output-Videos directory, finds every encode that is
missing or was interrupted (has .bin but no .report), cleans the
interrupted folders, then re-runs only the missing combinations using
the same two-encoder (baseline / optimized) strategy as
ComparisonOrchestrator.

Hard-coded sequence registry mirrors the 22-sequence JVET CTC list.
Each entry carries the output-directory name, the per-sequence .cfg
file name, and the YUV file name so the tab can auto-locate sources.
"""

from __future__ import annotations

import itertools
import re
import shutil
import subprocess
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from PySide6.QtCore import QObject, QThread, Signal

from utils.parser import parse_time_profile


# ─────────────────────────────────────────────────────────────────────────────
# Sequence registry  (name visible in UI, output dir, per-seq cfg, yuv stem)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class SeqInfo:
    display:  str   # human-readable name shown in the UI
    out_dir:  str   # sub-directory inside the output root
    cfg_file: str   # filename inside the per-sequence cfg folder
    yuv_file: str   # YUV filename to look for in the YUV source root
    cls:      str   # JVET class (A1, A2, B, C, D, E)


SEQUENCE_REGISTRY: list[SeqInfo] = [
    SeqInfo("Tango2",          "Tango2_3840x2160_60fps_10bit_420",
            "Tango2.cfg",          "Tango2_3840x2160_60fps_10bit_420.yuv",          "A1"),
    SeqInfo("FoodMarket4",     "FoodMarket4_3840x2160_60fps_10bit_420",
            "FoodMarket4.cfg",     "FoodMarket4_3840x2160_60fps_10bit_420.yuv",     "A1"),
    SeqInfo("Campfire",        "Campfire_3840x2160_30fps_bt709_420_videoRange",
            "Campfire.cfg",        "Campfire_3840x2160_30fps_10bit_420_bt709_videoRange.yuv", "A1"),
    SeqInfo("CatRobot1",       "CatRobot_3840x2160_60fps_10bit_420_jvet",
            "CatRobot.cfg",        "CatRobot_3840x2160_60fps_10bit_420_jvet.yuv",   "A2"),
    SeqInfo("DaylightRoad2",   "DaylightRoad2_3840x2160_60fps_10bit_420",
            "DaylightRoad2.cfg",   "DaylightRoad2_3840x2160_60fps_10bit_420.yuv",   "A2"),
    SeqInfo("ParkRunning3",    "ParkRunning3_3840x2160_50fps_10bit_420",
            "ParkRunning3.cfg",    "ParkRunning3_3840x2160_50fps_10bit_420.yuv",    "A2"),
    SeqInfo("MarketPlace",     "MarketPlace_1920x1080_60fps_10bit_420",
            "MarketPlace.cfg",     "MarketPlace_1920x1080_60fps_10bit_420.yuv",     "B"),
    SeqInfo("RitualDance",     "RitualDance_1920x1080_60fps_10bit_420",
            "RitualDance.cfg",     "RitualDance_1920x1080_60fps_10bit_420.yuv",     "B"),
    SeqInfo("Cactus",          "Cactus_1920x1080_50",
            "Cactus.cfg",          "Cactus_1920x1080_50.yuv",                       "B"),
    SeqInfo("BasketballDrive", "BasketballDrive_1920x1080_50",
            "BasketballDrive.cfg", "BasketballDrive_1920x1080_50.yuv",              "B"),
    SeqInfo("BQTerrace",       "BQTerrace_1920x1080_60",
            "BQTerrace.cfg",       "BQTerrace_1920x1080_60.yuv",                   "B"),
    SeqInfo("RaceHorses (C)",  "RaceHorses_832x480_30",
            "RaceHorsesC.cfg",     "RaceHorses_832x480_30.yuv",                    "C"),
    SeqInfo("BQMall",          "BQMall_832x480_60",
            "BQMall.cfg",          "BQMall_832x480_60.yuv",                        "C"),
    SeqInfo("PartyScene",      "PartyScene_832x480_50",
            "PartyScene.cfg",      "PartyScene_832x480_50.yuv",                    "C"),
    SeqInfo("BasketballDrill", "BasketballDrill_832x480_50",
            "BasketballDrill.cfg", "BasketballDrill_832x480_50.yuv",               "C"),
    SeqInfo("RaceHorses (D)",  "RaceHorses_416x240_30",
            "RaceHorses.cfg",      "RaceHorses_416x240_30.yuv",                    "D"),
    SeqInfo("BQSquare",        "BQSquare_416x240_60",
            "BQSquare.cfg",        "BQSquare_416x240_60.yuv",                      "D"),
    SeqInfo("BlowingBubbles",  "BlowingBubbles_416x240_50",
            "BlowingBubbles.cfg",  "BlowingBubbles_416x240_50.yuv",               "D"),
    SeqInfo("BasketballPass",  "BasketballPass_416x240_50",
            "BasketballPass.cfg",  "BasketballPass_416x240_50.yuv",               "D"),
    SeqInfo("FourPeople",      "FourPeople_1280x720_60",
            "FourPeople.cfg",      "FourPeople_1280x720_60.yuv",                  "E"),
    SeqInfo("Johnny",          "Johnny_1280x720_60",
            "Johnny.cfg",          "Johnny_1280x720_60.yuv",                       "E"),
    SeqInfo("KristenAndSara",  "KristenAndSara_1280x720_60",
            "KristenAndSara.cfg",  "KristenAndSara_1280x720_60.yuv",              "E"),
]

# Config codes used in the comparison
CONFIG_INFO: dict[str, dict[str, str]] = {
    "LD": {"cfg_file": "encoder_lowdelay_vtm.cfg",    "folder": "Low_Delay",     "short": "LD"},
    "RA": {"cfg_file": "encoder_randomaccess_vtm.cfg", "folder": "Random_Access", "short": "RA"},
}

EXE_LABELS = ("baseline", "optimized")


# ─────────────────────────────────────────────────────────────────────────────
# Scan result types
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class RepStatus:
    exe:   str
    mode:  str   # "LD" or "RA"
    qp:    int
    rep:   int
    state: str   # "complete" | "interrupted" | "missing"


@dataclass
class SeqScanResult:
    info:        SeqInfo
    yuv_path:    str           # resolved full path, "" if not found
    seq_cfg:     str           # resolved full path, "" if not found
    rep_statuses: list[RepStatus] = field(default_factory=list)

    @property
    def interrupted(self) -> list[RepStatus]:
        return [r for r in self.rep_statuses if r.state == "interrupted"]

    @property
    def missing(self) -> list[RepStatus]:
        return [r for r in self.rep_statuses if r.state == "missing"]

    @property
    def complete_count(self) -> int:
        return sum(1 for r in self.rep_statuses if r.state == "complete")

    @property
    def total_count(self) -> int:
        return len(self.rep_statuses)

    @property
    def summary(self) -> str:
        c = self.complete_count
        t = self.total_count
        ni = len(self.interrupted)
        nm = len(self.missing)
        if ni == 0 and nm == 0:
            return "Complete"
        parts = []
        if ni:
            parts.append(f"{ni} interrupted")
        if nm:
            parts.append(f"{nm} missing")
        return f"{c}/{t}  ({', '.join(parts)})"

    @property
    def needs_work(self) -> bool:
        return bool(self.interrupted or self.missing)

    @property
    def can_encode(self) -> bool:
        return bool(self.yuv_path and self.seq_cfg)


# ─────────────────────────────────────────────────────────────────────────────
# Config for the orchestrator
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class CatchUpConfig:
    output_root:   str
    yuv_root:      str
    seq_cfg_folder: str          # folder with per-sequence .cfg files
    baseline_exe:  str
    optimized_exe: str
    cfg_folder:    str           # VTM main encoder cfg folder (LD/RA cfgs)
    qps:           list[int]
    configs:       list[str]     # subset of ["LD", "RA"]
    frames:        int
    repetitions:   int
    parallel_jobs: int = 1

    def exe_for(self, label: str) -> str:
        return self.baseline_exe if label == "baseline" else self.optimized_exe


# ─────────────────────────────────────────────────────────────────────────────
# Scan helper (pure function, no Qt, can be called from UI thread)
# ─────────────────────────────────────────────────────────────────────────────

def scan_output_root(
    output_root: str,
    yuv_root: str,
    seq_cfg_folder: str,
    qps: list[int],
    configs: list[str],
    repetitions: int,
) -> list[SeqScanResult]:
    """Inspect the output root and return a SeqScanResult per sequence."""
    root = Path(output_root)
    results: list[SeqScanResult] = []

    for info in SEQUENCE_REGISTRY:
        yuv_path = _find_yuv(yuv_root, info.yuv_file)
        seq_cfg  = _find_seq_cfg(seq_cfg_folder, info.cfg_file)
        seq_dir  = root / info.out_dir
        statuses: list[RepStatus] = []

        for exe in EXE_LABELS:
            for config in configs:
                mode_folder = CONFIG_INFO[config]["folder"]
                for qp in qps:
                    for rep in range(repetitions):
                        rep_dir = seq_dir / exe / mode_folder / f"QP{qp}" / f"rep_{rep}"
                        stem    = f"{info.out_dir}_{qp}_{CONFIG_INFO[config]['short']}_{exe}_{rep}"
                        bin_p   = rep_dir / f"{stem}.bin"
                        rpt_p   = rep_dir / f"{stem}.report"

                        if not rep_dir.exists():
                            state = "missing"
                        elif not bin_p.exists():
                            # folder present but no .bin → treat as missing
                            state = "missing"
                        elif not rpt_p.exists():
                            # .bin present but no .report → interrupted
                            state = "interrupted"
                        else:
                            state = "complete"

                        statuses.append(RepStatus(exe=exe, mode=config, qp=qp, rep=rep, state=state))

        results.append(SeqScanResult(
            info=info,
            yuv_path=yuv_path,
            seq_cfg=seq_cfg,
            rep_statuses=statuses,
        ))

    return results


def _find_yuv(yuv_root: str, yuv_filename: str) -> str:
    if not yuv_root:
        return ""
    p = Path(yuv_root) / yuv_filename
    return str(p) if p.is_file() else ""


def _find_seq_cfg(seq_cfg_folder: str, cfg_filename: str) -> str:
    if not seq_cfg_folder:
        return ""
    p = Path(seq_cfg_folder) / cfg_filename
    return str(p) if p.is_file() else ""


# ─────────────────────────────────────────────────────────────────────────────
# Signals
# ─────────────────────────────────────────────────────────────────────────────

class CatchUpSignals(QObject):
    log_line = Signal(str)
    progress = Signal(int)
    finished = Signal(bool, str)


# ─────────────────────────────────────────────────────────────────────────────
# Orchestrator
# ─────────────────────────────────────────────────────────────────────────────

class CatchUpOrchestrator(QThread):
    """
    Background thread that:
      1. Deletes interrupted rep folders (has .bin but no .report).
      2. Encodes every missing (seq, exe, config, qp, rep) combination.
    """

    def __init__(
        self,
        cfg: CatchUpConfig,
        scan_results: list[SeqScanResult],
        parent: Optional[QObject] = None,
    ):
        super().__init__(parent)
        self.signals = CatchUpSignals()
        self._cfg = cfg
        self._scan = scan_results
        self._cancelled = False

        self._processes: dict[int, subprocess.Popen] = {}
        self._processes_lock = threading.Lock()
        self._proc_seq = itertools.count(1)

        self._task_progress: dict[str, int] = {}
        self._progress_lock = threading.Lock()
        self._total_tasks = 1

        self._tmp_cfg_dir: Optional[Path] = None
        self._opt_main_cfg: dict[str, str] = {}

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
    # Main thread body
    # ------------------------------------------------------------------

    def run(self) -> None:
        import time
        cfg = self._cfg
        start = time.time()

        try:
            # Prepare optimized main cfgs (inject EncoderConfig: LD/RA).
            self._tmp_cfg_dir = Path(tempfile.mkdtemp(prefix="vtm_cu_cfg_"))
            for code in cfg.configs:
                original = str(Path(cfg.cfg_folder) / CONFIG_INFO[code]["cfg_file"])
                self._opt_main_cfg[code] = self._make_optimized_cfg(
                    original, CONFIG_INFO[code]["short"], self._tmp_cfg_dir
                )

            # Phase 1 — clean interrupted rep folders.
            self._clean_interrupted()
            if self._cancelled:
                self._finish(False, "Cancelled")
                return

            # Phase 2 — build task list (only what is missing after cleanup).
            tasks = self._build_tasks()
            self._total_tasks = max(1, len(tasks))

            if not tasks:
                self.signals.progress.emit(100)
                self._finish(True, "Nothing to encode — all sequences are complete.")
                return

            self._log(f"\nCatch-Up encode — {len(tasks)} task(s) to run.")
            if not self._run_all(tasks):
                self._finish(False, "Cancelled" if self._cancelled else "One or more encodes failed")
                return

            elapsed = time.time() - start
            self.signals.progress.emit(100)
            self._finish(True, f"Catch-Up complete in {elapsed:.1f}s")

        except Exception as exc:  # pylint: disable=broad-except
            self._log(f"❌ Unexpected error: {exc}")
            self._finish(False, str(exc))
        finally:
            self._cleanup_tmp()

    # ------------------------------------------------------------------
    # Phase 1 — cleanup
    # ------------------------------------------------------------------

    def _clean_interrupted(self) -> None:
        cfg = self._cfg
        root = Path(cfg.output_root)
        cleaned = 0
        for result in self._scan:
            if not result.interrupted:
                continue
            for rs in result.interrupted:
                if self._cancelled:
                    return
                mode_folder = CONFIG_INFO[rs.mode]["folder"]
                rep_dir = (
                    root / result.info.out_dir / rs.exe
                    / mode_folder / f"QP{rs.qp}" / f"rep_{rs.rep}"
                )
                if rep_dir.exists():
                    self._log(f"🗑  Removing interrupted: {rep_dir.relative_to(root)}")
                    shutil.rmtree(rep_dir, ignore_errors=True)
                    cleaned += 1
        if cleaned:
            self._log(f"   Removed {cleaned} interrupted rep folder(s).\n")
        else:
            self._log("   No interrupted folders to remove.\n")

    # ------------------------------------------------------------------
    # Phase 2 — encode missing
    # ------------------------------------------------------------------

    def _build_tasks(self) -> list[tuple]:
        """Return list of (seq_result, exe, config, qp, rep) for everything missing."""
        cfg = self._cfg
        root = Path(cfg.output_root)
        tasks = []
        for result in self._scan:
            if not result.can_encode:
                continue
            for rs in result.rep_statuses:
                mode_folder = CONFIG_INFO[rs.mode]["folder"]
                rep_dir = (
                    root / result.info.out_dir / rs.exe
                    / mode_folder / f"QP{rs.qp}" / f"rep_{rs.rep}"
                )
                # After cleanup, interrupted folders were deleted → check again
                stem  = f"{result.info.out_dir}_{rs.qp}_{CONFIG_INFO[rs.mode]['short']}_{rs.exe}_{rs.rep}"
                bin_p = rep_dir / f"{stem}.bin"
                rpt_p = rep_dir / f"{stem}.report"
                already_done = rep_dir.exists() and bin_p.exists() and rpt_p.exists()
                if not already_done:
                    tasks.append((result, rs.exe, rs.mode, rs.qp, rs.rep))
        return tasks

    def _run_all(self, tasks: list[tuple]) -> bool:
        max_workers = max(1, self._cfg.parallel_jobs)
        succeeded = True
        executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="vtm-cu")
        try:
            futures = {
                executor.submit(self._encode_one, i, *t): i
                for i, t in enumerate(tasks)
            }
            for fut in as_completed(futures):
                try:
                    ok = fut.result()
                except Exception as exc:  # pylint: disable=broad-except
                    self._log(f"❌ Worker raised: {exc}")
                    ok = False
                if not ok:
                    succeeded = False
        finally:
            executor.shutdown(wait=True, cancel_futures=True)
        return succeeded and not self._cancelled

    def _encode_one(
        self,
        idx: int,
        result: SeqScanResult,
        exe_label: str,
        config: str,
        qp: int,
        rep: int,
    ) -> bool:
        if self._cancelled:
            return False

        cfg = self._cfg
        info = CONFIG_INFO[config]
        root = Path(cfg.output_root)
        rep_dir = (
            root / result.info.out_dir / exe_label
            / info["folder"] / f"QP{qp}" / f"rep_{rep}"
        )
        rep_dir.mkdir(parents=True, exist_ok=True)

        stem       = f"{result.info.out_dir}_{qp}_{info['short']}_{exe_label}_{rep}"
        bin_path   = rep_dir / f"{stem}.bin"
        report_path = rep_dir / f"{stem}.report"

        # Main cfg (with EncoderConfig injected for optimized)
        if exe_label == "optimized" and config in self._opt_main_cfg:
            main_cfg = self._opt_main_cfg[config]
        else:
            main_cfg = str(Path(cfg.cfg_folder) / info["cfg_file"])

        command = [cfg.exe_for(exe_label), "-c", main_cfg]
        if result.seq_cfg:
            command += ["-c", result.seq_cfg]
        command += [
            "-i", result.yuv_path,
            "-f", str(cfg.frames),
            "-q", str(qp),
            "-b", str(bin_path),
        ]

        task_id = f"t{idx:05d}"
        prefix  = (
            f"[{idx + 1}/{self._total_tasks} "
            f"{exe_label}/{info['short']} "
            f"{result.info.display} qp{qp} rep{rep}]"
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

        try:
            report_path.write_text("\n".join(run_logs), encoding="utf-8")
        except OSError as exc:
            self._log(f"⚠ Could not write report: {exc}")

        parsed = parse_time_profile("\n".join(run_logs))
        self._set_task_progress(task_id, 100)

        enc = parsed["stages"].get("ENCODER")
        summary = (
            f"ENCODER {enc / 1000:.1f}s" if enc is not None
            else f"elapsed {parsed.get('total_elapsed_s', '?')}s"
        )
        self._log(f"  ✅ {prefix} {summary}")
        return True

    # ------------------------------------------------------------------
    # Helpers shared with ComparisonOrchestrator
    # ------------------------------------------------------------------

    def _make_optimized_cfg(self, original_cfg: str, tag: str, dest_dir: Path) -> str:
        src = Path(original_cfg)
        try:
            text = src.read_text(encoding="utf-8", errors="ignore")
        except OSError as exc:
            self._log(f"⚠ Could not read {src}: {exc}")
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
        content = "\n".join(lines) + "\n\n"
        dest = dest_dir / f"cu_{tag}_{src.name or 'config.cfg'}"
        try:
            dest.write_text(content, encoding="utf-8")
        except OSError as exc:
            self._log(f"⚠ Could not write optimized cfg {dest}: {exc}")
            return original_cfg
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
        parse_progress=None,
        line_capture=None,
        on_progress=None,
    ) -> bool:
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
        except (FileNotFoundError, PermissionError, OSError) as exc:
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
                    self._log(f"⛔ [{label}] cancelled.")
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
