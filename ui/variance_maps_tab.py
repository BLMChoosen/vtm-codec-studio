"""
Variance Maps Tab
=================
UI panel for computing per-block variance maps from original and
decoded YUV files.  Results are written as CSV files.

Depths   0 → 128×128 | 1 → 64×64 | 2 → 32×32 | 3 → 16×16
Frame 0 is skipped.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from PySide6.QtCore import Qt, Slot
from PySide6.QtWidgets import (
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QVBoxLayout,
    QWidget,
)

from core.variance_maps import VarianceJob, VarianceMapsWorker
from ui.widgets import FilePickerRow, LogPanel, ScrollSafeComboBox, ScrollSafeSpinBox
from utils.config import ConfigManager
from utils.validators import validate_file_exists, validate_output_path


class VarianceMapsTab(QWidget):
    """Widget containing the full variance-maps interface."""

    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self._config = ConfigManager()

        self._workers:           dict[VarianceMapsWorker, VarianceJob] = {}
        self._worker_progress:   dict[VarianceMapsWorker, int] = {}
        self._worker_logs:       dict[VarianceMapsWorker, list[str]] = {}
        self._worker_job_index:  dict[VarianceMapsWorker, int] = {}

        self._queue:                  list[VarianceJob] = []
        self._queue_running:          bool = False
        self._queue_cancel_requested: bool = False
        self._queue_next_index:       int  = 0
        self._queue_completed:        int  = 0
        self._queue_total:            int  = 0
        self._queue_results:          list[bool] = []

        self._build_ui()

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(10)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QScrollArea.Shape.NoFrame)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        scroll.setStyleSheet("QScrollArea { background: transparent; }")

        form = QWidget()
        form.setMinimumWidth(620)
        fl = QVBoxLayout(form)
        fl.setContentsMargins(4, 4, 4, 4)
        fl.setSpacing(14)

        # ── Original YUV ──────────────────────────────────────────────
        orig_group = QGroupBox("Original YUV")
        orig_layout = QVBoxLayout(orig_group)
        orig_layout.setSpacing(10)

        self._orig_picker = FilePickerRow(
            "Original .yuv:",
            file_filter="YUV Files (*.yuv);;All Files (*)",
            placeholder="Drag & drop or browse for original YUV",
        )
        orig_layout.addWidget(self._orig_picker)

        res_row = QHBoxLayout()
        res_lbl = QLabel("Resolution:")
        res_lbl.setMinimumWidth(160)
        res_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        res_row.addWidget(res_lbl)

        self._width_spin = ScrollSafeSpinBox()
        self._width_spin.setRange(16, 7680)
        self._width_spin.setSingleStep(16)
        self._width_spin.setValue(1920)
        self._width_spin.setToolTip("Frame width in pixels (must be divisible by 16)")
        res_row.addWidget(self._width_spin)

        res_row.addWidget(QLabel("×"))

        self._height_spin = ScrollSafeSpinBox()
        self._height_spin.setRange(16, 4320)
        self._height_spin.setSingleStep(16)
        self._height_spin.setValue(1080)
        self._height_spin.setToolTip("Frame height in pixels (must be divisible by 16)")
        res_row.addWidget(self._height_spin)

        res_presets_lbl = QLabel("  Presets:")
        res_row.addWidget(res_presets_lbl)

        self._res_preset = ScrollSafeComboBox()
        for label, w, h in [
            ("3840×2160", 3840, 2160),
            ("1920×1080", 1920, 1080),
            ("832×480",    832,  480),
            ("416×240",    416,  240),
        ]:
            self._res_preset.addItem(label, (w, h))
        self._res_preset.currentIndexChanged.connect(self._apply_res_preset)
        res_row.addWidget(self._res_preset)
        res_row.addStretch()
        orig_layout.addLayout(res_row)

        bd_row = QHBoxLayout()
        bd_lbl = QLabel("Bit Depth:")
        bd_lbl.setMinimumWidth(160)
        bd_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        bd_row.addWidget(bd_lbl)

        self._bitdepth_combo = ScrollSafeComboBox()
        self._bitdepth_combo.addItem("8-bit",  8)
        self._bitdepth_combo.addItem("10-bit", 10)
        self._bitdepth_combo.setCurrentIndex(1)
        bd_row.addWidget(self._bitdepth_combo)
        bd_row.addStretch()
        orig_layout.addLayout(bd_row)

        frames_row = QHBoxLayout()
        frames_lbl = QLabel("Frames to Read:")
        frames_lbl.setMinimumWidth(160)
        frames_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        frames_row.addWidget(frames_lbl)

        self._frames_spin = ScrollSafeSpinBox()
        self._frames_spin.setRange(2, 33)
        self._frames_spin.setValue(33)
        self._frames_spin.setToolTip(
            "Total frames to read (includes frame 0 which is skipped). Max 33."
        )
        frames_row.addWidget(self._frames_spin)
        frames_row.addStretch()
        orig_layout.addLayout(frames_row)

        fl.addWidget(orig_group)

        # ── Decoded YUV ───────────────────────────────────────────────
        dec_group = QGroupBox("Decoded YUV (10-bit, VTM output)")
        dec_layout = QVBoxLayout(dec_group)
        dec_layout.setSpacing(10)

        self._dec_ld_picker = FilePickerRow(
            "Decoded LD .yuv:",
            file_filter="YUV Files (*.yuv);;All Files (*)",
            placeholder="Decoded with Low Delay config",
        )
        dec_layout.addWidget(self._dec_ld_picker)

        self._dec_ra_picker = FilePickerRow(
            "Decoded RA .yuv:",
            file_filter="YUV Files (*.yuv);;All Files (*)",
            placeholder="Decoded with Random Access config",
        )
        dec_layout.addWidget(self._dec_ra_picker)
        fl.addWidget(dec_group)

        # ── Output ────────────────────────────────────────────────────
        out_group = QGroupBox("Output")
        out_layout = QVBoxLayout(out_group)
        out_layout.setSpacing(10)

        self._csv_picker = FilePickerRow(
            "Output .csv:",
            file_filter="CSV Files (*.csv);;All Files (*)",
            placeholder="Path for output variance CSV",
            mode="save",
        )
        out_layout.addWidget(self._csv_picker)
        fl.addWidget(out_group)

        # ── Queue ─────────────────────────────────────────────────────
        queue_group = QGroupBox("Variance Maps Queue")
        queue_layout = QVBoxLayout(queue_group)
        queue_layout.setSpacing(10)

        self._queue_list = QListWidget()
        self._queue_list.setMinimumHeight(120)
        self._queue_list.currentRowChanged.connect(lambda _: self._update_queue_controls())
        queue_layout.addWidget(self._queue_list)

        queue_btn_row = QHBoxLayout()
        self._add_queue_btn = QPushButton("+ Add Current Settings")
        self._add_queue_btn.clicked.connect(self._add_current_to_queue)
        queue_btn_row.addWidget(self._add_queue_btn)

        self._remove_queue_btn = QPushButton("Remove Selected")
        self._remove_queue_btn.clicked.connect(self._remove_selected_queue_item)
        queue_btn_row.addWidget(self._remove_queue_btn)

        self._clear_queue_btn = QPushButton("Clear Queue")
        self._clear_queue_btn.setObjectName("dangerButton")
        self._clear_queue_btn.clicked.connect(self._clear_queue)
        queue_btn_row.addWidget(self._clear_queue_btn)
        queue_layout.addLayout(queue_btn_row)

        parallel_row = QHBoxLayout()
        par_lbl = QLabel("Parallel Jobs:")
        par_lbl.setMinimumWidth(160)
        par_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        parallel_row.addWidget(par_lbl)

        self._parallel_spin = ScrollSafeSpinBox()
        self._parallel_spin.setRange(1, 8)
        self._parallel_spin.setValue(
            max(1, int(self._config.get("variance_parallel_jobs", 2) or 2))
        )
        self._parallel_spin.setToolTip("Maximum number of variance-map jobs running in parallel.")
        parallel_row.addWidget(self._parallel_spin)
        parallel_row.addStretch()
        queue_layout.addLayout(parallel_row)

        fl.addWidget(queue_group)

        # ── Action buttons ────────────────────────────────────────────
        btn_row = QHBoxLayout()
        btn_row.addStretch()

        self._start_btn = QPushButton("▶  Start Queue")
        self._start_btn.setObjectName("primaryButton")
        self._start_btn.clicked.connect(self._start_queue)
        btn_row.addWidget(self._start_btn)

        self._cancel_btn = QPushButton("■  Cancel")
        self._cancel_btn.setObjectName("dangerButton")
        self._cancel_btn.setEnabled(False)
        self._cancel_btn.clicked.connect(self._cancel_all)
        btn_row.addWidget(self._cancel_btn)

        btn_row.addStretch()
        fl.addLayout(btn_row)

        # ── Log ───────────────────────────────────────────────────────
        self._log = LogPanel()
        self._log.setMinimumHeight(220)
        fl.addWidget(self._log)

        scroll.setWidget(form)
        root.addWidget(scroll, stretch=1)

        self._update_queue_controls()

    # ------------------------------------------------------------------
    # Resolution preset
    # ------------------------------------------------------------------

    @Slot(int)
    def _apply_res_preset(self, _: int) -> None:
        data = self._res_preset.currentData()
        if data:
            w, h = data
            self._width_spin.setValue(w)
            self._height_spin.setValue(h)

    # ------------------------------------------------------------------
    # Validation / job building
    # ------------------------------------------------------------------

    def _validate_form(self) -> tuple[bool, str]:
        checks = [
            validate_file_exists(self._orig_picker.path(),   "Original YUV"),
            validate_file_exists(self._dec_ld_picker.path(), "Decoded LD YUV"),
            validate_file_exists(self._dec_ra_picker.path(), "Decoded RA YUV"),
            validate_output_path(self._csv_picker.path(), ".csv", "Output CSV"),
        ]
        for ok, msg in checks:
            if not ok:
                return False, msg
        if self._width_spin.value() % 16 != 0 or self._height_spin.value() % 16 != 0:
            return False, "Width and height must be divisible by 16."
        return True, ""

    def _build_job_from_form(self) -> Optional[VarianceJob]:
        ok, msg = self._validate_form()
        if not ok:
            QMessageBox.warning(self, "Validation Error", msg)
            return None
        return VarianceJob(
            original_yuv=   self._orig_picker.path(),
            decoded_yuv_ld= self._dec_ld_picker.path(),
            decoded_yuv_ra= self._dec_ra_picker.path(),
            width=          self._width_spin.value(),
            height=         self._height_spin.value(),
            bitdepth=       self._bitdepth_combo.currentData(),
            frames=         self._frames_spin.value(),
            output_csv=     self._csv_picker.path(),
        )

    def _sync_form_to_job(self, job: VarianceJob) -> None:
        self._orig_picker.set_path(job.original_yuv)
        self._dec_ld_picker.set_path(job.decoded_yuv_ld)
        self._dec_ra_picker.set_path(job.decoded_yuv_ra)
        self._width_spin.setValue(job.width)
        self._height_spin.setValue(job.height)
        self._csv_picker.set_path(job.output_csv)

    # ------------------------------------------------------------------
    # Queue helpers
    # ------------------------------------------------------------------

    def _job_summary(self, job: VarianceJob) -> str:
        orig = Path(job.original_yuv).name
        ld   = Path(job.decoded_yuv_ld).name
        ra   = Path(job.decoded_yuv_ra).name
        csv  = Path(job.output_csv).name
        return f"Orig: {orig} | LD: {ld} | RA: {ra} | {job.width}×{job.height} | CSV: {csv}"

    def _refresh_queue_view(self) -> None:
        prev = self._queue_list.currentRow()
        self._queue_list.clear()
        for idx, job in enumerate(self._queue, 1):
            self._queue_list.addItem(QListWidgetItem(f"{idx:02d}. {self._job_summary(job)}"))
        if self._queue_list.count() > 0:
            row = prev if 0 <= prev < self._queue_list.count() else 0
            self._queue_list.setCurrentRow(row)
        self._update_queue_controls()

    def _update_queue_controls(self) -> None:
        running  = self._is_busy()
        has_jobs = bool(self._queue)
        selected = self._queue_list.currentRow() >= 0

        self._start_btn.setEnabled((not running) and has_jobs)
        self._remove_queue_btn.setEnabled((not running) and selected)
        self._clear_queue_btn.setEnabled((not running) and has_jobs)

    def _is_busy(self) -> bool:
        return self._queue_running or bool(self._workers)

    def _max_parallel(self) -> int:
        return max(1, self._parallel_spin.value())

    def _update_status(self) -> None:
        if not self._queue_running:
            return
        active = len(self._workers)
        if self._queue_cancel_requested:
            self._log.set_status(
                f"⛔ Cancelling… active: {active}, done: {self._queue_completed}/{self._queue_total}.",
                "#ffc857",
            )
        else:
            self._log.set_status(
                f"⏳ Running {active} job(s) (max {self._max_parallel()}) — "
                f"{self._queue_completed}/{self._queue_total} done.",
                "#ffc857",
            )

    def _update_progress(self) -> None:
        if self._queue_running and self._queue_total > 0:
            active_sum   = sum(self._worker_progress.values())
            total_prog   = self._queue_completed * 100 + active_sum
            self._log.set_progress(min(int(total_prog / self._queue_total), 100))
        elif self._worker_progress:
            self._log.set_progress(max(self._worker_progress.values()))

    def _cleanup_worker(self, worker: VarianceMapsWorker) -> None:
        self._workers.pop(worker, None)
        self._worker_progress.pop(worker, None)
        self._worker_logs.pop(worker, None)
        self._worker_job_index.pop(worker, None)

    def _launch_more(self) -> None:
        if not self._queue_running:
            return
        while (
            not self._queue_cancel_requested
            and len(self._workers) < self._max_parallel()
            and self._queue_next_index < self._queue_total
        ):
            qi = self._queue_next_index
            self._queue_next_index += 1
            job = self._queue[qi]
            self._sync_form_to_job(job)
            self._log.append("")
            self._log.append(f"===== Queue job {qi + 1}/{self._queue_total} =====")
            self._log.append(self._job_summary(job))
            if not self._start_worker(job, qi):
                self._queue_results.append(False)
                self._queue_completed += 1

        self._update_status()
        self._update_progress()

        if self._queue_cancel_requested and not self._workers:
            self._finish_queue(cancelled=True)
            return
        if self._queue_completed >= self._queue_total and not self._workers:
            self._finish_queue(cancelled=False)

    # ------------------------------------------------------------------
    # Queue lifecycle
    # ------------------------------------------------------------------

    @Slot()
    def _add_current_to_queue(self) -> None:
        if self._is_busy():
            return
        job = self._build_job_from_form()
        if job is None:
            return
        self._queue.append(job)
        self._refresh_queue_view()
        self._log.set_status(f"Queued {len(self._queue)} variance job(s).", "#8a90a4")

    @Slot()
    def _remove_selected_queue_item(self) -> None:
        if self._is_busy():
            return
        row = self._queue_list.currentRow()
        if 0 <= row < len(self._queue):
            del self._queue[row]
            self._refresh_queue_view()

    @Slot()
    def _clear_queue(self) -> None:
        if self._is_busy() or not self._queue:
            return
        if QMessageBox.question(
            self, "Clear Queue", "Remove all jobs from the variance queue?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        ) == QMessageBox.StandardButton.Yes:
            self._queue.clear()
            self._refresh_queue_view()

    @Slot()
    def _start_queue(self) -> None:
        if self._is_busy() or not self._queue:
            return
        self._queue_running          = True
        self._queue_cancel_requested = False
        self._queue_next_index       = 0
        self._queue_completed        = 0
        self._queue_total            = len(self._queue)
        self._queue_results          = []
        self._workers.clear()
        self._worker_progress.clear()
        self._worker_logs.clear()
        self._worker_job_index.clear()
        self._config.set("variance_parallel_jobs", self._parallel_spin.value())

        self._log.clear()
        self._log.set_progress(0)
        self._log.set_status(
            f"Starting queue ({self._queue_total} jobs, max {self._max_parallel()} parallel)…",
            "#ffc857",
        )
        self._set_running(True)
        self._launch_more()

    def _start_worker(self, job: VarianceJob, qi: int) -> bool:
        worker = VarianceMapsWorker(job)
        self._workers[worker]          = job
        self._worker_progress[worker]  = 0
        self._worker_logs[worker]      = []
        self._worker_job_index[worker] = qi

        worker.signals.log_line.connect(lambda line, w=worker: self._on_log_line(w, line))
        worker.signals.progress.connect(lambda val,  w=worker: self._on_progress(w, val))
        worker.signals.started.connect( lambda        w=worker: self._update_status())
        worker.signals.finished.connect(lambda ok, msg, w=worker: self._on_finished(w, ok, msg))
        worker.start()
        return True

    def _finish_queue(self, cancelled: bool) -> None:
        ok_count   = sum(1 for r in self._queue_results if r)
        fail_count = self._queue_completed - ok_count
        if cancelled:
            msg   = (f"⛔ Queue cancelled ({self._queue_completed}/{self._queue_total} jobs). "
                     f"Success: {ok_count}, failed: {fail_count}.")
            color = "#ffc857"
        else:
            msg   = f"✅ Queue finished. Success: {ok_count}, failed: {fail_count}."
            color = "#4cda8a" if fail_count == 0 else "#ffc857"
            self._log.set_progress(100)

        self._log.set_status(msg, color)
        self._queue.clear()
        self._refresh_queue_view()
        self._workers.clear()
        self._worker_progress.clear()
        self._worker_logs.clear()
        self._worker_job_index.clear()
        self._queue_running          = False
        self._queue_cancel_requested = False
        self._queue_next_index       = 0
        self._queue_completed        = 0
        self._queue_total            = 0
        self._set_running(False)

    @Slot()
    def _cancel_all(self) -> None:
        if not self._is_busy():
            return
        self._queue_cancel_requested = True
        self._log.append("\n⛔ Queue cancellation requested…")
        for w in list(self._workers.keys()):
            w.cancel()
        self._update_status()

    # ------------------------------------------------------------------
    # Worker signal handlers
    # ------------------------------------------------------------------

    def _on_log_line(self, worker: VarianceMapsWorker, line: str) -> None:
        qi  = self._worker_job_index.get(worker, -1)
        out = f"[Job {qi + 1:02d}] {line}" if (self._queue_running and qi >= 0) else line
        self._log.append(out)
        if worker in self._worker_logs:
            self._worker_logs[worker].append(line)

    def _on_progress(self, worker: VarianceMapsWorker, val: int) -> None:
        if worker in self._worker_progress:
            self._worker_progress[worker] = max(0, min(val, 100))
        self._update_progress()

    def _on_finished(self, worker: VarianceMapsWorker, success: bool, message: str) -> None:
        qi = self._worker_job_index.get(worker, -1)
        self._cleanup_worker(worker)

        if self._queue_running:
            self._queue_results.append(success)
            self._queue_completed += 1
            prefix = "✅" if success else ("⛔" if self._queue_cancel_requested else "❌")
            label  = (f"{qi + 1}/{self._queue_total}" if qi >= 0
                      else f"{self._queue_completed}/{self._queue_total}")
            self._log.append(f"{prefix} Job {label}: {message}")
            self._log.set_status(
                f"{prefix} Job {label}: {message}",
                "#4cda8a" if success else ("#ffc857" if self._queue_cancel_requested else "#ff6b7a"),
            )
            self._update_progress()
            if self._queue_cancel_requested:
                if not self._workers:
                    self._finish_queue(cancelled=True)
                else:
                    self._update_status()
                return
            self._launch_more()
            return

        self._set_running(False)
        self._log.set_status(
            f"{'✅' if success else '❌'} {message}",
            "#4cda8a" if success else "#ff6b7a",
        )

    # ------------------------------------------------------------------

    def _set_running(self, running: bool) -> None:
        self._add_queue_btn.setEnabled(not running)
        self._cancel_btn.setEnabled(running)
        self._parallel_spin.setEnabled(not running)
        self._update_queue_controls()
