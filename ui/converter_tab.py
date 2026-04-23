"""
Converter Tab
=============
UI panel for queue-based .y4m -> .yuv conversion and sequence cfg generation.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from PySide6.QtCore import Qt, Slot
from PySide6.QtWidgets import (
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QVBoxLayout,
    QWidget,
)

from core.converter import ConverterWorker
from ui.widgets import FilePickerRow, LogPanel, ScrollSafeSpinBox
from utils.config import ConfigManager
from utils.validators import (
    validate_executable,
    validate_extension,
    validate_file_exists,
    validate_output_path,
)


@dataclass
class ConvertJob:
    """Immutable payload for one queued conversion."""

    input_y4m: str
    output_yuv: str
    sequence_cfg_output: str
    level: str


class ConverterTab(QWidget):
    """Widget containing queue-based Y4M -> YUV conversion."""

    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self._config = ConfigManager()
        self._worker: Optional[ConverterWorker] = None
        self._active_job: Optional[ConvertJob] = None
        self._workers: dict[ConverterWorker, ConvertJob] = {}
        self._worker_progress: dict[ConverterWorker, int] = {}
        self._worker_job_index: dict[ConverterWorker, int] = {}

        self._queue: list[ConvertJob] = []
        self._queue_running = False
        self._queue_cancel_requested = False
        self._queue_next_index = 0
        self._queue_completed = 0
        self._queue_total = 0
        self._queue_results: list[bool] = []

        self._log_history: list[str] = []

        self._build_ui()

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
        form.setMinimumWidth(600)
        form_layout = QVBoxLayout(form)
        form_layout.setContentsMargins(4, 4, 4, 4)
        form_layout.setSpacing(14)

        conversion_group = QGroupBox("Y4M to YUV Conversion")
        conversion_layout = QVBoxLayout(conversion_group)
        conversion_layout.setSpacing(12)

        self._input_picker = FilePickerRow(
            "Input .y4m:",
            file_filter="Y4M Files (*.y4m);;All Files (*)",
            placeholder="Drag & drop or browse for .y4m file",
        )
        self._input_picker.path_changed.connect(self._on_input_changed)
        conversion_layout.addWidget(self._input_picker)

        self._output_picker = FilePickerRow(
            "Output .yuv:",
            file_filter="YUV Files (*.yuv);;All Files (*)",
            placeholder="Output raw YUV path",
            mode="save",
        )
        conversion_layout.addWidget(self._output_picker)

        self._sequence_cfg_picker = FilePickerRow(
            "Sequence Config (.cfg):",
            file_filter="Config Files (*.cfg);;All Files (*)",
            placeholder="Output sequence config file",
            mode="save",
        )
        conversion_layout.addWidget(self._sequence_cfg_picker)

        level_row = QHBoxLayout()
        level_lbl = QLabel("Level:")
        level_lbl.setMinimumWidth(160)
        level_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        level_row.addWidget(level_lbl)

        self._level_edit = QLineEdit("4.1")
        self._level_edit.setPlaceholderText("e.g., 4.1")
        level_row.addWidget(self._level_edit, stretch=1)

        conversion_layout.addLayout(level_row)
        form_layout.addWidget(conversion_group)

        queue_group = QGroupBox("Convert Queue")
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
        parallel_lbl = QLabel("Parallel Jobs:")
        parallel_lbl.setMinimumWidth(160)
        parallel_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        parallel_row.addWidget(parallel_lbl)

        self._parallel_spin = ScrollSafeSpinBox()
        self._parallel_spin.setRange(1, 32)
        self._parallel_spin.setValue(2)
        self._parallel_spin.setToolTip("Maximum number of queued conversion jobs running at the same time.")
        try:
            self._parallel_spin.setValue(max(1, int(self._config.get("converter_parallel_jobs", 2))))
        except (TypeError, ValueError):
            self._parallel_spin.setValue(2)
        parallel_row.addWidget(self._parallel_spin)
        parallel_row.addStretch()
        queue_layout.addLayout(parallel_row)

        form_layout.addWidget(queue_group)

        btn_row = QHBoxLayout()
        btn_row.addStretch()

        self._start_queue_btn = QPushButton("▶  Start Queue")
        self._start_queue_btn.setObjectName("primaryButton")
        self._start_queue_btn.setMinimumWidth(150)
        self._start_queue_btn.setMinimumHeight(42)
        self._start_queue_btn.clicked.connect(self._start_queue)
        btn_row.addWidget(self._start_queue_btn)

        self._cancel_btn = QPushButton("■  Cancel")
        self._cancel_btn.setObjectName("dangerButton")
        self._cancel_btn.setMinimumWidth(120)
        self._cancel_btn.setMinimumHeight(42)
        self._cancel_btn.setEnabled(False)
        self._cancel_btn.clicked.connect(self._cancel_conversion)
        btn_row.addWidget(self._cancel_btn)

        btn_row.addStretch()
        form_layout.addLayout(btn_row)

        self._log = LogPanel()
        self._log.setMinimumHeight(220)
        self._log.set_status("Ready", "#8a90a4")
        form_layout.addWidget(self._log)

        scroll.setWidget(form)
        root.addWidget(scroll, stretch=1)

        self._update_queue_controls()

    def _is_busy(self) -> bool:
        return self._queue_running or bool(self._workers)

    def _max_parallel_jobs(self) -> int:
        return max(1, self._parallel_spin.value())

    def _validate_form(self) -> tuple[bool, str]:
        checks = [
            validate_file_exists(self._input_picker.path(), "Input .y4m"),
            validate_extension(self._input_picker.path(), ".y4m", "Input .y4m"),
            validate_output_path(self._output_picker.path(), ".yuv", "Output .yuv"),
            validate_output_path(self._sequence_cfg_picker.path(), ".cfg", "Sequence config"),
        ]

        ffmpeg_exe = self._config.ffmpeg_path()
        if not ffmpeg_exe:
            return False, "FFmpeg executable path is not set.\nGo to Settings to configure it."
        checks.append(validate_executable(ffmpeg_exe, "FFmpeg executable"))

        if not self._level_edit.text().strip():
            return False, "Level is empty."

        for ok, msg in checks:
            if not ok:
                return False, msg
        return True, ""

    def _validate_job(self, job: ConvertJob) -> tuple[bool, str]:
        checks = [
            validate_file_exists(job.input_y4m, "Input .y4m"),
            validate_extension(job.input_y4m, ".y4m", "Input .y4m"),
            validate_output_path(job.output_yuv, ".yuv", "Output .yuv"),
            validate_output_path(job.sequence_cfg_output, ".cfg", "Sequence config"),
        ]

        ffmpeg_exe = self._config.ffmpeg_path()
        if not ffmpeg_exe:
            return False, "FFmpeg executable path is not set.\nGo to Settings to configure it."
        checks.append(validate_executable(ffmpeg_exe, "FFmpeg executable"))

        if not job.level.strip():
            return False, "Level is empty."

        for ok, msg in checks:
            if not ok:
                return False, msg
        return True, ""

    def _build_job_from_form(self) -> Optional[ConvertJob]:
        ok, msg = self._validate_form()
        if not ok:
            QMessageBox.warning(self, "Validation Error", msg)
            return None

        return ConvertJob(
            input_y4m=self._input_picker.path(),
            output_yuv=self._output_picker.path(),
            sequence_cfg_output=self._sequence_cfg_picker.path(),
            level=self._level_edit.text().strip(),
        )

    def _sync_form_to_job(self, job: ConvertJob) -> None:
        self._input_picker.set_path(job.input_y4m)
        self._output_picker.set_path(job.output_yuv)
        self._sequence_cfg_picker.set_path(job.sequence_cfg_output)
        self._level_edit.setText(job.level)

    def _job_summary(self, job: ConvertJob) -> str:
        input_name = Path(job.input_y4m).name
        output_name = Path(job.output_yuv).name
        cfg_name = Path(job.sequence_cfg_output).name
        return f"Input: {input_name} | Output: {output_name} | CFG: {cfg_name} | Level: {job.level}"

    def _refresh_queue_view(self) -> None:
        previous_row = self._queue_list.currentRow()
        self._queue_list.clear()
        for idx, job in enumerate(self._queue, start=1):
            self._queue_list.addItem(QListWidgetItem(f"{idx:02d}. {self._job_summary(job)}"))

        if self._queue_list.count() > 0:
            if 0 <= previous_row < self._queue_list.count():
                self._queue_list.setCurrentRow(previous_row)
            else:
                self._queue_list.setCurrentRow(0)

        self._update_queue_controls()

    def _update_queue_controls(self) -> None:
        running = self._is_busy()
        has_queue = bool(self._queue)
        selected = self._queue_list.currentRow() >= 0

        self._start_queue_btn.setEnabled((not running) and has_queue)
        self._remove_queue_btn.setEnabled((not running) and selected)
        self._clear_queue_btn.setEnabled((not running) and has_queue)

    def _update_running_status(self) -> None:
        if not self._queue_running:
            return

        active_workers = len(self._workers)
        if self._queue_cancel_requested:
            self._log.set_status(
                f"⛔ Cancelling queue... active: {active_workers}, completed: {self._queue_completed}/{self._queue_total}.",
                "#ffc857",
            )
            return

        self._log.set_status(
            (
                f"⏳ Running {active_workers} job(s) in parallel "
                f"(max {self._max_parallel_jobs()}) — "
                f"completed {self._queue_completed}/{self._queue_total}."
            ),
            "#ffc857",
        )

    def _update_overall_progress(self) -> None:
        if self._queue_running and self._queue_total > 0:
            active_progress_sum = sum(self._worker_progress.values())
            total_progress = (self._queue_completed * 100) + active_progress_sum
            overall = int(total_progress / self._queue_total)
            self._log.set_progress(min(overall, 100))
            return

        if self._worker_progress:
            self._log.set_progress(max(self._worker_progress.values()))

    def _cleanup_worker_state(self, worker: ConverterWorker) -> None:
        self._workers.pop(worker, None)
        self._worker_progress.pop(worker, None)
        self._worker_job_index.pop(worker, None)
        self._worker = next(iter(self._workers), None)
        self._active_job = self._workers.get(self._worker) if self._worker else None

    def _launch_more_queue_jobs(self) -> None:
        if not self._queue_running:
            return

        while (
            not self._queue_cancel_requested
            and len(self._workers) < self._max_parallel_jobs()
            and self._queue_next_index < self._queue_total
        ):
            queue_index = self._queue_next_index
            self._queue_next_index += 1

            job = self._queue[queue_index]
            self._sync_form_to_job(job)
            self._log.append("")
            self._log.append(f"===== Queue job {queue_index + 1}/{self._queue_total} =====")
            self._log.append(self._job_summary(job))

            if not self._start_worker_for_job(job, queue_index):
                self._queue_results.append(False)
                self._queue_completed += 1

        self._update_running_status()
        self._update_overall_progress()

        if self._queue_cancel_requested and not self._workers:
            self._finish_queue(cancelled=True)
            return

        if self._queue_completed >= self._queue_total and not self._workers:
            self._finish_queue(cancelled=False)

    @Slot(str)
    def _on_input_changed(self, input_path: str) -> None:
        if not input_path:
            return

        source = Path(input_path)
        self._output_picker.set_path(str(source.with_suffix(".yuv")))
        self._sequence_cfg_picker.set_path(str(source.with_name(f"{source.stem}_sequence.cfg")))

    @Slot()
    def _add_current_to_queue(self) -> None:
        if self._is_busy():
            return

        job = self._build_job_from_form()
        if job is None:
            return

        self._queue.append(job)
        self._refresh_queue_view()
        self._log.set_status(f"Queued {len(self._queue)} convert job(s).", "#8a90a4")

    @Slot()
    def _remove_selected_queue_item(self) -> None:
        if self._is_busy():
            return

        row = self._queue_list.currentRow()
        if row < 0 or row >= len(self._queue):
            return

        del self._queue[row]
        self._refresh_queue_view()

    @Slot()
    def _clear_queue(self) -> None:
        if self._is_busy() or not self._queue:
            return

        confirm = QMessageBox.question(
            self,
            "Clear Queue",
            "Remove all jobs from the convert queue?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if confirm == QMessageBox.StandardButton.Yes:
            self._queue.clear()
            self._refresh_queue_view()

    @Slot()
    def _start_queue(self) -> None:
        if self._is_busy():
            return

        if not self._queue:
            QMessageBox.information(self, "Queue Empty", "Add at least one job to the queue first.")
            return

        for idx, job in enumerate(self._queue, start=1):
            ok, msg = self._validate_job(job)
            if not ok:
                QMessageBox.warning(self, "Validation Error", f"Job {idx}:\n{msg}")
                return

        self._queue_running = True
        self._queue_cancel_requested = False
        self._queue_next_index = 0
        self._queue_completed = 0
        self._queue_total = len(self._queue)
        self._queue_results = []
        self._workers.clear()
        self._worker_progress.clear()
        self._worker_job_index.clear()
        self._worker = None
        self._active_job = None
        self._config.set("converter_parallel_jobs", self._parallel_spin.value())

        self._log.clear()
        self._log_history.clear()
        self._log.set_progress(0)
        self._log.set_status(
            f"Starting queue ({self._queue_total} jobs, max {self._max_parallel_jobs()} parallel)…",
            "#ffc857",
        )
        self._set_running(True)
        self._launch_more_queue_jobs()

    def _start_worker_for_job(self, job: ConvertJob, queue_index: int) -> bool:
        ok, msg = self._validate_job(job)
        if not ok:
            self._log.append(f"❌ Validation failed for job {queue_index + 1}/{self._queue_total}:\n{msg}")
            return False

        worker = ConverterWorker(
            ffmpeg_exe=self._config.ffmpeg_path(),
            input_y4m=job.input_y4m,
            output_yuv=job.output_yuv,
            sequence_cfg_output=job.sequence_cfg_output,
            level=job.level,
        )

        self._workers[worker] = job
        self._worker_progress[worker] = 0
        self._worker_job_index[worker] = queue_index

        worker.signals.log_line.connect(lambda line, w=worker: self._handle_log_line(w, line))
        worker.signals.progress.connect(lambda value, w=worker: self._handle_worker_progress(w, value))
        worker.signals.started.connect(lambda w=worker: self._on_started(w))
        worker.signals.finished.connect(lambda success, message, w=worker: self._on_finished(w, success, message))
        worker.start()

        self._worker = next(iter(self._workers), None)
        self._active_job = self._workers.get(self._worker) if self._worker else None

        self._config.add_recent_file("input", job.input_y4m)
        self._config.add_recent_file("output", job.output_yuv)
        return True

    def _finish_queue(self, cancelled: bool) -> None:
        completed = self._queue_completed
        success_count = sum(1 for ok in self._queue_results if ok)
        failed_count = completed - success_count

        if cancelled:
            status = (
                f"⛔ Queue cancelled ({completed}/{self._queue_total} jobs). "
                f"Success: {success_count}, failed: {failed_count}."
            )
            color = "#ffc857"
        else:
            status = f"✅ Queue finished. Success: {success_count}, failed: {failed_count}."
            color = "#4cda8a" if failed_count == 0 else "#ffc857"
            self._log.set_progress(100)

        self._log.set_status(status, color)
        self._queue.clear()
        self._refresh_queue_view()

        self._workers.clear()
        self._worker_progress.clear()
        self._worker_job_index.clear()
        self._worker = None
        self._active_job = None
        self._queue_running = False
        self._queue_cancel_requested = False
        self._queue_next_index = 0
        self._queue_completed = 0
        self._queue_total = 0
        self._set_running(False)

    @Slot()
    def _cancel_conversion(self) -> None:
        if not self._is_busy():
            return

        self._queue_cancel_requested = True
        self._log.append("\n⛔ Queue cancellation requested. Stopping active jobs…")
        for worker in list(self._workers.keys()):
            worker.cancel()
        self._update_running_status()

    def _on_started(self, worker: ConverterWorker) -> None:
        if worker not in self._workers:
            return
        self._update_running_status()

    def _handle_log_line(self, worker: ConverterWorker, line: str) -> None:
        queue_index = self._worker_job_index.get(worker, -1)
        if self._queue_running and queue_index >= 0:
            output_line = f"[Job {queue_index + 1:02d}] {line}"
        else:
            output_line = line

        self._log.append(output_line)
        self._log_history.append(output_line)

    def _handle_worker_progress(self, worker: ConverterWorker, value: int) -> None:
        if worker in self._worker_progress:
            self._worker_progress[worker] = max(0, min(value, 100))
        self._update_overall_progress()

    def _on_finished(self, worker: ConverterWorker, success: bool, message: str) -> None:
        queue_index = self._worker_job_index.get(worker, -1)

        if self._queue_running:
            self._queue_results.append(success)
            self._queue_completed += 1
            color = "#4cda8a" if success else ("#ffc857" if self._queue_cancel_requested else "#ff6b7a")
            prefix = "✅" if success else ("⛔" if self._queue_cancel_requested else "❌")
            job_label = f"{queue_index + 1}/{self._queue_total}" if queue_index >= 0 else f"{self._queue_completed}/{self._queue_total}"
            self._log.append(f"{prefix} Job {job_label}: {message}")
            self._log.set_status(f"{prefix} Job {job_label}: {message}", color)

            self._cleanup_worker_state(worker)
            self._update_overall_progress()

            if self._queue_cancel_requested:
                if not self._workers:
                    self._finish_queue(cancelled=True)
                else:
                    self._update_running_status()
                return

            self._launch_more_queue_jobs()
            return

        self._cleanup_worker_state(worker)
        self._set_running(False)

        if success:
            self._log.set_status(f"✅ {message}", "#4cda8a")
            self._log.set_progress(100)
        else:
            if "cancel" in message.lower():
                self._log.set_status(f"⛔ {message}", "#ffc857")
            else:
                self._log.set_status(f"❌ {message}", "#ff6b7a")

    def _set_running(self, running: bool) -> None:
        self._add_queue_btn.setEnabled(not running)
        self._cancel_btn.setEnabled(running)
        self._parallel_spin.setEnabled(not running)
        self._update_queue_controls()
