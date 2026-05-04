"""
Decoder Tab
===========
UI panel for VTM bitstream decoding.
Provides queue-based decoding (single file or folder batch)
with real-time log output.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from PySide6.QtCore import Qt, Slot
from PySide6.QtWidgets import (
    QFileDialog,
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

from core.decoder import DecoderWorker
from ui.widgets import FilePickerRow, LogPanel, ScrollSafeSpinBox
from utils.csv_export import write_metrics_csv
from utils.config import ConfigManager
from utils.parser import parse_vtm_log
from utils.preview import launch_yuview
from utils.validators import (
    validate_directory,
    validate_extension,
    validate_file_exists,
    validate_output_path,
)


@dataclass
class DecodeJob:
    """Immutable payload for one queued decode."""

    input_bin: str
    output_yuv: str
    output_csv: str


class DecoderTab(QWidget):
    """Widget containing the full decoder interface."""

    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self._config = ConfigManager()
        self._worker: Optional[DecoderWorker] = None
        self._active_job: Optional[DecodeJob] = None
        self._workers: dict[DecoderWorker, DecodeJob] = {}
        self._worker_progress: dict[DecoderWorker, int] = {}
        self._worker_logs: dict[DecoderWorker, list[str]] = {}
        self._worker_job_index: dict[DecoderWorker, int] = {}

        self._queue: list[DecodeJob] = []
        self._queue_running = False
        self._queue_cancel_requested = False
        self._queue_next_index = 0
        self._queue_completed = 0
        self._queue_total = 0
        self._queue_results: list[bool] = []

        self._log_history: list[str] = []

        self._build_ui()

    # ------------------------------------------------------------------
    # UI Construction
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
        form.setMinimumWidth(600)
        form_layout = QVBoxLayout(form)
        form_layout.setContentsMargins(4, 4, 4, 4)
        form_layout.setSpacing(14)

        in_group = QGroupBox("Input Bitstream")
        in_layout = QVBoxLayout(in_group)
        in_layout.setSpacing(10)

        self._input_picker = FilePickerRow(
            "Input .bin:",
            file_filter="VVC Bitstream (*.bin);;All Files (*)",
            placeholder="Drag & drop or browse for .bin file",
        )
        in_layout.addWidget(self._input_picker)
        form_layout.addWidget(in_group)

        folder_group = QGroupBox("Batch Decode (Folder)")
        folder_layout = QVBoxLayout(folder_group)
        folder_layout.setSpacing(10)

        self._input_dir_picker = FilePickerRow(
            "Input Folder:",
            placeholder="Choose folder with .bin files",
            mode="directory",
        )
        folder_layout.addWidget(self._input_dir_picker)

        self._output_dir_picker = FilePickerRow(
            "Output Folder:",
            placeholder="Choose folder for decoded .yuv files",
            mode="directory",
        )
        folder_layout.addWidget(self._output_dir_picker)

        folder_btn_row = QHBoxLayout()
        folder_btn_row.addStretch()

        self._add_files_btn = QPushButton("+ Add Multiple .bin Files…")
        self._add_files_btn.setToolTip(
            "Pick several .bin files at once. Each one is queued with auto-generated .yuv and .csv outputs in the chosen output folder."
        )
        self._add_files_btn.clicked.connect(self._add_files_to_queue)
        folder_btn_row.addWidget(self._add_files_btn)

        self._add_folder_btn = QPushButton("+ Add Folder .bin Files")
        self._add_folder_btn.clicked.connect(self._add_folder_to_queue)
        folder_btn_row.addWidget(self._add_folder_btn)
        folder_layout.addLayout(folder_btn_row)

        form_layout.addWidget(folder_group)

        out_group = QGroupBox("Reconstructed Output")
        out_layout = QVBoxLayout(out_group)
        out_layout.setSpacing(10)

        self._output_picker = FilePickerRow(
            "Output .yuv:",
            file_filter="YUV Files (*.yuv);;All Files (*)",
            placeholder="Output reconstructed YUV path",
            mode="save",
        )
        out_layout.addWidget(self._output_picker)

        self._output_csv_picker = FilePickerRow(
            "Output .csv:",
            file_filter="CSV Files (*.csv);;All Files (*)",
            placeholder="Output report with metrics (CSV)",
            mode="save",
        )
        out_layout.addWidget(self._output_csv_picker)

        preview_out_row = QHBoxLayout()
        preview_out_row.addStretch()
        self._preview_output_btn = QPushButton("Preview YUV in YUView")
        self._preview_output_btn.clicked.connect(self._preview_output)
        preview_out_row.addWidget(self._preview_output_btn)
        out_layout.addLayout(preview_out_row)

        form_layout.addWidget(out_group)

        queue_group = QGroupBox("Decode Queue")
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
        self._parallel_spin.setToolTip("Maximum number of queued decode jobs running at the same time.")
        try:
            self._parallel_spin.setValue(max(1, int(self._config.get("decoder_parallel_jobs", 2))))
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
        self._start_queue_btn.clicked.connect(self._start_queue)
        btn_row.addWidget(self._start_queue_btn)

        self._cancel_btn = QPushButton("■  Cancel")
        self._cancel_btn.setObjectName("dangerButton")
        self._cancel_btn.setEnabled(False)
        self._cancel_btn.clicked.connect(self._cancel_decoding)
        btn_row.addWidget(self._cancel_btn)

        btn_row.addStretch()
        form_layout.addLayout(btn_row)

        self._log = LogPanel()
        self._log.setMinimumHeight(220)
        form_layout.addWidget(self._log)

        scroll.setWidget(form)

        # Entire decoder page (including log) is now scrollable
        root.addWidget(scroll, stretch=1)


        self._update_queue_controls()

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate_form(self) -> tuple[bool, str]:
        checks = [
            validate_file_exists(self._input_picker.path(), "Input .bin"),
            validate_extension(self._input_picker.path(), ".bin", "Input .bin"),
            validate_output_path(self._output_picker.path(), ".yuv", "Output .yuv"),
            validate_output_path(self._output_csv_picker.path(), ".csv", "Output CSV"),
        ]

        decoder_exe = self._config.decoder_path()
        if not decoder_exe:
            return False, "Decoder executable path is not set.\nGo to Settings to configure it."

        checks.append(validate_file_exists(decoder_exe, "Decoder executable"))

        for ok, msg in checks:
            if not ok:
                return False, msg
        return True, ""

    def _validate_job(self, job: DecodeJob) -> tuple[bool, str]:
        checks = [
            validate_file_exists(job.input_bin, "Input .bin"),
            validate_extension(job.input_bin, ".bin", "Input .bin"),
            validate_output_path(job.output_yuv, ".yuv", "Output .yuv"),
            validate_output_path(job.output_csv, ".csv", "Output CSV"),
        ]

        decoder_exe = self._config.decoder_path()
        if not decoder_exe:
            return False, "Decoder executable path is not set.\nGo to Settings to configure it."

        checks.append(validate_file_exists(decoder_exe, "Decoder executable"))

        for ok, msg in checks:
            if not ok:
                return False, msg
        return True, ""

    def _build_job_from_form(self) -> Optional[DecodeJob]:
        ok, msg = self._validate_form()
        if not ok:
            QMessageBox.warning(self, "Validation Error", msg)
            return None

        return DecodeJob(
            input_bin=self._input_picker.path(),
            output_yuv=self._output_picker.path(),
            output_csv=self._output_csv_picker.path(),
        )

    def _sync_form_to_job(self, job: DecodeJob) -> None:
        self._input_picker.set_path(job.input_bin)
        self._output_picker.set_path(job.output_yuv)
        self._output_csv_picker.set_path(job.output_csv)

    def _build_jobs_from_folder(self) -> tuple[list[DecodeJob], str]:
        checks = [
            validate_directory(self._input_dir_picker.path(), "Input folder"),
            validate_directory(self._output_dir_picker.path(), "Output folder"),
        ]

        for ok, msg in checks:
            if not ok:
                return [], msg

        input_dir = Path(self._input_dir_picker.path())
        output_dir = Path(self._output_dir_picker.path())
        bin_files = sorted(
            candidate
            for candidate in input_dir.iterdir()
            if candidate.is_file() and candidate.suffix.lower() == ".bin"
        )

        if not bin_files:
            return [], f"No .bin files found in folder:\n{input_dir}"

        return self._build_jobs_for_bin_paths(bin_files, output_dir), ""

    def _build_jobs_for_bin_paths(
        self,
        bin_files: list[Path],
        output_dir: Path,
    ) -> list[DecodeJob]:
        existing_outputs = {Path(job.output_yuv).resolve().as_posix().casefold() for job in self._queue}
        used_stems: dict[str, int] = {}
        jobs: list[DecodeJob] = []

        for bin_file in bin_files:
            stem = bin_file.stem
            yuv_path = output_dir / f"{stem}.yuv"
            csv_path = output_dir / f"{stem}.csv"
            counter = used_stems.get(stem, 0)

            while (
                yuv_path.resolve().as_posix().casefold() in existing_outputs
                or any(
                    Path(j.output_yuv).resolve().as_posix().casefold()
                    == yuv_path.resolve().as_posix().casefold()
                    for j in jobs
                )
            ):
                counter += 1
                yuv_path = output_dir / f"{stem}_{counter}.yuv"
                csv_path = output_dir / f"{stem}_{counter}.csv"

            used_stems[stem] = counter
            jobs.append(
                DecodeJob(
                    input_bin=str(bin_file),
                    output_yuv=str(yuv_path),
                    output_csv=str(csv_path),
                )
            )
        return jobs

    # ------------------------------------------------------------------
    # Queue helpers
    # ------------------------------------------------------------------

    def _job_summary(self, job: DecodeJob) -> str:
        input_name = Path(job.input_bin).name
        yuv_name = Path(job.output_yuv).name
        csv_name = Path(job.output_csv).name
        return f"Input: {input_name} | Output: {yuv_name} | CSV: {csv_name}"

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

    def _is_busy(self) -> bool:
        return self._queue_running or bool(self._workers)

    def _max_parallel_jobs(self) -> int:
        return max(1, self._parallel_spin.value())

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

    def _cleanup_worker_state(self, worker: DecoderWorker) -> None:
        self._workers.pop(worker, None)
        self._worker_progress.pop(worker, None)
        self._worker_logs.pop(worker, None)
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

    # ------------------------------------------------------------------
    # Decoding lifecycle
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
        self._log.set_status(f"Queued {len(self._queue)} decode job(s).", "#8a90a4")

    @Slot()
    def _add_folder_to_queue(self) -> None:
        if self._is_busy():
            return

        jobs, msg = self._build_jobs_from_folder()
        if not jobs:
            QMessageBox.warning(self, "Validation Error", msg)
            return

        self._queue.extend(jobs)
        self._refresh_queue_view()
        self._log.set_status(
            f"Queued {len(jobs)} folder job(s). Total in queue: {len(self._queue)}.",
            "#8a90a4",
        )

    @Slot()
    def _add_files_to_queue(self) -> None:
        if self._is_busy():
            return

        start_dir = self._input_dir_picker.path() or ""
        if not start_dir and self._input_picker.path():
            start_dir = str(Path(self._input_picker.path()).parent)

        paths, _ = QFileDialog.getOpenFileNames(
            self,
            "Select .bin files",
            start_dir,
            "VVC Bitstream (*.bin);;All Files (*)",
        )
        if not paths:
            return

        output_dir_text = self._output_dir_picker.path()
        if not output_dir_text:
            output_dir_text = QFileDialog.getExistingDirectory(
                self,
                "Select output folder for decoded .yuv files",
                start_dir,
            )
            if not output_dir_text:
                return
            self._output_dir_picker.set_path(output_dir_text)

        ok, msg = validate_directory(output_dir_text, "Output folder")
        if not ok:
            QMessageBox.warning(self, "Validation Error", msg)
            return

        bin_files: list[Path] = []
        for path in paths:
            candidate = Path(path)
            if not candidate.is_file():
                QMessageBox.warning(self, "Validation Error", f"File not found:\n{candidate}")
                return
            if candidate.suffix.lower() != ".bin":
                QMessageBox.warning(
                    self,
                    "Validation Error",
                    f"Only .bin files are accepted:\n{candidate}",
                )
                return
            bin_files.append(candidate)

        jobs = self._build_jobs_for_bin_paths(bin_files, Path(output_dir_text))
        if not jobs:
            return

        self._queue.extend(jobs)
        self._refresh_queue_view()
        self._log.set_status(
            f"Queued {len(jobs)} .bin file(s). Total in queue: {len(self._queue)}.",
            "#8a90a4",
        )

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
            "Remove all jobs from the decode queue?",
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

        self._queue_running = True
        self._queue_cancel_requested = False
        self._queue_next_index = 0
        self._queue_completed = 0
        self._queue_total = len(self._queue)
        self._queue_results = []
        self._workers.clear()
        self._worker_progress.clear()
        self._worker_logs.clear()
        self._worker_job_index.clear()
        self._worker = None
        self._active_job = None
        self._config.set("decoder_parallel_jobs", self._parallel_spin.value())

        self._log.clear()
        self._log_history.clear()
        self._log.set_progress(0)
        self._log.set_status(
            f"Starting queue ({self._queue_total} jobs, max {self._max_parallel_jobs()} parallel)…",
            "#ffc857",
        )
        self._set_running(True)
        self._launch_more_queue_jobs()

    def _start_worker_for_job(self, job: DecodeJob, queue_index: int) -> bool:
        ok, msg = self._validate_job(job)
        if not ok:
            self._log.append(f"❌ Validation failed for job {queue_index + 1}/{self._queue_total}:\n{msg}")
            return False

        worker = DecoderWorker(
            decoder_exe=self._config.decoder_path(),
            input_bin=job.input_bin,
            output_yuv=job.output_yuv,
        )

        self._workers[worker] = job
        self._worker_progress[worker] = 0
        self._worker_logs[worker] = []
        self._worker_job_index[worker] = queue_index

        worker.signals.log_line.connect(lambda line, w=worker: self._handle_log_line(w, line))
        worker.signals.progress.connect(lambda value, w=worker: self._handle_worker_progress(w, value))
        worker.signals.started.connect(lambda w=worker: self._on_started(w))
        worker.signals.finished.connect(lambda success, message, w=worker: self._on_finished(w, success, message))
        worker.start()

        self._worker = next(iter(self._workers), None)
        self._active_job = self._workers.get(self._worker) if self._worker else None

        self._config.add_recent_file("input", job.input_bin)
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
        self._worker_logs.clear()
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
    def _cancel_decoding(self) -> None:
        if not self._is_busy():
            return

        self._queue_cancel_requested = True
        self._log.append("\n⛔ Queue cancellation requested. Stopping active jobs…")
        for worker in list(self._workers.keys()):
            worker.cancel()
        self._update_running_status()

    def _on_started(self, worker: DecoderWorker) -> None:
        if worker not in self._workers:
            return
        self._update_running_status()

    def _handle_log_line(self, worker: DecoderWorker, line: str) -> None:
        queue_index = self._worker_job_index.get(worker, -1)
        if self._queue_running and queue_index >= 0:
            output_line = f"[Job {queue_index + 1:02d}] {line}"
        else:
            output_line = line

        self._log.append(output_line)
        self._log_history.append(output_line)

        if worker in self._worker_logs:
            self._worker_logs[worker].append(line)

    def _handle_worker_progress(self, worker: DecoderWorker, value: int) -> None:
        if worker in self._worker_progress:
            self._worker_progress[worker] = max(0, min(value, 100))
        self._update_overall_progress()

    def _on_finished(self, worker: DecoderWorker, success: bool, message: str) -> None:
        job = self._workers.get(worker)
        queue_index = self._worker_job_index.get(worker, -1)
        job_logs = self._worker_logs.get(worker, [])

        if success and job is not None:
            metrics = parse_vtm_log("\n".join(job_logs), job.output_yuv)
            try:
                write_metrics_csv(job.output_csv, metrics)
                self._log.append(f"📝 Metrics CSV saved: {job.output_csv}")
            except OSError as exc:
                self._log.append(f"❌ Failed to write CSV report: {exc}")

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
        else:
            self._log.set_status(f"❌ {message}", "#ff6b7a")

    def _set_running(self, running: bool) -> None:
        self._add_queue_btn.setEnabled(not running)
        self._add_folder_btn.setEnabled(not running)
        self._add_files_btn.setEnabled(not running)
        self._cancel_btn.setEnabled(running)
        self._preview_output_btn.setEnabled(not running)
        self._parallel_spin.setEnabled(not running)
        self._update_queue_controls()

    # ------------------------------------------------------------------
    # YUView preview
    # ------------------------------------------------------------------

    @Slot()
    def _preview_output(self) -> None:
        self._open_preview(self._output_picker.path())

    def _open_preview(self, media_path: str) -> None:
        ok, message = launch_yuview(self._config.yuview_path(), media_path)
        if ok:
            self._log.set_status(f"👁 {message}", "#4cda8a")
            return
        QMessageBox.warning(self, "YUView Preview", message)
