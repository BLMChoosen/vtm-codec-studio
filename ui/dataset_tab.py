"""
Dataset Tab
===========
UI panel for building VTM quadtree split-decision datasets from
trace files and variance-map CSVs.

Generates four CSV files (one per QT depth level 0-3) in the chosen
output folder.  Each row represents one CU split decision and carries:
  video; config; height; qp; blockVar; diffVar; prevSplit; decisionSplit
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from PySide6.QtCore import Qt, Slot
from PySide6.QtWidgets import (
    QCheckBox,
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

from core.dataset_builder import DatasetBuilderWorker, DatasetJob, scan_cfg_folder
from ui.widgets import FilePickerRow, LogPanel, ScrollSafeSpinBox
from utils.config import ConfigManager
from utils.validators import validate_directory


_QPS     = ['22', '27', '32', '37']
_CONFIGS = ['LD', 'RA']


class DatasetTab(QWidget):
    """Widget containing the full dataset-builder interface."""

    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self._config = ConfigManager()

        self._workers:           dict[DatasetBuilderWorker, DatasetJob] = {}
        self._worker_progress:   dict[DatasetBuilderWorker, int] = {}
        self._worker_logs:       dict[DatasetBuilderWorker, list[str]] = {}
        self._worker_job_index:  dict[DatasetBuilderWorker, int] = {}

        self._queue:                  list[DatasetJob] = []
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

        # ── Input Files ───────────────────────────────────────────────
        in_group = QGroupBox("Input Files")
        in_layout = QVBoxLayout(in_group)
        in_layout.setSpacing(10)

        self._cfgs_picker = FilePickerRow(
            "Sequence CFGs Folder:",
            placeholder="Folder with {video}.cfg files (SourceWidth, SourceHeight, InputBitDepth)",
            mode="directory",
        )
        self._cfgs_picker.path_changed.connect(self._on_cfgs_folder_changed)
        in_layout.addWidget(self._cfgs_picker)

        self._trace_picker = FilePickerRow(
            "Trace Files Folder:",
            placeholder="Folder containing {video}_{qp}_{config}.csv trace files",
            mode="directory",
        )
        in_layout.addWidget(self._trace_picker)

        self._variance_picker = FilePickerRow(
            "Variance Maps Folder:",
            placeholder="Folder containing {video}_{qp}_{config}-data.csv files",
            mode="directory",
        )
        in_layout.addWidget(self._variance_picker)
        fl.addWidget(in_group)

        # ── Output ────────────────────────────────────────────────────
        out_group = QGroupBox("Output")
        out_layout = QVBoxLayout(out_group)
        out_layout.setSpacing(10)

        self._output_picker = FilePickerRow(
            "Output Folder:",
            placeholder="Folder where dataset_depth_0..3.csv will be written",
            mode="directory",
        )
        out_layout.addWidget(self._output_picker)

        append_row = QHBoxLayout()
        append_lbl = QLabel("Write Mode:")
        append_lbl.setMinimumWidth(160)
        append_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        append_row.addWidget(append_lbl)

        self._append_cb = QCheckBox("Append to existing files  (uncheck = overwrite with header)")
        append_row.addWidget(self._append_cb)
        append_row.addStretch()
        out_layout.addLayout(append_row)
        fl.addWidget(out_group)

        # ── Experiment Settings ───────────────────────────────────────
        exp_group = QGroupBox("Experiment Settings")
        exp_outer = QHBoxLayout(exp_group)
        exp_outer.setSpacing(16)

        # Videos (left column)
        vid_col = QVBoxLayout()
        vid_col.setSpacing(6)

        vid_header = QHBoxLayout()
        vid_header.addWidget(QLabel("Videos:"))
        vid_header.addStretch()
        self._vid_count_lbl = QLabel("(select Sequence CFGs Folder)")
        self._vid_count_lbl.setStyleSheet("color: #8a90a4; font-size: 11px;")
        vid_header.addWidget(self._vid_count_lbl)
        vid_col.addLayout(vid_header)

        self._videos_list = QListWidget()
        self._videos_list.setMinimumHeight(160)
        self._videos_list.setMaximumHeight(220)
        vid_col.addWidget(self._videos_list)

        vid_btn_row = QHBoxLayout()
        all_btn = QPushButton("All")
        all_btn.clicked.connect(lambda: self._set_all_videos(True))
        vid_btn_row.addWidget(all_btn)
        none_btn = QPushButton("None")
        none_btn.clicked.connect(lambda: self._set_all_videos(False))
        vid_btn_row.addWidget(none_btn)
        vid_col.addLayout(vid_btn_row)
        exp_outer.addLayout(vid_col, stretch=2)

        # QPs / configs / frames (right column)
        right_col = QVBoxLayout()
        right_col.setSpacing(10)

        right_col.addWidget(QLabel("QPs:"))
        qp_row = QHBoxLayout()
        self._qp_cbs: dict[str, QCheckBox] = {}
        for qp in _QPS:
            cb = QCheckBox(qp)
            cb.setChecked(True)
            self._qp_cbs[qp] = cb
            qp_row.addWidget(cb)
        qp_row.addStretch()
        right_col.addLayout(qp_row)

        right_col.addWidget(QLabel("Configs:"))
        cfg_row = QHBoxLayout()
        self._cfg_cbs: dict[str, QCheckBox] = {}
        for cfg in _CONFIGS:
            cb = QCheckBox(cfg)
            cb.setChecked(True)
            self._cfg_cbs[cfg] = cb
            cfg_row.addWidget(cb)
        cfg_row.addStretch()
        right_col.addLayout(cfg_row)

        frames_row = QHBoxLayout()
        frames_lbl = QLabel("Frames:")
        frames_row.addWidget(frames_lbl)
        self._frames_spin = ScrollSafeSpinBox()
        self._frames_spin.setRange(2, 33)
        self._frames_spin.setValue(33)
        self._frames_spin.setToolTip("Total frames to process (frame 0 skipped). Max 33.")
        frames_row.addWidget(self._frames_spin)
        frames_row.addStretch()
        right_col.addLayout(frames_row)

        right_col.addStretch()
        exp_outer.addLayout(right_col, stretch=1)
        fl.addWidget(exp_group)

        # ── Queue ─────────────────────────────────────────────────────
        queue_group = QGroupBox("Dataset Queue")
        queue_layout = QVBoxLayout(queue_group)
        queue_layout.setSpacing(10)

        self._queue_list = QListWidget()
        self._queue_list.setMinimumHeight(100)
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

        note_lbl = QLabel("Jobs run sequentially — all experiments in one job write to the same four output CSVs.")
        note_lbl.setWordWrap(True)
        note_lbl.setStyleSheet("color: #8a90a4; font-size: 11px;")
        queue_layout.addWidget(note_lbl)

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
    # CFGs folder scan
    # ------------------------------------------------------------------

    @Slot(str)
    def _on_cfgs_folder_changed(self, folder: str) -> None:
        self._videos_list.clear()
        if not folder or not folder.strip():
            self._vid_count_lbl.setText("(select Sequence CFGs Folder)")
            return
        try:
            names = scan_cfg_folder(folder)
        except Exception:
            names = []

        for name in names:
            item = QListWidgetItem(name)
            item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            item.setCheckState(Qt.CheckState.Checked)
            self._videos_list.addItem(item)

        if names:
            self._vid_count_lbl.setText(f"{len(names)} CFG(s) found")
        else:
            self._vid_count_lbl.setText("No .cfg files found")

    # ------------------------------------------------------------------
    # Videos toggle helpers
    # ------------------------------------------------------------------

    @Slot()
    def _set_all_videos(self, checked: bool) -> None:
        state = Qt.CheckState.Checked if checked else Qt.CheckState.Unchecked
        for i in range(self._videos_list.count()):
            self._videos_list.item(i).setCheckState(state)

    def _get_selected_videos(self) -> list[str]:
        return [
            self._videos_list.item(i).text()
            for i in range(self._videos_list.count())
            if self._videos_list.item(i).checkState() == Qt.CheckState.Checked
        ]

    def _get_selected_qps(self) -> list[str]:
        return [qp for qp, cb in self._qp_cbs.items() if cb.isChecked()]

    def _get_selected_configs(self) -> list[str]:
        return [cfg for cfg, cb in self._cfg_cbs.items() if cb.isChecked()]

    # ------------------------------------------------------------------
    # Validation / job building
    # ------------------------------------------------------------------

    def _validate_form(self) -> tuple[bool, str]:
        checks = [
            validate_directory(self._cfgs_picker.path(),     "Sequence CFGs Folder"),
            validate_directory(self._trace_picker.path(),    "Trace Files Folder"),
            validate_directory(self._variance_picker.path(), "Variance Maps Folder"),
        ]
        for ok, msg in checks:
            if not ok:
                return False, msg

        out = self._output_picker.path()
        if not out or not out.strip():
            return False, "Output Folder path is empty."

        if not self._get_selected_videos():
            return False, "Select at least one video."
        if not self._get_selected_qps():
            return False, "Select at least one QP."
        if not self._get_selected_configs():
            return False, "Select at least one config (LD / RA)."

        return True, ""

    def _build_job_from_form(self) -> Optional[DatasetJob]:
        ok, msg = self._validate_form()
        if not ok:
            QMessageBox.warning(self, "Validation Error", msg)
            return None
        return DatasetJob(
            trace_files_path=   self._trace_picker.path(),
            variance_maps_path= self._variance_picker.path(),
            sequence_cfgs_path= self._cfgs_picker.path(),
            output_path=        self._output_picker.path(),
            videos=             self._get_selected_videos(),
            qps=                self._get_selected_qps(),
            configs=            self._get_selected_configs(),
            frames=             self._frames_spin.value(),
            append_mode=        self._append_cb.isChecked(),
        )

    def _sync_form_to_job(self, job: DatasetJob) -> None:
        self._cfgs_picker.set_path(job.sequence_cfgs_path)
        self._trace_picker.set_path(job.trace_files_path)
        self._variance_picker.set_path(job.variance_maps_path)
        self._output_picker.set_path(job.output_path)

    # ------------------------------------------------------------------
    # Queue helpers
    # ------------------------------------------------------------------

    def _job_summary(self, job: DatasetJob) -> str:
        n_v   = len(job.videos)
        cfgs  = '+'.join(job.configs)
        qps   = ','.join(job.qps)
        out   = Path(job.output_path).name
        mode  = "append" if job.append_mode else "overwrite"
        return f"{n_v} video(s) | {cfgs} | QP {qps} | → {out} [{mode}]"

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

    def _update_status(self) -> None:
        if not self._queue_running:
            return
        active = len(self._workers)
        if self._queue_cancel_requested:
            self._log.set_status(
                f"Cancelling… active: {active}, done: {self._queue_completed}/{self._queue_total}.",
                "#ffc857",
            )
        else:
            self._log.set_status(
                f"Running job {self._queue_completed + active}/{self._queue_total}…",
                "#ffc857",
            )

    def _update_progress(self) -> None:
        if self._queue_running and self._queue_total > 0:
            active_sum = sum(self._worker_progress.values())
            total_prog = self._queue_completed * 100 + active_sum
            self._log.set_progress(min(int(total_prog / self._queue_total), 100))
        elif self._worker_progress:
            self._log.set_progress(max(self._worker_progress.values()))

    def _cleanup_worker(self, worker: DatasetBuilderWorker) -> None:
        self._workers.pop(worker, None)
        self._worker_progress.pop(worker, None)
        self._worker_logs.pop(worker, None)
        self._worker_job_index.pop(worker, None)

    def _launch_more(self) -> None:
        if not self._queue_running:
            return
        while (
            not self._queue_cancel_requested
            and len(self._workers) < 1          # always sequential
            and self._queue_next_index < self._queue_total
        ):
            qi  = self._queue_next_index
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
        self._log.set_status(f"Queued {len(self._queue)} dataset job(s).", "#8a90a4")

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
            self, "Clear Queue", "Remove all jobs from the dataset queue?",
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

        self._log.clear()
        self._log.set_progress(0)
        self._log.set_status(
            f"Starting queue ({self._queue_total} job(s))…",
            "#ffc857",
        )
        self._set_running(True)
        self._launch_more()

    def _start_worker(self, job: DatasetJob, qi: int) -> bool:
        worker = DatasetBuilderWorker(job)
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
            msg   = (f"Queue cancelled ({self._queue_completed}/{self._queue_total} jobs). "
                     f"Success: {ok_count}, failed: {fail_count}.")
            color = "#ffc857"
        else:
            msg   = f"Queue finished. Success: {ok_count}, failed: {fail_count}."
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
        self._log.append("\nQueue cancellation requested…")
        for w in list(self._workers.keys()):
            w.cancel()
        self._update_status()

    # ------------------------------------------------------------------
    # Worker signal handlers
    # ------------------------------------------------------------------

    def _on_log_line(self, worker: DatasetBuilderWorker, line: str) -> None:
        qi  = self._worker_job_index.get(worker, -1)
        out = f"[Job {qi + 1:02d}] {line}" if (self._queue_running and qi >= 0) else line
        self._log.append(out)
        if worker in self._worker_logs:
            self._worker_logs[worker].append(line)

    def _on_progress(self, worker: DatasetBuilderWorker, val: int) -> None:
        if worker in self._worker_progress:
            self._worker_progress[worker] = max(0, min(val, 100))
        self._update_progress()

    def _on_finished(self, worker: DatasetBuilderWorker, success: bool, message: str) -> None:
        qi = self._worker_job_index.get(worker, -1)
        self._cleanup_worker(worker)

        if self._queue_running:
            self._queue_results.append(success)
            self._queue_completed += 1
            prefix = "" if success else ("" if self._queue_cancel_requested else "")
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
            f"{'Done' if success else 'Failed'} — {message}",
            "#4cda8a" if success else "#ff6b7a",
        )

    # ------------------------------------------------------------------

    def _set_running(self, running: bool) -> None:
        self._add_queue_btn.setEnabled(not running)
        self._cancel_btn.setEnabled(running)
        self._update_queue_controls()
