"""
Comparison Tab
==============
Benchmarks two encoder executables (baseline = optimization OFF,
optimized = optimization ON) on the same experiments and averages the
per-stage Time Profile across repetitions.

For every  video × executable × config (LD/RA) × QP × repetition  it runs
the encoder, saves the full stdout as a .report, parses the appended
Time Profile + Total Time, and writes per-QP averages plus a consolidated
baseline-vs-optimized summary.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Optional

from PySide6.QtCore import Qt, Slot
from PySide6.QtWidgets import (
    QCheckBox,
    QFileDialog,
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

from core.comparison import (
    CONFIG_INFO,
    EXE_LABELS,
    ComparisonConfig,
    ComparisonOrchestrator,
    ComparisonVideo,
)
from ui.widgets import FilePickerRow, LogPanel, ScrollSafeSpinBox
from utils.config import ConfigManager
from utils.validators import validate_directory, validate_file_exists


_DEFAULT_QPS = "22, 27, 32, 37"


def _sanitize_name(stem: str) -> str:
    cleaned = stem.strip().replace(" ", "_")
    cleaned = re.sub(r'[<>:"/\\|?*\x00-\x1f]+', "", cleaned)
    return cleaned.strip("._-") or stem.strip()


class ComparisonTab(QWidget):
    """Widget hosting the baseline-vs-optimized comparison UI."""

    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self._config = ConfigManager()
        self._orchestrator: Optional[ComparisonOrchestrator] = None
        self._videos: list[ComparisonVideo] = []
        self._build_ui()
        self._restore_state()

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

        body = QWidget()
        body.setMinimumWidth(640)
        body_layout = QVBoxLayout(body)
        body_layout.setContentsMargins(4, 4, 4, 4)
        body_layout.setSpacing(14)

        body_layout.addWidget(self._build_intro_box())
        body_layout.addWidget(self._build_inputs_group())
        body_layout.addWidget(self._build_output_group())
        body_layout.addWidget(self._build_executables_group())
        body_layout.addWidget(self._build_encoder_group())
        body_layout.addWidget(self._build_parallel_group())
        body_layout.addLayout(self._build_action_row())

        self._log = LogPanel()
        self._log.setMinimumHeight(260)
        self._log.set_status("Ready", "#8a90a4")
        body_layout.addWidget(self._log)

        scroll.setWidget(body)
        root.addWidget(scroll, stretch=1)

        self._refresh_inputs_view()
        self._update_buttons()

    def _build_intro_box(self) -> QGroupBox:
        group = QGroupBox("Comparison Overview")
        layout = QVBoxLayout(group)
        info = QLabel(
            "Runs every  video × executable × config × QP × repetition  with both "
            "encoders, then averages the per-stage Time Profile.\n"
            "Layout:  <root>/<video>/<baseline|optimized>/<Low_Delay|Random_Access>/"
            "QP<qp>/{rep_N, Average}.\n"
            "Each QP folder gets an Average/average.csv; a comparison_summary.csv is "
            "written at the output root."
        )
        info.setWordWrap(True)
        info.setStyleSheet("color: #b8bdcb;")
        layout.addWidget(info)
        return group

    def _build_inputs_group(self) -> QGroupBox:
        group = QGroupBox("Input Videos (.yuv) + per-sequence .cfg")
        layout = QVBoxLayout(group)
        layout.setSpacing(8)

        self._inputs_list = QListWidget()
        self._inputs_list.setMinimumHeight(140)
        self._inputs_list.setSelectionMode(QListWidget.SelectionMode.SingleSelection)
        self._inputs_list.currentRowChanged.connect(lambda _: self._update_buttons())
        layout.addWidget(self._inputs_list)

        btn_row = QHBoxLayout()
        self._add_files_btn = QPushButton("+ Add YUV…")
        self._add_files_btn.setToolTip("Add one or more .yuv videos. You'll be asked for each video's per-sequence .cfg.")
        self._add_files_btn.clicked.connect(self._on_add_inputs)
        btn_row.addWidget(self._add_files_btn)

        self._set_cfg_btn = QPushButton("Set CFG for Selected…")
        self._set_cfg_btn.clicked.connect(self._on_set_cfg_for_selected)
        btn_row.addWidget(self._set_cfg_btn)

        self._remove_btn = QPushButton("Remove Selected")
        self._remove_btn.clicked.connect(self._on_remove_selected)
        btn_row.addWidget(self._remove_btn)

        self._clear_btn = QPushButton("Clear All")
        self._clear_btn.setObjectName("dangerButton")
        self._clear_btn.clicked.connect(self._on_clear_inputs)
        btn_row.addWidget(self._clear_btn)

        btn_row.addStretch()
        layout.addLayout(btn_row)

        hint = QLabel(
            "Each .yuv needs a per-sequence .cfg (resolution / bit depth). A sibling "
            "<video>.cfg is picked up automatically when present."
        )
        hint.setWordWrap(True)
        hint.setStyleSheet("color: #8a90a4; font-size: 12px;")
        layout.addWidget(hint)
        return group

    def _build_output_group(self) -> QGroupBox:
        group = QGroupBox("Root Output Folder")
        layout = QVBoxLayout(group)
        self._output_picker = FilePickerRow(
            "Output root:",
            placeholder="Folder where <video>/<baseline|optimized>/… trees are created",
            mode="directory",
        )
        layout.addWidget(self._output_picker)
        return group

    def _build_executables_group(self) -> QGroupBox:
        group = QGroupBox("Encoder Executables")
        layout = QVBoxLayout(group)
        layout.setSpacing(10)

        exe_filter = "Executables (*.exe);;All Files (*)" if os.name == "nt" else "All Files (*)"

        self._baseline_picker = FilePickerRow(
            "Baseline (opt OFF):",
            file_filter=exe_filter,
            placeholder="EncoderApp WITHOUT the optimization",
        )
        layout.addWidget(self._baseline_picker)

        self._optimized_picker = FilePickerRow(
            "Optimized (opt ON):",
            file_filter=exe_filter,
            placeholder="EncoderApp WITH the optimization",
        )
        layout.addWidget(self._optimized_picker)

        hint = QLabel(
            "Both encoders must emit the appended “Time Profile” block in stdout. "
            "Main RA/LD configs are taken from the cfg folder set under Settings."
        )
        hint.setWordWrap(True)
        hint.setStyleSheet("color: #8a90a4; font-size: 12px;")
        layout.addWidget(hint)
        return group

    def _build_encoder_group(self) -> QGroupBox:
        group = QGroupBox("Experiment Settings")
        layout = QVBoxLayout(group)
        layout.setSpacing(8)

        # Configs
        cfg_row = QHBoxLayout()
        cfg_lbl = QLabel("Configs:")
        cfg_lbl.setMinimumWidth(160)
        cfg_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        cfg_row.addWidget(cfg_lbl)
        self._cfg_ld_check = QCheckBox("Low Delay (LD)")
        self._cfg_ld_check.setChecked(True)
        cfg_row.addWidget(self._cfg_ld_check)
        self._cfg_ra_check = QCheckBox("Random Access (RA)")
        self._cfg_ra_check.setChecked(True)
        cfg_row.addWidget(self._cfg_ra_check)
        cfg_row.addStretch()
        layout.addLayout(cfg_row)

        # QPs
        qp_row = QHBoxLayout()
        qp_lbl = QLabel("QPs (-q):")
        qp_lbl.setMinimumWidth(160)
        qp_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        qp_row.addWidget(qp_lbl)
        self._qps_edit = QLineEdit(_DEFAULT_QPS)
        self._qps_edit.setPlaceholderText("Comma-separated, e.g. 22, 27, 32, 37 (each in 0–63)")
        qp_row.addWidget(self._qps_edit, stretch=1)
        layout.addLayout(qp_row)

        # Frames (single global value)
        frames_row = QHBoxLayout()
        frames_lbl = QLabel("Frames (-f):")
        frames_lbl.setMinimumWidth(160)
        frames_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        frames_row.addWidget(frames_lbl)
        self._frames_spin = ScrollSafeSpinBox()
        self._frames_spin.setRange(1, 1_000_000)
        self._frames_spin.setValue(33)
        self._frames_spin.setToolTip("Number of frames (-f) encoded for every video.")
        frames_row.addWidget(self._frames_spin)
        frames_row.addStretch()
        layout.addLayout(frames_row)

        # Repetitions
        reps_row = QHBoxLayout()
        reps_lbl = QLabel("Repetitions:")
        reps_lbl.setMinimumWidth(160)
        reps_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        reps_row.addWidget(reps_lbl)
        self._reps_spin = ScrollSafeSpinBox()
        self._reps_spin.setRange(1, 1000)
        self._reps_spin.setValue(3)
        self._reps_spin.setToolTip("How many times each experiment is repeated. The Time Profile is averaged across repetitions.")
        reps_row.addWidget(self._reps_spin)
        reps_row.addStretch()
        layout.addLayout(reps_row)

        return group

    def _build_parallel_group(self) -> QGroupBox:
        group = QGroupBox("Parallel Execution")
        layout = QVBoxLayout(group)
        layout.setSpacing(8)

        row = QHBoxLayout()
        lbl = QLabel("Parallel Jobs:")
        lbl.setMinimumWidth(160)
        lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        row.addWidget(lbl)
        self._parallel_spin = ScrollSafeSpinBox()
        self._parallel_spin.setRange(1, 32)
        self._parallel_spin.setValue(1)
        self._parallel_spin.setToolTip("Number of encodes running at the same time.")
        row.addWidget(self._parallel_spin)
        row.addStretch()
        layout.addLayout(row)

        warn = QLabel(
            "⚠ Keep this at 1 for trustworthy timing. Running encodes in parallel "
            "skews the measured times because of CPU contention."
        )
        warn.setWordWrap(True)
        warn.setStyleSheet("color: #ffc857; font-size: 11px;")
        layout.addWidget(warn)
        return group

    def _build_action_row(self) -> QHBoxLayout:
        row = QHBoxLayout()
        row.addStretch()
        self._start_btn = QPushButton("▶  Start Comparison")
        self._start_btn.setObjectName("primaryButton")
        self._start_btn.setMinimumWidth(190)
        self._start_btn.setMinimumHeight(42)
        self._start_btn.clicked.connect(self._on_start)
        row.addWidget(self._start_btn)

        self._cancel_btn = QPushButton("■  Cancel")
        self._cancel_btn.setObjectName("dangerButton")
        self._cancel_btn.setMinimumWidth(120)
        self._cancel_btn.setMinimumHeight(42)
        self._cancel_btn.setEnabled(False)
        self._cancel_btn.clicked.connect(self._on_cancel)
        row.addWidget(self._cancel_btn)
        row.addStretch()
        return row

    # ------------------------------------------------------------------
    # State persistence
    # ------------------------------------------------------------------

    def _restore_state(self) -> None:
        cfg = self._config
        self._output_picker.set_path(cfg.get("comparison_output_root", ""))
        self._baseline_picker.set_path(cfg.get("comparison_baseline_exe", ""))
        self._optimized_picker.set_path(cfg.get("comparison_optimized_exe", ""))
        self._qps_edit.setText(cfg.get("comparison_qps", _DEFAULT_QPS))
        self._cfg_ld_check.setChecked(bool(cfg.get("comparison_cfg_ld", True)))
        self._cfg_ra_check.setChecked(bool(cfg.get("comparison_cfg_ra", True)))
        try:
            self._frames_spin.setValue(max(1, int(cfg.get("comparison_frames", 33))))
        except (TypeError, ValueError):
            self._frames_spin.setValue(33)
        try:
            self._reps_spin.setValue(max(1, int(cfg.get("comparison_reps", 3))))
        except (TypeError, ValueError):
            self._reps_spin.setValue(3)
        try:
            self._parallel_spin.setValue(max(1, int(cfg.get("comparison_parallel_jobs", 1))))
        except (TypeError, ValueError):
            self._parallel_spin.setValue(1)

    def _save_state(self) -> None:
        self._config.update({
            "comparison_output_root":   self._output_picker.path(),
            "comparison_baseline_exe":  self._baseline_picker.path(),
            "comparison_optimized_exe": self._optimized_picker.path(),
            "comparison_qps":           self._qps_edit.text(),
            "comparison_cfg_ld":        self._cfg_ld_check.isChecked(),
            "comparison_cfg_ra":        self._cfg_ra_check.isChecked(),
            "comparison_frames":        self._frames_spin.value(),
            "comparison_reps":          self._reps_spin.value(),
            "comparison_parallel_jobs": self._parallel_spin.value(),
        })

    # ------------------------------------------------------------------
    # Inputs management
    # ------------------------------------------------------------------

    def _input_label(self, item: ComparisonVideo) -> str:
        cfg_part = f"   ↦ cfg: {item.sequence_cfg}" if item.sequence_cfg else "   ↦ cfg: (needs cfg)"
        return f"[YUV]  {item.yuv}{cfg_part}"

    def _refresh_inputs_view(self) -> None:
        previous = self._inputs_list.currentRow()
        self._inputs_list.clear()
        for item in self._videos:
            self._inputs_list.addItem(QListWidgetItem(self._input_label(item)))
        if self._inputs_list.count() > 0:
            row = previous if 0 <= previous < self._inputs_list.count() else 0
            self._inputs_list.setCurrentRow(row)
        self._update_buttons()

    def _existing_paths(self) -> set[str]:
        return {Path(item.yuv).resolve().as_posix().casefold() for item in self._videos}

    def _add_input_path(self, path: str) -> None:
        path = path.strip()
        if not path:
            return
        if not path.lower().endswith(".yuv"):
            QMessageBox.warning(self, "Invalid Input", f"Only .yuv files are accepted:\n{path}")
            return
        if not Path(path).is_file():
            QMessageBox.warning(self, "File not found", f"File does not exist:\n{path}")
            return
        if Path(path).resolve().as_posix().casefold() in self._existing_paths():
            return

        guess = Path(path).with_suffix(".cfg")
        self._videos.append(ComparisonVideo(
            yuv=path,
            sequence_cfg=str(guess) if guess.is_file() else "",
            name=_sanitize_name(Path(path).stem),
        ))

    @Slot()
    def _on_add_inputs(self) -> None:
        if self._is_running():
            return
        last_dir = str(Path(self._videos[-1].yuv).parent) if self._videos else ""
        paths, _ = QFileDialog.getOpenFileNames(
            self, "Select .yuv files", last_dir, "YUV Files (*.yuv);;All Files (*)",
        )
        if not paths:
            return
        for path in paths:
            self._add_input_path(path)
        # Prompt cfg for any newly-added video still missing one.
        for item in self._videos:
            if not item.sequence_cfg:
                self._prompt_cfg_for_item(item)
        self._refresh_inputs_view()

    def _prompt_cfg_for_item(self, item: ComparisonVideo) -> None:
        start_dir = str(Path(item.yuv).parent) if item.yuv else ""
        path, _ = QFileDialog.getOpenFileName(
            self,
            f"Select per-sequence .cfg for {Path(item.yuv).name}",
            start_dir,
            "Config Files (*.cfg);;All Files (*)",
        )
        if path:
            item.sequence_cfg = path

    @Slot()
    def _on_set_cfg_for_selected(self) -> None:
        if self._is_running():
            return
        row = self._inputs_list.currentRow()
        if 0 <= row < len(self._videos):
            self._prompt_cfg_for_item(self._videos[row])
            self._refresh_inputs_view()

    @Slot()
    def _on_remove_selected(self) -> None:
        if self._is_running():
            return
        row = self._inputs_list.currentRow()
        if 0 <= row < len(self._videos):
            del self._videos[row]
            self._refresh_inputs_view()

    @Slot()
    def _on_clear_inputs(self) -> None:
        if self._is_running() or not self._videos:
            return
        confirm = QMessageBox.question(
            self, "Clear Inputs", "Remove all videos from the list?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if confirm == QMessageBox.StandardButton.Yes:
            self._videos.clear()
            self._refresh_inputs_view()

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _selected_configs(self) -> list[str]:
        configs: list[str] = []
        if self._cfg_ld_check.isChecked():
            configs.append("LD")
        if self._cfg_ra_check.isChecked():
            configs.append("RA")
        return configs

    def _parse_qps(self) -> tuple[list[int], str]:
        tokens = [t for t in re.split(r"[\s,;]+", self._qps_edit.text().strip()) if t]
        if not tokens:
            return [], "QPs list is empty."
        seen: set[int] = set()
        qps: list[int] = []
        for token in tokens:
            try:
                qp = int(token)
            except ValueError:
                return [], f"Invalid QP value: '{token}'"
            if not 0 <= qp <= 63:
                return [], f"QP must be between 0 and 63. Got: {qp}"
            if qp not in seen:
                seen.add(qp)
                qps.append(qp)
        return qps, ""

    def _validate(self) -> tuple[bool, str, Optional[ComparisonConfig]]:
        if not self._videos:
            return False, "Add at least one .yuv video.", None
        for item in self._videos:
            ok, msg = validate_file_exists(item.yuv, "Input YUV")
            if not ok:
                return False, msg, None
            if not item.sequence_cfg:
                return False, (
                    f"Video '{Path(item.yuv).name}' has no per-sequence .cfg — "
                    "set it first."
                ), None
            ok, msg = validate_file_exists(item.sequence_cfg, "Per-sequence .cfg")
            if not ok:
                return False, msg, None

        ok, msg = validate_directory(self._output_picker.path(), "Output root")
        if not ok:
            return False, msg, None

        baseline = self._baseline_picker.path()
        optimized = self._optimized_picker.path()
        for label, path in (("Baseline encoder", baseline), ("Optimized encoder", optimized)):
            if not path:
                return False, f"{label} executable is not set.", None
            ok, msg = validate_file_exists(path, label)
            if not ok:
                return False, msg, None

        configs = self._selected_configs()
        if not configs:
            return False, "Select at least one config (LD / RA).", None

        cfg_folder = self._config.cfg_folder()
        if not cfg_folder:
            return False, (
                "Encoder cfg folder is not set. Configure it under Settings — "
                "the comparison needs the main RA/LD config files."
            ), None
        for code in configs:
            main_cfg = Path(cfg_folder) / CONFIG_INFO[code]["cfg_file"]
            if not main_cfg.is_file():
                return False, f"Missing encoder config: {main_cfg}", None

        qps, err = self._parse_qps()
        if err:
            return False, err, None

        config = ComparisonConfig(
            videos=list(self._videos),
            output_root=self._output_picker.path(),
            baseline_exe=baseline,
            optimized_exe=optimized,
            cfg_folder=cfg_folder,
            qps=qps,
            configs=configs,
            frames=int(self._frames_spin.value()),
            repetitions=int(self._reps_spin.value()),
            parallel_jobs=max(1, int(self._parallel_spin.value())),
        )
        return True, "", config

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _is_running(self) -> bool:
        return self._orchestrator is not None and self._orchestrator.isRunning()

    def _update_buttons(self) -> None:
        running = self._is_running()
        self._start_btn.setEnabled(not running)
        self._cancel_btn.setEnabled(running)

        self._add_files_btn.setEnabled(not running)
        selected = 0 <= self._inputs_list.currentRow() < len(self._videos)
        self._set_cfg_btn.setEnabled(not running and selected)
        self._remove_btn.setEnabled(not running and selected)
        self._clear_btn.setEnabled(not running and bool(self._videos))

        self._output_picker.setEnabled(not running)
        self._baseline_picker.setEnabled(not running)
        self._optimized_picker.setEnabled(not running)
        self._cfg_ld_check.setEnabled(not running)
        self._cfg_ra_check.setEnabled(not running)
        self._qps_edit.setEnabled(not running)
        self._frames_spin.setEnabled(not running)
        self._reps_spin.setEnabled(not running)
        self._parallel_spin.setEnabled(not running)

    @Slot()
    def _on_start(self) -> None:
        if self._is_running():
            return
        ok, msg, cfg = self._validate()
        if not ok or cfg is None:
            QMessageBox.warning(self, "Validation Error", msg)
            return

        self._save_state()

        total = (
            len(cfg.videos) * len(EXE_LABELS) * len(cfg.configs)
            * len(cfg.qps) * cfg.repetitions
        )
        confirm = QMessageBox.question(
            self,
            "Start Comparison",
            (
                f"Run the comparison with this plan?\n\n"
                f"  Videos:        {len(cfg.videos)}\n"
                f"  Executables:   2 (baseline + optimized)\n"
                f"  Configs:       {', '.join(cfg.configs)}\n"
                f"  QPs:           {', '.join(str(q) for q in cfg.qps)}\n"
                f"  Frames:        {cfg.frames}\n"
                f"  Repetitions:   {cfg.repetitions}\n"
                f"  Total encodes: {total}\n"
                f"  Parallel jobs: {cfg.parallel_jobs}"
                f"{'  (sequential)' if cfg.parallel_jobs == 1 else '  (timing may be skewed)'}\n\n"
                f"Output root: {cfg.output_root}"
            ),
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if confirm != QMessageBox.StandardButton.Yes:
            return

        self._log.clear()
        self._log.set_progress(0)
        self._log.set_status("⏳ Comparison starting…", "#ffc857")

        self._orchestrator = ComparisonOrchestrator(cfg, parent=self)
        self._orchestrator.signals.log_line.connect(self._on_log_line)
        self._orchestrator.signals.progress.connect(self._on_progress)
        self._orchestrator.signals.finished.connect(self._on_finished)
        self._orchestrator.start()
        self._update_buttons()

    @Slot()
    def _on_cancel(self) -> None:
        if not self._is_running():
            return
        self._log.append("⛔ Cancellation requested.")
        self._log.set_status("⛔ Cancelling…", "#ffc857")
        self._orchestrator.cancel()

    # ------------------------------------------------------------------
    # Orchestrator signal handlers
    # ------------------------------------------------------------------

    @Slot(str)
    def _on_log_line(self, line: str) -> None:
        self._log.append(line)

    @Slot(int)
    def _on_progress(self, value: int) -> None:
        self._log.set_progress(max(0, min(100, value)))

    @Slot(bool, str)
    def _on_finished(self, success: bool, message: str) -> None:
        color = "#4cda8a" if success else "#ff6b7a"
        prefix = "✅" if success else "❌"
        self._log.set_status(f"{prefix} {message}", color)
        if success:
            self._log.set_progress(100)
        self._orchestrator = None
        self._update_buttons()
