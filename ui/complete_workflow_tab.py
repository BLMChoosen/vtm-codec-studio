"""
Complete Workflow Tab
=====================
Single-pipeline UI that runs Convert -> Encode -> Decode -> Variance Maps ->
Create Dataset on the same set of inputs. Each stage is optional and the
output of every executed stage feeds automatically into the next one.
"""

from __future__ import annotations

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
    QProgressBar,
    QPushButton,
    QScrollArea,
    QVBoxLayout,
    QWidget,
)

from core.workflow import (
    InputItem,
    MODE_INFO,
    WorkflowConfig,
    WorkflowOrchestrator,
    WorkflowSteps,
)
from ui.widgets import FilePickerRow, LogPanel, ScrollSafeSpinBox
from utils.config import ConfigManager
from utils.validators import validate_directory, validate_file_exists


_VALID_EXTS = (".y4m", ".yuv")
_DEFAULT_QPS = "22, 27, 32, 37"


def _sanitize_name(stem: str) -> str:
    cleaned = stem.strip().replace(" ", "_")
    cleaned = re.sub(r'[<>:"/\\|?*\x00-\x1f]+', "", cleaned)
    return cleaned.strip("._-") or stem.strip()


class CompleteWorkflowTab(QWidget):
    """Widget hosting the end-to-end pipeline UI."""

    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self._config = ConfigManager()
        self._orchestrator: Optional[WorkflowOrchestrator] = None
        self._inputs: list[InputItem] = []
        self._current_stage: str = ""
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
        body_layout.addWidget(self._build_steps_group())
        body_layout.addWidget(self._build_converter_group())
        body_layout.addWidget(self._build_encoder_group())
        body_layout.addWidget(self._build_variance_group())
        body_layout.addLayout(self._build_action_row())

        # Stage-level progress (per-step), shown above the LogPanel which has
        # its own overall-progress bar.
        body_layout.addWidget(self._build_stage_progress_group())

        # Log
        self._log = LogPanel()
        self._log.setMinimumHeight(260)
        self._log.set_status("Ready", "#8a90a4")
        body_layout.addWidget(self._log)

        scroll.setWidget(body)
        root.addWidget(scroll, stretch=1)

        self._refresh_inputs_view()
        self._update_buttons()

    def _build_intro_box(self) -> QGroupBox:
        group = QGroupBox("Pipeline Overview")
        layout = QVBoxLayout(group)
        info = QLabel(
            "Single integrated pipeline:\n"
            "    Convert (.y4m → .yuv + .cfg) → Encode (LD/RA × QPs) → "
            "Decode → Variance Maps → Create Dataset.\n"
            "Each stage is optional. Outputs of every executed stage feed "
            "automatically into the next."
        )
        info.setWordWrap(True)
        info.setStyleSheet("color: #b8bdcb;")
        layout.addWidget(info)
        return group

    def _build_inputs_group(self) -> QGroupBox:
        group = QGroupBox("Input Files (.y4m or .yuv)")
        layout = QVBoxLayout(group)
        layout.setSpacing(8)

        self._inputs_list = QListWidget()
        self._inputs_list.setMinimumHeight(140)
        self._inputs_list.setSelectionMode(QListWidget.SelectionMode.SingleSelection)
        self._inputs_list.currentRowChanged.connect(lambda _: self._update_buttons())
        layout.addWidget(self._inputs_list)

        btn_row = QHBoxLayout()
        self._add_files_btn = QPushButton("+ Add Inputs…")
        self._add_files_btn.setToolTip("Add one or more .y4m and/or .yuv files. For .yuv files, you'll be asked for the per-sequence .cfg.")
        self._add_files_btn.clicked.connect(self._on_add_inputs)
        btn_row.addWidget(self._add_files_btn)

        self._set_cfg_btn = QPushButton("Set CFG for Selected…")
        self._set_cfg_btn.setToolTip("Pick the per-sequence .cfg for the selected .yuv input (ignored for .y4m).")
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
            "• .y4m inputs are auto-converted (when Converter is enabled) and the "
            "per-sequence .cfg is generated for you.\n"
            "• .yuv inputs require an existing per-sequence .cfg (use the "
            "“Set CFG for Selected…” button)."
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
            placeholder="Folder where converter/, encoder/, decode/, variance_maps/, dataset/ are created",
            mode="directory",
        )
        layout.addWidget(self._output_picker)

        hint = QLabel(
            "All workflow outputs are placed here. Each pipeline run creates "
            "fresh execution_NNN folders so existing executions are kept."
        )
        hint.setWordWrap(True)
        hint.setStyleSheet("color: #8a90a4; font-size: 12px;")
        layout.addWidget(hint)
        return group

    def _build_steps_group(self) -> QGroupBox:
        group = QGroupBox("Stages to Execute")
        layout = QVBoxLayout(group)
        layout.setSpacing(6)

        self._step_converter = QCheckBox("Converter — convert .y4m → .yuv and generate per-sequence.cfg")
        self._step_encode    = QCheckBox("Encode — run encoder for the selected QPs and modes")
        self._step_decode    = QCheckBox("Decode — decode the generated .bin into reconstructed YUV + metrics CSV")
        self._step_variance  = QCheckBox("Variance Maps — compute per-block variance from original + LD/RA decoded YUVs")
        self._step_dataset   = QCheckBox("Create Dataset — build the consolidated dataset.csv + metadata.json")
        for cb in (
            self._step_converter, self._step_encode, self._step_decode,
            self._step_variance, self._step_dataset,
        ):
            cb.setChecked(True)
            cb.toggled.connect(self._update_buttons)
            layout.addWidget(cb)

        warn = QLabel(
            "Note: Variance Maps and the Dataset stage require BOTH Lowdelay and "
            "Random Access modes to be enabled below."
        )
        warn.setWordWrap(True)
        warn.setStyleSheet("color: #8a90a4; font-size: 12px;")
        layout.addWidget(warn)

        return group

    def _build_converter_group(self) -> QGroupBox:
        group = QGroupBox("Converter Settings")
        layout = QVBoxLayout(group)
        layout.setSpacing(8)

        self._convert_all_check = QCheckBox("Convert all frames")
        self._convert_all_check.setChecked(True)
        self._convert_all_check.toggled.connect(self._on_convert_mode_toggle)
        layout.addWidget(self._convert_all_check)

        limit_row = QHBoxLayout()
        limit_lbl = QLabel("Frames to convert:")
        limit_lbl.setMinimumWidth(160)
        limit_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        limit_row.addWidget(limit_lbl)

        self._convert_limit_spin = ScrollSafeSpinBox()
        self._convert_limit_spin.setRange(1, 100000)
        self._convert_limit_spin.setValue(33)
        self._convert_limit_spin.setEnabled(False)
        self._convert_limit_spin.setToolTip("Convert only the first N frames of each .y4m input.")
        limit_row.addWidget(self._convert_limit_spin)
        limit_row.addStretch()
        layout.addLayout(limit_row)

        level_row = QHBoxLayout()
        level_lbl = QLabel("Sequence cfg level:")
        level_lbl.setMinimumWidth(160)
        level_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        level_row.addWidget(level_lbl)
        self._level_edit = QLineEdit("4.1")
        self._level_edit.setPlaceholderText("e.g. 4.1")
        level_row.addWidget(self._level_edit, stretch=1)
        layout.addLayout(level_row)

        return group

    def _build_encoder_group(self) -> QGroupBox:
        group = QGroupBox("Encoder Settings")
        layout = QVBoxLayout(group)
        layout.setSpacing(8)

        # Modes
        modes_row = QHBoxLayout()
        modes_lbl = QLabel("Modes:")
        modes_lbl.setMinimumWidth(160)
        modes_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        modes_row.addWidget(modes_lbl)
        self._mode_ld_check = QCheckBox("Lowdelay (LD)")
        self._mode_ld_check.setChecked(True)
        self._mode_ld_check.toggled.connect(self._update_buttons)
        modes_row.addWidget(self._mode_ld_check)
        self._mode_ra_check = QCheckBox("Random Access (RA)")
        self._mode_ra_check.setChecked(True)
        self._mode_ra_check.toggled.connect(self._update_buttons)
        modes_row.addWidget(self._mode_ra_check)
        modes_row.addStretch()
        layout.addLayout(modes_row)

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

        # Frames
        frames_row = QHBoxLayout()
        frames_lbl = QLabel("Frames (-f):")
        frames_lbl.setMinimumWidth(160)
        frames_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        frames_row.addWidget(frames_lbl)
        self._encode_frames_spin = ScrollSafeSpinBox()
        self._encode_frames_spin.setRange(1, 100000)
        self._encode_frames_spin.setValue(33)
        frames_row.addWidget(self._encode_frames_spin)
        frames_row.addStretch()
        layout.addLayout(frames_row)

        return group

    def _build_variance_group(self) -> QGroupBox:
        group = QGroupBox("Variance Maps / Dataset Settings")
        layout = QVBoxLayout(group)
        layout.setSpacing(8)

        frames_row = QHBoxLayout()
        frames_lbl = QLabel("Frames (max 33):")
        frames_lbl.setMinimumWidth(160)
        frames_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        frames_row.addWidget(frames_lbl)

        self._variance_frames_spin = ScrollSafeSpinBox()
        self._variance_frames_spin.setRange(2, 33)
        self._variance_frames_spin.setValue(33)
        self._variance_frames_spin.setToolTip("Total frames to read for variance and dataset (frame 0 is skipped). Max 33.")
        frames_row.addWidget(self._variance_frames_spin)
        frames_row.addStretch()
        layout.addLayout(frames_row)

        return group

    def _build_stage_progress_group(self) -> QGroupBox:
        group = QGroupBox("Stage Progress")
        layout = QVBoxLayout(group)
        layout.setSpacing(6)

        self._stage_label = QLabel("Idle")
        self._stage_label.setStyleSheet("color: #b8bdcb;")
        layout.addWidget(self._stage_label)

        self._stage_progress = QProgressBar()
        self._stage_progress.setRange(0, 100)
        self._stage_progress.setValue(0)
        self._stage_progress.setTextVisible(True)
        self._stage_progress.setFormat("%p%")
        layout.addWidget(self._stage_progress)

        hint = QLabel(
            "Stage Progress: progress within the current stage / sub-step. "
            "Overall workflow progress is shown in the Output Log below."
        )
        hint.setWordWrap(True)
        hint.setStyleSheet("color: #8a90a4; font-size: 11px;")
        layout.addWidget(hint)
        return group

    def _build_action_row(self) -> QHBoxLayout:
        row = QHBoxLayout()
        row.addStretch()
        self._start_btn = QPushButton("▶  Start Workflow")
        self._start_btn.setObjectName("primaryButton")
        self._start_btn.setMinimumWidth(180)
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
    # State persistence (best-effort)
    # ------------------------------------------------------------------

    def _restore_state(self) -> None:
        cfg = self._config
        self._output_picker.set_path(cfg.get("workflow_output_root", ""))
        self._convert_all_check.setChecked(bool(cfg.get("workflow_convert_all", True)))
        try:
            self._convert_limit_spin.setValue(max(1, int(cfg.get("workflow_convert_limit", 33))))
        except (TypeError, ValueError):
            self._convert_limit_spin.setValue(33)
        self._convert_limit_spin.setEnabled(not self._convert_all_check.isChecked())

        self._level_edit.setText(cfg.get("workflow_level", "4.1") or "4.1")
        self._qps_edit.setText(cfg.get("workflow_qps", _DEFAULT_QPS))

        try:
            self._encode_frames_spin.setValue(max(1, int(cfg.get("workflow_encode_frames", 33))))
        except (TypeError, ValueError):
            self._encode_frames_spin.setValue(33)

        try:
            self._variance_frames_spin.setValue(
                max(2, min(33, int(cfg.get("workflow_variance_frames", 33))))
            )
        except (TypeError, ValueError):
            self._variance_frames_spin.setValue(33)

        self._step_converter.setChecked(bool(cfg.get("workflow_step_converter", True)))
        self._step_encode.setChecked(bool(cfg.get("workflow_step_encode", True)))
        self._step_decode.setChecked(bool(cfg.get("workflow_step_decode", True)))
        self._step_variance.setChecked(bool(cfg.get("workflow_step_variance", True)))
        self._step_dataset.setChecked(bool(cfg.get("workflow_step_dataset", True)))
        self._mode_ld_check.setChecked(bool(cfg.get("workflow_mode_ld", True)))
        self._mode_ra_check.setChecked(bool(cfg.get("workflow_mode_ra", True)))

    def _save_state(self) -> None:
        self._config.update({
            "workflow_output_root":      self._output_picker.path(),
            "workflow_convert_all":      self._convert_all_check.isChecked(),
            "workflow_convert_limit":    self._convert_limit_spin.value(),
            "workflow_level":            self._level_edit.text().strip() or "4.1",
            "workflow_qps":              self._qps_edit.text(),
            "workflow_encode_frames":    self._encode_frames_spin.value(),
            "workflow_variance_frames":  self._variance_frames_spin.value(),
            "workflow_step_converter":   self._step_converter.isChecked(),
            "workflow_step_encode":      self._step_encode.isChecked(),
            "workflow_step_decode":      self._step_decode.isChecked(),
            "workflow_step_variance":    self._step_variance.isChecked(),
            "workflow_step_dataset":     self._step_dataset.isChecked(),
            "workflow_mode_ld":          self._mode_ld_check.isChecked(),
            "workflow_mode_ra":          self._mode_ra_check.isChecked(),
        })

    # ------------------------------------------------------------------
    # Inputs management
    # ------------------------------------------------------------------

    def _input_label(self, item: InputItem) -> str:
        kind = "Y4M" if item.is_y4m else "YUV"
        cfg_part = ""
        if item.is_yuv:
            cfg_part = f"   ↦ cfg: {item.per_sequence_cfg}" if item.per_sequence_cfg else "   ↦ cfg: (needs cfg)"
        return f"[{kind}]  {item.path}{cfg_part}"

    def _refresh_inputs_view(self) -> None:
        previous = self._inputs_list.currentRow()
        self._inputs_list.clear()
        for item in self._inputs:
            list_item = QListWidgetItem(self._input_label(item))
            self._inputs_list.addItem(list_item)
        if self._inputs_list.count() > 0:
            row = previous if 0 <= previous < self._inputs_list.count() else 0
            self._inputs_list.setCurrentRow(row)
        self._update_buttons()

    def _existing_paths(self) -> set[str]:
        return {Path(item.path).resolve().as_posix().casefold() for item in self._inputs}

    def _add_input_path(self, path: str) -> None:
        path = path.strip()
        if not path:
            return
        if not path.lower().endswith(_VALID_EXTS):
            QMessageBox.warning(
                self, "Invalid Input",
                f"Only .y4m and .yuv files are accepted:\n{path}",
            )
            return
        if not Path(path).is_file():
            QMessageBox.warning(self, "File not found", f"File does not exist:\n{path}")
            return
        if Path(path).resolve().as_posix().casefold() in self._existing_paths():
            return  # already added; skip silently

        item = InputItem(
            path=path,
            per_sequence_cfg="",
            name=_sanitize_name(Path(path).stem),
        )
        self._inputs.append(item)

    @Slot()
    def _on_add_inputs(self) -> None:
        if self._is_running():
            return
        last_dir = ""
        if self._inputs:
            last_dir = str(Path(self._inputs[-1].path).parent)

        paths, _ = QFileDialog.getOpenFileNames(
            self,
            "Select .y4m and/or .yuv files",
            last_dir,
            "Video files (*.y4m *.yuv);;Y4M (*.y4m);;YUV (*.yuv);;All Files (*)",
        )
        if not paths:
            return
        added_yuv: list[InputItem] = []
        for path in paths:
            self._add_input_path(path)
            if path.lower().endswith(".yuv"):
                added_yuv.append(self._inputs[-1])

        # If a single .yuv was added, prompt for its cfg right away.
        if len(added_yuv) == 1:
            self._prompt_cfg_for_item(added_yuv[0])

        self._refresh_inputs_view()

    def _prompt_cfg_for_item(self, item: InputItem) -> None:
        start_dir = ""
        guess = Path(item.path).with_suffix(".cfg")
        if guess.is_file():
            item.per_sequence_cfg = str(guess)
            return
        if item.path:
            start_dir = str(Path(item.path).parent)
        path, _ = QFileDialog.getOpenFileName(
            self,
            f"Select per-sequence .cfg for {Path(item.path).name}",
            start_dir,
            "Config Files (*.cfg);;All Files (*)",
        )
        if path:
            item.per_sequence_cfg = path

    @Slot()
    def _on_set_cfg_for_selected(self) -> None:
        if self._is_running():
            return
        row = self._inputs_list.currentRow()
        if row < 0 or row >= len(self._inputs):
            return
        item = self._inputs[row]
        if item.is_y4m:
            QMessageBox.information(
                self, "Not needed",
                "The Converter stage will generate the per-sequence cfg for .y4m inputs.",
            )
            return
        self._prompt_cfg_for_item(item)
        self._refresh_inputs_view()

    @Slot()
    def _on_remove_selected(self) -> None:
        if self._is_running():
            return
        row = self._inputs_list.currentRow()
        if 0 <= row < len(self._inputs):
            del self._inputs[row]
            self._refresh_inputs_view()

    @Slot()
    def _on_clear_inputs(self) -> None:
        if self._is_running() or not self._inputs:
            return
        confirm = QMessageBox.question(
            self, "Clear Inputs",
            "Remove all input files from the list?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if confirm == QMessageBox.StandardButton.Yes:
            self._inputs.clear()
            self._refresh_inputs_view()

    @Slot(bool)
    def _on_convert_mode_toggle(self, checked: bool) -> None:
        self._convert_limit_spin.setEnabled(not checked)

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _selected_modes(self) -> list[str]:
        modes: list[str] = []
        if self._mode_ld_check.isChecked(): modes.append("LD")
        if self._mode_ra_check.isChecked(): modes.append("RA")
        return modes

    def _parse_qps(self) -> tuple[list[int], str]:
        raw = self._qps_edit.text()
        tokens = [t for t in re.split(r"[\s,;]+", raw.strip()) if t]
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
            if qp in seen:
                continue
            seen.add(qp)
            qps.append(qp)
        return qps, ""

    def _validate(self) -> tuple[bool, str, Optional[WorkflowConfig]]:
        if not self._inputs:
            return False, "Add at least one input file.", None

        for item in self._inputs:
            ok, msg = validate_file_exists(item.path, "Input")
            if not ok:
                return False, msg, None
            if item.is_yuv:
                if not item.per_sequence_cfg:
                    return False, (
                        f"Input '{Path(item.path).name}' is a .yuv file — "
                        "set its per-sequence .cfg first."
                    ), None
                ok, msg = validate_file_exists(item.per_sequence_cfg, "Per-sequence .cfg")
                if not ok:
                    return False, msg, None

        ok, msg = validate_directory(self._output_picker.path(), "Output root")
        if not ok:
            return False, msg, None

        steps = WorkflowSteps(
            converter=self._step_converter.isChecked(),
            encode=self._step_encode.isChecked(),
            decode=self._step_decode.isChecked(),
            variance_maps=self._step_variance.isChecked(),
            dataset=self._step_dataset.isChecked(),
        )
        if not any([steps.converter, steps.encode, steps.decode, steps.variance_maps, steps.dataset]):
            return False, "Select at least one stage to execute.", None

        modes = self._selected_modes()
        if (steps.encode or steps.decode or steps.variance_maps or steps.dataset) and not modes:
            return False, "Select at least one encoder mode (Lowdelay / Random Access).", None

        if (steps.variance_maps or steps.dataset) and not ("LD" in modes and "RA" in modes):
            return False, (
                "Variance Maps and the Dataset stage require BOTH Lowdelay and Random Access "
                "modes to be enabled."
            ), None

        qps, err = self._parse_qps()
        if (steps.encode or steps.decode or steps.variance_maps or steps.dataset):
            if err:
                return False, err, None
            if not qps:
                return False, "Provide at least one QP value.", None

        # Tool checks for the relevant stages
        if steps.converter and any(item.is_y4m for item in self._inputs):
            ffmpeg = self._config.ffmpeg_path()
            if not ffmpeg:
                return False, "FFmpeg path is not set. Configure it under Settings.", None
            ok, msg = validate_file_exists(ffmpeg, "FFmpeg executable")
            if not ok:
                return False, msg, None

        if steps.encode:
            encoder = self._config.encoder_path()
            if not encoder:
                return False, "Encoder path is not set. Configure it under Settings.", None
            ok, msg = validate_file_exists(encoder, "Encoder executable")
            if not ok:
                return False, msg, None
            cfg_folder = self._config.cfg_folder()
            if not cfg_folder:
                return False, (
                    "Encoder cfg folder is not set. Configure it under Settings — "
                    "the workflow needs encoder_lowdelay_vtm.cfg / encoder_randomaccess_vtm.cfg."
                ), None
            for code in modes:
                main_cfg = Path(cfg_folder) / MODE_INFO[code]["cfg_file"]
                if not main_cfg.is_file():
                    return False, f"Missing encoder config: {main_cfg}", None

        if steps.decode:
            decoder = self._config.decoder_path()
            if not decoder:
                return False, "Decoder path is not set. Configure it under Settings.", None
            ok, msg = validate_file_exists(decoder, "Decoder executable")
            if not ok:
                return False, msg, None

        # Build the final workflow config
        config = WorkflowConfig(
            inputs=list(self._inputs),
            output_root=self._output_picker.path(),
            steps=steps,
            encoder_exe=self._config.encoder_path(),
            decoder_exe=self._config.decoder_path(),
            ffmpeg_exe=self._config.ffmpeg_path(),
            cfg_folder=self._config.cfg_folder(),
            converter_max_frames=(
                None if self._convert_all_check.isChecked()
                else int(self._convert_limit_spin.value())
            ),
            converter_level=self._level_edit.text().strip() or "4.1",
            encode_qps=qps if qps else [],
            encode_modes=modes,
            encode_frames=int(self._encode_frames_spin.value()),
            variance_frames=int(self._variance_frames_spin.value()),
        )
        return True, "", config

    # ------------------------------------------------------------------
    # Workflow lifecycle
    # ------------------------------------------------------------------

    def _is_running(self) -> bool:
        return self._orchestrator is not None and self._orchestrator.isRunning()

    def _update_buttons(self) -> None:
        running = self._is_running()
        self._start_btn.setEnabled(not running)
        self._cancel_btn.setEnabled(running)

        # Inputs management buttons
        self._add_files_btn.setEnabled(not running)
        self._set_cfg_btn.setEnabled(
            not running
            and self._inputs_list.currentRow() >= 0
            and 0 <= self._inputs_list.currentRow() < len(self._inputs)
            and self._inputs[self._inputs_list.currentRow()].is_yuv
        )
        self._remove_btn.setEnabled(not running and self._inputs_list.currentRow() >= 0)
        self._clear_btn.setEnabled(not running and bool(self._inputs))

    @Slot()
    def _on_start(self) -> None:
        if self._is_running():
            return

        ok, msg, cfg = self._validate()
        if not ok or cfg is None:
            QMessageBox.warning(self, "Validation Error", msg)
            return

        self._save_state()

        # Confirm overall plan to the user.
        n_exec = len(cfg.inputs) * max(1, len(cfg.encode_qps))
        confirm = QMessageBox.question(
            self,
            "Start Workflow",
            (
                f"Run workflow with the following plan?\n\n"
                f"  Inputs:        {len(cfg.inputs)}\n"
                f"  QPs:           {', '.join(str(q) for q in cfg.encode_qps) or '-'}\n"
                f"  Modes:         {', '.join(cfg.encode_modes) or '-'}\n"
                f"  Executions:    {n_exec} (per mode)\n"
                f"  Encode frames: {cfg.encode_frames}\n"
                f"  Stages:        {self._stage_summary(cfg.steps)}\n\n"
                f"Output root: {cfg.output_root}"
            ),
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if confirm != QMessageBox.StandardButton.Yes:
            return

        self._log.clear()
        self._log.set_progress(0)
        self._stage_progress.setValue(0)
        self._stage_label.setText("⏳ Workflow starting…")
        self._log.set_status("⏳ Workflow starting…", "#ffc857")

        self._orchestrator = WorkflowOrchestrator(cfg, parent=self)
        self._orchestrator.signals.log_line.connect(self._on_log_line)
        self._orchestrator.signals.progress_overall.connect(self._on_progress_overall)
        self._orchestrator.signals.progress_step.connect(self._on_progress_step)
        self._orchestrator.signals.stage_started.connect(self._on_stage_started)
        self._orchestrator.signals.stage_finished.connect(self._on_stage_finished)
        self._orchestrator.signals.finished_workflow.connect(self._on_workflow_finished)
        self._orchestrator.start()
        self._update_buttons()

    @Slot()
    def _on_cancel(self) -> None:
        if not self._is_running():
            return
        self._log.append("⛔ Cancellation requested.")
        self._log.set_status("⛔ Cancelling workflow…", "#ffc857")
        self._orchestrator.cancel()

    # ------------------------------------------------------------------
    # Orchestrator signal handlers
    # ------------------------------------------------------------------

    @Slot(str)
    def _on_log_line(self, line: str) -> None:
        self._log.append(line)

    @Slot(int)
    def _on_progress_overall(self, value: int) -> None:
        self._log.set_progress(value)

    @Slot(int)
    def _on_progress_step(self, value: int) -> None:
        self._stage_progress.setValue(max(0, min(100, value)))

    @Slot(str)
    def _on_stage_started(self, name: str) -> None:
        self._current_stage = name
        self._stage_label.setText(f"⏳ Stage: {name}")
        self._stage_progress.setValue(0)
        self._log.set_status(f"⏳ Stage: {name}", "#ffc857")

    @Slot(str, bool)
    def _on_stage_finished(self, name: str, ok: bool) -> None:
        color = "#4cda8a" if ok else "#ff6b7a"
        prefix = "✅" if ok else "❌"
        self._stage_label.setText(f"{prefix} {name} — finished")
        self._stage_progress.setValue(100 if ok else 0)
        self._log.set_status(f"{prefix} Stage finished: {name}", color)

    @Slot(bool, str)
    def _on_workflow_finished(self, success: bool, message: str) -> None:
        color = "#4cda8a" if success else "#ff6b7a"
        prefix = "✅" if success else "❌"
        self._stage_label.setText(f"{prefix} {message}")
        self._log.set_status(f"{prefix} {message}", color)
        if success:
            self._log.set_progress(100)
            self._stage_progress.setValue(100)
        self._orchestrator = None
        self._update_buttons()

    # ------------------------------------------------------------------

    @staticmethod
    def _stage_summary(steps: WorkflowSteps) -> str:
        labels = []
        if steps.converter:     labels.append("Converter")
        if steps.encode:        labels.append("Encode")
        if steps.decode:        labels.append("Decode")
        if steps.variance_maps: labels.append("Variance Maps")
        if steps.dataset:       labels.append("Create Dataset")
        return ", ".join(labels) or "(none)"
