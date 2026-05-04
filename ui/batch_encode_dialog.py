"""
Batch Encode Dialog
===================
Modal dialog that builds a Cartesian product of EncodeJobs for the
encoder queue: multiple input videos × multiple QPs × multiple
configurations, sharing the same frames count, output folder,
artifacts folder, and filename rules.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from PySide6.QtCore import Qt, Slot
from PySide6.QtWidgets import (
    QCheckBox,
    QDialog,
    QDialogButtonBox,
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

from ui.widgets import FilePickerRow
from utils.validators import (
    validate_directory,
    validate_extension,
    validate_file_exists,
    validate_positive_int,
)


CONFIG_OPTIONS: list[tuple[str, str, str]] = [
    ("encoder_intra_vtm.cfg", "All Intra (AI)", "intra"),
    ("encoder_lowdelay_vtm.cfg", "Low Delay (LD)", "ld"),
    ("encoder_randomaccess_vtm.cfg", "Random Access (RA)", "ra"),
]

QP_PRESETS: list[tuple[str, str]] = [
    ("Common (22, 27, 32, 37)", "22, 27, 32, 37"),
    ("Wide (17, 22, 27, 32, 37, 42)", "17, 22, 27, 32, 37, 42"),
    ("Low (17, 22, 27)", "17, 22, 27"),
    ("High (32, 37, 42, 47)", "32, 37, 42, 47"),
]


@dataclass
class BatchEncodePlan:
    """One row of the Cartesian product produced by the dialog."""

    input_yuv: str
    sequence_cfg: str
    main_config: str
    config_short: str
    frames: int
    qp: int
    output_bin: str
    artifacts_dir: str


class BatchEncodeDialog(QDialog):
    """Dialog that returns a list of BatchEncodePlan when accepted."""

    def __init__(
        self,
        parent: Optional[QWidget] = None,
        defaults: Optional[dict] = None,
    ):
        super().__init__(parent)
        self.setWindowTitle("Batch Add — Encode Queue")
        self.setModal(True)
        self.resize(720, 760)

        self._defaults = defaults or {}
        self._plans: list[BatchEncodePlan] = []

        self._build_ui()
        self._apply_defaults()
        self._refresh_preview()

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

        body = QWidget()
        body_layout = QVBoxLayout(body)
        body_layout.setContentsMargins(4, 4, 4, 4)
        body_layout.setSpacing(14)

        body_layout.addWidget(self._build_inputs_group())
        body_layout.addWidget(self._build_seqcfg_group())
        body_layout.addWidget(self._build_variants_group())
        body_layout.addWidget(self._build_output_group())
        body_layout.addWidget(self._build_naming_group())

        scroll.setWidget(body)
        root.addWidget(scroll, stretch=1)

        # Preview label
        self._preview_label = QLabel("No jobs.")
        self._preview_label.setStyleSheet(
            "padding: 6px 10px; border-radius: 6px; background: #1f2533; color: #d9deeb;"
        )
        root.addWidget(self._preview_label)

        # Buttons
        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        self._ok_btn = buttons.button(QDialogButtonBox.StandardButton.Ok)
        self._ok_btn.setText("Add to Queue")
        self._ok_btn.setObjectName("primaryButton")
        buttons.accepted.connect(self._on_accept)
        buttons.rejected.connect(self.reject)
        root.addWidget(buttons)

    def _build_inputs_group(self) -> QGroupBox:
        group = QGroupBox("Input Videos (.yuv)")
        layout = QVBoxLayout(group)
        layout.setSpacing(8)

        self._inputs_list = QListWidget()
        self._inputs_list.setMinimumHeight(110)
        self._inputs_list.setSelectionMode(QListWidget.SelectionMode.ExtendedSelection)
        layout.addWidget(self._inputs_list)

        btn_row = QHBoxLayout()
        self._add_files_btn = QPushButton("+ Add YUV Files…")
        self._add_files_btn.clicked.connect(self._pick_input_files)
        btn_row.addWidget(self._add_files_btn)

        self._remove_input_btn = QPushButton("Remove Selected")
        self._remove_input_btn.clicked.connect(self._remove_selected_inputs)
        btn_row.addWidget(self._remove_input_btn)

        self._clear_inputs_btn = QPushButton("Clear")
        self._clear_inputs_btn.setObjectName("dangerButton")
        self._clear_inputs_btn.clicked.connect(self._clear_inputs)
        btn_row.addWidget(self._clear_inputs_btn)

        btn_row.addStretch()
        layout.addLayout(btn_row)

        hint = QLabel(
            "Tip: select multiple .yuv files at once in the file dialog. "
            "Every file produces (QPs × Configurations) jobs."
        )
        hint.setWordWrap(True)
        hint.setStyleSheet("color: #8a90a4; font-size: 12px;")
        layout.addWidget(hint)

        return group

    def _build_seqcfg_group(self) -> QGroupBox:
        group = QGroupBox("Sequence Config (optional, applied to all inputs)")
        layout = QVBoxLayout(group)
        self._seq_cfg_picker = FilePickerRow(
            "Sequence Config:",
            file_filter="Config Files (*.cfg);;All Files (*)",
            placeholder="Optional shared per-sequence .cfg",
        )
        layout.addWidget(self._seq_cfg_picker)
        return group

    def _build_variants_group(self) -> QGroupBox:
        group = QGroupBox("Compression Variants")
        layout = QVBoxLayout(group)
        layout.setSpacing(10)

        # QPs row
        qp_row = QHBoxLayout()
        qp_lbl = QLabel("QPs (-q):")
        qp_lbl.setMinimumWidth(160)
        qp_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        qp_row.addWidget(qp_lbl)
        self._qps_edit = QLineEdit()
        self._qps_edit.setPlaceholderText("e.g., 22, 27, 32, 37 (range 0–63)")
        self._qps_edit.textChanged.connect(self._refresh_preview)
        qp_row.addWidget(self._qps_edit, stretch=1)
        layout.addLayout(qp_row)

        # QP presets row
        preset_row = QHBoxLayout()
        preset_lbl = QLabel("Presets:")
        preset_lbl.setMinimumWidth(160)
        preset_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        preset_row.addWidget(preset_lbl)
        for label, values in QP_PRESETS:
            btn = QPushButton(label)
            btn.clicked.connect(lambda _checked=False, v=values: self._apply_qp_preset(v))
            preset_row.addWidget(btn)
        preset_row.addStretch()
        layout.addLayout(preset_row)

        # Configs row
        cfg_row = QHBoxLayout()
        cfg_lbl = QLabel("Configurations:")
        cfg_lbl.setMinimumWidth(160)
        cfg_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        cfg_row.addWidget(cfg_lbl)

        self._config_checks: dict[str, QCheckBox] = {}
        cfg_box = QVBoxLayout()
        cfg_box.setSpacing(4)
        for cfg_file, label, _short in CONFIG_OPTIONS:
            check = QCheckBox(f"{label}  —  {cfg_file}")
            check.toggled.connect(self._refresh_preview)
            self._config_checks[cfg_file] = check
            cfg_box.addWidget(check)
        cfg_row.addLayout(cfg_box, stretch=1)
        layout.addLayout(cfg_row)

        # Frames row
        frames_row = QHBoxLayout()
        frames_lbl = QLabel("Frames (-f):")
        frames_lbl.setMinimumWidth(160)
        frames_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        frames_row.addWidget(frames_lbl)
        self._frames_edit = QLineEdit()
        self._frames_edit.setPlaceholderText("e.g., 100")
        self._frames_edit.textChanged.connect(self._refresh_preview)
        frames_row.addWidget(self._frames_edit, stretch=1)
        layout.addLayout(frames_row)

        return group

    def _build_output_group(self) -> QGroupBox:
        group = QGroupBox("Output Locations")
        layout = QVBoxLayout(group)
        layout.setSpacing(10)

        self._output_dir_picker = FilePickerRow(
            ".bin folder:",
            placeholder="Folder where all generated .bin files will be saved",
            mode="directory",
        )
        layout.addWidget(self._output_dir_picker)

        self._artifacts_dir_picker = FilePickerRow(
            "Artifacts folder:",
            placeholder="Base folder for reports, tracefiles and metrics",
            mode="directory",
        )
        layout.addWidget(self._artifacts_dir_picker)

        return group

    def _build_naming_group(self) -> QGroupBox:
        group = QGroupBox("Output File Name Format")
        layout = QVBoxLayout(group)
        layout.setSpacing(6)

        self._name_custom_check = QCheckBox("Custom prefix")
        self._name_custom_check.toggled.connect(self._on_custom_toggle)
        layout.addWidget(self._name_custom_check)

        custom_row = QHBoxLayout()
        custom_lbl = QLabel("Value:")
        custom_lbl.setMinimumWidth(160)
        custom_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        custom_row.addWidget(custom_lbl)
        self._name_custom_edit = QLineEdit()
        self._name_custom_edit.setPlaceholderText("e.g., test")
        self._name_custom_edit.setEnabled(False)
        self._name_custom_edit.textChanged.connect(self._refresh_preview)
        custom_row.addWidget(self._name_custom_edit, stretch=1)
        layout.addLayout(custom_row)

        self._name_q_check = QCheckBox("Quantization (q##)")
        self._name_q_check.setChecked(True)
        self._name_q_check.toggled.connect(self._refresh_preview)
        layout.addWidget(self._name_q_check)

        self._name_frames_check = QCheckBox("Frames (f##)")
        self._name_frames_check.setChecked(True)
        self._name_frames_check.toggled.connect(self._refresh_preview)
        layout.addWidget(self._name_frames_check)

        self._name_yuv_check = QCheckBox("YUV stem")
        self._name_yuv_check.setChecked(True)
        self._name_yuv_check.toggled.connect(self._refresh_preview)
        layout.addWidget(self._name_yuv_check)

        self._name_config_check = QCheckBox("Config short name (intra/ld/ra)")
        self._name_config_check.setChecked(True)
        self._name_config_check.setToolTip(
            "Required when more than one configuration is selected so output files do not collide."
        )
        self._name_config_check.toggled.connect(self._refresh_preview)
        layout.addWidget(self._name_config_check)

        hint = QLabel(
            "When more than one configuration is selected the config short name is forced ON "
            "to keep filenames unique."
        )
        hint.setWordWrap(True)
        hint.setStyleSheet("color: #8a90a4; font-size: 12px;")
        layout.addWidget(hint)

        return group

    # ------------------------------------------------------------------
    # Defaults
    # ------------------------------------------------------------------

    def _apply_defaults(self) -> None:
        d = self._defaults

        seq_cfg = d.get("sequence_cfg", "")
        if seq_cfg:
            self._seq_cfg_picker.set_path(seq_cfg)

        if d.get("frames"):
            self._frames_edit.setText(str(d["frames"]))

        last_qp = d.get("last_qp")
        if last_qp:
            self._qps_edit.setText(str(last_qp))

        last_cfg = d.get("last_config")
        if last_cfg and last_cfg in self._config_checks:
            self._config_checks[last_cfg].setChecked(True)

        if d.get("output_dir"):
            self._output_dir_picker.set_path(d["output_dir"])
        if d.get("artifacts_dir"):
            self._artifacts_dir_picker.set_path(d["artifacts_dir"])

        self._name_custom_check.setChecked(bool(d.get("name_custom_enabled", False)))
        self._name_custom_edit.setText(d.get("name_custom_text", ""))
        self._name_q_check.setChecked(bool(d.get("name_include_q", True)))
        self._name_frames_check.setChecked(bool(d.get("name_include_frames", True)))
        self._name_yuv_check.setChecked(bool(d.get("name_include_yuv", True)))
        self._name_custom_edit.setEnabled(self._name_custom_check.isChecked())

        seed_inputs = d.get("seed_inputs") or []
        for path in seed_inputs:
            if path:
                self._add_input(path)

    @Slot(bool)
    def _on_custom_toggle(self, enabled: bool) -> None:
        self._name_custom_edit.setEnabled(enabled)
        self._refresh_preview()

    # ------------------------------------------------------------------
    # Input management
    # ------------------------------------------------------------------

    def _existing_inputs(self) -> set[str]:
        return {
            self._inputs_list.item(i).data(Qt.ItemDataRole.UserRole)
            for i in range(self._inputs_list.count())
        }

    def _add_input(self, path: str) -> None:
        if not path or path in self._existing_inputs():
            return
        item = QListWidgetItem(path)
        item.setData(Qt.ItemDataRole.UserRole, path)
        self._inputs_list.addItem(item)

    @Slot()
    def _pick_input_files(self) -> None:
        last_dir = self._defaults.get("last_input_dir", "") or ""
        paths, _ = QFileDialog.getOpenFileNames(
            self,
            "Select .yuv files",
            last_dir,
            "YUV Files (*.yuv);;All Files (*)",
        )
        for path in paths:
            self._add_input(path)
        self._refresh_preview()

    @Slot()
    def _remove_selected_inputs(self) -> None:
        for item in self._inputs_list.selectedItems():
            self._inputs_list.takeItem(self._inputs_list.row(item))
        self._refresh_preview()

    @Slot()
    def _clear_inputs(self) -> None:
        if self._inputs_list.count() == 0:
            return
        confirm = QMessageBox.question(
            self,
            "Clear Inputs",
            "Remove all input videos?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if confirm == QMessageBox.StandardButton.Yes:
            self._inputs_list.clear()
            self._refresh_preview()

    # ------------------------------------------------------------------
    # QP / Config helpers
    # ------------------------------------------------------------------

    @Slot(str)
    def _apply_qp_preset(self, values: str) -> None:
        self._qps_edit.setText(values)

    def _parse_qps(self) -> tuple[list[int], str]:
        raw = self._qps_edit.text()
        if not raw.strip():
            return [], "QPs list is empty."

        tokens = [t for t in re.split(r"[\s,;]+", raw.strip()) if t]
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

        if not qps:
            return [], "QPs list is empty."
        return qps, ""

    def _selected_inputs(self) -> list[str]:
        return [
            self._inputs_list.item(i).data(Qt.ItemDataRole.UserRole)
            for i in range(self._inputs_list.count())
        ]

    def _selected_configs(self) -> list[tuple[str, str]]:
        result: list[tuple[str, str]] = []
        for cfg_file, _label, short in CONFIG_OPTIONS:
            check = self._config_checks.get(cfg_file)
            if check is not None and check.isChecked():
                result.append((cfg_file, short))
        return result

    # ------------------------------------------------------------------
    # Filename composition
    # ------------------------------------------------------------------

    @staticmethod
    def _sanitize(value: str) -> str:
        cleaned = value.strip().replace(" ", "_")
        cleaned = re.sub(r'[<>:"/\\|?*\x00-\x1f]+', "", cleaned)
        return cleaned.strip("._-")

    def _force_config_in_name(self) -> bool:
        return len(self._selected_configs()) > 1

    def _compose_filename(
        self,
        yuv_path: str,
        frames: int,
        qp: int,
        config_short: str,
    ) -> tuple[bool, str]:
        parts: list[str] = []

        if self._name_custom_check.isChecked():
            custom = self._sanitize(self._name_custom_edit.text())
            if not custom:
                return False, "Custom prefix is enabled but the value is empty."
            parts.append(custom)

        if self._name_q_check.isChecked():
            parts.append(f"q{qp}")

        if self._name_frames_check.isChecked():
            parts.append(f"f{frames}")

        if self._name_yuv_check.isChecked():
            stem = self._sanitize(Path(yuv_path).stem)
            if not stem:
                return False, f"Could not derive a valid name from '{yuv_path}'."
            parts.append(stem)

        include_config = self._name_config_check.isChecked() or self._force_config_in_name()
        if include_config and config_short:
            parts.append(config_short)

        if not parts:
            return False, "Select at least one filename component."

        return True, f"{'-'.join(parts)}.bin"

    # ------------------------------------------------------------------
    # Preview & validation
    # ------------------------------------------------------------------

    def _refresh_preview(self, *_args) -> None:
        # Force config name when multiple configs are picked
        if self._force_config_in_name():
            if not self._name_config_check.isChecked():
                self._name_config_check.blockSignals(True)
                self._name_config_check.setChecked(True)
                self._name_config_check.blockSignals(False)
            self._name_config_check.setEnabled(False)
        else:
            self._name_config_check.setEnabled(True)

        inputs = self._selected_inputs()
        qps, qp_err = self._parse_qps()
        configs = self._selected_configs()

        problems: list[str] = []
        if not inputs:
            problems.append("Add at least one input .yuv file.")
        if qp_err:
            problems.append(qp_err)
        if not configs:
            problems.append("Select at least one configuration.")
        if not self._frames_edit.text().strip():
            problems.append("Enter the number of frames.")

        total = len(inputs) * max(len(qps), 0) * max(len(configs), 0)

        if problems:
            self._preview_label.setText(
                f"⚠ {total} planned job(s). Issues: " + " · ".join(problems)
            )
            self._ok_btn.setEnabled(False)
            return

        sample_input = inputs[0]
        sample_qp = qps[0]
        sample_cfg_short = configs[0][1]
        try:
            sample_frames = int(self._frames_edit.text())
        except ValueError:
            sample_frames = 0
        ok, sample_name = self._compose_filename(sample_input, sample_frames, sample_qp, sample_cfg_short)
        if not ok:
            self._preview_label.setText(f"⚠ {sample_name}")
            self._ok_btn.setEnabled(False)
            return

        self._preview_label.setText(
            f"✅ {total} job(s) ready — "
            f"{len(inputs)} video(s) × {len(qps)} QP(s) × {len(configs)} config(s). "
            f"Sample: {sample_name}"
        )
        self._ok_btn.setEnabled(True)

    def _validate(self) -> tuple[bool, str]:
        inputs = self._selected_inputs()
        if not inputs:
            return False, "Add at least one input .yuv file."

        for path in inputs:
            ok, msg = validate_file_exists(path, "Input YUV")
            if not ok:
                return False, msg
            ok, msg = validate_extension(path, ".yuv", "Input YUV")
            if not ok:
                return False, msg

        seq_cfg = self._seq_cfg_picker.path()
        if seq_cfg:
            ok, msg = validate_file_exists(seq_cfg, "Sequence Config")
            if not ok:
                return False, msg
            ok, msg = validate_extension(seq_cfg, ".cfg", "Sequence Config")
            if not ok:
                return False, msg

        ok, msg = validate_positive_int(self._frames_edit.text(), "Frames")
        if not ok:
            return False, msg

        qps, msg = self._parse_qps()
        if msg:
            return False, msg

        configs = self._selected_configs()
        if not configs:
            return False, "Select at least one configuration."

        ok, msg = validate_directory(self._output_dir_picker.path(), ".bin folder")
        if not ok:
            return False, msg

        ok, msg = validate_directory(self._artifacts_dir_picker.path(), "Artifacts folder")
        if not ok:
            return False, msg

        return True, ""

    # ------------------------------------------------------------------
    # Build plans
    # ------------------------------------------------------------------

    def _build_plans(self) -> tuple[list[BatchEncodePlan], str]:
        ok, msg = self._validate()
        if not ok:
            return [], msg

        inputs = self._selected_inputs()
        qps, _ = self._parse_qps()
        configs = self._selected_configs()
        frames = int(self._frames_edit.text())

        output_dir = str(Path(self._output_dir_picker.path()).resolve())
        artifacts_dir = str(Path(self._artifacts_dir_picker.path()).resolve())
        seq_cfg = self._seq_cfg_picker.path()

        plans: list[BatchEncodePlan] = []
        used_paths: set[str] = set()

        for yuv_path in inputs:
            for cfg_file, short in configs:
                for qp in qps:
                    ok, name_or_msg = self._compose_filename(yuv_path, frames, qp, short)
                    if not ok:
                        return [], name_or_msg
                    output_bin = str(Path(output_dir) / name_or_msg)
                    key = output_bin.casefold()
                    if key in used_paths:
                        return [], (
                            "Output filenames would collide. Enable more name components "
                            "(e.g., YUV stem or config short name) or shorten the QP list. "
                            f"Conflict: {output_bin}"
                        )
                    used_paths.add(key)

                    plans.append(
                        BatchEncodePlan(
                            input_yuv=yuv_path,
                            sequence_cfg=seq_cfg,
                            main_config=cfg_file,
                            config_short=short,
                            frames=frames,
                            qp=qp,
                            output_bin=output_bin,
                            artifacts_dir=artifacts_dir,
                        )
                    )

        if not plans:
            return [], "No jobs were generated."

        return plans, ""

    # ------------------------------------------------------------------
    # Public accessors
    # ------------------------------------------------------------------

    def plans(self) -> list[BatchEncodePlan]:
        return self._plans

    @Slot()
    def _on_accept(self) -> None:
        plans, msg = self._build_plans()
        if not plans:
            QMessageBox.warning(self, "Validation Error", msg or "No jobs generated.")
            return
        self._plans = plans
        self.accept()
