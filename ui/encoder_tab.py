"""
Encoder Tab
===========
Full encoder UI panel: file pickers, parameters, preset management,
compression profiles, queue processing, and process control.
"""

import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from PySide6.QtCore import Qt, Slot, Signal
from PySide6.QtWidgets import (
    QCheckBox,
    QGroupBox,
    QHBoxLayout,
    QInputDialog,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from core.encoder import EncoderWorker
from ui.widgets import FilePickerRow, LogPanel, ScrollSafeComboBox, ScrollSafeSpinBox
from utils.csv_export import write_metrics_csv
from utils.config import ConfigManager
from utils.parser import parse_vtm_log
from utils.presets import (
    delete_compression_profile,
    delete_preset,
    list_compression_profiles,
    list_presets,
    load_compression_profile,
    load_preset,
    save_compression_profile,
    save_preset,
)
from utils.validators import (
    validate_directory,
    validate_extension,
    validate_file_exists,
    validate_output_path,
    validate_positive_int,
    validate_qp,
)


# The three standard VTM encoder configurations
ENCODER_CONFIGS = [
    "encoder_intra_vtm.cfg",
    "encoder_lowdelay_vtm.cfg",
    "encoder_randomaccess_vtm.cfg",
]

TRACE_RULE_DEFAULT = "D_BLOCK_STATISTICS_CODED:poc>=0"


@dataclass
class EncodeJob:
    """Immutable payload for one queued encode."""

    input_yuv: str
    sequence_cfg: str
    main_config: str
    frames: int
    qp: int
    output_bin: str
    artifacts_dir: str


class EncoderTab(QWidget):
    """Widget containing the full encoder interface."""
    
    metrics_ready = Signal(dict)

    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self._config = ConfigManager()
        self._worker: Optional[EncoderWorker] = None
        self._active_job: Optional[EncodeJob] = None
        self._workers: dict[EncoderWorker, EncodeJob] = {}
        self._worker_progress: dict[EncoderWorker, int] = {}
        self._worker_logs: dict[EncoderWorker, list[str]] = {}
        self._worker_job_index: dict[EncoderWorker, int] = {}

        self._queue: list[EncodeJob] = []
        self._queue_running = False
        self._queue_cancel_requested = False
        self._queue_next_index = 0
        self._queue_completed = 0
        self._queue_total = 0
        self._queue_results: list[bool] = []

        self._log_history: list[str] = []

        self._build_ui()
        self._restore_state()

    # ------------------------------------------------------------------
    # UI Construction
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(10)

        # ── Top: Scrollable parameters area ──
        from PySide6.QtWidgets import QScrollArea

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

        # -- Input files group --
        files_group = QGroupBox("Input Files")
        files_layout = QVBoxLayout(files_group)
        files_layout.setSpacing(12)

        self._input_picker = FilePickerRow(
            "Input YUV:",
            file_filter="YUV Files (*.yuv);;All Files (*)",
            placeholder="Drag & drop or browse for .yuv file",
        )
        files_layout.addWidget(self._input_picker)

        self._seq_cfg_picker = FilePickerRow(
            "Sequence Config:",
            file_filter="Config Files (*.cfg);;All Files (*)",
            placeholder="(Optional) per-sequence .cfg file",
        )
        files_layout.addWidget(self._seq_cfg_picker)

        form_layout.addWidget(files_group)

        # -- Encoder configuration group --
        cfg_group = QGroupBox("Encoder Configuration")
        cfg_layout = QVBoxLayout(cfg_group)
        cfg_layout.setSpacing(12)

        # Main config dropdown
        cfg_row = QHBoxLayout()
        lbl = QLabel("Main Config:")
        lbl.setMinimumWidth(160)
        lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        cfg_row.addWidget(lbl)
        self._cfg_combo = ScrollSafeComboBox()
        self._cfg_combo.addItems(ENCODER_CONFIGS)
        cfg_row.addWidget(self._cfg_combo, stretch=1)
        cfg_layout.addLayout(cfg_row)

        # Frames — own row
        frames_row = QHBoxLayout()
        lbl2 = QLabel("Frames (-f):")
        lbl2.setMinimumWidth(160)
        lbl2.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        frames_row.addWidget(lbl2)
        self._frames_edit = QLineEdit()
        self._frames_edit.setPlaceholderText("e.g., 100")
        frames_row.addWidget(self._frames_edit, stretch=1)
        cfg_layout.addLayout(frames_row)

        # QP — own row
        qp_row = QHBoxLayout()
        lbl3 = QLabel("QP (-q):")
        lbl3.setMinimumWidth(160)
        lbl3.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        qp_row.addWidget(lbl3)
        self._qp_edit = QLineEdit()
        self._qp_edit.setPlaceholderText("0 – 63")
        qp_row.addWidget(self._qp_edit, stretch=1)
        cfg_layout.addLayout(qp_row)

        form_layout.addWidget(cfg_group)

        # -- Output group --
        out_group = QGroupBox("Output")
        out_layout = QVBoxLayout(out_group)
        out_layout.setSpacing(12)

        self._output_dir_picker = FilePickerRow(
            ".bin folder:",
            placeholder="Choose the folder where .bin files will be saved",
            mode="directory",
        )
        out_layout.addWidget(self._output_dir_picker)

        name_group = QGroupBox("File Name Format")
        name_layout = QVBoxLayout(name_group)
        name_layout.setSpacing(8)

        self._name_custom_check = QCheckBox("Custom")
        self._name_custom_check.toggled.connect(self._toggle_custom_name)
        name_layout.addWidget(self._name_custom_check)

        custom_row = QHBoxLayout()
        custom_lbl = QLabel("Valor:")
        custom_lbl.setMinimumWidth(160)
        custom_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        custom_row.addWidget(custom_lbl)
        self._name_custom_edit = QLineEdit()
        self._name_custom_edit.setPlaceholderText("e.g., test")
        self._name_custom_edit.setEnabled(False)
        self._name_custom_edit.textChanged.connect(self._refresh_output_name_preview)
        custom_row.addWidget(self._name_custom_edit, stretch=1)
        name_layout.addLayout(custom_row)

        self._name_q_check = QCheckBox("Quantization")
        self._name_q_check.setChecked(True)
        self._name_q_check.toggled.connect(self._refresh_output_name_preview)
        name_layout.addWidget(self._name_q_check)

        self._name_frames_check = QCheckBox("Frames")
        self._name_frames_check.setChecked(True)
        self._name_frames_check.toggled.connect(self._refresh_output_name_preview)
        name_layout.addWidget(self._name_frames_check)

        self._name_yuv_check = QCheckBox("YUV filename")
        self._name_yuv_check.setChecked(True)
        self._name_yuv_check.toggled.connect(self._refresh_output_name_preview)
        name_layout.addWidget(self._name_yuv_check)

        preview_row = QHBoxLayout()
        preview_lbl = QLabel("Preview:")
        preview_lbl.setMinimumWidth(160)
        preview_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        preview_row.addWidget(preview_lbl)
        self._output_name_preview = QLineEdit()
        self._output_name_preview.setReadOnly(True)
        self._output_name_preview.setPlaceholderText("Select fields to generate the name")
        preview_row.addWidget(self._output_name_preview, stretch=1)
        name_layout.addLayout(preview_row)

        out_layout.addWidget(name_group)
        form_layout.addWidget(out_group)

        self._input_picker.path_changed.connect(self._refresh_output_name_preview)
        self._frames_edit.textChanged.connect(self._refresh_output_name_preview)
        self._qp_edit.textChanged.connect(self._refresh_output_name_preview)

        # -- Project presets row --
        preset_group = QGroupBox("Project Presets")
        preset_layout = QHBoxLayout(preset_group)
        preset_layout.setSpacing(10)

        self._preset_combo = ScrollSafeComboBox()
        self._preset_combo.setMinimumWidth(180)
        self._refresh_presets()
        preset_layout.addWidget(self._preset_combo, stretch=1)

        self._load_preset_btn = QPushButton("Load")
        self._load_preset_btn.setMinimumWidth(80)
        self._load_preset_btn.clicked.connect(self._load_preset)
        preset_layout.addWidget(self._load_preset_btn)

        self._save_preset_btn = QPushButton("Save")
        self._save_preset_btn.setMinimumWidth(80)
        self._save_preset_btn.clicked.connect(self._save_preset)
        preset_layout.addWidget(self._save_preset_btn)

        self._delete_preset_btn = QPushButton("Delete")
        self._delete_preset_btn.setObjectName("dangerButton")
        self._delete_preset_btn.setMinimumWidth(80)
        self._delete_preset_btn.clicked.connect(self._delete_preset)
        preset_layout.addWidget(self._delete_preset_btn)

        form_layout.addWidget(preset_group)

        # -- Compression profiles row --
        profile_group = QGroupBox("Compression Profiles")
        profile_layout = QHBoxLayout(profile_group)
        profile_layout.setSpacing(10)

        self._profile_combo = ScrollSafeComboBox()
        self._profile_combo.setMinimumWidth(180)
        self._refresh_compression_profiles()
        profile_layout.addWidget(self._profile_combo, stretch=1)

        self._load_profile_btn = QPushButton("Load")
        self._load_profile_btn.setMinimumWidth(80)
        self._load_profile_btn.clicked.connect(self._load_compression_profile)
        profile_layout.addWidget(self._load_profile_btn)

        self._save_profile_btn = QPushButton("Save")
        self._save_profile_btn.setMinimumWidth(80)
        self._save_profile_btn.clicked.connect(self._save_compression_profile)
        profile_layout.addWidget(self._save_profile_btn)

        self._delete_profile_btn = QPushButton("Delete")
        self._delete_profile_btn.setObjectName("dangerButton")
        self._delete_profile_btn.setMinimumWidth(80)
        self._delete_profile_btn.clicked.connect(self._delete_compression_profile)
        profile_layout.addWidget(self._delete_profile_btn)

        form_layout.addWidget(profile_group)

        # -- Queue controls --
        queue_group = QGroupBox("Encode Queue")
        queue_layout = QVBoxLayout(queue_group)
        queue_layout.setSpacing(10)

        self._queue_list = QListWidget()
        self._queue_list.setMinimumHeight(140)
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
        self._parallel_spin.setToolTip("Maximum number of queued encode jobs running at the same time.")
        parallel_row.addWidget(self._parallel_spin)
        parallel_row.addStretch()
        queue_layout.addLayout(parallel_row)

        # -- Execution artifacts controls --
        artifacts_group = QGroupBox("Execution Artifacts")
        artifacts_layout = QVBoxLayout(artifacts_group)
        artifacts_layout.setSpacing(10)

        self._artifacts_dir_picker = FilePickerRow(
            "Artifacts folder:",
            placeholder="Choose the base folder for reports, tracefiles, and metrics",
            mode="directory",
        )
        artifacts_layout.addWidget(self._artifacts_dir_picker)

        artifacts_hint = QLabel("Subfolders created automatically: reports, tracefiles, metrics")
        artifacts_hint.setStyleSheet("font-size: 12px; color: #8a90a4;")
        artifacts_layout.addWidget(artifacts_hint)

        artifacts_note = QLabel("Each run generates 3 files: report (.txt), tracefile (.csv), and metrics (.csv)")
        artifacts_note.setStyleSheet("font-size: 12px; color: #8a90a4;")
        artifacts_layout.addWidget(artifacts_note)

        form_layout.addWidget(artifacts_group)
        form_layout.addWidget(queue_group)

        # -- Action buttons --
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
        self._cancel_btn.clicked.connect(self._cancel_encoding)
        btn_row.addWidget(self._cancel_btn)

        btn_row.addStretch()
        form_layout.addLayout(btn_row)

        # -- Output log (scrolls with the encoder page) --
        self._log = LogPanel()
        self._log.setMinimumHeight(220)
        form_layout.addWidget(self._log)

        scroll.setWidget(form)

        # Entire encoder page (including log) is now scrollable
        root.addWidget(scroll, stretch=1)

        self._update_queue_controls()

    # ------------------------------------------------------------------
    # State persistence
    # ------------------------------------------------------------------

    @Slot(bool)
    def _toggle_custom_name(self, enabled: bool) -> None:
        self._name_custom_edit.setEnabled(enabled)
        self._refresh_output_name_preview()

    def _sanitize_name_part(self, value: str) -> str:
        cleaned = value.strip().replace(" ", "_")
        cleaned = re.sub(r'[<>:"/\\|?*\x00-\x1f]+', "", cleaned)
        return cleaned.strip("._-")

    def _compose_output_filename(self, input_yuv: str, frames: int, qp: int) -> tuple[bool, str]:
        parts: list[str] = []

        if self._name_custom_check.isChecked():
            custom_value = self._sanitize_name_part(self._name_custom_edit.text())
            if not custom_value:
                return False, "Custom is checked, but the value is empty."
            parts.append(custom_value)

        if self._name_q_check.isChecked():
            parts.append(f"q{qp}")

        if self._name_frames_check.isChecked():
            parts.append(f"f{frames}")

        if self._name_yuv_check.isChecked():
            yuv_name = self._sanitize_name_part(Path(input_yuv).stem)
            if not yuv_name:
                return False, "Could not extract a valid name from the input YUV file."
            parts.append(yuv_name)

        if not parts:
            return False, "Select at least one checkbox to compose the output filename."

        return True, f"{'-'.join(parts)}.bin"

    def _resolve_output_bin_path(self, input_yuv: str, frames: int, qp: int) -> tuple[bool, str]:
        output_dir = self._output_dir_picker.path()
        ok, msg = validate_directory(output_dir, "Output folder")
        if not ok:
            return False, msg

        ok, filename_or_msg = self._compose_output_filename(input_yuv, frames, qp)
        if not ok:
            return False, filename_or_msg

        return True, str(Path(output_dir) / filename_or_msg)

    def _refresh_output_name_preview(self, *_args) -> None:
        preview_input = self._input_picker.path() or "input.yuv"
        frames_text = self._frames_edit.text().strip()
        qp_text = self._qp_edit.text().strip()

        try:
            preview_frames = int(frames_text)
        except ValueError:
            preview_frames = 0

        try:
            preview_qp = int(qp_text)
        except ValueError:
            preview_qp = 0

        ok, filename_or_msg = self._compose_output_filename(preview_input, preview_frames, preview_qp)
        if ok:
            self._output_name_preview.setText(filename_or_msg)
            return

        self._output_name_preview.clear()
        self._output_name_preview.setPlaceholderText(filename_or_msg)

    def _restore_state(self) -> None:
        cfg = self._config
        idx = self._cfg_combo.findText(cfg.get("last_encoder_config", ENCODER_CONFIGS[0]))
        if idx >= 0:
            self._cfg_combo.setCurrentIndex(idx)
        self._frames_edit.setText(cfg.get("last_frames", ""))
        self._qp_edit.setText(cfg.get("last_qp", ""))
        self._output_dir_picker.set_path(cfg.get("encoder_output_dir", ""))
        self._name_custom_check.setChecked(bool(cfg.get("encoder_name_custom_enabled", False)))
        self._name_custom_edit.setText(cfg.get("encoder_name_custom_text", ""))
        self._name_q_check.setChecked(bool(cfg.get("encoder_name_include_q", True)))
        self._name_frames_check.setChecked(bool(cfg.get("encoder_name_include_frames", True)))
        self._name_yuv_check.setChecked(bool(cfg.get("encoder_name_include_yuv", True)))

        artifacts_dir = cfg.get("encoder_artifacts_dir", "")
        if not artifacts_dir:
            legacy_metrics = cfg.get("metrics_csv_path", cfg.get("queue_single_csv_path", ""))
            if legacy_metrics:
                artifacts_dir = str(Path(legacy_metrics).parent)
        self._artifacts_dir_picker.set_path(artifacts_dir)
        self._toggle_custom_name(self._name_custom_check.isChecked())
        parallel_jobs = cfg.get("encoder_parallel_jobs", 2)
        try:
            self._parallel_spin.setValue(max(1, int(parallel_jobs)))
        except (TypeError, ValueError):
            self._parallel_spin.setValue(2)
        self._refresh_output_name_preview()

    def _save_state(self) -> None:
        self._config.update({
            "last_encoder_config": self._cfg_combo.currentText(),
            "last_frames": self._frames_edit.text(),
            "last_qp": self._qp_edit.text(),
            "encoder_output_dir": self._output_dir_picker.path(),
            "encoder_name_custom_enabled": self._name_custom_check.isChecked(),
            "encoder_name_custom_text": self._name_custom_edit.text(),
            "encoder_name_include_q": self._name_q_check.isChecked(),
            "encoder_name_include_frames": self._name_frames_check.isChecked(),
            "encoder_name_include_yuv": self._name_yuv_check.isChecked(),
            "encoder_artifacts_dir": self._artifacts_dir_picker.path(),
            "encoder_parallel_jobs": self._parallel_spin.value(),
        })

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate_form(self) -> tuple[bool, str]:
        """Run form validations, returning (ok, error_message)."""
        checks = [
            validate_file_exists(self._input_picker.path(), "Input YUV"),
            validate_extension(self._input_picker.path(), ".yuv", "Input YUV"),
            validate_positive_int(self._frames_edit.text(), "Frames"),
            validate_qp(self._qp_edit.text()),
            validate_directory(self._output_dir_picker.path(), "Output folder"),
            validate_directory(self._artifacts_dir_picker.path(), "Artifacts folder"),
        ]

        # Sequence config is optional — validate only if provided
        seq = self._seq_cfg_picker.path()
        if seq:
            checks.append(validate_file_exists(seq, "Sequence Config"))
            checks.append(validate_extension(seq, ".cfg", "Sequence Config"))

        # Check encoder executable
        encoder_exe = self._config.encoder_path()
        if not encoder_exe:
            return False, "Encoder executable path is not set.\nGo to Settings to configure it."

        for ok, msg in checks:
            if not ok:
                return False, msg

        frames = int(self._frames_edit.text())
        qp = int(self._qp_edit.text())
        ok, msg = self._compose_output_filename(self._input_picker.path(), frames, qp)
        if not ok:
            return False, msg
        return True, ""

    def _validate_job(self, job: EncodeJob) -> tuple[bool, str]:
        """Validate a queued encode job right before execution."""
        checks = [
            validate_file_exists(job.input_yuv, "Input YUV"),
            validate_extension(job.input_yuv, ".yuv", "Input YUV"),
            validate_positive_int(str(job.frames), "Frames"),
            validate_qp(str(job.qp)),
            validate_output_path(job.output_bin, ".bin", "Output"),
            validate_directory(job.artifacts_dir, "Artifacts folder"),
        ]

        if job.sequence_cfg:
            checks.append(validate_file_exists(job.sequence_cfg, "Sequence Config"))
            checks.append(validate_extension(job.sequence_cfg, ".cfg", "Sequence Config"))

        main_cfg_path = self._resolve_main_cfg(job.main_config)
        checks.append(validate_file_exists(main_cfg_path, "Main Config"))
        checks.append(validate_extension(main_cfg_path, ".cfg", "Main Config"))

        encoder_exe = self._config.encoder_path()
        if not encoder_exe:
            return False, "Encoder executable path is not set.\nGo to Settings to configure it."
        checks.append(validate_file_exists(encoder_exe, "Encoder executable"))

        for ok, msg in checks:
            if not ok:
                return False, msg
        return True, ""

    def _artifact_dirs_for_job(self, job: EncodeJob) -> tuple[Path, Path, Path]:
        root_path = Path(job.artifacts_dir)
        return (
            root_path / "reports",
            root_path / "tracefiles",
            root_path / "metrics",
        )

    def _prepare_artifact_exports_for_job(self, job: EncodeJob) -> tuple[bool, str]:
        """Create output folders for reports, tracefiles and metrics for one queued job."""
        ok, msg = validate_directory(job.artifacts_dir, "Artifacts folder")
        if not ok:
            return False, msg

        reports_dir, tracefiles_dir, metrics_dir = self._artifact_dirs_for_job(job)
        try:
            reports_dir.mkdir(parents=True, exist_ok=True)
            tracefiles_dir.mkdir(parents=True, exist_ok=True)
            metrics_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            return False, f"Failed to prepare output folders: {exc}"
        return True, ""

    def _resolve_main_cfg(self, main_config: str) -> str:
        """Resolve selected main config against cfg folder when available."""
        cfg_folder = self._config.cfg_folder()
        if cfg_folder:
            return str(Path(cfg_folder) / main_config)
        return main_config

    def _build_job_from_form(self) -> Optional[EncodeJob]:
        """Convert current form fields into an encode job after validation."""
        ok, msg = self._validate_form()
        if not ok:
            QMessageBox.warning(self, "Validation Error", msg)
            return None

        frames = int(self._frames_edit.text())
        qp = int(self._qp_edit.text())
        ok, output_bin_or_msg = self._resolve_output_bin_path(self._input_picker.path(), frames, qp)
        if not ok:
            QMessageBox.warning(self, "Validation Error", output_bin_or_msg)
            return None

        self._save_state()
        artifacts_dir = str(Path(self._artifacts_dir_picker.path()).resolve())
        return EncodeJob(
            input_yuv=self._input_picker.path(),
            sequence_cfg=self._seq_cfg_picker.path(),
            main_config=self._cfg_combo.currentText(),
            frames=frames,
            qp=qp,
            output_bin=output_bin_or_msg,
            artifacts_dir=artifacts_dir,
        )

    def _sync_form_to_job(self, job: EncodeJob) -> None:
        """Reflect queued job values in the form for visibility."""
        self._input_picker.set_path(job.input_yuv)
        self._seq_cfg_picker.set_path(job.sequence_cfg)
        idx = self._cfg_combo.findText(job.main_config)
        if idx >= 0:
            self._cfg_combo.setCurrentIndex(idx)
        self._frames_edit.setText(str(job.frames))
        self._qp_edit.setText(str(job.qp))
        self._output_dir_picker.set_path(str(Path(job.output_bin).parent))
        self._artifacts_dir_picker.set_path(job.artifacts_dir)
        self._output_name_preview.setText(Path(job.output_bin).name)

    def _start_worker_for_job(self, job: EncodeJob, queue_index: int) -> bool:
        """Create and start one worker for the given queued job."""
        ok, msg = self._validate_job(job)
        if not ok:
            self._log.append(f"❌ Validation failed for job {queue_index + 1}/{self._queue_total}:\n{msg}")
            return False

        ok, msg = self._prepare_artifact_exports_for_job(job)
        if not ok:
            self._log.append(
                f"❌ Failed to prepare artifact folders for job {queue_index + 1}/{self._queue_total}:\n{msg}"
            )
            return False

        trace_csv_path = self._trace_csv_path(job, queue_index)

        worker = EncoderWorker(
            encoder_exe=self._config.encoder_path(),
            main_cfg=self._resolve_main_cfg(job.main_config),
            sequence_cfg=job.sequence_cfg,
            input_yuv=job.input_yuv,
            frames=job.frames,
            qp=job.qp,
            output_bin=job.output_bin,
            trace_file=trace_csv_path or None,
            trace_rule=TRACE_RULE_DEFAULT if trace_csv_path else None,
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

        self._config.add_recent_file("input", job.input_yuv)
        self._config.add_recent_file("output", job.output_bin)
        return True

    def _job_summary(self, job: EncodeJob) -> str:
        input_name = Path(job.input_yuv).name
        output_name = Path(job.output_bin).name
        return (
            f"Input: {input_name} | Output: {output_name} | "
            f"Config: {job.main_config} | Frames: {job.frames} | QP: {job.qp} | "
            f"Artifacts: {job.artifacts_dir}"
        )

    def _artifact_stem(self, job: EncodeJob, queue_index: int) -> str:
        stem = self._sanitize_name_part(Path(job.output_bin).stem)
        if not stem:
            stem = f"job_{queue_index + 1:02d}"
        return f"{queue_index + 1:02d}_{stem}"

    def _trace_csv_path(self, job: EncodeJob, queue_index: int) -> str:
        _, tracefiles_dir, _ = self._artifact_dirs_for_job(job)
        return str(tracefiles_dir / f"{self._artifact_stem(job, queue_index)}.csv")

    def _write_execution_report(
        self,
        job: EncodeJob,
        queue_index: int,
        success: bool,
        message: str,
        job_logs: list[str],
        metrics: dict,
    ) -> None:
        reports_dir, _, _ = self._artifact_dirs_for_job(job)
        report_path = reports_dir / f"{self._artifact_stem(job, queue_index)}.txt"
        status_text = "SUCCESS" if success else "FAILED"

        cmd_parts = [
            self._config.encoder_path(),
            "-c",
            self._resolve_main_cfg(job.main_config),
        ]
        if job.sequence_cfg:
            cmd_parts.extend(["-c", job.sequence_cfg])
        cmd_parts.extend([
            "-i",
            job.input_yuv,
            "-f",
            str(job.frames),
            "-q",
            str(job.qp),
            "-b",
            job.output_bin,
        ])
        trace_csv_path = self._trace_csv_path(job, queue_index)
        if trace_csv_path:
            cmd_parts.extend([
                f"--TraceFile={trace_csv_path}",
                f"--TraceRule={TRACE_RULE_DEFAULT}",
            ])

        report_lines = [
            "VTM Codec Studio - Execution Report",
            f"timestamp: {datetime.now().isoformat(timespec='seconds')}",
            f"queue_job: {queue_index + 1}/{self._queue_total}",
            f"status: {status_text}",
            f"summary: {message}",
            f"input_yuv: {job.input_yuv}",
            f"output_bin: {job.output_bin}",
            f"main_config: {job.main_config}",
            f"sequence_config: {job.sequence_cfg or '-'}",
            f"frames: {job.frames}",
            f"qp: {job.qp}",
            f"command: {' '.join(cmd_parts)}",
            "",
            "metrics:",
            f"time: {metrics.get('time', '-')}",
            f"psnr_y: {metrics.get('psnr_y', '-')}",
            f"psnr_u: {metrics.get('psnr_u', '-')}",
            f"psnr_v: {metrics.get('psnr_v', '-')}",
            f"psnr_yuv: {metrics.get('psnr_yuv', '-')}",
            f"bitrate: {metrics.get('bitrate', '-')}",
            f"ssim: {metrics.get('ssim', '-')}",
            f"entropy: {metrics.get('entropy', '-')}",
            f"size: {metrics.get('size', '-')}",
            "",
            "process_log:",
        ]
        report_lines.extend(job_logs)

        try:
            report_path.write_text("\n".join(report_lines), encoding="utf-8")
            self._log.append(f"📝 Report TXT saved: {report_path}")
        except OSError as exc:
            self._log.append(f"❌ Failed to write report TXT: {exc}")

    def _write_tracefile(self, job: EncodeJob, queue_index: int, _job_logs: list[str]) -> None:
        trace_csv = self._trace_csv_path(job, queue_index)
        if not trace_csv:
            return

        trace_path = Path(trace_csv)
        if trace_path.exists():
            self._log.append(f"📝 VTM Trace CSV saved: {trace_path}")
        else:
            self._log.append(f"⚠ VTM Trace CSV was not generated: {trace_path}")

    def _write_metrics_artifact(self, job: EncodeJob, queue_index: int, metrics: dict) -> None:
        _, _, metrics_dir = self._artifact_dirs_for_job(job)
        metrics_path = metrics_dir / f"{self._artifact_stem(job, queue_index)}.csv"
        try:
            write_metrics_csv(str(metrics_path), metrics)
            self._log.append(f"📝 Metrics CSV saved: {metrics_path}")
        except OSError as exc:
            self._log.append(f"❌ Failed to write metrics CSV: {exc}")

    def _queue_artifacts_count(self) -> int:
        unique_dirs: set[str] = set()
        for job in self._queue:
            if not job.artifacts_dir.strip():
                continue
            unique_dirs.add(str(Path(job.artifacts_dir).resolve()).casefold())
        return len(unique_dirs)

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

    def _cleanup_worker_state(self, worker: EncoderWorker) -> None:
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

    def _refresh_queue_view(self) -> None:
        previous_row = self._queue_list.currentRow()
        self._queue_list.clear()
        for idx, job in enumerate(self._queue, start=1):
            item = QListWidgetItem(f"{idx:02d}. {self._job_summary(job)}")
            self._queue_list.addItem(item)
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

    # ------------------------------------------------------------------
    # Encoding lifecycle
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
        artifacts_count = self._queue_artifacts_count()
        self._log.set_status(
            f"Queued {len(self._queue)} encode job(s) across {artifacts_count} artifact folder(s).",
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
            "Remove all jobs from the encode queue?",
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
            ok, msg = validate_directory(job.artifacts_dir, f"Artifacts folder (job {idx})")
            if not ok:
                QMessageBox.warning(self, "Validation Error", msg)
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

        self._save_state()
        self._log.clear()
        self._log_history.clear()
        self._log.set_progress(0)
        artifacts_count = self._queue_artifacts_count()
        self._log.set_status(
            (
                f"Starting queue ({self._queue_total} jobs, {artifacts_count} artifact folder(s), "
                f"max {self._max_parallel_jobs()} parallel)…"
            ),
            "#ffc857",
        )
        self._set_running(True)
        self._launch_more_queue_jobs()

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
    def _cancel_encoding(self) -> None:
        if not self._is_busy():
            return

        self._queue_cancel_requested = True
        self._log.append("\n⛔ Queue cancellation requested. Stopping active jobs…")
        for worker in list(self._workers.keys()):
            worker.cancel()
        self._update_running_status()

    def _handle_log_line(self, worker: EncoderWorker, line: str) -> None:
        queue_index = self._worker_job_index.get(worker, -1)
        if self._queue_running and queue_index >= 0:
            output_line = f"[Job {queue_index + 1:02d}] {line}"
        else:
            output_line = line

        self._log.append(output_line)
        self._log_history.append(output_line)

        if worker in self._worker_logs:
            self._worker_logs[worker].append(line)

    def _handle_worker_progress(self, worker: EncoderWorker, value: int) -> None:
        if worker in self._worker_progress:
            self._worker_progress[worker] = max(0, min(value, 100))
        self._update_overall_progress()

    def _on_started(self, worker: EncoderWorker) -> None:
        if worker not in self._workers:
            return
        self._update_running_status()

    def _on_finished(self, worker: EncoderWorker, success: bool, message: str) -> None:
        job = self._workers.get(worker)
        queue_index = self._worker_job_index.get(worker, -1)
        job_logs = self._worker_logs.get(worker, [])

        metrics = {}
        if job is not None:
            metrics = parse_vtm_log("\n".join(job_logs), job.output_bin)
            if success:
                self.metrics_ready.emit(metrics)

            self._write_execution_report(job, queue_index, success, message, job_logs, metrics)
            self._write_tracefile(job, queue_index, job_logs)
            self._write_metrics_artifact(job, queue_index, metrics)

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
        self._cancel_btn.setEnabled(running)
        self._save_preset_btn.setEnabled(not running)
        self._load_preset_btn.setEnabled(not running)
        self._delete_preset_btn.setEnabled(not running)
        self._save_profile_btn.setEnabled(not running)
        self._load_profile_btn.setEnabled(not running)
        self._delete_profile_btn.setEnabled(not running)
        self._artifacts_dir_picker.setEnabled(not running)
        self._parallel_spin.setEnabled(not running)
        self._update_queue_controls()

    # ------------------------------------------------------------------
    # Presets
    # ------------------------------------------------------------------

    def _refresh_presets(self) -> None:
        self._preset_combo.clear()
        self._preset_combo.addItems(list_presets())

    def _current_preset_data(self) -> dict:
        return {
            "input_yuv": self._input_picker.path(),
            "sequence_cfg": self._seq_cfg_picker.path(),
            "main_config": self._cfg_combo.currentText(),
            "frames": self._frames_edit.text(),
            "qp": self._qp_edit.text(),
            "output_dir": self._output_dir_picker.path(),
            "name_custom_enabled": self._name_custom_check.isChecked(),
            "name_custom_text": self._name_custom_edit.text(),
            "name_include_q": self._name_q_check.isChecked(),
            "name_include_frames": self._name_frames_check.isChecked(),
            "name_include_yuv": self._name_yuv_check.isChecked(),
            "artifacts_dir": self._artifacts_dir_picker.path(),
        }

    def _refresh_compression_profiles(self) -> None:
        self._profile_combo.clear()
        self._profile_combo.addItems(list_compression_profiles())

    def _current_compression_profile_data(self) -> dict:
        return {
            "main_config": self._cfg_combo.currentText(),
            "sequence_cfg": self._seq_cfg_picker.path(),
            "frames": self._frames_edit.text(),
            "qp": self._qp_edit.text(),
        }

    @Slot()
    def _save_preset(self) -> None:
        name, ok = QInputDialog.getText(self, "Save Preset", "Preset name:")
        if ok and name.strip():
            save_preset(name.strip(), self._current_preset_data())
            self._refresh_presets()
            idx = self._preset_combo.findText(name.strip())
            if idx >= 0:
                self._preset_combo.setCurrentIndex(idx)

    @Slot()
    def _load_preset(self) -> None:
        name = self._preset_combo.currentText()
        if not name:
            return
        data = load_preset(name)
        if data is None:
            QMessageBox.warning(self, "Error", f"Could not load preset '{name}'.")
            return
        self._input_picker.set_path(data.get("input_yuv", ""))
        self._seq_cfg_picker.set_path(data.get("sequence_cfg", ""))
        idx = self._cfg_combo.findText(data.get("main_config", ""))
        if idx >= 0:
            self._cfg_combo.setCurrentIndex(idx)
        self._frames_edit.setText(data.get("frames", ""))
        self._qp_edit.setText(data.get("qp", ""))

        output_dir = data.get("output_dir", "")
        if not output_dir:
            legacy_output = data.get("output_bin", "")
            if legacy_output:
                output_dir = str(Path(legacy_output).parent)
        self._output_dir_picker.set_path(output_dir)

        if "name_custom_enabled" in data:
            self._name_custom_check.setChecked(bool(data.get("name_custom_enabled", False)))
            self._name_custom_edit.setText(data.get("name_custom_text", ""))
            self._name_q_check.setChecked(bool(data.get("name_include_q", True)))
            self._name_frames_check.setChecked(bool(data.get("name_include_frames", True)))
            self._name_yuv_check.setChecked(bool(data.get("name_include_yuv", True)))
        else:
            legacy_output = data.get("output_bin", "")
            legacy_stem = Path(legacy_output).stem if legacy_output else ""
            self._name_custom_check.setChecked(True)
            self._name_custom_edit.setText(legacy_stem)
            self._name_q_check.setChecked(False)
            self._name_frames_check.setChecked(False)
            self._name_yuv_check.setChecked(False)

        artifacts_dir = data.get("artifacts_dir", "")
        if not artifacts_dir:
            legacy_metrics = data.get("metrics_csv_path", data.get("queue_single_csv_path", ""))
            if legacy_metrics:
                artifacts_dir = str(Path(legacy_metrics).parent)
        self._artifacts_dir_picker.set_path(artifacts_dir)
        self._toggle_custom_name(self._name_custom_check.isChecked())
        self._refresh_output_name_preview()

    @Slot()
    def _delete_preset(self) -> None:
        name = self._preset_combo.currentText()
        if not name:
            return
        confirm = QMessageBox.question(
            self, "Delete Preset",
            f"Delete preset '{name}'?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if confirm == QMessageBox.StandardButton.Yes:
            delete_preset(name)
            self._refresh_presets()

    @Slot()
    def _save_compression_profile(self) -> None:
        checks = [
            validate_positive_int(self._frames_edit.text(), "Frames"),
            validate_qp(self._qp_edit.text()),
        ]

        seq = self._seq_cfg_picker.path()
        if seq:
            checks.append(validate_file_exists(seq, "Sequence Config"))
            checks.append(validate_extension(seq, ".cfg", "Sequence Config"))

        for ok, msg in checks:
            if not ok:
                QMessageBox.warning(self, "Validation Error", msg)
                return

        name, ok = QInputDialog.getText(self, "Save Compression Profile", "Profile name:")
        if ok and name.strip():
            save_compression_profile(name.strip(), self._current_compression_profile_data())
            self._refresh_compression_profiles()
            idx = self._profile_combo.findText(name.strip())
            if idx >= 0:
                self._profile_combo.setCurrentIndex(idx)

    @Slot()
    def _load_compression_profile(self) -> None:
        name = self._profile_combo.currentText()
        if not name:
            return

        data = load_compression_profile(name)
        if data is None:
            QMessageBox.warning(self, "Error", f"Could not load compression profile '{name}'.")
            return

        idx = self._cfg_combo.findText(data.get("main_config", ""))
        if idx >= 0:
            self._cfg_combo.setCurrentIndex(idx)
        self._seq_cfg_picker.set_path(data.get("sequence_cfg", ""))
        self._frames_edit.setText(data.get("frames", ""))
        self._qp_edit.setText(data.get("qp", ""))

    @Slot()
    def _delete_compression_profile(self) -> None:
        name = self._profile_combo.currentText()
        if not name:
            return

        confirm = QMessageBox.question(
            self,
            "Delete Compression Profile",
            f"Delete compression profile '{name}'?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if confirm == QMessageBox.StandardButton.Yes:
            delete_compression_profile(name)
            self._refresh_compression_profiles()

