"""
Settings Dialog
===============
Modal dialog for configuring VTM executable and configuration paths.
Settings are persisted through ConfigManager.
"""

from PySide6.QtCore import Slot
from PySide6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QPushButton,
    QVBoxLayout,
)

from ui.widgets import FilePickerRow
from utils.config import ConfigManager
from utils.validators import validate_directory, validate_executable


class SettingsDialog(QDialog):
    """
    Modal dialog where the user configures:
      - VTM root folder
      - cfg folder (where .cfg files live)
      - Encoder executable path
      - Decoder executable path
            - YUView executable path
            - FFmpeg executable path
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Settings — VTM Codec Studio")
        self.setMinimumWidth(700)
        self.setMinimumHeight(650)
        self._config = ConfigManager()

        self._build_ui()
        self._load_current()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setSpacing(16)
        layout.setContentsMargins(24, 24, 24, 24)

        # ── Title ──
        title = QLabel("⚙  Application Settings")
        title.setStyleSheet("font-size: 18px; font-weight: 700;")
        layout.addWidget(title)

        desc = QLabel("Configure paths to VTM executables, FFmpeg, YUView, and configuration files.")
        desc.setStyleSheet("font-size: 13px; color: #8a90a4; margin-bottom: 8px;")
        layout.addWidget(desc)

        # ── Paths group ──
        paths_group = QGroupBox("VTM Paths")
        paths_layout = QVBoxLayout(paths_group)
        paths_layout.setSpacing(12)

        self._vtm_root = FilePickerRow(
            "VTM Root Folder:",
            placeholder="Root directory of VTM installation",
            mode="directory",
        )
        paths_layout.addWidget(self._vtm_root)

        self._cfg_folder = FilePickerRow(
            "Config Folder:",
            placeholder="Folder containing .cfg files",
            mode="directory",
        )
        paths_layout.addWidget(self._cfg_folder)

        layout.addWidget(paths_group)

        # ── Executables group ──
        exe_group = QGroupBox("Executables")
        exe_layout = QVBoxLayout(exe_group)
        exe_layout.setSpacing(12)

        self._encoder_exe = FilePickerRow(
            "Encoder (.exe):",
            file_filter="Executable (*.exe);;All Files (*)",
            placeholder="Path to EncoderAppStatic.exe",
        )
        exe_layout.addWidget(self._encoder_exe)

        self._decoder_exe = FilePickerRow(
            "Decoder (.exe):",
            file_filter="Executable (*.exe);;All Files (*)",
            placeholder="Path to DecoderAppStatic.exe",
        )
        exe_layout.addWidget(self._decoder_exe)

        self._yuview_exe = FilePickerRow(
            "YUView (.exe):",
            file_filter="Executable (*.exe);;All Files (*)",
            placeholder="Path to YUView executable",
        )
        exe_layout.addWidget(self._yuview_exe)

        self._ffmpeg_exe = FilePickerRow(
            "FFmpeg (.exe):",
            file_filter="Executable (*.exe);;All Files (*)",
            placeholder="Path to ffmpeg.exe",
        )
        exe_layout.addWidget(self._ffmpeg_exe)

        layout.addWidget(exe_group)

        layout.addStretch()

        # ── Buttons ──
        btn_row = QHBoxLayout()

        reset_btn = QPushButton("Reset to Defaults")
        reset_btn.setObjectName("dangerButton")
        reset_btn.clicked.connect(self._reset)
        btn_row.addWidget(reset_btn)

        btn_row.addStretch()

        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Save | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self._save)
        button_box.rejected.connect(self.reject)
        btn_row.addWidget(button_box)

        layout.addLayout(btn_row)

    def _load_current(self) -> None:
        """Populate fields from current config."""
        cfg = self._config
        self._vtm_root.set_path(cfg.get("vtm_root_folder", ""))
        self._cfg_folder.set_path(cfg.get("cfg_folder", ""))
        self._encoder_exe.set_path(cfg.get("encoder_executable", ""))
        self._decoder_exe.set_path(cfg.get("decoder_executable", ""))
        self._yuview_exe.set_path(cfg.get("yuview_executable", ""))
        self._ffmpeg_exe.set_path(cfg.get("ffmpeg_executable", ""))

    @Slot()
    def _save(self) -> None:
        """Validate and persist settings."""
        # Light validation — warn but don't block
        warnings = []
        enc = self._encoder_exe.path()
        dec = self._decoder_exe.path()
        yuview = self._yuview_exe.path()
        ffmpeg = self._ffmpeg_exe.path()
        cfg = self._cfg_folder.path()

        if enc:
            ok, msg = validate_executable(enc, "Encoder")
            if not ok:
                warnings.append(msg)
        if dec:
            ok, msg = validate_executable(dec, "Decoder")
            if not ok:
                warnings.append(msg)
        if yuview:
            ok, msg = validate_executable(yuview, "YUView")
            if not ok:
                warnings.append(msg)
        if ffmpeg:
            ok, msg = validate_executable(ffmpeg, "FFmpeg")
            if not ok:
                warnings.append(msg)
        if cfg:
            ok, msg = validate_directory(cfg, "Config Folder")
            if not ok:
                warnings.append(msg)

        if warnings:
            proceed = QMessageBox.warning(
                self, "Validation Warnings",
                "Some paths may be invalid:\n\n" + "\n".join(warnings) +
                "\n\nSave anyway?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            )
            if proceed != QMessageBox.StandardButton.Yes:
                return

        self._config.update({
            "vtm_root_folder": self._vtm_root.path(),
            "cfg_folder": self._cfg_folder.path(),
            "encoder_executable": self._encoder_exe.path(),
            "decoder_executable": self._decoder_exe.path(),
            "yuview_executable": self._yuview_exe.path(),
            "ffmpeg_executable": self._ffmpeg_exe.path(),
        })
        self.accept()

    @Slot()
    def _reset(self) -> None:
        confirm = QMessageBox.question(
            self, "Reset Settings",
            "This will restore all settings to their defaults.\nContinue?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if confirm == QMessageBox.StandardButton.Yes:
            self._config.reset()
            self._load_current()
