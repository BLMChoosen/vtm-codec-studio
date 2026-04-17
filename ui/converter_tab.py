"""
Converter Tab
=============
UI panel for converting .y4m files to .yuv and generating sequence config files.
"""

from pathlib import Path
from typing import Optional

from PySide6.QtCore import Qt, Slot
from PySide6.QtWidgets import (
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QVBoxLayout,
    QWidget,
)

from core.converter import ConverterWorker
from ui.widgets import FilePickerRow, LogPanel
from utils.config import ConfigManager
from utils.validators import (
    validate_executable,
    validate_extension,
    validate_file_exists,
    validate_output_path,
)


class ConverterTab(QWidget):
    """Widget containing Y4M -> YUV conversion and sequence cfg generation."""

    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self._config = ConfigManager()
        self._worker: Optional[ConverterWorker] = None

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

        btn_row = QHBoxLayout()
        btn_row.addStretch()

        self._start_btn = QPushButton("▶  Convert")
        self._start_btn.setObjectName("primaryButton")
        self._start_btn.setMinimumWidth(150)
        self._start_btn.setMinimumHeight(42)
        self._start_btn.clicked.connect(self._start_conversion)
        btn_row.addWidget(self._start_btn)

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

    def _is_busy(self) -> bool:
        return self._worker is not None

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

    @Slot(str)
    def _on_input_changed(self, input_path: str) -> None:
        if not input_path:
            return

        source = Path(input_path)
        self._output_picker.set_path(str(source.with_suffix(".yuv")))
        self._sequence_cfg_picker.set_path(str(source.with_name(f"{source.stem}_sequence.cfg")))

    @Slot()
    def _start_conversion(self) -> None:
        if self._is_busy():
            return

        ok, msg = self._validate_form()
        if not ok:
            QMessageBox.warning(self, "Validation Error", msg)
            return

        self._worker = ConverterWorker(
            ffmpeg_exe=self._config.ffmpeg_path(),
            input_y4m=self._input_picker.path(),
            output_yuv=self._output_picker.path(),
            sequence_cfg_output=self._sequence_cfg_picker.path(),
            level=self._level_edit.text().strip(),
        )

        self._worker.signals.log_line.connect(self._on_log_line)
        self._worker.signals.progress.connect(self._log.set_progress)
        self._worker.signals.started.connect(self._on_started)
        self._worker.signals.finished.connect(self._on_finished)

        self._config.add_recent_file("input", self._input_picker.path())
        self._config.add_recent_file("output", self._output_picker.path())

        self._log.clear()
        self._log.set_progress(0)
        self._log.set_status("Starting conversion...", "#ffc857")
        self._set_running(True)
        self._worker.start()

    @Slot()
    def _cancel_conversion(self) -> None:
        if self._worker is None:
            return
        self._log.append("\n⛔ Cancellation requested...")
        self._worker.cancel()

    @Slot()
    def _on_started(self) -> None:
        self._log.set_status("⏳ Conversion running...", "#ffc857")

    @Slot(str)
    def _on_log_line(self, line: str) -> None:
        self._log.append(line)

    @Slot(bool, str)
    def _on_finished(self, success: bool, message: str) -> None:
        self._set_running(False)

        if success:
            self._log.set_status(f"✅ {message}", "#4cda8a")
            self._log.set_progress(100)
        else:
            if "cancel" in message.lower():
                self._log.set_status(f"⛔ {message}", "#ffc857")
            else:
                self._log.set_status(f"❌ {message}", "#ff6b7a")

        self._worker = None

    def _set_running(self, running: bool) -> None:
        self._start_btn.setEnabled(not running)
        self._cancel_btn.setEnabled(running)
