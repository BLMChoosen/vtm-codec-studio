"""
Custom Widgets
==============
Reusable, polished UI components used across the application.
"""

from pathlib import Path
from typing import Optional

from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QDragEnterEvent, QDropEvent, QWheelEvent
from PySide6.QtWidgets import (
    QComboBox,
    QAbstractSpinBox,
    QSpinBox,
    QDoubleSpinBox,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

# ─────────────────────────────────────────────────────────────────────
# Scroll-Safe Input Widgets
# ─────────────────────────────────────────────────────────────────────
class ScrollSafeComboBox(QComboBox):
    def wheelEvent(self, event: QWheelEvent) -> None:
        event.ignore()

class ScrollSafeSpinBox(QSpinBox):
    def wheelEvent(self, event: QWheelEvent) -> None:
        event.ignore()

class ScrollSafeDoubleSpinBox(QDoubleSpinBox):
    def wheelEvent(self, event: QWheelEvent) -> None:
        event.ignore()



# ─────────────────────────────────────────────────────────────────────
# File Picker Row
# ─────────────────────────────────────────────────────────────────────
class FilePickerRow(QWidget):
    """
    Horizontal row with a label, read-only line edit, and a Browse button.
    Supports drag-and-drop of files directly onto the line edit.
    """

    path_changed = Signal(str)

    def __init__(
        self,
        label: str,
        file_filter: str = "All Files (*)",
        placeholder: str = "",
        mode: str = "open",          # "open" or "save"
        parent: Optional[QWidget] = None,
    ):
        super().__init__(parent)
        self._filter = file_filter
        self._mode = mode

        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        # Label
        lbl = QLabel(label)
        lbl.setMinimumWidth(160)
        lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        layout.addWidget(lbl)

        # Line edit with drag-and-drop
        self._edit = _DroppableLineEdit(placeholder)
        self._edit.setReadOnly(True)
        self._edit.drop_received.connect(self._on_drop)
        layout.addWidget(self._edit, stretch=1)

        # Browse button
        btn = QPushButton("Browse…")
        btn.setFixedWidth(100)
        btn.clicked.connect(self._browse)
        layout.addWidget(btn)

    # ── Public API ──
    def path(self) -> str:
        return self._edit.text().strip()

    def set_path(self, path: str) -> None:
        self._edit.setText(path)
        self.path_changed.emit(path)

    # ── Private ──
    def _browse(self) -> None:
        if self._mode == "directory":
            path = QFileDialog.getExistingDirectory(self, "Select Folder", "")
        elif self._mode == "save":
            path, _ = QFileDialog.getSaveFileName(self, "Save File", "", self._filter)
        else:
            path, _ = QFileDialog.getOpenFileName(self, "Select File", "", self._filter)
        if path:
            self.set_path(path)

    def _on_drop(self, filepath: str) -> None:
        self.set_path(filepath)


class _DroppableLineEdit(QLineEdit):
    """Line edit that accepts file drops."""

    drop_received = Signal(str)

    def __init__(self, placeholder: str = "", parent=None):
        super().__init__(parent)
        self.setPlaceholderText(placeholder)
        self.setAcceptDrops(True)

    def dragEnterEvent(self, event: QDragEnterEvent) -> None:
        if event.mimeData().hasUrls():
            event.acceptProposedAction()
        else:
            super().dragEnterEvent(event)

    def dropEvent(self, event: QDropEvent) -> None:
        urls = event.mimeData().urls()
        if urls:
            filepath = urls[0].toLocalFile()
            self.setText(filepath)
            self.drop_received.emit(filepath)


# ─────────────────────────────────────────────────────────────────────
# Log Panel
# ─────────────────────────────────────────────────────────────────────
class LogPanel(QWidget):
    """
    A scrollable terminal-style log panel with a clear button,
    progress bar, and status label.
    """

    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)

        # ── Header row: title + clear button ──
        header = QHBoxLayout()
        title = QLabel("📋  Output Log")
        title.setStyleSheet("font-weight: 700; font-size: 14px;")
        header.addWidget(title)
        header.addStretch()

        self._status_label = QLabel("")
        self._status_label.setStyleSheet("font-size: 12px;")
        header.addWidget(self._status_label)

        self._clear_btn = QPushButton("Clear")
        self._clear_btn.clicked.connect(self.clear)
        header.addWidget(self._clear_btn)
        layout.addLayout(header)

        # ── Progress bar ──
        self._progress = QProgressBar()
        self._progress.setRange(0, 100)
        self._progress.setValue(0)
        self._progress.setTextVisible(True)
        self._progress.setFormat("%p%")
        layout.addWidget(self._progress)

        # ── Log text ──
        self._text = QPlainTextEdit()
        self._text.setReadOnly(True)
        self._text.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)
        self._text.setMaximumBlockCount(10_000)  # Prevent unbounded memory
        layout.addWidget(self._text, stretch=1)

    # ── Public API ──

    def append(self, text: str) -> None:
        """Append a line and auto-scroll to the bottom."""
        self._text.appendPlainText(text)
        scrollbar = self._text.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    def set_progress(self, value: int) -> None:
        self._progress.setValue(value)

    def set_status(self, text: str, color: str = "") -> None:
        style = f"font-size: 12px; color: {color};" if color else "font-size: 12px;"
        self._status_label.setStyleSheet(style)
        self._status_label.setText(text)

    def clear(self) -> None:
        self._text.clear()
        self._progress.setValue(0)
        self._status_label.setText("")

    def set_indeterminate(self, active: bool) -> None:
        """Switch progress bar to indeterminate (bouncing) mode."""
        if active:
            self._progress.setRange(0, 0)
        else:
            self._progress.setRange(0, 100)
