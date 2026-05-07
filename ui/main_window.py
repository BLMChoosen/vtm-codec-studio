"""
Main Window
===========
The root application window containing the tab interface,
menu bar, status bar, and global actions.
"""

from PySide6.QtCore import QSize, Qt, Slot
from PySide6.QtGui import QAction, QIcon
from PySide6.QtWidgets import (
    QApplication,
    QLabel,
    QMainWindow,
    QMenu,
    QMenuBar,
    QMessageBox,
    QStatusBar,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from ui.dataset_tab import DatasetTab
from ui.decoder_tab import DecoderTab
from ui.encoder_tab import EncoderTab
from ui.converter_tab import ConverterTab
from ui.variance_maps_tab import VarianceMapsTab
from ui.settings_dialog import SettingsDialog
from ui.theme import ACCENT, BG_DARK
from utils.config import ConfigManager


class MainWindow(QMainWindow):
    """
    Top-level application window.
    Hosts Encoder and Decoder tabs, a menu bar with settings/about,
    and a styled status bar.
    """

    def __init__(self):
        super().__init__()
        self._config = ConfigManager()

        self.setWindowTitle("VTM Codec Studio")
        self.setMinimumSize(QSize(960, 700))

        # Restore geometry if saved
        geom = self._config.get("window_geometry")
        if geom:
            try:
                self.resize(geom["w"], geom["h"])
                self.move(geom["x"], geom["y"])
            except (TypeError, KeyError):
                self._centre()
        else:
            self._centre()

        self._build_menu_bar()
        self._build_central()
        self._build_status_bar()
        self.showMaximized()

    # ------------------------------------------------------------------
    # Construction helpers
    # ------------------------------------------------------------------

    def _centre(self) -> None:
        """Centre the window on screen."""
        self.resize(1060, 780)

    def _build_menu_bar(self) -> None:
        menu_bar = self.menuBar()

        # ── File menu ──
        file_menu = menu_bar.addMenu("&File")

        recent_menu = QMenu("Recent Files", self)
        self._recent_menu = recent_menu
        self._populate_recent_menu()
        file_menu.addMenu(recent_menu)

        file_menu.addSeparator()

        settings_action = QAction("⚙  Settings…", self)
        settings_action.setShortcut("Ctrl+,")
        settings_action.triggered.connect(self._open_settings)
        file_menu.addAction(settings_action)

        file_menu.addSeparator()

        exit_action = QAction("Exit", self)
        exit_action.setShortcut("Ctrl+Q")
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)

        # ── Help menu ──
        help_menu = menu_bar.addMenu("&Help")

        about_action = QAction("About…", self)
        about_action.triggered.connect(self._show_about)
        help_menu.addAction(about_action)

        about_qt_action = QAction("About Qt…", self)
        about_qt_action.triggered.connect(lambda: QMessageBox.aboutQt(self))
        help_menu.addAction(about_qt_action)

    def _build_central(self) -> None:
        central = QWidget()
        layout = QVBoxLayout(central)
        layout.setContentsMargins(8, 8, 8, 4)
        layout.setSpacing(0)

        # ── Header banner ──
        header = QLabel("VTM Codec Studio")
        header.setAlignment(Qt.AlignmentFlag.AlignCenter)
        header.setStyleSheet(f"""
            font-size: 22px;
            font-weight: 800;
            letter-spacing: 1px;
            color: {ACCENT};
            padding: 14px 0 6px 0;
        """)
        layout.addWidget(header)

        subtitle = QLabel("Versatile Video Coding — Encode, Decode & Convert")
        subtitle.setAlignment(Qt.AlignmentFlag.AlignCenter)
        subtitle.setStyleSheet("font-size: 13px; color: #8a90a4; padding-bottom: 10px;")
        layout.addWidget(subtitle)

        # ── Tabs ──
        self._tabs = QTabWidget()
        self._encoder_tab = EncoderTab()
        self._decoder_tab = DecoderTab()
        self._converter_tab = ConverterTab()
        self._variance_maps_tab = VarianceMapsTab()
        self._dataset_tab = DatasetTab()

        self._tabs.addTab(self._encoder_tab,      "🎬  Encoder")
        self._tabs.addTab(self._decoder_tab,      "📼  Decoder")
        self._tabs.addTab(self._converter_tab,    "🔁  Converter")
        self._tabs.addTab(self._variance_maps_tab, "📊  Variance Maps")
        self._tabs.addTab(self._dataset_tab,      "🗄  Criar Dataset")
        layout.addWidget(self._tabs, stretch=1)

        self.setCentralWidget(central)

    def _build_status_bar(self) -> None:
        status = QStatusBar()
        status.showMessage("Ready")
        self.setStatusBar(status)

    # ------------------------------------------------------------------
    # Menu actions
    # ------------------------------------------------------------------

    def _populate_recent_menu(self) -> None:
        self._recent_menu.clear()
        recent = self._config.get_recent_files("input")
        if not recent:
            action = QAction("(no recent files)", self)
            action.setEnabled(False)
            self._recent_menu.addAction(action)
        else:
            for filepath in recent[:10]:
                action = QAction(filepath, self)
                # Capture filepath in lambda's default arg
                action.triggered.connect(lambda checked, f=filepath: self._open_recent(f))
                self._recent_menu.addAction(action)

    @Slot()
    def _open_settings(self) -> None:
        dialog = SettingsDialog(self)
        dialog.exec()

    @Slot()
    def _show_about(self) -> None:
        QMessageBox.about(
            self,
            "About VTM Codec Studio",
            "<h2>VTM Codec Studio</h2>"
            "<p>A modern desktop application for encoding and decoding "
            "VVC (Versatile Video Coding) bitstreams using the VTM reference software.</p>"
            "<p><b>Features:</b></p>"
            "<ul>"
            "<li>VTM Encoder with full parameter control</li>"
            "<li>Queue multiple encode jobs</li>"
            "<li>Queue multiple decode jobs with real-time logs</li>"
            "<li>Queue multiple .y4m conversions to .yuv and sequence config</li>"
            "<li>TXT reports, tracefiles and CSV metrics in Encoder</li>"
            "<li>CSV output reports in Decoder</li>"
            "<li>Project presets and compression profiles</li>"
            "<li>YUView preview for decoded YUV</li>"
            "<li>Drag-and-drop support</li>"
            "</ul>"
            "<p style='color: #8a90a4; font-size: 11px;'>Built with Python &amp; PySide6</p>"
        )

    def _open_recent(self, filepath: str) -> None:
        """Set the recent file in the currently active tab's input picker."""
        current = self._tabs.currentWidget()
        if hasattr(current, "_input_picker"):
            current._input_picker.set_path(filepath)

    # ------------------------------------------------------------------
    # Window lifecycle
    # ------------------------------------------------------------------

    def closeEvent(self, event) -> None:
        """Save window geometry on close."""
        geo = self.geometry()
        self._config.set("window_geometry", {
            "x": geo.x(), "y": geo.y(),
            "w": geo.width(), "h": geo.height(),
        })
        super().closeEvent(event)
