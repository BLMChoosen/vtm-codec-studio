"""
VTM Codec Studio — Main Entry Point
====================================
Launch the PySide6 application with the dark theme applied.

Usage:
    python main.py
"""

import sys

from PySide6.QtWidgets import QApplication

from ui.main_window import MainWindow
from ui.theme import get_stylesheet


def main() -> None:
    """Initialise the Qt application and show the main window."""
    app = QApplication(sys.argv)

    # Apply the global dark stylesheet
    app.setStyleSheet(get_stylesheet())

    # Set application metadata
    app.setApplicationName("VTM Codec Studio")
    app.setOrganizationName("VTMCodecStudio")
    app.setApplicationVersion("1.0.0")

    window = MainWindow()
    window.show()

    sys.exit(app.exec())


if __name__ == "__main__":
    main()
