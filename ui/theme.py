"""
Dark Theme
==========
A premium dark-mode stylesheet for PySide6, inspired by modern IDEs
and video-editing software. Uses a curated color palette with subtle
gradients and accent highlights.
"""

# ── Colour Palette ──────────────────────────────────────────────────
BG_DARKEST   = "#0d0f12"
BG_DARK      = "#141720"
BG_MID       = "#1a1e2a"
BG_LIGHT     = "#232838"
BG_LIGHTER   = "#2c3247"
BORDER       = "#353b50"
BORDER_FOCUS = "#6c8cff"
TEXT_PRIMARY  = "#e6e9f0"
TEXT_SECONDARY= "#8a90a4"
TEXT_DISABLED = "#4e5366"
ACCENT       = "#6c8cff"
ACCENT_HOVER = "#8da6ff"
ACCENT_PRESSED = "#5070e0"
SUCCESS      = "#4cda8a"
ERROR        = "#ff6b7a"
WARNING      = "#ffc857"
PROGRESS_BG  = "#1a1e2a"
SCROLL_HANDLE= "#3a4060"
SCROLL_HOVER = "#4d5580"


def get_stylesheet() -> str:
    """Return the complete QSS stylesheet string."""
    return f"""
    /* ── Global ──────────────────────────────────────────── */
    * {{
        font-family: "Segoe UI", "Inter", "Roboto", sans-serif;
        font-size: 13px;
        color: {TEXT_PRIMARY};
    }}

    QMainWindow, QDialog {{
        background-color: {BG_DARKEST};
    }}

    QWidget {{
        background-color: transparent;
    }}

    /* ── Menu Bar ────────────────────────────────────────── */
    QMenuBar {{
        background-color: {BG_DARK};
        border-bottom: 1px solid {BORDER};
        padding: 2px 0;
    }}
    QMenuBar::item {{
        padding: 6px 14px;
        border-radius: 4px;
    }}
    QMenuBar::item:selected {{
        background-color: {BG_LIGHTER};
    }}
    QMenu {{
        background-color: {BG_MID};
        border: 1px solid {BORDER};
        border-radius: 6px;
        padding: 4px;
    }}
    QMenu::item {{
        padding: 8px 28px 8px 14px;
        border-radius: 4px;
    }}
    QMenu::item:selected {{
        background-color: {ACCENT};
        color: #ffffff;
    }}

    /* ── Tab Widget ──────────────────────────────────────── */
    QTabWidget::pane {{
        border: 1px solid {BORDER};
        border-radius: 8px;
        background-color: {BG_DARK};
        top: -1px;
    }}
    QTabBar::tab {{
        background-color: {BG_MID};
        border: 1px solid {BORDER};
        border-bottom: none;
        padding: 10px 28px;
        margin-right: 2px;
        border-top-left-radius: 8px;
        border-top-right-radius: 8px;
        color: {TEXT_SECONDARY};
        font-weight: 600;
    }}
    QTabBar::tab:selected {{
        background-color: {BG_DARK};
        color: {ACCENT};
        border-bottom: 2px solid {ACCENT};
    }}
    QTabBar::tab:hover:!selected {{
        background-color: {BG_LIGHT};
        color: {TEXT_PRIMARY};
    }}

    /* ── Group Box ───────────────────────────────────────── */
    QGroupBox {{
        background-color: {BG_MID};
        border: 1px solid {BORDER};
        border-radius: 10px;
        margin-top: 18px;
        padding: 20px 16px 14px 16px;
        font-weight: 700;
        font-size: 13px;
    }}
    QGroupBox::title {{
        subcontrol-origin: margin;
        subcontrol-position: top left;
        left: 16px;
        padding: 0 8px;
        color: {ACCENT};
    }}

    /* ── Labels ──────────────────────────────────────────── */
    QLabel {{
        color: {TEXT_PRIMARY};
        padding: 1px 0;
    }}

    /* ── Line Edit / Spin Box ────────────────────────────── */
    QLineEdit, QSpinBox {{
        background-color: {BG_LIGHT};
        border: 1px solid {BORDER};
        border-radius: 6px;
        padding: 8px 12px;
        color: {TEXT_PRIMARY};
        selection-background-color: {ACCENT};
    }}
    QLineEdit:focus, QSpinBox:focus {{
        border-color: {BORDER_FOCUS};
    }}
    QLineEdit:disabled, QSpinBox:disabled {{
        color: {TEXT_DISABLED};
        background-color: {BG_DARKEST};
    }}

    /* ── Check Box ──────────────────────────────────────── */
    QCheckBox {{
        spacing: 8px;
        color: {TEXT_PRIMARY};
        font-weight: 600;
    }}
    QCheckBox::indicator {{
        width: 16px;
        height: 16px;
        border: 1px solid {BORDER};
        border-radius: 4px;
        background-color: {BG_LIGHT};
    }}
    QCheckBox::indicator:checked {{
        background-color: {ACCENT};
        border-color: {ACCENT};
    }}

    /* Highlighted metrics-export toggle */
    QCheckBox#metricsCsvToggle {{
        font-size: 14px;
        font-weight: 800;
        color: #ffffff;
        background-color: {BG_LIGHTER};
        border: 1px solid {ACCENT};
        border-radius: 8px;
        padding: 8px 12px;
    }}
    QCheckBox#metricsCsvToggle:hover {{
        background-color: {BORDER};
    }}
    QCheckBox#metricsCsvToggle:disabled {{
        color: {TEXT_DISABLED};
        border-color: {BORDER};
        background-color: {BG_DARK};
    }}
    QCheckBox#metricsCsvToggle::indicator {{
        width: 20px;
        height: 20px;
        border: 1px solid {ACCENT};
        border-radius: 5px;
        background-color: {BG_DARK};
    }}
    QCheckBox#metricsCsvToggle::indicator:checked {{
        background-color: {ACCENT};
        border-color: {ACCENT_HOVER};
    }}

    /* ── Combo Box ───────────────────────────────────────── */
    QComboBox {{
        background-color: {BG_LIGHT};
        border: 1px solid {BORDER};
        border-radius: 6px;
        padding: 8px 12px;
        color: {TEXT_PRIMARY};
        min-width: 160px;
    }}
    QComboBox:focus {{
        border-color: {BORDER_FOCUS};
    }}
    QComboBox::drop-down {{
        border: none;
        width: 30px;
    }}
    QComboBox::down-arrow {{
        image: none;
        border-left: 5px solid transparent;
        border-right: 5px solid transparent;
        border-top: 6px solid {TEXT_SECONDARY};
        margin-right: 10px;
    }}
    QComboBox QAbstractItemView {{
        background-color: {BG_MID};
        border: 1px solid {BORDER};
        border-radius: 6px;
        selection-background-color: {ACCENT};
        selection-color: #ffffff;
        padding: 4px;
    }}

    /* ── Push Button ─────────────────────────────────────── */
    QPushButton {{
        background-color: {BG_LIGHTER};
        border: 1px solid {BORDER};
        border-radius: 6px;
        padding: 9px 22px;
        color: {TEXT_PRIMARY};
        font-weight: 600;
    }}
    QPushButton:hover {{
        background-color: {BORDER};
        border-color: {TEXT_SECONDARY};
    }}
    QPushButton:pressed {{
        background-color: {BG_MID};
    }}
    QPushButton:disabled {{
        color: {TEXT_DISABLED};
        background-color: {BG_DARKEST};
        border-color: {BG_MID};
    }}

    /* Primary action button (uses object name) */
    QPushButton#primaryButton {{
        background-color: {ACCENT};
        border: none;
        color: #ffffff;
        padding: 11px 32px;
        font-size: 14px;
        font-weight: 700;
        border-radius: 8px;
    }}
    QPushButton#primaryButton:hover {{
        background-color: {ACCENT_HOVER};
    }}
    QPushButton#primaryButton:pressed {{
        background-color: {ACCENT_PRESSED};
    }}
    QPushButton#primaryButton:disabled {{
        background-color: {BG_LIGHTER};
        color: {TEXT_DISABLED};
    }}

    /* Danger button */
    QPushButton#dangerButton {{
        background-color: transparent;
        border: 1px solid {ERROR};
        color: {ERROR};
    }}
    QPushButton#dangerButton:hover {{
        background-color: {ERROR};
        color: #ffffff;
    }}

    /* ── Text Edit (log panel) ───────────────────────────── */
    QPlainTextEdit, QTextEdit {{
        background-color: {BG_DARKEST};
        border: 1px solid {BORDER};
        border-radius: 8px;
        padding: 10px;
        font-family: "Cascadia Code", "Consolas", "Fira Code", monospace;
        font-size: 12px;
        color: {TEXT_PRIMARY};
        selection-background-color: {ACCENT};
    }}

    /* ── Progress Bar ────────────────────────────────────── */
    QProgressBar {{
        background-color: {PROGRESS_BG};
        border: 1px solid {BORDER};
        border-radius: 6px;
        text-align: center;
        color: {TEXT_PRIMARY};
        font-weight: 600;
        height: 22px;
    }}
    QProgressBar::chunk {{
        background: qlineargradient(
            x1:0, y1:0, x2:1, y2:0,
            stop:0 {ACCENT}, stop:1 {ACCENT_HOVER}
        );
        border-radius: 5px;
    }}

    /* ── Scroll Bars ─────────────────────────────────────── */
    QScrollBar:vertical {{
        background-color: transparent;
        width: 10px;
        margin: 0;
    }}
    QScrollBar::handle:vertical {{
        background-color: {SCROLL_HANDLE};
        border-radius: 5px;
        min-height: 30px;
    }}
    QScrollBar::handle:vertical:hover {{
        background-color: {SCROLL_HOVER};
    }}
    QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{
        height: 0;
    }}
    QScrollBar:horizontal {{
        background-color: transparent;
        height: 10px;
    }}
    QScrollBar::handle:horizontal {{
        background-color: {SCROLL_HANDLE};
        border-radius: 5px;
        min-width: 30px;
    }}
    QScrollBar::handle:horizontal:hover {{
        background-color: {SCROLL_HOVER};
    }}
    QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {{
        width: 0;
    }}

    /* ── Status Bar ──────────────────────────────────────── */
    QStatusBar {{
        background-color: {BG_DARK};
        border-top: 1px solid {BORDER};
        color: {TEXT_SECONDARY};
        font-size: 12px;
        padding: 4px 8px;
    }}

    /* ── Tool Tips ────────────────────────────────────────── */
    QToolTip {{
        background-color: {BG_LIGHT};
        border: 1px solid {BORDER};
        border-radius: 4px;
        padding: 6px 10px;
        color: {TEXT_PRIMARY};
        font-size: 12px;
    }}

    /* ── Splitter ─────────────────────────────────────────── */
    QSplitter::handle {{
        background-color: {BORDER};
    }}
    QSplitter::handle:horizontal {{
        width: 2px;
    }}
    QSplitter::handle:vertical {{
        height: 2px;
    }}
    """
