"""
Analysis Tab
============
Turns a single experiment folder (the ``optimized`` **or** ``baseline`` tree
produced by the Comparison tab) into averaged charts and summary text.

The only input is the experiment folder. Inside it the tab expects
``Low_Delay`` / ``Random_Access`` → ``QP<n>`` → ``rep_*`` → ``*.bin`` +
``*.report``. Every feature shown is the **mean across the repetitions** of one
(config, QP) experiment.

It renders RD curves with BD-Rate/BD-PSNR (the sibling baseline/optimized folder
is detected automatically for the anchor curve), YUV-PSNR, bitrate, execution
time, total time, the decision-tree time (``DT_MODEL``) and a per-stage Time
Profile breakdown. A config selector toggles Low Delay, Random Access or both
overlaid, and the PSNR component (Y/U/V/YUV) is selectable.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Callable, Optional

os.environ.setdefault("QT_API", "pyside6")

import matplotlib
matplotlib.use("QtAgg")
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qtagg import NavigationToolbar2QT as NavigationToolbar
from matplotlib.figure import Figure

from PySide6.QtCore import Qt, Slot
from PySide6.QtGui import QGuiApplication
from PySide6.QtWidgets import (
    QComboBox,
    QFileDialog,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QScrollArea,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from core.analysis import (
    CONFIG_INFO,
    AnalysisResult,
    ExperimentData,
    QPPoint,
    analyze_folder,
    bd_psnr,
    bd_rate,
)
from ui.theme import (
    ACCENT,
    BG_DARK,
    BG_DARKEST,
    BORDER,
    ERROR,
    SUCCESS,
    TEXT_PRIMARY,
    TEXT_SECONDARY,
    WARNING,
)
from ui.widgets import FilePickerRow
from utils.config import ConfigManager
from utils.validators import validate_directory


# Curve colours: the selected/optimized experiment vs the baseline anchor.
TEST_COLOR = ACCENT          # blue
ANCHOR_COLOR = "#ff9f43"     # warm orange
# Per-config line styling so both configs stay legible when overlaid.
CONFIG_STYLE = {
    "LD": {"linestyle": "-", "marker": "o"},
    "RA": {"linestyle": "--", "marker": "s"},
}
# Palette for the stacked Time Profile breakdown.
STAGE_COLORS = [
    "#6c8cff", "#4cda8a", "#ffc857", "#ff9f43", "#ff6b7a",
    "#9b8cff", "#42d6c3", "#c0c6da", "#e07be0", "#7a8196",
]


def _grid(rgba: str = BORDER):
    return dict(color=rgba, alpha=0.35, linewidth=0.6)


class ChartCard(QWidget):
    """A single dark-themed matplotlib figure with its navigation toolbar."""

    def __init__(self, height: int = 360, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self.figure = Figure(figsize=(6, 3.4), dpi=100)
        self.figure.patch.set_facecolor(BG_DARK)
        self.canvas = FigureCanvas(self.figure)
        self.canvas.setMinimumHeight(height)

        self.toolbar = NavigationToolbar(self.canvas, self)
        self.toolbar.setStyleSheet(f"background: {BG_DARK}; border: none;")

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(2)
        layout.addWidget(self.canvas)
        layout.addWidget(self.toolbar)

    def add_axes(self):
        ax = self.figure.add_subplot(111)
        ax.set_facecolor(BG_DARKEST)
        for spine in ax.spines.values():
            spine.set_color(BORDER)
        ax.tick_params(colors=TEXT_SECONDARY, labelsize=9)
        ax.grid(True, **_grid())
        return ax

    def style(self, ax, title: str, xlabel: str, ylabel: str) -> None:
        ax.set_title(title, color=TEXT_PRIMARY, fontsize=12, fontweight="bold", pad=10)
        ax.set_xlabel(xlabel, color=TEXT_SECONDARY, fontsize=10)
        ax.set_ylabel(ylabel, color=TEXT_SECONDARY, fontsize=10)

    def finish(self) -> None:
        try:
            self.figure.tight_layout()
        except Exception:  # pylint: disable=broad-except
            pass
        self.canvas.draw_idle()


class AnalysisTab(QWidget):
    """Charts + averages for one experiment folder."""

    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self._config = ConfigManager()
        self._result: Optional[AnalysisResult] = None
        self._cards: list[ChartCard] = []
        self._build_ui()
        self._restore_state()

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(10)

        root.addWidget(self._build_intro_box())
        root.addWidget(self._build_controls_group())

        self._inner_tabs = QTabWidget()
        self._inner_tabs.addTab(self._build_charts_view(), "📈  Charts")
        self._inner_tabs.addTab(self._build_averages_view(), "📋  Averages")
        root.addWidget(self._inner_tabs, stretch=1)

        self._status = QLabel("Ready — pick an experiment folder and click Analyze.")
        self._status.setStyleSheet(f"color: {TEXT_SECONDARY}; font-size: 12px;")
        root.addWidget(self._status)

        self._update_buttons()

    def _build_intro_box(self) -> QGroupBox:
        group = QGroupBox("Analysis Overview")
        layout = QVBoxLayout(group)
        info = QLabel(
            "Point at one experiment folder — the optimized or baseline tree "
            "(Low_Delay / Random_Access → QP<n> → rep_* → .bin + .report).\n"
            "Every value is averaged across the repetitions. BD-Rate / BD-PSNR use the "
            "sibling folder as the anchor (optimized vs baseline) when it exists."
        )
        info.setWordWrap(True)
        info.setStyleSheet(f"color: {TEXT_SECONDARY};")
        layout.addWidget(info)
        return group

    def _build_controls_group(self) -> QGroupBox:
        group = QGroupBox("Experiment Folder")
        layout = QVBoxLayout(group)
        layout.setSpacing(10)

        self._folder_picker = FilePickerRow(
            "Experiment folder:",
            placeholder="…/<video>/optimized  or  …/<video>/baseline",
            mode="directory",
        )
        layout.addWidget(self._folder_picker)

        row = QHBoxLayout()
        row.setSpacing(10)

        cfg_lbl = QLabel("Config:")
        cfg_lbl.setStyleSheet("font-weight: 600;")
        row.addWidget(cfg_lbl)
        self._config_combo = QComboBox()
        self._config_combo.addItem("Low Delay (LD)", "LD")
        self._config_combo.addItem("Random Access (RA)", "RA")
        self._config_combo.addItem("Both (overlay)", "BOTH")
        self._config_combo.currentIndexChanged.connect(self._on_options_changed)
        row.addWidget(self._config_combo)

        psnr_lbl = QLabel("PSNR:")
        psnr_lbl.setStyleSheet("font-weight: 600;")
        row.addWidget(psnr_lbl)
        self._psnr_combo = QComboBox()
        for comp in ("YUV", "Y", "U", "V"):
            self._psnr_combo.addItem(f"{comp}-PSNR", comp)
        self._psnr_combo.currentIndexChanged.connect(self._on_options_changed)
        row.addWidget(self._psnr_combo)

        row.addStretch()

        self._analyze_btn = QPushButton("📊  Analyze")
        self._analyze_btn.setObjectName("primaryButton")
        self._analyze_btn.setMinimumWidth(150)
        self._analyze_btn.setMinimumHeight(38)
        self._analyze_btn.clicked.connect(self._on_analyze)
        row.addWidget(self._analyze_btn)

        self._export_btn = QPushButton("⬇  Export")
        self._export_btn.setMinimumHeight(38)
        self._export_btn.setToolTip("Save the charts as PNGs and the averaged features as CSV.")
        self._export_btn.clicked.connect(self._on_export)
        row.addWidget(self._export_btn)

        layout.addLayout(row)
        return group

    def _build_charts_view(self) -> QWidget:
        self._charts_scroll = QScrollArea()
        self._charts_scroll.setWidgetResizable(True)
        self._charts_scroll.setFrameShape(QScrollArea.Shape.NoFrame)
        self._charts_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)

        self._charts_host = QWidget()
        self._charts_layout = QVBoxLayout(self._charts_host)
        self._charts_layout.setContentsMargins(6, 6, 6, 6)
        self._charts_layout.setSpacing(16)

        placeholder = QLabel("Charts appear here after you analyze a folder.")
        placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
        placeholder.setStyleSheet(f"color: {TEXT_SECONDARY}; padding: 40px;")
        self._charts_layout.addWidget(placeholder)
        self._charts_layout.addStretch()

        self._charts_scroll.setWidget(self._charts_host)
        return self._charts_scroll

    def _build_averages_view(self) -> QWidget:
        self._avg_text = QPlainTextEdit()
        self._avg_text.setReadOnly(True)
        self._avg_text.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)
        self._avg_text.setPlaceholderText("Averaged per-QP features appear here after you analyze a folder.")
        return self._avg_text

    # ------------------------------------------------------------------
    # State persistence
    # ------------------------------------------------------------------

    def _restore_state(self) -> None:
        cfg = self._config
        self._folder_picker.set_path(cfg.get("analysis_folder", ""))
        idx = self._config_combo.findData(cfg.get("analysis_config", "BOTH"))
        if idx >= 0:
            self._config_combo.setCurrentIndex(idx)
        idx = self._psnr_combo.findData(cfg.get("analysis_psnr", "YUV"))
        if idx >= 0:
            self._psnr_combo.setCurrentIndex(idx)

    def _save_state(self) -> None:
        self._config.update({
            "analysis_folder": self._folder_picker.path(),
            "analysis_config": self._config_combo.currentData(),
            "analysis_psnr": self._psnr_combo.currentData(),
        })

    # ------------------------------------------------------------------
    # Option getters
    # ------------------------------------------------------------------

    def _component(self) -> str:
        return self._psnr_combo.currentData() or "YUV"

    def _selected_configs(self) -> list[str]:
        """The configs to plot, intersected with what the data actually has."""
        if not self._result:
            return []
        available = self._result.configs()
        choice = self._config_combo.currentData()
        if choice == "BOTH":
            return available
        return [choice] if choice in available else []

    def _update_buttons(self) -> None:
        has_result = self._result is not None
        self._export_btn.setEnabled(has_result and bool(self._cards))

    # ------------------------------------------------------------------
    # Analyze
    # ------------------------------------------------------------------

    @Slot()
    def _on_analyze(self) -> None:
        folder = self._folder_picker.path()
        ok, msg = validate_directory(folder, "Experiment folder")
        if not ok:
            QMessageBox.warning(self, "Invalid folder", msg)
            return

        self._save_state()
        self._set_status("⏳ Loading reports…", WARNING)
        QGuiApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
        try:
            result = analyze_folder(folder, self._component())
        except Exception as exc:  # pylint: disable=broad-except
            QGuiApplication.restoreOverrideCursor()
            self._set_status(f"❌ {exc}", ERROR)
            QMessageBox.critical(self, "Analysis failed", str(exc))
            return
        finally:
            QGuiApplication.restoreOverrideCursor()

        self._result = result
        if not result.configs():
            self._set_status(
                "❌ No usable .report files were found in this folder.", ERROR)
            self._clear_charts("No data — check the folder layout (QP<n>/rep_*/*.report).")
            self._avg_text.setPlainText("\n".join(result.warnings) or "No data found.")
            self._update_buttons()
            return

        self._render()

    @Slot()
    def _on_options_changed(self) -> None:
        if self._result is not None:
            self._save_state()
            self._render()

    # ------------------------------------------------------------------
    # Rendering
    # ------------------------------------------------------------------

    def _render(self) -> None:
        cfgs = self._selected_configs()
        component = self._component()
        if not cfgs:
            self._clear_charts("The selected config has no data in this folder.")
            self._avg_text.setPlainText(self._build_averages_text(self._result, component))
            self._update_buttons()
            return

        self._clear_charts(None)
        self._build_charts(cfgs, component)
        self._avg_text.setPlainText(self._build_averages_text(self._result, component))

        anchor = "with baseline anchor" if self._result.has_anchor else "single curve (no sibling)"
        bits = []
        for code in cfgs:
            br, bp = self._bd_for(code, component)
            if br is not None:
                bits.append(f"{CONFIG_INFO[code]['short']} BD-Rate {br:+.2f}%")
        suffix = ("  ·  " + ", ".join(bits)) if bits else ""
        self._set_status(f"✅ Analyzed {self._result.test.label} ({anchor}){suffix}", SUCCESS)
        self._update_buttons()

    def _bd_for(self, code: str, component: str) -> tuple[Optional[float], Optional[float]]:
        res = self._result
        if not res or not res.anchor:
            return None, None
        t = res.test.configs.get(code)
        a = res.anchor.configs.get(code)
        if not t or not a:
            return None, None
        ra, pa = a.rd_arrays(component)
        rb, pb = t.rd_arrays(component)
        if len(ra) < 2 or len(rb) < 2:
            return None, None
        return bd_rate(ra, pa, rb, pb), bd_psnr(ra, pa, rb, pb)

    def _clear_charts(self, message: Optional[str]) -> None:
        for card in self._cards:
            card.setParent(None)
            card.deleteLater()
        self._cards = []
        while self._charts_layout.count():
            item = self._charts_layout.takeAt(0)
            w = item.widget()
            if w is not None:
                w.setParent(None)
                w.deleteLater()
        if message is not None:
            label = QLabel(message)
            label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            label.setStyleSheet(f"color: {TEXT_SECONDARY}; padding: 40px;")
            self._charts_layout.addWidget(label)
            self._charts_layout.addStretch()

    def _add_card(self, height: int = 360) -> ChartCard:
        card = ChartCard(height=height)
        self._cards.append(card)
        self._charts_layout.addWidget(card)
        return card

    def _build_charts(self, cfgs: list[str], component: str) -> None:
        self._chart_rd(cfgs, component)
        self._chart_vs_qp(
            cfgs, lambda p: p.psnr(component),
            f"{component}-PSNR vs QP", f"{component}-PSNR (dB)")
        self._chart_vs_qp(
            cfgs, lambda p: p.bitrate,
            "Bitrate vs QP", "Bitrate (kbps)")
        self._chart_vs_qp(
            cfgs, lambda p: p.encoder_s,
            "Execution time (ENCODER) vs QP", "ENCODER time (s)")
        self._chart_vs_qp(
            cfgs, lambda p: p.total_elapsed_s,
            "Total time (elapsed) vs QP", "Total elapsed (s)")
        self._chart_vs_qp(
            cfgs, lambda p: p.dt_model_ms,
            "Decision-tree time (DT_MODEL) vs QP", "DT_MODEL (ms)")
        self._chart_vs_qp(
            cfgs, self._inter_seconds,
            "INTER time — baseline vs optimized", "INTER time (s)")
        for code in cfgs:
            self._chart_stage_breakdown(code)
        self._charts_layout.addStretch()

    # ── individual charts ─────────────────────────────────────────────

    @staticmethod
    def _inter_seconds(p: QPPoint) -> Optional[float]:
        """INTER stage time in seconds (ms in the Time Profile / 1000)."""
        ms = p.stages_ms.get("INTER")
        return None if ms is None else ms / 1000.0

    def _experiments(self) -> list[tuple[ExperimentData, bool]]:
        """(experiment, is_test) pairs that exist, test first."""
        out: list[tuple[ExperimentData, bool]] = [(self._result.test, True)]
        if self._result.anchor is not None:
            out.append((self._result.anchor, False))
        return out

    def _chart_rd(self, cfgs: list[str], component: str) -> None:
        card = self._add_card()
        ax = card.add_axes()
        plotted = False
        for exp, is_test in self._experiments():
            color = TEST_COLOR if is_test else ANCHOR_COLOR
            for code in cfgs:
                series = exp.configs.get(code)
                if not series:
                    continue
                rates, psnrs = series.rd_arrays(component)
                if len(rates) < 1:
                    continue
                st = CONFIG_STYLE.get(code, CONFIG_STYLE["LD"])
                ax.plot(
                    rates, psnrs,
                    color=color, linestyle=st["linestyle"], marker=st["marker"],
                    markersize=5, linewidth=1.8,
                    label=f"{exp.label} · {CONFIG_INFO[code]['short']}",
                )
                plotted = True

        if not plotted:
            self._empty(ax, "No rate-distortion points")
            card.finish()
            return

        ax.set_xscale("log")
        bd_bits = []
        for code in cfgs:
            br, bp = self._bd_for(code, component)
            if br is not None and bp is not None:
                bd_bits.append(
                    f"{CONFIG_INFO[code]['short']}: BD-Rate {br:+.2f}% · BD-PSNR {bp:+.3f} dB")
        title = "Rate-Distortion (BD-Rate / BD-PSNR)"
        card.style(ax, title, "Bitrate (kbps, log)", f"{component}-PSNR (dB)")
        if bd_bits:
            ax.text(
                0.02, 0.02, "\n".join(bd_bits),
                transform=ax.transAxes, fontsize=9, color=TEXT_PRIMARY,
                va="bottom", ha="left",
                bbox=dict(boxstyle="round,pad=0.4", facecolor=BG_DARK, edgecolor=BORDER, alpha=0.9),
            )
        self._legend(ax)
        card.finish()

    def _chart_vs_qp(self, cfgs: list[str], getter: Callable[[QPPoint], Optional[float]],
                     title: str, ylabel: str) -> None:
        card = self._add_card()
        ax = card.add_axes()
        plotted = False
        for exp, is_test in self._experiments():
            color = TEST_COLOR if is_test else ANCHOR_COLOR
            for code in cfgs:
                series = exp.configs.get(code)
                if not series:
                    continue
                pts = series.sorted_points()
                xy = [(p.qp, getter(p)) for p in pts]
                xy = [(x, y) for x, y in xy if y is not None]
                if not xy:
                    continue
                xs = [x for x, _ in xy]
                ys = [y for _, y in xy]
                st = CONFIG_STYLE.get(code, CONFIG_STYLE["LD"])
                ax.plot(
                    xs, ys,
                    color=color, linestyle=st["linestyle"], marker=st["marker"],
                    markersize=5, linewidth=1.8,
                    label=f"{exp.label} · {CONFIG_INFO[code]['short']}",
                )
                plotted = True

        if not plotted:
            self._empty(ax, "No data for this metric")
            card.finish()
            return
        card.style(ax, title, "QP", ylabel)
        self._legend(ax)
        card.finish()

    def _chart_stage_breakdown(self, code: str) -> None:
        """Stacked per-stage Time Profile (ms) across QPs, for the test folder."""
        series = self._result.test.configs.get(code)
        if not series or not series.points:
            return
        pts = series.sorted_points()
        qps = [p.qp for p in pts]

        from core.analysis import STAGE_ORDER  # local import to avoid clutter
        stages = [s for s in STAGE_ORDER if any(p.stages_ms.get(s) for p in pts)]
        extras = sorted({
            s for p in pts for s in p.stages_ms
            if s not in stages and s != "ENCODER"
        })
        stages += [s for s in extras if any(p.stages_ms.get(s) for p in pts)]
        if not stages:
            return

        card = self._add_card(height=380)
        ax = card.add_axes()
        x = list(range(len(qps)))
        bottoms = [0.0] * len(qps)
        for i, stage in enumerate(stages):
            heights = [float(p.stages_ms.get(stage) or 0.0) for p in pts]
            ax.bar(
                x, heights, bottom=bottoms, width=0.62,
                color=STAGE_COLORS[i % len(STAGE_COLORS)], label=stage,
                edgecolor=BG_DARKEST, linewidth=0.4,
            )
            bottoms = [b + h for b, h in zip(bottoms, heights)]

        ax.set_xticks(x)
        ax.set_xticklabels([f"QP{q}" for q in qps])
        card.style(
            ax,
            f"Time Profile breakdown — {CONFIG_INFO[code]['label']} ({self._result.test.label})",
            "QP", "Stage time (ms)")
        self._legend(ax, ncol=2)
        card.finish()

    def _empty(self, ax, message: str) -> None:
        ax.text(0.5, 0.5, message, transform=ax.transAxes,
                ha="center", va="center", color=TEXT_SECONDARY, fontsize=11)
        ax.set_xticks([])
        ax.set_yticks([])

    def _legend(self, ax, ncol: int = 1) -> None:
        leg = ax.legend(
            loc="best", fontsize=8, ncol=ncol, framealpha=0.85,
            facecolor=BG_DARK, edgecolor=BORDER,
        )
        if leg:
            for text in leg.get_texts():
                text.set_color(TEXT_PRIMARY)

    # ------------------------------------------------------------------
    # Averages text
    # ------------------------------------------------------------------

    def _build_averages_text(self, result: AnalysisResult, component: str) -> str:
        lines: list[str] = []
        lines.append(f"Experiment : {result.test.path}")
        lines.append(f"Test       : {result.test.label}")
        lines.append(f"Anchor     : {result.anchor.label if result.anchor else '(none — BD unavailable)'}")
        lines.append(f"PSNR shown : {component}-PSNR")
        if result.warnings:
            lines.append("")
            for w in result.warnings:
                lines.append(f"⚠ {w}")
        lines.append("")

        cfgs = self._selected_configs() or result.configs()
        header = (
            f"{'QP':>3} | {'reps':>4} | {'Bitrate':>9} | {'Y-PSNR':>7} | {'U-PSNR':>7} | "
            f"{'V-PSNR':>7} | {'YUV-PSNR':>8} | {'ENCODER(s)':>10} | {'Elapsed(s)':>10} | "
            f"{'DT_MODEL(ms)':>12} | {'FEAT(ms)':>9} | {'bin(KB)':>9}"
        )

        for code in cfgs:
            lines.append("═" * len(header))
            lines.append(f"  {CONFIG_INFO[code]['label']} ({code})")
            lines.append("═" * len(header))
            for exp, role in self._role_iter(result):
                series = exp.configs.get(code)
                if not series or not series.points:
                    continue
                lines.append(f"[{role}: {exp.label}]")
                lines.append(header)
                lines.append("-" * len(header))
                for p in series.sorted_points():
                    lines.append(
                        f"{p.qp:>3} | {p.n_reps:>4} | {self._f(p.bitrate, 4):>9} | "
                        f"{self._f(p.psnr_y):>7} | {self._f(p.psnr_u):>7} | "
                        f"{self._f(p.psnr_v):>7} | {self._f(p.psnr_yuv):>8} | "
                        f"{self._f(p.encoder_s, 2):>10} | {self._f(p.total_elapsed_s, 2):>10} | "
                        f"{self._f(p.dt_model_ms, 1):>12} | {self._f(p.features_ms, 1):>9} | "
                        f"{self._f(p.bin_kb, 2):>9}"
                    )
                lines.append("")

            # Percentage difference (optimized vs baseline) for the key metrics.
            lines.extend(self._diff_lines(result, code, component))

            # BD summary for this config.
            br, bp = self._bd_for(code, component)
            if br is not None and bp is not None:
                lines.append(
                    f"  ➤ BD-Rate (optimized vs baseline, {component}): {br:+.2f} %   "
                    f"|   BD-PSNR: {bp:+.3f} dB")
            elif result.anchor is not None:
                lines.append("  ➤ BD-Rate/BD-PSNR: not enough common QP points.")
            else:
                lines.append("  ➤ BD-Rate/BD-PSNR: needs the baseline/optimized sibling folder.")
            lines.append("")

        return "\n".join(lines)

    def _diff_lines(self, result: AnalysisResult, code: str, component: str) -> list[str]:
        """Per-QP percentage difference of the key metrics, optimized vs baseline.

        Δ% = (optimized − baseline) / baseline × 100. Negative bitrate/time means
        the optimized encoder is lower (less bitrate or faster); positive PSNR
        means higher quality.
        """
        if result.anchor is None:
            return []
        opt = result.test.configs.get(code)        # test == optimized (see _role_split)
        base = result.anchor.configs.get(code)      # anchor == baseline
        if not opt or not base:
            return []

        # Match points by QP.
        base_by_qp = {p.qp: p for p in base.points}
        metrics: list[tuple[str, Callable[[QPPoint], Optional[float]]]] = [
            ("Bitrate", lambda p: p.bitrate),
            (f"{component}-PSNR", lambda p: p.psnr(component)),
            ("ENCODER", lambda p: p.encoder_s),
            ("INTER", self._inter_seconds),
            ("Total", lambda p: p.total_elapsed_s),
            ("DT_MODEL", lambda p: p.dt_model_ms),
        ]

        head = "  " + " | ".join([f"{'QP':>3}"] + [f"{title:>10}" for title, _ in metrics])
        lines = [
            f"  Δ% (optimized vs baseline) — neg. bitrate/time ⇒ optimized lower; "
            f"pos. PSNR ⇒ better",
            head,
            "  " + "-" * (len(head) - 2),
        ]

        sums: list[list[float]] = [[] for _ in metrics]
        rows = 0
        for p in opt.sorted_points():
            bp = base_by_qp.get(p.qp)
            if bp is None:
                continue
            rows += 1
            cells = [f"{p.qp:>3}"]
            for i, (_, getter) in enumerate(metrics):
                pct = self._pct_diff(getter(p), getter(bp))
                if pct is not None:
                    sums[i].append(pct)
                cells.append(f"{self._pct(pct):>10}")
            lines.append("  " + " | ".join(cells))

        if rows == 0:
            return []

        # Average row across the matched QPs.
        avg_cells = [f"{'avg':>3}"]
        for col in sums:
            avg = (sum(col) / len(col)) if col else None
            avg_cells.append(f"{self._pct(avg):>10}")
        lines.append("  " + "-" * (len(head) - 2))
        lines.append("  " + " | ".join(avg_cells))
        lines.append("")
        return lines

    @staticmethod
    def _pct_diff(opt: Optional[float], base: Optional[float]) -> Optional[float]:
        if opt is None or base is None or base == 0:
            return None
        return (opt - base) / base * 100.0

    @staticmethod
    def _pct(value: Optional[float]) -> str:
        return "-" if value is None else f"{value:+.2f}%"

    @staticmethod
    def _role_iter(result: AnalysisResult):
        yield (result.test, "test")
        if result.anchor is not None:
            yield (result.anchor, "anchor")

    @staticmethod
    def _f(value: Optional[float], decimals: int = 3) -> str:
        if value is None:
            return "-"
        return f"{value:.{decimals}f}"

    # ------------------------------------------------------------------
    # Export
    # ------------------------------------------------------------------

    @Slot()
    def _on_export(self) -> None:
        if not self._result or not self._cards:
            return
        out_dir = QFileDialog.getExistingDirectory(
            self, "Choose export folder", str(self._result.test.path))
        if not out_dir:
            return
        target = Path(out_dir)
        try:
            target.mkdir(parents=True, exist_ok=True)
            written = 0
            for i, card in enumerate(self._cards, start=1):
                png = target / f"chart_{i:02d}.png"
                card.figure.savefig(png, dpi=150, facecolor=BG_DARK, bbox_inches="tight")
                written += 1
            csv_path = target / "analysis_averages.csv"
            self._write_csv(csv_path)
        except OSError as exc:
            QMessageBox.warning(self, "Export failed", str(exc))
            return
        self._set_status(f"✅ Exported {written} chart(s) + CSV → {target}", SUCCESS)
        QMessageBox.information(
            self, "Export complete",
            f"Saved {written} chart PNG(s) and analysis_averages.csv to:\n{target}")

    def _write_csv(self, path: Path) -> None:
        import csv
        with open(path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh, delimiter=";")
            writer.writerow([
                "role", "experiment", "config", "qp", "reps",
                "bitrate_kbps", "psnr_y", "psnr_u", "psnr_v", "psnr_yuv",
                "encoder_s", "total_elapsed_s", "total_user_s",
                "dt_model_ms", "features_ms", "bin_kb",
            ])
            for exp, role in self._role_iter(self._result):
                for code in self._result.configs():
                    series = exp.configs.get(code)
                    if not series:
                        continue
                    for p in series.sorted_points():
                        writer.writerow([
                            role, exp.label, code, p.qp, p.n_reps,
                            self._csv(p.bitrate), self._csv(p.psnr_y), self._csv(p.psnr_u),
                            self._csv(p.psnr_v), self._csv(p.psnr_yuv),
                            self._csv(p.encoder_s), self._csv(p.total_elapsed_s),
                            self._csv(p.total_user_s), self._csv(p.dt_model_ms),
                            self._csv(p.features_ms), self._csv(p.bin_kb),
                        ])

    @staticmethod
    def _csv(value: Optional[float]) -> str:
        return "" if value is None else "%.10g" % value

    # ------------------------------------------------------------------
    # Misc
    # ------------------------------------------------------------------

    def _set_status(self, text: str, color: str = TEXT_SECONDARY) -> None:
        self._status.setStyleSheet(f"color: {color}; font-size: 12px;")
        self._status.setText(text)
