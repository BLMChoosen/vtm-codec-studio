"""
Catch-Up Tab  (temporary)
=========================
Scans the Output-Videos directory for incomplete or interrupted encodes,
shows a per-sequence status table, then re-encodes only what is missing.

Workflow:
  1. Fill in the four path fields (output root, YUV root, seq-cfg folder,
     VTM cfg folder) and the two encoder executables.
  2. Click "Scan" to analyse the directory and populate the status table.
  3. Review the table (colour-coded: green = complete, orange = partial,
     red = YUV/cfg missing).
  4. Click "Start Catch-Up" to delete interrupted rep folders and encode
     every missing combination.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Optional

from PySide6.QtCore import Qt, Slot
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QCheckBox,
    QFileDialog,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from core.catchup import (
    CONFIG_INFO,
    CatchUpConfig,
    CatchUpOrchestrator,
    SeqScanResult,
    scan_output_root,
)
from ui.widgets import FilePickerRow, LogPanel, ScrollSafeSpinBox
from utils.config import ConfigManager
from utils.validators import validate_directory, validate_file_exists


_DEFAULT_QPS  = "22, 27, 32, 37"
_DEFAULT_FRAMES = 33
_DEFAULT_REPS   = 2

# Colour palette (matches dark theme)
_COL_COMPLETE    = QColor("#2d5a3d")   # dark green
_COL_PARTIAL     = QColor("#5a4a1a")   # dark amber
_COL_NO_SOURCE   = QColor("#5a2a2a")   # dark red
_COL_TEXT_GOOD   = QColor("#4cda8a")
_COL_TEXT_WARN   = QColor("#ffc857")
_COL_TEXT_ERR    = QColor("#ff6b7a")
_COL_TEXT_NORMAL = QColor("#c8cdd8")


class CatchUpTab(QWidget):
    """Widget for the Catch-Up (gap-filling) workflow."""

    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self._config = ConfigManager()
        self._orchestrator: Optional[CatchUpOrchestrator] = None
        self._scan_results: list[SeqScanResult] = []
        self._build_ui()
        self._restore_state()

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        root_layout = QVBoxLayout(self)
        root_layout.setContentsMargins(12, 12, 12, 12)
        root_layout.setSpacing(10)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QScrollArea.Shape.NoFrame)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        scroll.setStyleSheet("QScrollArea { background: transparent; }")

        body = QWidget()
        body.setMinimumWidth(680)
        body_layout = QVBoxLayout(body)
        body_layout.setContentsMargins(4, 4, 4, 4)
        body_layout.setSpacing(14)

        body_layout.addWidget(self._build_intro())
        body_layout.addWidget(self._build_paths_group())
        body_layout.addWidget(self._build_executables_group())
        body_layout.addWidget(self._build_encoding_group())
        body_layout.addLayout(self._build_action_row())
        body_layout.addWidget(self._build_scan_table_group())

        self._log_panel = LogPanel()
        self._log_panel.setMinimumHeight(240)
        self._log_panel.set_status("Ready — click Scan first.", "#8a90a4")
        body_layout.addWidget(self._log_panel)

        scroll.setWidget(body)
        root_layout.addWidget(scroll, stretch=1)

        self._update_buttons()

    def _build_intro(self) -> QGroupBox:
        group = QGroupBox("Catch-Up Overview")
        layout = QVBoxLayout(group)
        info = QLabel(
            "Temporary tab for filling encoding gaps in the Output Videos directory.\n"
            "Scan detects three states per (sequence, codec, mode, QP, rep):\n"
            "  • Complete     — .bin + .report both present  →  left untouched\n"
            "  • Interrupted  — .bin exists but .report is missing  →  folder deleted, then re-encoded\n"
            "  • Missing      — rep folder absent entirely  →  encoded from scratch\n\n"
            "Sequences whose YUV source or per-sequence .cfg cannot be found are "
            "highlighted in red and skipped automatically."
        )
        info.setWordWrap(True)
        info.setStyleSheet("color: #b8bdcb;")
        layout.addWidget(info)
        return group

    def _build_paths_group(self) -> QGroupBox:
        group = QGroupBox("Paths")
        layout = QVBoxLayout(group)
        layout.setSpacing(10)

        self._output_picker = FilePickerRow(
            "Output root:",
            placeholder="The Output Videos folder (contains per-sequence sub-dirs)",
            mode="directory",
        )
        layout.addWidget(self._output_picker)

        self._yuv_picker = FilePickerRow(
            "YUV source root:",
            placeholder="Folder that contains the .yuv source files",
            mode="directory",
        )
        layout.addWidget(self._yuv_picker)

        self._seq_cfg_picker = FilePickerRow(
            "Per-seq cfg folder:",
            placeholder="Folder with per-sequence .cfg files  (e.g. …/VTM 24.0/cfg/per-sequence)",
            mode="directory",
        )
        layout.addWidget(self._seq_cfg_picker)

        self._vtm_cfg_picker = FilePickerRow(
            "VTM main cfg folder:",
            placeholder="Folder with encoder_lowdelay_vtm.cfg / encoder_randomaccess_vtm.cfg",
            mode="directory",
        )
        layout.addWidget(self._vtm_cfg_picker)

        hint = QLabel(
            "The VTM main cfg folder can also be set globally under File > Settings."
        )
        hint.setWordWrap(True)
        hint.setStyleSheet("color: #8a90a4; font-size: 12px;")
        layout.addWidget(hint)
        return group

    def _build_executables_group(self) -> QGroupBox:
        group = QGroupBox("Encoder Executables")
        layout = QVBoxLayout(group)
        layout.setSpacing(10)

        exe_filter = "Executables (*.exe);;All Files (*)" if os.name == "nt" else "All Files (*)"

        self._baseline_picker = FilePickerRow(
            "Baseline (opt OFF):",
            file_filter=exe_filter,
            placeholder="EncoderApp WITHOUT the DT optimization",
        )
        layout.addWidget(self._baseline_picker)

        self._optimized_picker = FilePickerRow(
            "Optimized (opt ON):",
            file_filter=exe_filter,
            placeholder="EncoderApp WITH the DT optimization",
        )
        layout.addWidget(self._optimized_picker)
        return group

    def _build_encoding_group(self) -> QGroupBox:
        group = QGroupBox("Encoding Parameters")
        layout = QVBoxLayout(group)
        layout.setSpacing(8)

        # Configs checkboxes
        cfg_row = QHBoxLayout()
        cfg_lbl = QLabel("Configs:")
        cfg_lbl.setMinimumWidth(160)
        cfg_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        cfg_row.addWidget(cfg_lbl)
        self._ld_check = QCheckBox("Low Delay (LD)")
        self._ld_check.setChecked(True)
        cfg_row.addWidget(self._ld_check)
        self._ra_check = QCheckBox("Random Access (RA)")
        self._ra_check.setChecked(True)
        cfg_row.addWidget(self._ra_check)
        cfg_row.addStretch()
        layout.addLayout(cfg_row)

        # QPs
        qp_row = QHBoxLayout()
        qp_lbl = QLabel("QPs (-q):")
        qp_lbl.setMinimumWidth(160)
        qp_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        qp_row.addWidget(qp_lbl)
        self._qps_edit = QLineEdit(_DEFAULT_QPS)
        self._qps_edit.setPlaceholderText("Comma-separated, e.g. 22, 27, 32, 37")
        qp_row.addWidget(self._qps_edit, stretch=1)
        layout.addLayout(qp_row)

        # Frames
        frames_row = QHBoxLayout()
        frames_lbl = QLabel("Frames (-f):")
        frames_lbl.setMinimumWidth(160)
        frames_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        frames_row.addWidget(frames_lbl)
        self._frames_spin = ScrollSafeSpinBox()
        self._frames_spin.setRange(1, 1_000_000)
        self._frames_spin.setValue(_DEFAULT_FRAMES)
        self._frames_spin.setToolTip("Must match the frame count used in existing encodes (33).")
        frames_row.addWidget(self._frames_spin)
        frames_row.addStretch()
        layout.addLayout(frames_row)

        # Repetitions
        reps_row = QHBoxLayout()
        reps_lbl = QLabel("Repetitions:")
        reps_lbl.setMinimumWidth(160)
        reps_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        reps_row.addWidget(reps_lbl)
        self._reps_spin = ScrollSafeSpinBox()
        self._reps_spin.setRange(1, 100)
        self._reps_spin.setValue(_DEFAULT_REPS)
        reps_row.addWidget(self._reps_spin)
        reps_row.addStretch()
        layout.addLayout(reps_row)

        # Parallel jobs
        par_row = QHBoxLayout()
        par_lbl = QLabel("Parallel Jobs:")
        par_lbl.setMinimumWidth(160)
        par_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        par_row.addWidget(par_lbl)
        self._parallel_spin = ScrollSafeSpinBox()
        self._parallel_spin.setRange(1, 32)
        self._parallel_spin.setValue(1)
        self._parallel_spin.setToolTip("Keep at 1 for trustworthy timing measurements.")
        par_row.addWidget(self._parallel_spin)
        par_row.addStretch()
        layout.addLayout(par_row)

        warn = QLabel(
            "⚠ Keep Parallel Jobs = 1 for reliable timing. "
            "Frames must match the value used in existing encodes (default: 33)."
        )
        warn.setWordWrap(True)
        warn.setStyleSheet("color: #ffc857; font-size: 11px;")
        layout.addWidget(warn)
        return group

    def _build_action_row(self) -> QHBoxLayout:
        row = QHBoxLayout()
        row.addStretch()

        self._scan_btn = QPushButton("🔍  Scan")
        self._scan_btn.setMinimumWidth(130)
        self._scan_btn.setMinimumHeight(42)
        self._scan_btn.setToolTip("Analyse the output root and populate the status table.")
        self._scan_btn.clicked.connect(self._on_scan)
        row.addWidget(self._scan_btn)

        self._start_btn = QPushButton("▶  Start Catch-Up")
        self._start_btn.setObjectName("primaryButton")
        self._start_btn.setMinimumWidth(190)
        self._start_btn.setMinimumHeight(42)
        self._start_btn.setEnabled(False)
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

    def _build_scan_table_group(self) -> QGroupBox:
        group = QGroupBox("Scan Results")
        layout = QVBoxLayout(group)

        self._table = QTableWidget(0, 6)
        self._table.setHorizontalHeaderLabels([
            "Class", "Sequence", "Status", "To encode", "YUV", "Seq CFG",
        ])
        self._table.horizontalHeader().setStretchLastSection(False)
        self._table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._table.setAlternatingRowColors(True)
        self._table.verticalHeader().setVisible(False)
        self._table.setMinimumHeight(300)
        self._table.setColumnWidth(0, 55)
        self._table.setColumnWidth(1, 160)
        self._table.setColumnWidth(2, 120)
        self._table.setColumnWidth(3, 90)
        self._table.setColumnWidth(4, 40)
        self._table.setColumnWidth(5, 40)
        self._table.cellDoubleClicked.connect(self._on_cell_double_clicked)
        layout.addWidget(self._table)

        legend_row = QHBoxLayout()
        for color, text in [
            (_COL_TEXT_GOOD, "■  Complete"),
            (_COL_TEXT_WARN, "■  Incomplete"),
            (_COL_TEXT_ERR,  "■  YUV/CFG missing  (double-click to set manually)"),
        ]:
            lbl = QLabel(text)
            lbl.setStyleSheet(f"color: {color.name()}; font-size: 11px;")
            legend_row.addWidget(lbl)
        legend_row.addStretch()
        layout.addLayout(legend_row)

        return group

    # ------------------------------------------------------------------
    # State persistence
    # ------------------------------------------------------------------

    def _restore_state(self) -> None:
        c = self._config
        self._output_picker.set_path(c.get("catchup_output_root", ""))
        self._yuv_picker.set_path(c.get("catchup_yuv_root", ""))

        seq_cfg = c.get("catchup_seq_cfg_folder", "")
        if not seq_cfg:
            seq_cfg = c.get("catchup_seq_cfg_folder", "")
        self._seq_cfg_picker.set_path(seq_cfg)

        vtm_cfg = c.get("catchup_vtm_cfg_folder", "") or c.cfg_folder() or ""
        self._vtm_cfg_picker.set_path(vtm_cfg)

        self._baseline_picker.set_path(c.get("catchup_baseline_exe", "") or c.get("comparison_baseline_exe", ""))
        self._optimized_picker.set_path(c.get("catchup_optimized_exe", "") or c.get("comparison_optimized_exe", ""))
        self._qps_edit.setText(c.get("catchup_qps", _DEFAULT_QPS))
        self._ld_check.setChecked(bool(c.get("catchup_cfg_ld", True)))
        self._ra_check.setChecked(bool(c.get("catchup_cfg_ra", True)))
        try:
            self._frames_spin.setValue(max(1, int(c.get("catchup_frames", _DEFAULT_FRAMES))))
        except (TypeError, ValueError):
            self._frames_spin.setValue(_DEFAULT_FRAMES)
        try:
            self._reps_spin.setValue(max(1, int(c.get("catchup_reps", _DEFAULT_REPS))))
        except (TypeError, ValueError):
            self._reps_spin.setValue(_DEFAULT_REPS)
        try:
            self._parallel_spin.setValue(max(1, int(c.get("catchup_parallel", 1))))
        except (TypeError, ValueError):
            self._parallel_spin.setValue(1)

    def _save_state(self) -> None:
        self._config.update({
            "catchup_output_root":   self._output_picker.path(),
            "catchup_yuv_root":      self._yuv_picker.path(),
            "catchup_seq_cfg_folder": self._seq_cfg_picker.path(),
            "catchup_vtm_cfg_folder": self._vtm_cfg_picker.path(),
            "catchup_baseline_exe":  self._baseline_picker.path(),
            "catchup_optimized_exe": self._optimized_picker.path(),
            "catchup_qps":           self._qps_edit.text(),
            "catchup_cfg_ld":        self._ld_check.isChecked(),
            "catchup_cfg_ra":        self._ra_check.isChecked(),
            "catchup_frames":        self._frames_spin.value(),
            "catchup_reps":          self._reps_spin.value(),
            "catchup_parallel":      self._parallel_spin.value(),
        })

    # ------------------------------------------------------------------
    # Scan
    # ------------------------------------------------------------------

    @Slot()
    def _on_scan(self) -> None:
        if self._is_running():
            return

        output_root    = self._output_picker.path()
        yuv_root       = self._yuv_picker.path()
        seq_cfg_folder = self._seq_cfg_picker.path()

        ok, msg = validate_directory(output_root, "Output root")
        if not ok:
            QMessageBox.warning(self, "Scan Error", msg)
            return

        qps, err = self._parse_qps()
        if err:
            QMessageBox.warning(self, "Scan Error", err)
            return

        configs = self._selected_configs()
        if not configs:
            QMessageBox.warning(self, "Scan Error", "Select at least one config (LD / RA).")
            return

        self._log_panel.clear()
        self._log_panel.set_status("⏳ Scanning…", "#ffc857")
        self._log_panel.set_indeterminate(True)

        self._save_state()

        self._scan_results = scan_output_root(
            output_root=output_root,
            yuv_root=yuv_root,
            seq_cfg_folder=seq_cfg_folder,
            qps=qps,
            configs=configs,
            repetitions=self._reps_spin.value(),
        )

        self._log_panel.set_indeterminate(False)
        self._populate_table()
        self._update_buttons()

        needs_work  = sum(1 for r in self._scan_results if r.needs_work)
        can_encode  = sum(1 for r in self._scan_results if r.needs_work and r.can_encode)
        skipped     = needs_work - can_encode
        complete    = sum(1 for r in self._scan_results if not r.needs_work)

        lines = [
            f"Scan complete — {len(self._scan_results)} sequences checked.",
            f"  Complete: {complete}",
            f"  Needs work: {needs_work}  ({can_encode} can be encoded, {skipped} missing YUV/CFG)",
        ]
        for line in lines:
            self._log_panel.append(line)
        self._log_panel.set_status("Scan done.", "#4cda8a")

    def _populate_table(self) -> None:
        self._table.setRowCount(0)
        for result in self._scan_results:
            row = self._table.rowCount()
            self._table.insertRow(row)
            self._fill_table_row(row, result)
        self._table.resizeRowsToContents()

    def _fill_table_row(self, row: int, result: SeqScanResult) -> None:
        n_missing  = len(result.missing) + len(result.interrupted)
        total      = result.total_count
        needs_work = result.needs_work
        has_source = result.can_encode

        if not needs_work:
            bg     = _COL_COMPLETE
            status = "Complete"
            fg     = _COL_TEXT_GOOD
            tip    = ""
        elif not has_source:
            bg     = _COL_NO_SOURCE
            status = "No source"
            fg     = _COL_TEXT_ERR
            missing_parts = []
            if not result.yuv_path:
                missing_parts.append(".yuv")
            if not result.seq_cfg:
                missing_parts.append("seq .cfg")
            tip = f"Double-click to set {' and '.join(missing_parts)} manually"
        else:
            bg     = _COL_PARTIAL
            status = "Incomplete"
            fg     = _COL_TEXT_WARN
            tip    = ""

        def _cell(text: str, align=Qt.AlignmentFlag.AlignLeft) -> QTableWidgetItem:
            item = QTableWidgetItem(str(text))
            item.setBackground(bg)
            item.setForeground(fg)
            item.setTextAlignment(align | Qt.AlignmentFlag.AlignVCenter)
            if tip:
                item.setToolTip(tip)
            return item

        self._table.setItem(row, 0, _cell(result.info.cls, Qt.AlignmentFlag.AlignCenter))
        self._table.setItem(row, 1, _cell(result.info.display))
        self._table.setItem(row, 2, _cell(status, Qt.AlignmentFlag.AlignCenter))
        self._table.setItem(row, 3, _cell(
            f"{n_missing} / {total}" if needs_work else "—",
            Qt.AlignmentFlag.AlignCenter,
        ))
        self._table.setItem(row, 4, _cell(
            "✔" if result.yuv_path else "✘", Qt.AlignmentFlag.AlignCenter,
        ))
        self._table.setItem(row, 5, _cell(
            "✔" if result.seq_cfg else "✘", Qt.AlignmentFlag.AlignCenter,
        ))

    # ------------------------------------------------------------------
    # Validation helpers
    # ------------------------------------------------------------------

    def _selected_configs(self) -> list[str]:
        configs = []
        if self._ld_check.isChecked():
            configs.append("LD")
        if self._ra_check.isChecked():
            configs.append("RA")
        return configs

    def _parse_qps(self) -> tuple[list[int], str]:
        tokens = [t for t in re.split(r"[\s,;]+", self._qps_edit.text().strip()) if t]
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
                return [], f"QP must be 0–63. Got: {qp}"
            if qp not in seen:
                seen.add(qp)
                qps.append(qp)
        return qps, ""

    def _validate_for_start(self) -> tuple[bool, str, Optional[CatchUpConfig]]:
        if not self._scan_results:
            return False, "Run a Scan first.", None

        actionable = [r for r in self._scan_results if r.needs_work and r.can_encode]
        if not actionable:
            return False, "No actionable sequences found. All complete or YUV/CFG missing.", None

        ok, msg = validate_directory(self._output_picker.path(), "Output root")
        if not ok:
            return False, msg, None

        yuv_root = self._yuv_picker.path()
        seq_cfg_folder = self._seq_cfg_picker.path()

        vtm_cfg = self._vtm_cfg_picker.path() or self._config.cfg_folder()
        if not vtm_cfg:
            return False, (
                "VTM main cfg folder is not set. Fill it in above or set it "
                "globally under File > Settings."
            ), None

        configs = self._selected_configs()
        if not configs:
            return False, "Select at least one config (LD / RA).", None

        for code in configs:
            main_cfg = Path(vtm_cfg) / CONFIG_INFO[code]["cfg_file"]
            if not main_cfg.is_file():
                return False, f"Missing VTM main config: {main_cfg}", None

        baseline  = self._baseline_picker.path()
        optimized = self._optimized_picker.path()
        for label, path in (("Baseline encoder", baseline), ("Optimized encoder", optimized)):
            if not path:
                return False, f"{label} executable is not set.", None
            ok, msg = validate_file_exists(path, label)
            if not ok:
                return False, msg, None

        qps, err = self._parse_qps()
        if err:
            return False, err, None

        cfg = CatchUpConfig(
            output_root=self._output_picker.path(),
            yuv_root=yuv_root,
            seq_cfg_folder=seq_cfg_folder,
            baseline_exe=baseline,
            optimized_exe=optimized,
            cfg_folder=vtm_cfg,
            qps=qps,
            configs=configs,
            frames=int(self._frames_spin.value()),
            repetitions=int(self._reps_spin.value()),
            parallel_jobs=max(1, int(self._parallel_spin.value())),
        )
        return True, "", cfg

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _is_running(self) -> bool:
        return self._orchestrator is not None and self._orchestrator.isRunning()

    def _update_buttons(self) -> None:
        running = self._is_running()
        has_scan = bool(self._scan_results)
        actionable = any(r.needs_work and r.can_encode for r in self._scan_results)

        self._scan_btn.setEnabled(not running)
        self._start_btn.setEnabled(not running and has_scan and actionable)
        self._cancel_btn.setEnabled(running)

        for w in (
            self._output_picker, self._yuv_picker, self._seq_cfg_picker,
            self._vtm_cfg_picker, self._baseline_picker, self._optimized_picker,
            self._qps_edit, self._ld_check, self._ra_check,
            self._frames_spin, self._reps_spin, self._parallel_spin,
        ):
            w.setEnabled(not running)

    # ------------------------------------------------------------------
    # Slots
    # ------------------------------------------------------------------

    @Slot(int, int)
    def _on_cell_double_clicked(self, row: int, col: int) -> None:
        if self._is_running() or row >= len(self._scan_results):
            return
        result = self._scan_results[row]
        if result.can_encode:
            return  # only red (no-source) rows need manual assignment

        changed = False
        start_yuv = self._yuv_picker.path() or ""
        start_cfg = self._seq_cfg_picker.path() or ""

        if not result.yuv_path:
            yuv_file, _ = QFileDialog.getOpenFileName(
                self,
                f"Select YUV for  {result.info.display}",
                start_yuv,
                "YUV Files (*.yuv);;All Files (*)",
            )
            if yuv_file:
                result.yuv_path = yuv_file
                changed = True

        if not result.seq_cfg:
            cfg_file, _ = QFileDialog.getOpenFileName(
                self,
                f"Select seq .cfg for  {result.info.display}",
                start_cfg,
                "Config Files (*.cfg);;All Files (*)",
            )
            if cfg_file:
                result.seq_cfg = cfg_file
                changed = True

        if changed:
            self._fill_table_row(row, result)
            self._update_buttons()

    @Slot()
    def _on_start(self) -> None:
        if self._is_running():
            return

        ok, msg, cfg = self._validate_for_start()
        if not ok or cfg is None:
            QMessageBox.warning(self, "Validation Error", msg)
            return

        self._save_state()

        actionable = [r for r in self._scan_results if r.needs_work and r.can_encode]
        interrupted_total = sum(len(r.interrupted) for r in actionable)
        missing_total     = sum(len(r.missing)     for r in actionable)
        # After cleanup, interrupted become missing too
        encodes_total = missing_total + interrupted_total

        confirm = QMessageBox.question(
            self,
            "Start Catch-Up",
            (
                f"This will:\n\n"
                f"  1. Delete {interrupted_total} interrupted rep folder(s) (.bin without .report)\n"
                f"  2. Encode {encodes_total} missing combination(s) across "
                f"{len(actionable)} sequence(s)\n\n"
                f"  Configs:    {', '.join(cfg.configs)}\n"
                f"  QPs:        {', '.join(str(q) for q in cfg.qps)}\n"
                f"  Frames:     {cfg.frames}\n"
                f"  Reps:       {cfg.repetitions}\n"
                f"  Parallel:   {cfg.parallel_jobs}\n\n"
                f"Sequences without YUV/CFG will be skipped silently.\n"
                f"Proceed?"
            ),
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if confirm != QMessageBox.StandardButton.Yes:
            return

        self._log_panel.clear()
        self._log_panel.set_progress(0)
        self._log_panel.set_status("⏳ Catch-Up starting…", "#ffc857")

        self._orchestrator = CatchUpOrchestrator(cfg, self._scan_results, parent=self)
        self._orchestrator.signals.log_line.connect(self._on_log_line)
        self._orchestrator.signals.progress.connect(self._on_progress)
        self._orchestrator.signals.finished.connect(self._on_finished)
        self._orchestrator.start()
        self._update_buttons()

    @Slot()
    def _on_cancel(self) -> None:
        if not self._is_running():
            return
        self._log_panel.append("⛔ Cancellation requested…")
        self._log_panel.set_status("⛔ Cancelling…", "#ffc857")
        self._orchestrator.cancel()

    @Slot(str)
    def _on_log_line(self, line: str) -> None:
        self._log_panel.append(line)

    @Slot(int)
    def _on_progress(self, value: int) -> None:
        self._log_panel.set_progress(max(0, min(100, value)))

    @Slot(bool, str)
    def _on_finished(self, success: bool, message: str) -> None:
        color  = "#4cda8a" if success else "#ff6b7a"
        prefix = "✅" if success else "❌"
        self._log_panel.set_status(f"{prefix} {message}", color)
        if success:
            self._log_panel.set_progress(100)
        self._orchestrator = None
        self._update_buttons()

        # Refresh the table so the user can see what changed.
        if success:
            self._on_scan()
