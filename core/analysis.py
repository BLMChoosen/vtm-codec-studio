"""
Experiment Analysis
===================
Loads a single experiment folder produced by the Comparison tab and turns its
per-repetition ``.report`` files into averaged, plot-ready features.

Input folder (the only thing the user provides) is one experiment tree, i.e.
the ``optimized`` **or** the ``baseline`` folder:

    <experiment>/                      <- selected by the user (optimized | baseline)
        Low_Delay/
            QP22/
                rep_0/  -> <stem>.bin + <stem>.report
                rep_1/
                ...
            QP27/ …
        Random_Access/
            QP22/ …

Every numeric feature is the **mean across the repetitions** of one
(config, QP) experiment. Features come from each ``.report`` (the full encoder
stdout):

    • summary line  -> Bitrate (kbps), Y/U/V/YUV-PSNR (dB), Total Frames
    • Time Profile  -> QT_*, INTER, ENCODER, FEATURES_EXTRACTION, DT_MODEL (ms)
    • Total Time    -> user / elapsed (s)
    • .bin size     -> bytes on disk

BD-Rate / BD-PSNR compare two RD curves. The anchor curve is read from the
**sibling** folder automatically: selecting ``optimized`` pulls in ``baseline``
(and vice-versa). BD is always reported as *optimized relative to baseline*.
When no sibling is found, only the single curve is plotted and BD is unavailable.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import numpy as np

from utils.parser import parse_time_profile


# Config code -> on-disk folder name + short tag (mirrors core.comparison).
CONFIG_INFO: dict[str, dict[str, str]] = {
    "LD": {"folder": "Low_Delay", "short": "LD", "label": "Low Delay"},
    "RA": {"folder": "Random_Access", "short": "RA", "label": "Random Access"},
}

# Recognised experiment-folder names and their role in the BD comparison.
EXE_LABELS = ("baseline", "optimized")

# PSNR components selectable for the RD curve / PSNR charts.
PSNR_KEYS = {
    "YUV": "psnr_yuv",
    "Y": "psnr_y",
    "U": "psnr_u",
    "V": "psnr_v",
}

# Time Profile stages that make up the per-frame stacked breakdown, in order.
STAGE_ORDER = ["QT_0", "QT_1", "QT_2", "QT_3", "QT_4", "INTER",
               "FEATURES_EXTRACTION", "DT_MODEL"]


# ─────────────────────────────────────────────────────────────────────────────
# Report parsing
# ─────────────────────────────────────────────────────────────────────────────

# Average summary row, e.g.:
#   \t 33           a  141.6582     28.5890  34.4185  34.2569  29.7048
_SUMMARY_RE = re.compile(
    r"^\s*(\d+)\s+a\s+"
    r"([0-9]+\.?[0-9]*)\s+"      # bitrate
    r"([0-9]+\.?[0-9]*)\s+"      # Y-PSNR
    r"([0-9]+\.?[0-9]*)\s+"      # U-PSNR
    r"([0-9]+\.?[0-9]*)\s+"      # V-PSNR
    r"([0-9]+\.?[0-9]*)",        # YUV-PSNR
    re.MULTILINE,
)


def parse_report(text: str) -> dict:
    """Parse one ``.report`` (full encoder stdout) into a flat metrics dict.

    Returns numeric values (or ``None`` when absent):
        frames, bitrate, psnr_y, psnr_u, psnr_v, psnr_yuv,
        stages (dict, ms), total_user_s, total_elapsed_s
    """
    out: dict = {
        "frames": None,
        "bitrate": None,
        "psnr_y": None,
        "psnr_u": None,
        "psnr_v": None,
        "psnr_yuv": None,
    }

    m = _SUMMARY_RE.search(text)
    if m:
        out["frames"] = float(m.group(1))
        out["bitrate"] = float(m.group(2))
        out["psnr_y"] = float(m.group(3))
        out["psnr_u"] = float(m.group(4))
        out["psnr_v"] = float(m.group(5))
        out["psnr_yuv"] = float(m.group(6))

    profile = parse_time_profile(text)
    out["stages"] = profile["stages"]
    out["total_user_s"] = profile["total_user_s"]
    out["total_elapsed_s"] = profile["total_elapsed_s"]
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Data model
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class QPPoint:
    """Mean of every feature across the repetitions of one (config, QP)."""

    qp: int
    n_reps: int
    bitrate: Optional[float] = None
    psnr_y: Optional[float] = None
    psnr_u: Optional[float] = None
    psnr_v: Optional[float] = None
    psnr_yuv: Optional[float] = None
    encoder_s: Optional[float] = None        # ENCODER stage, seconds
    total_elapsed_s: Optional[float] = None
    total_user_s: Optional[float] = None
    dt_model_ms: Optional[float] = None
    features_ms: Optional[float] = None
    bin_kb: Optional[float] = None
    stages_ms: dict[str, float] = field(default_factory=dict)  # mean per stage

    def psnr(self, component: str) -> Optional[float]:
        return getattr(self, PSNR_KEYS.get(component, "psnr_yuv"), None)


@dataclass
class ConfigSeries:
    """All QP points for one config (LD or RA), sorted by QP ascending."""

    config: str                       # "LD" | "RA"
    points: list[QPPoint] = field(default_factory=list)

    def sorted_points(self) -> list[QPPoint]:
        return sorted(self.points, key=lambda p: p.qp)

    def rd_arrays(self, component: str = "YUV") -> tuple[list[float], list[float]]:
        """Return (rates, psnrs) for points that have both, sorted by rate."""
        pairs = []
        for p in self.sorted_points():
            r = p.bitrate
            d = p.psnr(component)
            if r is not None and d is not None and r > 0:
                pairs.append((r, d))
        pairs.sort(key=lambda t: t[0])
        rates = [t[0] for t in pairs]
        psnrs = [t[1] for t in pairs]
        return rates, psnrs


@dataclass
class ExperimentData:
    """One experiment tree (a baseline or optimized folder)."""

    path: Path
    label: str                        # "baseline" | "optimized" | folder name
    configs: dict[str, ConfigSeries] = field(default_factory=dict)

    def available_configs(self) -> list[str]:
        return [c for c in ("LD", "RA") if c in self.configs and self.configs[c].points]


@dataclass
class BDResult:
    """Bjøntegaard deltas for one config (test vs anchor)."""

    config: str
    component: str
    bd_rate: Optional[float]          # percent
    bd_psnr: Optional[float]          # dB
    note: str = ""


@dataclass
class AnalysisResult:
    """Everything the Analysis tab needs to render."""

    test: ExperimentData              # the folder the user selected (or optimized)
    anchor: Optional[ExperimentData]  # sibling (or baseline); None when absent
    bd: dict[str, BDResult] = field(default_factory=dict)  # by config code
    warnings: list[str] = field(default_factory=list)

    @property
    def has_anchor(self) -> bool:
        return self.anchor is not None

    def configs(self) -> list[str]:
        seen = list(self.test.available_configs())
        if self.anchor:
            for c in self.anchor.available_configs():
                if c not in seen:
                    seen.append(c)
        return seen


# ─────────────────────────────────────────────────────────────────────────────
# Loading
# ─────────────────────────────────────────────────────────────────────────────

_QP_RE = re.compile(r"QP\s*(\d+)", re.IGNORECASE)


def _mean(values: list) -> Optional[float]:
    nums = [v for v in values if v is not None]
    if not nums:
        return None
    return sum(nums) / len(nums)


def _iter_rep_reports(qp_dir: Path) -> list[Path]:
    """Return every ``.report`` under a QP folder, one per repetition.

    Looks inside ``rep_*`` sub-folders first; if none carry a report, falls
    back to reports sitting directly in the QP folder. The Comparison tab's
    ``Average`` folder never holds a ``.report`` so it is skipped naturally.
    """
    reports: list[Path] = []
    for sub in sorted(qp_dir.iterdir()):
        if sub.is_dir():
            found = sorted(sub.glob("*.report"))
            if found:
                reports.append(found[0])
    if not reports:
        reports = sorted(qp_dir.glob("*.report"))
    return reports


def _build_qp_point(qp: int, reports: list[Path]) -> Optional[QPPoint]:
    parsed: list[dict] = []
    sizes: list[float] = []
    for rep in reports:
        try:
            text = rep.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        parsed.append(parse_report(text))
        bin_path = rep.with_suffix(".bin")
        if bin_path.is_file():
            try:
                sizes.append(bin_path.stat().st_size / 1024.0)
            except OSError:
                pass

    if not parsed:
        return None

    # Stage names in first-appearance order across the reps.
    stage_names: list[str] = []
    seen: set[str] = set()
    for res in parsed:
        for name in res.get("stages", {}):
            if name not in seen:
                seen.add(name)
                stage_names.append(name)
    stages_ms = {
        name: _mean([res.get("stages", {}).get(name) for res in parsed])
        for name in stage_names
    }

    encoder_ms = stages_ms.get("ENCODER")
    return QPPoint(
        qp=qp,
        n_reps=len(parsed),
        bitrate=_mean([r["bitrate"] for r in parsed]),
        psnr_y=_mean([r["psnr_y"] for r in parsed]),
        psnr_u=_mean([r["psnr_u"] for r in parsed]),
        psnr_v=_mean([r["psnr_v"] for r in parsed]),
        psnr_yuv=_mean([r["psnr_yuv"] for r in parsed]),
        encoder_s=(encoder_ms / 1000.0) if encoder_ms is not None else None,
        total_elapsed_s=_mean([r["total_elapsed_s"] for r in parsed]),
        total_user_s=_mean([r["total_user_s"] for r in parsed]),
        dt_model_ms=stages_ms.get("DT_MODEL"),
        features_ms=stages_ms.get("FEATURES_EXTRACTION"),
        bin_kb=_mean(sizes) if sizes else None,
        stages_ms={k: v for k, v in stages_ms.items() if v is not None},
    )


def load_experiment(folder: Path) -> ExperimentData:
    """Scan one experiment tree into an :class:`ExperimentData`."""
    folder = Path(folder)
    label = folder.name.lower()
    if label not in EXE_LABELS:
        label = folder.name
    exp = ExperimentData(path=folder, label=label)

    for code, info in CONFIG_INFO.items():
        cfg_dir = folder / info["folder"]
        if not cfg_dir.is_dir():
            continue
        series = ConfigSeries(config=code)
        for qp_dir in sorted(cfg_dir.iterdir()):
            if not qp_dir.is_dir():
                continue
            m = _QP_RE.match(qp_dir.name)
            if not m:
                continue
            qp = int(m.group(1))
            reports = _iter_rep_reports(qp_dir)
            if not reports:
                continue
            point = _build_qp_point(qp, reports)
            if point is not None:
                series.points.append(point)
        if series.points:
            exp.configs[code] = series
    return exp


def find_sibling(folder: Path) -> Optional[Path]:
    """Return the baseline/optimized sibling of *folder*, if it exists."""
    folder = Path(folder)
    name = folder.name.lower()
    if name == "optimized":
        other = folder.parent / "baseline"
    elif name == "baseline":
        other = folder.parent / "optimized"
    else:
        return None
    return other if other.is_dir() else None


# ─────────────────────────────────────────────────────────────────────────────
# Bjøntegaard metrics
# ─────────────────────────────────────────────────────────────────────────────

def _bd_degree(n: int) -> int:
    """Polynomial degree: cubic when ≥4 points, otherwise n-1."""
    return min(3, n - 1)


def bd_rate(rate_a: list[float], psnr_a: list[float],
            rate_b: list[float], psnr_b: list[float]) -> Optional[float]:
    """BD-Rate (%) of curve B relative to anchor A. Negative ⇒ B saves bitrate."""
    if len(rate_a) < 2 or len(rate_b) < 2:
        return None
    la = np.log10(np.asarray(rate_a, dtype=float))
    lb = np.log10(np.asarray(rate_b, dtype=float))
    pa = np.asarray(psnr_a, dtype=float)
    pb = np.asarray(psnr_b, dtype=float)

    deg = min(_bd_degree(len(rate_a)), _bd_degree(len(rate_b)))
    try:
        ca = np.polyfit(pa, la, deg)
        cb = np.polyfit(pb, lb, deg)
    except (np.linalg.LinAlgError, ValueError):
        return None

    lo = max(min(pa), min(pb))
    hi = min(max(pa), max(pb))
    if hi <= lo:
        return None

    ia = np.polyval(np.polyint(ca), hi) - np.polyval(np.polyint(ca), lo)
    ib = np.polyval(np.polyint(cb), hi) - np.polyval(np.polyint(cb), lo)
    avg_diff = (ib - ia) / (hi - lo)
    return float((10 ** avg_diff - 1) * 100)


def bd_psnr(rate_a: list[float], psnr_a: list[float],
            rate_b: list[float], psnr_b: list[float]) -> Optional[float]:
    """BD-PSNR (dB) of curve B relative to anchor A. Positive ⇒ B is better."""
    if len(rate_a) < 2 or len(rate_b) < 2:
        return None
    la = np.log10(np.asarray(rate_a, dtype=float))
    lb = np.log10(np.asarray(rate_b, dtype=float))
    pa = np.asarray(psnr_a, dtype=float)
    pb = np.asarray(psnr_b, dtype=float)

    deg = min(_bd_degree(len(rate_a)), _bd_degree(len(rate_b)))
    try:
        ca = np.polyfit(la, pa, deg)
        cb = np.polyfit(lb, pb, deg)
    except (np.linalg.LinAlgError, ValueError):
        return None

    lo = max(min(la), min(lb))
    hi = min(max(la), max(lb))
    if hi <= lo:
        return None

    ia = np.polyval(np.polyint(ca), hi) - np.polyval(np.polyint(ca), lo)
    ib = np.polyval(np.polyint(cb), hi) - np.polyval(np.polyint(cb), lo)
    return float((ib - ia) / (hi - lo))


# ─────────────────────────────────────────────────────────────────────────────
# Orchestration
# ─────────────────────────────────────────────────────────────────────────────

def _role_split(a: ExperimentData, b: Optional[ExperimentData]
                ) -> tuple[ExperimentData, Optional[ExperimentData]]:
    """Return (test, anchor) = (optimized, baseline) when labels allow.

    BD is reported as optimized-relative-to-baseline. When the folder names do
    not follow the baseline/optimized convention, the user-selected folder is
    treated as the test and its sibling as the anchor.
    """
    if b is None:
        return a, None
    if a.label == "optimized" and b.label == "baseline":
        return a, b
    if a.label == "baseline" and b.label == "optimized":
        return b, a
    return a, b


def analyze_folder(folder: str | Path, component: str = "YUV") -> AnalysisResult:
    """Load *folder* (+ its sibling) and compute averaged features and BD."""
    folder = Path(folder)
    selected = load_experiment(folder)

    sibling_path = find_sibling(folder)
    sibling = load_experiment(sibling_path) if sibling_path else None

    test, anchor = _role_split(selected, sibling)

    result = AnalysisResult(test=test, anchor=anchor)

    if not test.available_configs() and not (anchor and anchor.available_configs()):
        result.warnings.append(
            "No usable .report files were found under Low_Delay/ or Random_Access/."
        )

    if anchor is None:
        if sibling_path is None:
            result.warnings.append(
                "No baseline/optimized sibling found — BD-Rate/BD-PSNR need both "
                "curves, so only the selected experiment's curve is shown."
            )

    # BD per config (only when both curves exist).
    if anchor is not None:
        for code in ("LD", "RA"):
            t_series = test.configs.get(code)
            a_series = anchor.configs.get(code)
            if not t_series or not a_series:
                continue
            ra, pa = a_series.rd_arrays(component)
            rb, pb = t_series.rd_arrays(component)
            if len(ra) < 2 or len(rb) < 2:
                result.bd[code] = BDResult(
                    code, component, None, None,
                    note="Need ≥2 common QP points on both curves.",
                )
                continue
            brate = bd_rate(ra, pa, rb, pb)
            bpsnr = bd_psnr(ra, pa, rb, pb)
            note = ""
            if min(len(ra), len(rb)) < 4:
                note = f"low-order fit (degree {min(_bd_degree(len(ra)), _bd_degree(len(rb)))})"
            result.bd[code] = BDResult(code, component, brate, bpsnr, note)

    return result
