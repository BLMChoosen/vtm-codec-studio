"""
CSV Export
==========
Utilities to export codec metrics to CSV.
"""

import csv
from pathlib import Path

METRIC_FIELDS = [
    "time",
    "psnr_y",
    "psnr_u",
    "psnr_v",
    "psnr_yuv",
    "bitrate",
    "ssim",
    "entropy",
    "size",
]


def write_metrics_csv(csv_path: str, metrics: dict) -> None:
    """Write one metrics row to a CSV file with stable output-tab columns."""
    path = Path(csv_path)
    row = {field: metrics.get(field, "-") for field in METRIC_FIELDS}

    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=METRIC_FIELDS)
        writer.writeheader()
        writer.writerow(row)
