"""
Output Tab
==========
Displays analytical outputs such as compression time, PSNR, bitrate, SSIM, entropy, and file size.
"""

from PySide6.QtCore import Slot
from PySide6.QtWidgets import (
    QGroupBox,
    QFormLayout,
    QLabel,
    QVBoxLayout,
    QWidget,
)

class OutputTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        
        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(16)
        
        self.group = QGroupBox("Encoding Results")
        form_layout = QFormLayout(self.group)
        form_layout.setSpacing(12)
        
        self.lbl_time = QLabel("-")
        self.lbl_psnry = QLabel("-")
        self.lbl_psnru = QLabel("-")
        self.lbl_psnrv = QLabel("-")
        self.lbl_psnryuv = QLabel("-")
        self.lbl_bitrate = QLabel("-")
        self.lbl_ssim = QLabel("-")
        self.lbl_entropy = QLabel("-")
        self.lbl_size = QLabel("-")
        
        form_layout.addRow("Compression time:", self.lbl_time)
        form_layout.addRow("PSNR-Y:", self.lbl_psnry)
        form_layout.addRow("PSNR-U:", self.lbl_psnru)
        form_layout.addRow("PSNR-V:", self.lbl_psnrv)
        form_layout.addRow("PSNR-YUV:", self.lbl_psnryuv)
        form_layout.addRow("Bitrate:", self.lbl_bitrate)
        form_layout.addRow("SSIM:", self.lbl_ssim)
        form_layout.addRow("Entropy:", self.lbl_entropy)
        form_layout.addRow("File size:", self.lbl_size)
        
        layout.addWidget(self.group)
        layout.addStretch()

    @Slot(dict)
    def update_metrics(self, metrics: dict):
        self.lbl_time.setText(metrics.get("time", "-"))
        self.lbl_psnry.setText(metrics.get("psnr_y", "-"))
        self.lbl_psnru.setText(metrics.get("psnr_u", "-"))
        self.lbl_psnrv.setText(metrics.get("psnr_v", "-"))
        self.lbl_psnryuv.setText(metrics.get("psnr_yuv", "-"))
        self.lbl_bitrate.setText(metrics.get("bitrate", "-"))
        self.lbl_ssim.setText(metrics.get("ssim", "-"))
        self.lbl_entropy.setText(metrics.get("entropy", "-"))
        self.lbl_size.setText(metrics.get("size", "-"))
