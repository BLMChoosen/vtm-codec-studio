"""
Encoder Worker
==============
Builds the EncoderAppStatic command and estimates progress
by parsing VTM's POC (Picture Order Count) output.
"""

import re
from typing import Optional

from core.process_runner import ProcessWorker


class EncoderWorker(ProcessWorker):
    """
    Concrete worker for VTM encoding.

    Parameters
    ----------
    encoder_exe : str
        Full path to EncoderAppStatic.exe
    main_cfg : str
        Path to the main encoder configuration file (e.g. encoder_intra_vtm.cfg)
    sequence_cfg : str
        Path to the per-sequence configuration file
    input_yuv : str
        Path to the raw YUV input video
    frames : int
        Number of frames to encode (-f)
    qp : int
        Quantisation parameter (-q)
    output_bin : str
        Path for the output bitstream file (-b)
    """

    def __init__(
        self,
        encoder_exe: str,
        main_cfg: str,
        sequence_cfg: str,
        input_yuv: str,
        frames: int,
        qp: int,
        output_bin: str,
        parent=None,
    ):
        super().__init__(parent)
        self.encoder_exe = encoder_exe
        self.main_cfg = main_cfg
        self.sequence_cfg = sequence_cfg
        self.input_yuv = input_yuv
        self.frames = frames
        self.qp = qp
        self.output_bin = output_bin
        self._encoded_pocs: set[int] = set()

    def build_command(self) -> list[str]:
        """Assemble the EncoderAppStatic command line."""
        cmd = [
            self.encoder_exe,
            "-c", self.main_cfg,
        ]
        # Per-sequence config is optional
        if self.sequence_cfg:
            cmd += ["-c", self.sequence_cfg]
        cmd += [
            "-i", self.input_yuv,
            "-f", str(self.frames),
            "-q", str(self.qp),
            "-b", self.output_bin,
        ]
        return cmd

    def parse_progress_line(self, line: str) -> Optional[int]:
        """
        VTM prints lines like:
            POC    0 ... (  128 bits)
        In random-access configs, POCs are not emitted in ascending order
        (e.g. 0, 16, 8, ...), so progress must be based on how many frames
        were encoded, not on the latest POC value.
        """
        match = re.match(r"\s*POC\s+(\d+)", line)
        if match and self.frames > 0:
            poc = int(match.group(1))
            self._encoded_pocs.add(poc)
            encoded_frames = min(len(self._encoded_pocs), self.frames)
            return int(encoded_frames / self.frames * 100)
        return None
