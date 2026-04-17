"""
Decoder Worker
==============
Builds the DecoderAppStatic command and estimates progress
by parsing VTM's POC output lines.
"""

import re
from typing import Optional

from core.process_runner import ProcessWorker


class DecoderWorker(ProcessWorker):
    """
    Concrete worker for VTM decoding.

    Parameters
    ----------
    decoder_exe : str
        Full path to DecoderAppStatic.exe
    input_bin : str
        Path to the .bin bitstream file
    output_yuv : str
        Path for the reconstructed YUV output file
    """

    def __init__(
        self,
        decoder_exe: str,
        input_bin: str,
        output_yuv: str,
        parent=None,
    ):
        super().__init__(parent)
        self.decoder_exe = decoder_exe
        self.input_bin = input_bin
        self.output_yuv = output_yuv
        self._max_poc_seen = 0

    def build_command(self) -> list[str]:
        """Assemble the DecoderAppStatic command line."""
        return [
            self.decoder_exe,
            "-b", self.input_bin,
            "-o", self.output_yuv,
        ]

    def parse_progress_line(self, line: str) -> Optional[int]:
        """
        Decoder also prints POC lines. We track the highest POC seen
        and emit a coarse progress estimate.
        Since total frames is unknown, we use a heuristic
        (progress grows logarithmically, capping at 95 until done).
        """
        match = re.match(r"POC\s+(\d+)", line)
        if match:
            poc = int(match.group(1))
            self._max_poc_seen = max(self._max_poc_seen, poc + 1)
            # Rough heuristic: log-scale progress capped at 95%
            import math
            progress = min(int(math.log2(self._max_poc_seen + 1) * 10), 95)
            return progress
        return None
