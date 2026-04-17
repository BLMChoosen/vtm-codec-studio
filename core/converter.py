"""
Converter Worker
================
Converts .y4m inputs to raw .yuv with FFmpeg and generates a VTM sequence config.
"""

import math
import re
import subprocess
import time
from pathlib import Path
from typing import Optional

from PySide6.QtCore import QThread

from core.process_runner import ProcessSignals
from utils.y4m import (
    build_sequence_cfg_text,
    count_frames_in_raw_yuv,
    frame_size_bytes,
    parse_y4m_metadata,
)


class ConverterWorker(QThread):
    """Background worker for Y4M -> YUV conversion and sequence cfg generation."""

    signals = None

    def __init__(
        self,
        ffmpeg_exe: str,
        input_y4m: str,
        output_yuv: str,
        sequence_cfg_output: str,
        level: str,
        parent=None,
    ):
        super().__init__(parent)
        self.signals = ProcessSignals()
        self.ffmpeg_exe = ffmpeg_exe
        self.input_y4m = input_y4m
        self.output_yuv = output_yuv
        self.sequence_cfg_output = sequence_cfg_output
        self.level = level

        self._cancelled = False
        self._process: Optional[subprocess.Popen] = None

    def build_command(self, pix_fmt: str) -> list[str]:
        """Build ffmpeg command for raw YUV output."""
        cmd = [
            self.ffmpeg_exe,
            "-y",
            "-i",
            self.input_y4m,
        ]
        if pix_fmt:
            cmd += ["-pix_fmt", pix_fmt]
        cmd += [
            "-f",
            "rawvideo",
            self.output_yuv,
        ]
        return cmd

    def run(self) -> None:
        try:
            if self._cancelled:
                self.signals.finished.emit(False, "Cancelled")
                return

            self.signals.log_line.emit("🔍 Reading Y4M metadata...")
            metadata = parse_y4m_metadata(self.input_y4m)
            self.signals.log_line.emit(
                (
                    "ℹ Metadata: "
                    f"{metadata.width}x{metadata.height}, "
                    f"{metadata.frame_rate_num}/{metadata.frame_rate_den} fps "
                    f"(~{metadata.frame_rate}), "
                    f"{metadata.input_bit_depth}-bit {metadata.input_chroma_format}."
                )
            )
            self.signals.progress.emit(5)

            cmd = self.build_command(metadata.ffmpeg_pix_fmt)
            self.signals.log_line.emit(f"▶ Command:\n  {' '.join(cmd)}\n")
            self.signals.started.emit()

            self._process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0,
            )

            start_time = time.time()

            for line in iter(self._process.stdout.readline, ""):
                if self._cancelled:
                    self._process.terminate()
                    self.signals.log_line.emit("\n⛔ Conversion cancelled by user.\n")
                    self.signals.finished.emit(False, "Cancelled")
                    return

                stripped = line.rstrip("\n\r")
                self.signals.log_line.emit(stripped)

                frame_match = re.search(r"frame=\s*(\d+)", stripped)
                if frame_match:
                    frame = int(frame_match.group(1))
                    progress = min(int(math.log2(frame + 1) * 10), 92)
                    self.signals.progress.emit(progress)

            self._process.stdout.close()
            return_code = self._process.wait()
            elapsed = time.time() - start_time

            if return_code != 0:
                self.signals.log_line.emit(
                    f"\n❌ FFmpeg failed with exit code {return_code} after {elapsed:.1f}s\n"
                )
                self.signals.finished.emit(False, f"FFmpeg exit code {return_code}")
                return

            self.signals.progress.emit(95)
            one_frame = frame_size_bytes(metadata)
            frames = count_frames_in_raw_yuv(self.output_yuv, one_frame)

            cfg_text = build_sequence_cfg_text(
                metadata=metadata,
                input_file=Path(self.output_yuv).name,
                frames_to_encode=frames,
                level=self.level,
            )
            Path(self.sequence_cfg_output).write_text(cfg_text, encoding="utf-8")

            self.signals.log_line.emit(
                (
                    "📝 Sequence config generated: "
                    f"{self.sequence_cfg_output} "
                    f"(FramesToBeEncoded={frames})"
                )
            )
            self.signals.progress.emit(100)
            self.signals.log_line.emit(
                f"\n✅ Conversion completed successfully in {elapsed:.1f}s\n"
            )
            self.signals.finished.emit(True, f"Done in {elapsed:.1f}s")

        except FileNotFoundError as exc:
            self.signals.log_line.emit(f"\n❌ Executable not found: {exc}\n")
            self.signals.finished.emit(False, "Executable not found")
        except PermissionError as exc:
            self.signals.log_line.emit(f"\n❌ Permission denied: {exc}\n")
            self.signals.finished.emit(False, "Permission denied")
        except Exception as exc:
            self.signals.log_line.emit(f"\n❌ Unexpected error: {exc}\n")
            self.signals.finished.emit(False, str(exc))

    def cancel(self) -> None:
        """Request graceful cancellation of conversion."""
        self._cancelled = True
        if self._process and self._process.poll() is None:
            self._process.terminate()
