"""
Process Runner
==============
Asynchronous subprocess execution with real-time stdout/stderr streaming
via Qt signals. Both the Encoder and Decoder workers inherit from this base.
"""

import subprocess
import time
from typing import Optional

from PySide6.QtCore import QObject, QThread, Signal


class ProcessSignals(QObject):
    """Signals emitted by a running subprocess worker."""

    log_line = Signal(str)          # Each line of stdout/stderr
    progress = Signal(int)          # Estimated progress 0-100
    finished = Signal(bool, str)    # (success, summary_message)
    started = Signal()              # Process has launched


class ProcessWorker(QThread):
    """
    Base worker that runs an external command in a background thread,
    streaming output line-by-line through Qt signals.

    Subclasses should override ``build_command()`` and optionally
    ``parse_progress_line()`` to extract progress from output.
    """

    signals = None  # Assigned per-instance in __init__

    def __init__(self, parent: Optional[QObject] = None):
        super().__init__(parent)
        self.signals = ProcessSignals()
        self._process: Optional[subprocess.Popen] = None
        self._cancelled = False

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    def build_command(self) -> list[str]:
        """Return the command list to execute. Must be overridden."""
        raise NotImplementedError

    def parse_progress_line(self, line: str) -> Optional[int]:
        """
        Optionally parse a line of output and return a progress value (0-100).
        Return None to skip progress update for this line.
        """
        return None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Thread entry point — launch subprocess and stream output."""
        try:
            cmd = self.build_command()
            self.signals.log_line.emit(f"▶ Command:\n  {' '.join(cmd)}\n")
            self.signals.started.emit()

            self._process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,                   # Line-buffered
                creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0,
            )

            start_time = time.time()

            for line in iter(self._process.stdout.readline, ""):
                if self._cancelled:
                    self._process.terminate()
                    self.signals.log_line.emit("\n⛔ Process cancelled by user.\n")
                    self.signals.finished.emit(False, "Cancelled")
                    return

                stripped = line.rstrip("\n\r")
                self.signals.log_line.emit(stripped)

                # Try to extract progress
                progress = self.parse_progress_line(stripped)
                if progress is not None:
                    self.signals.progress.emit(min(progress, 100))

            self._process.stdout.close()
            return_code = self._process.wait()
            elapsed = time.time() - start_time

            if return_code == 0:
                self.signals.progress.emit(100)
                self.signals.log_line.emit(
                    f"\n✅ Process completed successfully in {elapsed:.1f}s (exit code 0)\n"
                )
                self.signals.finished.emit(True, f"Done in {elapsed:.1f}s")
            else:
                self.signals.log_line.emit(
                    f"\n❌ Process failed with exit code {return_code} after {elapsed:.1f}s\n"
                )
                self.signals.finished.emit(False, f"Exit code {return_code}")

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
        """Request graceful cancellation of the running process."""
        self._cancelled = True
        if self._process and self._process.poll() is None:
            self._process.terminate()
