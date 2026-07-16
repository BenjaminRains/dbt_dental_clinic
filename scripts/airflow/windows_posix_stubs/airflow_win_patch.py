"""Apply POSIX shims when running Airflow on Windows (dev laptop only)."""
from __future__ import annotations

import os
import signal
import socket
import sys
from pathlib import Path
from types import ModuleType

if sys.platform != "win32":
    raise ImportError("airflow_win_patch is for Windows only")

# Ensure fcntl is importable even when stubs are not on PYTHONPATH (site .pth bootstrap).
if "fcntl" not in sys.modules:
    _fcntl = ModuleType("fcntl")
    _fcntl.F_GETFD = 1
    _fcntl.F_SETFD = 2
    _fcntl.F_GETFL = 3
    _fcntl.F_SETFL = 4
    _fcntl.FD_CLOEXEC = 1
    _fcntl.LOCK_SH = 1
    _fcntl.LOCK_EX = 2
    _fcntl.LOCK_NB = 4
    _fcntl.LOCK_UN = 8
    _fcntl.fcntl = lambda fd, cmd, arg=0: 0
    _fcntl.flock = lambda fd, operation: None
    _fcntl.lockf = lambda fd, cmd, len=0, start=0, whence=0: None
    sys.modules["fcntl"] = _fcntl
    # Prefer repo stub if present on sys.path (keeps single source of truth).
    _stub_candidates = [
        Path(__file__).resolve().parent / "fcntl.py",
        Path(__file__).resolve().parents[1] / "windows_posix_stubs" / "fcntl.py",
    ]
    for _cand in _stub_candidates:
        if _cand.is_file():
            import importlib.util

            _spec = importlib.util.spec_from_file_location("fcntl", _cand)
            if _spec and _spec.loader:
                _mod = importlib.util.module_from_spec(_spec)
                _spec.loader.exec_module(_mod)
                sys.modules["fcntl"] = _mod
            break

if not hasattr(os, "geteuid"):
    os.geteuid = lambda: 1000
if not hasattr(os, "getegid"):
    os.getegid = lambda: 1000
if not hasattr(os, "getuid"):
    os.getuid = lambda: 1000
if not hasattr(os, "getgid"):
    os.getgid = lambda: 1000
if not hasattr(os, "getsid"):
    os.getsid = lambda pid: os.getpid()
if not hasattr(os, "setpgid"):
    os.setpgid = lambda pid, pgid: None
if not hasattr(os, "getpgid"):
    os.getpgid = lambda pid: pid

if not hasattr(socket, "AF_UNIX"):
    socket.AF_UNIX = 1
if not hasattr(socket, "SOCK_CLOEXEC"):
    socket.SOCK_CLOEXEC = 0

_UNIX_SIGNALS = {
    "SIGHUP": 1,
    "SIGINT": 2,
    "SIGQUIT": 3,
    "SIGILL": 4,
    "SIGABRT": 6,
    "SIGKILL": 9,
    "SIGUSR1": 10,
    "SIGUSR2": 12,
    "SIGPIPE": 13,
    "SIGALRM": 14,
    "SIGTERM": 15,
    "SIGCHLD": 17,
    "SIGTTIN": 21,
    "SIGTTOU": 22,
    "SIGWINCH": 28,
}
for _name, _value in _UNIX_SIGNALS.items():
    if not hasattr(signal, _name):
        setattr(signal, _name, _value)

if not hasattr(signal, "ITIMER_REAL"):
    signal.ITIMER_REAL = 0
if not hasattr(signal, "setitimer"):
    # DagBag import timeout uses setitimer/SIGALRM (POSIX-only). No-op on Windows.
    signal.setitimer = lambda *args, **kwargs: (0.0, 0.0)
if not hasattr(signal, "getitimer"):
    signal.getitimer = lambda *args, **kwargs: (0.0, 0.0)

if not hasattr(signal, "siginterrupt"):
    signal.siginterrupt = lambda signum, flag: None

_original_signal = signal.signal


def _patched_signal(signalnum, handler):
    try:
        return _original_signal(signalnum, handler)
    except (ValueError, OSError):
        return signal.SIG_DFL


signal.signal = _patched_signal


def _sanitize_windows_path(path) -> "Path":
    """Colons in Airflow run_id break mkdir on Windows (WinError 123)."""
    from pathlib import Path

    text = str(path)
    if len(text) >= 2 and text[1] == ":":
        text = text[:2] + text[2:].replace(":", "-")
    else:
        text = text.replace(":", "-")
    return Path(text)


def _patch_airflow_log_paths() -> None:
    from pathlib import Path

    from airflow.utils.log.file_task_handler import FileTaskHandler

    _orig_prepare = FileTaskHandler._prepare_log_folder
    _orig_init = FileTaskHandler._init_file

    def _prepare_log_folder_windows(self, directory: Path):
        return _orig_prepare(self, _sanitize_windows_path(directory))

    def _init_file_windows(self, ti):
        import os

        from airflow.configuration import conf

        new_file_permissions = int(
            conf.get("logging", "file_task_handler_new_file_permissions", fallback="0o664"), 8
        )
        local_relative_path = self._render_filename(ti, ti.try_number)
        full_path = os.path.join(self.local_base, local_relative_path)
        if ti.is_trigger_log_context is True:
            full_path = self.add_triggerer_suffix(full_path=full_path, job_id=ti.triggerer_job.id)
        safe_full_path = str(_sanitize_windows_path(full_path))
        self._prepare_log_folder(Path(safe_full_path).parent)
        if not os.path.exists(safe_full_path):
            open(safe_full_path, "a").close()
            try:
                os.chmod(safe_full_path, new_file_permissions)
            except OSError as e:
                import logging

                logging.warning("OSError while changing ownership of the log file. %s", e)
        return safe_full_path

    FileTaskHandler._prepare_log_folder = _prepare_log_folder_windows
    FileTaskHandler._init_file = _init_file_windows


_patch_airflow_log_paths()
