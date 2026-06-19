"""Apply POSIX shims when running Airflow on Windows (dev laptop only)."""
from __future__ import annotations

import os
import signal
import socket
import sys

if sys.platform != "win32":
    raise ImportError("airflow_win_patch is for Windows only")

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
    "SIGTERM": 15,
    "SIGCHLD": 17,
    "SIGTTIN": 21,
    "SIGTTOU": 22,
    "SIGWINCH": 28,
}
for _name, _value in _UNIX_SIGNALS.items():
    if not hasattr(signal, _name):
        setattr(signal, _name, _value)

if not hasattr(signal, "siginterrupt"):
    signal.siginterrupt = lambda signum, flag: None

_original_signal = signal.signal


def _patched_signal(signalnum, handler):
    try:
        return _original_signal(signalnum, handler)
    except (ValueError, OSError):
        return signal.SIG_DFL


signal.signal = _patched_signal
