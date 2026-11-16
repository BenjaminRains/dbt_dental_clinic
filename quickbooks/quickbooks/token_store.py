"""Encrypt QuickBooks tokens with Windows DPAPI.

Access tokens are encrypted and kept in process memory. Refresh tokens are
encrypted before they are written to the stage file. The ciphertext can be
decrypted only by the same Windows user on this machine.
"""

from __future__ import annotations

import base64
import ctypes
from ctypes import wintypes

_PREFIX = "dpapi:"
_UI_FORBIDDEN = 0x1


class _DataBlob(ctypes.Structure):
    _fields_ = [("cbData", wintypes.DWORD), ("pbData", ctypes.POINTER(ctypes.c_char))]


def protect(value: str) -> str:
    """Return a DPAPI ciphertext string. Empty input stays empty."""
    if not value:
        return ""
    encrypted = _crypt(value.encode("utf-8"), protect=True)
    return _PREFIX + base64.b64encode(encrypted).decode("ascii")


def unprotect(value: str) -> str:
    """Return the original token. A value without the DPAPI prefix is returned as-is."""
    if not value or not value.startswith(_PREFIX):
        return value
    raw = base64.b64decode(value[len(_PREFIX) :])
    return _crypt(raw, protect=False).decode("utf-8")


def _crypt(data: bytes, *, protect: bool) -> bytes:
    buffer = ctypes.create_string_buffer(data, len(data))
    incoming = _DataBlob(len(data), ctypes.cast(buffer, ctypes.POINTER(ctypes.c_char)))
    outgoing = _DataBlob()
    function = ctypes.windll.crypt32.CryptProtectData if protect else ctypes.windll.crypt32.CryptUnprotectData
    ok = function(
        ctypes.byref(incoming),
        None,
        None,
        None,
        None,
        _UI_FORBIDDEN,
        ctypes.byref(outgoing),
    )
    if not ok:
        raise OSError("Windows DPAPI could not protect the QuickBooks token")
    try:
        return ctypes.string_at(outgoing.pbData, outgoing.cbData)
    finally:
        ctypes.windll.kernel32.LocalFree(outgoing.pbData)
