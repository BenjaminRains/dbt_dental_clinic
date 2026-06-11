# api/cors_runtime.py
"""Runtime CORS allowlist for responses that bypass CORSMiddleware (e.g. rate-limit short-circuit)."""
from __future__ import annotations

_ALLOWED: frozenset[str] = frozenset()


def set_allowed_origins(origins: list[str]) -> None:
    """Called from main.py after computing cors_origins."""
    global _ALLOWED
    _ALLOWED = frozenset(o.strip() for o in origins if o and o.strip())


def apply_cors_to_response(request, response) -> None:
    """Attach CORS headers if Origin matches the same allowlist as CORSMiddleware."""
    origin = request.headers.get("origin")
    if not origin or origin not in _ALLOWED:
        return
    response.headers["Access-Control-Allow-Origin"] = origin
    response.headers["Access-Control-Allow-Credentials"] = "true"
    if "Vary" not in response.headers:
        response.headers["Vary"] = "Origin"
