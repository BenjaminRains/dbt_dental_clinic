"""Bootstrap Airflow CLI on Windows."""
from __future__ import annotations

import sys
from contextlib import contextmanager
from pathlib import Path

_STUBS = Path(__file__).resolve().parent / "windows_posix_stubs"
if str(_STUBS) not in sys.path:
    sys.path.insert(0, str(_STUBS))

import airflow_win_patch  # noqa: E402,F401

if sys.platform == "win32":
    import airflow.cli.commands.scheduler_command as _scheduler_command

    @contextmanager
    def _serve_logs_noop(skip_serve_logs: bool = False):
        yield

    _scheduler_command._serve_logs = _serve_logs_noop

    import airflow.www.app as _www_app
    from sqlalchemy.engine.url import make_url as _sqlalchemy_make_url

    _orig_create_app = _www_app.create_app

    def _make_url_windows(name_or_url):
        url = _sqlalchemy_make_url(name_or_url)
        if (
            url.drivername == "sqlite"
            and url.database
            and len(url.database) > 1
            and url.database[1] == ":"
        ):

            class _WinURL:
                __slots__ = ("_url",)

                def __init__(self, inner):
                    self._url = inner

                def __getattr__(self, item):
                    return getattr(self._url, item)

                @property
                def database(self):
                    return "/" + self._url.database

            return _WinURL(url)
        return url

    def _create_app_windows(config=None, testing=False):
        _www_app.make_url = _make_url_windows
        try:
            return _orig_create_app(config=config, testing=testing)
        finally:
            _www_app.make_url = _sqlalchemy_make_url

    _www_app.create_app = _create_app_windows

from airflow.__main__ import main

if __name__ == "__main__":
    main()
