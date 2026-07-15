"""Bootstrap Airflow CLI on Windows (Airflow 3.x)."""
from __future__ import annotations

import sys
from contextlib import contextmanager
from pathlib import Path

_STUBS = Path(__file__).resolve().parent / "windows_posix_stubs"
if str(_STUBS) not in sys.path:
    sys.path.insert(0, str(_STUBS))

import airflow_win_patch  # noqa: E402,F401

if sys.platform == "win32":
    try:
        import airflow.cli.commands.scheduler_command as _scheduler_command

        @contextmanager
        def _serve_logs_noop(skip_serve_logs: bool = False):
            yield

        _scheduler_command._serve_logs = _serve_logs_noop
    except Exception:
        pass  # scheduler log-serve patch optional; fail open

    # Airflow 2 Flask www patches — only apply if legacy www package exists.
    # Airflow 3 uses FastAPI api-server; these modules are absent or unused.
    try:
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

        def _patch_static_manifest_paths() -> None:
            """Airflow 2: os.path.join('dist', file) produced backslashes on Windows."""
            import os as _os

            import airflow.www.extensions.init_manifest_files as _imf

            _orig_configure = _imf.configure_manifest_files

            def configure_manifest_files(app):
                _orig_join = _os.path.join

                def _join(*parts):
                    path = _orig_join(*parts)
                    if len(parts) == 2 and parts[0] == "dist":
                        return path.replace("\\", "/")
                    return path

                _os.path.join = _join
                try:
                    return _orig_configure(app)
                finally:
                    _os.path.join = _orig_join

            _imf.configure_manifest_files = configure_manifest_files

        _patch_static_manifest_paths()
    except Exception:
        pass  # Airflow 3 api-server path — no Flask www patches needed

from airflow.__main__ import main

if __name__ == "__main__":
    main()
