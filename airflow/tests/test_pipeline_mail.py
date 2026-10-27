"""Success-email decision and a real SMTP conversation on localhost."""

from __future__ import annotations

import socket
import sys
import threading
from pathlib import Path

import pytest

_DAGS = Path(__file__).resolve().parents[1] / "dags"
if str(_DAGS) not in sys.path:
    sys.path.insert(0, str(_DAGS))

from lib.pipeline_mail import (  # noqa: E402
    PipelineMailSettings,
    email_nightly_success,
    nightly_run_succeeded,
)


def test_success_requires_clean_etl_and_publish_when_configured():
    assert nightly_run_succeeded(
        pipeline_success=True,
        publish_configured=True,
        publish_ok=True,
    )
    assert not nightly_run_succeeded(
        pipeline_success=True,
        publish_configured=True,
        publish_ok=None,
    )
    assert not nightly_run_succeeded(
        pipeline_success=False,
        publish_configured=True,
        publish_ok=True,
    )
    assert nightly_run_succeeded(
        pipeline_success=True,
        publish_configured=False,
        publish_ok=None,
    )


class _LoginSmtpServer(threading.Thread):
    """Minimal SMTP that accepts AUTH LOGIN and records the DATA payload."""

    def __init__(self):
        super().__init__(daemon=True)
        self._listener = socket.socket()
        self._listener.bind(("127.0.0.1", 0))
        self.port = self._listener.getsockname()[1]
        self._listener.listen(1)
        self.message = b""
        self.ready = threading.Event()
        self.done = threading.Event()

    def run(self):
        self.ready.set()
        conn, _addr = self._listener.accept()
        conn.settimeout(10)
        reader = conn.makefile("rb")

        def send(line: bytes) -> None:
            conn.sendall(line + b"\r\n")

        send(b"220 localhost ready")
        try:
            while True:
                raw = reader.readline()
                if not raw:
                    break
                command = raw.decode("utf-8", errors="replace").strip()
                upper = command.upper()
                if upper.startswith("EHLO") or upper.startswith("HELO"):
                    send(b"250-localhost")
                    send(b"250 AUTH LOGIN")
                elif upper.startswith("AUTH LOGIN"):
                    send(b"334 VXNlcm5hbWU6")
                    reader.readline()
                    send(b"334 UGFzc3dvcmQ6")
                    reader.readline()
                    send(b"235 ok")
                elif upper.startswith("MAIL FROM"):
                    send(b"250 ok")
                elif upper.startswith("RCPT TO"):
                    send(b"250 ok")
                elif upper.startswith("DATA"):
                    send(b"354 go")
                    chunks = []
                    while True:
                        data_line = reader.readline()
                        if data_line in (b".\r\n", b".\n"):
                            break
                        chunks.append(data_line)
                    self.message = b"".join(chunks)
                    send(b"250 queued")
                elif upper.startswith("QUIT"):
                    send(b"221 bye")
                    break
                else:
                    send(b"250 ok")
        finally:
            conn.close()
            self._listener.close()
            self.done.set()


def test_success_sends_through_smtp(monkeypatch):
    server = _LoginSmtpServer()
    server.start()
    assert server.ready.wait(5)

    monkeypatch.setenv("PIPELINE_MAIL_SMTP_HOST", "127.0.0.1")
    monkeypatch.setenv("PIPELINE_MAIL_SMTP_PORT", str(server.port))
    monkeypatch.setenv("PIPELINE_MAIL_SMTP_STARTTLS", "false")
    monkeypatch.setenv("PIPELINE_MAIL_SMTP_USER", "rains.bp@gmail.com")
    monkeypatch.setenv("PIPELINE_MAIL_SMTP_PASSWORD", "local-test-secret")
    monkeypatch.setenv("PIPELINE_MAIL_MAIL_FROM", "rains.bp@gmail.com")
    monkeypatch.setenv("PIPELINE_MAIL_MAIL_TO", "rains.bp@gmail.com")

    settings = PipelineMailSettings()
    sent = email_nightly_success(
        settings,
        environment="clinic",
        body="ETL 446/446\nPublish finished\n",
        pipeline_success=True,
        publish_configured=True,
        publish_ok=True,
    )

    assert sent is True
    assert server.done.wait(5)
    text = server.message.decode("utf-8")
    assert "Subject: Nightly ETL succeeded (clinic)" in text
    assert "To: rains.bp@gmail.com" in text
    assert "ETL 446/446" in text
    assert "local-test-secret" not in text


def test_failed_publish_does_not_open_smtp(monkeypatch):
    monkeypatch.setenv("PIPELINE_MAIL_SMTP_HOST", "127.0.0.1")
    monkeypatch.setenv("PIPELINE_MAIL_SMTP_PORT", "1")
    monkeypatch.setenv("PIPELINE_MAIL_SMTP_STARTTLS", "false")
    monkeypatch.setenv("PIPELINE_MAIL_SMTP_USER", "rains.bp@gmail.com")
    monkeypatch.setenv("PIPELINE_MAIL_SMTP_PASSWORD", "local-test-secret")
    monkeypatch.setenv("PIPELINE_MAIL_MAIL_FROM", "rains.bp@gmail.com")

    settings = PipelineMailSettings()
    sent = email_nightly_success(
        settings,
        environment="clinic",
        body="publish did not finish",
        pipeline_success=True,
        publish_configured=True,
        publish_ok=None,
    )
    assert sent is False


def test_blank_password_is_rejected(monkeypatch):
    monkeypatch.setenv("PIPELINE_MAIL_SMTP_USER", "rains.bp@gmail.com")
    monkeypatch.setenv("PIPELINE_MAIL_SMTP_PASSWORD", "   ")
    monkeypatch.setenv("PIPELINE_MAIL_MAIL_FROM", "rains.bp@gmail.com")
    with pytest.raises(ValueError, match="smtp password is empty"):
        PipelineMailSettings()
