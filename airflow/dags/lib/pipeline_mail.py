"""Nightly success email via SMTP.

Credentials are a pydantic settings model loaded from the process environment
(start-airflow-native.ps1 exports airflow/.env.native). This does not use
Airflow's smtp connection or email_on_failure.
"""

from __future__ import annotations

import logging
import smtplib
from email.message import EmailMessage

from pydantic import SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)

NOTIFY_EMAIL = "rains.bp@gmail.com"


class PipelineMailSettings(BaseSettings):
    """SMTP credentials for the nightly success email."""

    model_config = SettingsConfigDict(
        env_prefix="PIPELINE_MAIL_",
        extra="ignore",
    )

    smtp_host: str = "smtp.gmail.com"
    smtp_port: int = 587
    smtp_starttls: bool = True
    smtp_user: str
    smtp_password: SecretStr
    mail_from: str
    mail_to: str = NOTIFY_EMAIL

    @field_validator("smtp_user", "mail_from", "mail_to")
    @classmethod
    def _not_blank(cls, value: str) -> str:
        text = value.strip()
        if not text:
            raise ValueError("mail setting is empty")
        return text

    @field_validator("smtp_password")
    @classmethod
    def _password_not_blank(cls, value: SecretStr) -> SecretStr:
        if not value.get_secret_value().strip():
            raise ValueError("smtp password is empty")
        return value


def nightly_run_succeeded(
    *,
    pipeline_success: object,
    publish_configured: bool,
    publish_ok: object,
) -> bool:
    """True when ETL finished clean and publish finished if it was configured.

    dbt test ERRORs are soft: they do not change this. publish_ok is True only
    when the publish task pushed that XCom after a successful publish.
    """
    if pipeline_success is not True:
        return False
    if publish_configured and publish_ok is not True:
        return False
    return True


def success_subject(environment: str) -> str:
    return f"Nightly ETL succeeded ({environment})"


def send_pipeline_email(settings: PipelineMailSettings, subject: str, body: str) -> None:
    """Send one plain-text message. Raises if the SMTP conversation fails."""
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = settings.mail_from
    message["To"] = settings.mail_to
    message.set_content(body)

    with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=30) as smtp:
        smtp.ehlo()
        if settings.smtp_starttls:
            smtp.starttls()
            smtp.ehlo()
        smtp.login(settings.smtp_user, settings.smtp_password.get_secret_value())
        smtp.send_message(message)
    logger.info("Sent nightly email to %s", settings.mail_to)


def email_nightly_success(
    settings: PipelineMailSettings,
    *,
    environment: str,
    body: str,
    pipeline_success: object,
    publish_configured: bool,
    publish_ok: object,
) -> bool:
    """Send the success email when the run succeeded. Returns whether it sent."""
    if not nightly_run_succeeded(
        pipeline_success=pipeline_success,
        publish_configured=publish_configured,
        publish_ok=publish_ok,
    ):
        logger.info("Nightly email not sent: run did not succeed")
        return False
    send_pipeline_email(settings, success_subject(environment), body)
    return True
