"""
Alert sending functionality for the ETL pipeline.
Supports Slack and email notifications.
"""
import os
from typing import Optional, List
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import requests
from etl_pipeline.core.logger import get_logger

logger = get_logger(__name__)

class AlertManager:
    """Manages alert sending for the ETL pipeline."""
    
    def __init__(self):
        """Initialize alert manager with configuration."""
        self.slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL')
        self.smtp_host = os.getenv('SMTP_HOST')
        self.smtp_port = int(os.getenv('SMTP_PORT', '587'))
        self.smtp_user = os.getenv('SMTP_USER')
        self.smtp_password = os.getenv('SMTP_PASSWORD')
        self.alert_email = os.getenv('ALERT_EMAIL')
    
    def send_slack_alert(self, message: str, channel: Optional[str] = None) -> bool:
        """
        Send alert to Slack.
        
        Args:
            message: Alert message to send
            channel: Optional Slack channel to send to
            
        Returns:
            bool: True if alert was sent successfully
        """
        if not self.slack_webhook_url:
            logger.warning("Slack webhook URL not configured")
            return False
        
        try:
            payload = {'text': message}
            if channel:
                payload['channel'] = channel
            
            response = requests.post(
                self.slack_webhook_url,
                json=payload,
                timeout=10
            )
            response.raise_for_status()
            logger.info(f"Sent Slack alert: {message}")
            return True
        except Exception as e:
            logger.error(f"Failed to send Slack alert: {e}")
            return False
    
    def send_email_alert(
        self,
        subject: str,
        message: str,
        recipients: Optional[List[str]] = None
    ) -> bool:
        """
        Send alert via email.
        
        Args:
            subject: Email subject
            message: Email message body
            recipients: List of email recipients (defaults to ALERT_EMAIL)
            
        Returns:
            bool: True if alert was sent successfully
        """
        if not all([self.smtp_host, self.smtp_user, self.smtp_password]):
            logger.warning("SMTP configuration incomplete")
            return False
        
        if not recipients:
            if not self.alert_email:
                logger.warning("No alert email recipients configured")
                return False
            recipients = [self.alert_email]
        
        try:
            msg = MIMEMultipart()
            msg['From'] = self.smtp_user
            msg['To'] = ', '.join(recipients)
            msg['Subject'] = subject
            
            msg.attach(MIMEText(message, 'plain'))
            
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.send_message(msg)
            
            logger.info(f"Sent email alert to {', '.join(recipients)}")
            return True
        except Exception as e:
            logger.error(f"Failed to send email alert: {e}")
            return False
    
    def send_pipeline_alert(
        self,
        pipeline_id: str,
        status: str,
        details: str,
        error: Optional[str] = None
    ) -> None:
        """
        Send pipeline status alert through all configured channels.
        
        Args:
            pipeline_id: ID of the pipeline
            status: Pipeline status (e.g., 'completed', 'failed')
            details: Additional details about the status
            error: Optional error message if pipeline failed
        """
        message = f"Pipeline {pipeline_id} {status}\n{details}"
        if error:
            message += f"\nError: {error}"
        
        # Send to Slack
        self.send_slack_alert(message)
        
        # Send email
        subject = f"ETL Pipeline Alert: {pipeline_id} {status}"
        self.send_email_alert(subject, message)

# Create global alert manager instance
alert_manager = AlertManager() 