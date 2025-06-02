"""
Notification utilities for the ETL pipeline.
"""
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Dict, Any, Optional
import requests
from dataclasses import dataclass
import json
from datetime import datetime

logger = logging.getLogger(__name__)

@dataclass
class NotificationConfig:
    """Configuration for notification channels."""
    email: Optional[Dict[str, str]] = None  # SMTP settings
    slack: Optional[Dict[str, str]] = None  # Slack webhook URL
    teams: Optional[Dict[str, str]] = None  # Teams webhook URL
    custom_webhook: Optional[Dict[str, str]] = None  # Custom webhook settings

class NotificationManager:
    """Handles notifications for the ETL pipeline."""
    
    def __init__(self, config: NotificationConfig):
        self.config = config
        self.notification_history: List[Dict[str, Any]] = []
    
    def send_notification(self, subject: str, message: str,
                         severity: str = 'info',
                         channels: Optional[List[str]] = None) -> bool:
        """
        Send notification through configured channels.
        
        Args:
            subject: Notification subject
            message: Notification message
            severity: Message severity (info, warning, error)
            channels: List of channels to send to (default: all configured)
            
        Returns:
            bool: True if notification was sent successfully
        """
        if channels is None:
            channels = ['email', 'slack', 'teams', 'custom_webhook']
        
        success = True
        notification = {
            'subject': subject,
            'message': message,
            'severity': severity,
            'timestamp': str(datetime.now()),
            'channels': channels,
            'status': 'success'
        }
        
        for channel in channels:
            try:
                if channel == 'email' and self.config.email:
                    self._send_email(subject, message)
                elif channel == 'slack' and self.config.slack:
                    self._send_slack(subject, message, severity)
                elif channel == 'teams' and self.config.teams:
                    self._send_teams(subject, message, severity)
                elif channel == 'custom_webhook' and self.config.custom_webhook:
                    self._send_webhook(subject, message, severity)
            except Exception as e:
                logger.error(f"Failed to send {channel} notification: {str(e)}")
                success = False
                notification['status'] = 'failed'
                notification['error'] = str(e)
        
        self.notification_history.append(notification)
        return success
    
    def _send_email(self, subject: str, message: str) -> None:
        """Send email notification."""
        if not self.config.email:
            return
        
        msg = MIMEMultipart()
        msg['From'] = self.config.email['from']
        msg['To'] = self.config.email['to']
        msg['Subject'] = subject
        
        msg.attach(MIMEText(message, 'plain'))
        
        with smtplib.SMTP(self.config.email['smtp_server'],
                         int(self.config.email['smtp_port'])) as server:
            if self.config.email.get('use_tls'):
                server.starttls()
            if self.config.email.get('username'):
                server.login(self.config.email['username'],
                           self.config.email['password'])
            server.send_message(msg)
    
    def _send_slack(self, subject: str, message: str, severity: str) -> None:
        """Send Slack notification."""
        if not self.config.slack:
            return
        
        color = {
            'info': '#36a64f',
            'warning': '#ffcc00',
            'error': '#ff0000'
        }.get(severity, '#36a64f')
        
        payload = {
            'attachments': [{
                'color': color,
                'title': subject,
                'text': message,
                'ts': datetime.now().timestamp()
            }]
        }
        
        response = requests.post(
            self.config.slack['webhook_url'],
            json=payload
        )
        response.raise_for_status()
    
    def _send_teams(self, subject: str, message: str, severity: str) -> None:
        """Send Microsoft Teams notification."""
        if not self.config.teams:
            return
        
        payload = {
            'title': subject,
            'text': message,
            'themeColor': {
                'info': '0076D7',
                'warning': 'FFA500',
                'error': 'FF0000'
            }.get(severity, '0076D7')
        }
        
        response = requests.post(
            self.config.teams['webhook_url'],
            json=payload
        )
        response.raise_for_status()
    
    def _send_webhook(self, subject: str, message: str, severity: str) -> None:
        """Send custom webhook notification."""
        if not self.config.custom_webhook:
            return
        
        payload = {
            'subject': subject,
            'message': message,
            'severity': severity,
            'timestamp': str(datetime.now())
        }
        
        headers = {}
        if self.config.custom_webhook.get('auth_token'):
            headers['Authorization'] = f"Bearer {self.config.custom_webhook['auth_token']}"
        
        response = requests.post(
            self.config.custom_webhook['url'],
            json=payload,
            headers=headers
        )
        response.raise_for_status()
    
    def get_notification_history(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get notification history.
        
        Args:
            limit: Optional limit on number of notifications to return
            
        Returns:
            List[Dict[str, Any]]: Notification history
        """
        if limit:
            return self.notification_history[-limit:]
        return self.notification_history
    
    def clear_history(self) -> None:
        """Clear notification history."""
        self.notification_history.clear() 