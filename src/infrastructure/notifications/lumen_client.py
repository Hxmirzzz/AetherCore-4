import logging
import requests
import os
from typing import Dict,Any

logger = logging.getLogger(__name__)

class LumenNotificationClient:
    def __init__(self):
        self.base_url = os.getenv("LUMEN_API_URL")
        self.api_key = os.getenv("LUMEN_API_KEY")

        if not self.base_url or not self.api_key:
            logger.warning("No API key provided")

    def send_alert(self, app_name: str, subject: str, error_msg: str, channel: str = "ALL", severity: str = "CRITICAL") -> bool:
        """Envía la alerta a Lumen API"""
        payload: Dict[str, Any] = {
            "sourceApp": app_name,
            "subject": subject,
            "content": str(error_msg),
            "targetChannel": channel,
            "severityLevel": severity
        }

        headers = {
            "Content-Type": "application/json",
            "X-API-KEY": self.api_key
        }

        try:
            response = requests.post(self.base_url, json=payload, headers=headers, timeout=5)
            response.raise_for_status()
            logger.info("Lumen notification sent successfully")
            return True
        except Exception as e:
            logger.error(f"Failed notification: {e}")
            return False