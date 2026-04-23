import requests
import logging
from typing import Optional, Dict, Any


class ExternalApiClient:
    """
    Client for external API integrations.
    Extend this class for specific external service integrations.
    """
    
    def __init__(self, base_url: str, api_key: Optional[str] = None):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.session = requests.Session()
        self.logger = logging.getLogger(__name__)
        
        if api_key:
            self.session.headers.update({"Authorization": f"Bearer {api_key}"})
    
    def get(self, endpoint: str, **kwargs) -> Optional[Dict[str, Any]]:
        """Make a GET request to the external API."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        try:
            response = self.session.get(url, timeout=30, **kwargs)
            response.raise_for_status()
            return response.json() if response.text else {}
        except Exception as e:
            self.logger.error(f"Error in GET request to {url}: {e}")
            return None
    
    def post(self, endpoint: str, data: Optional[Dict] = None, **kwargs) -> Optional[Dict[str, Any]]:
        """Make a POST request to the external API."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        try:
            response = self.session.post(url, json=data, timeout=30, **kwargs)
            response.raise_for_status()
            return response.json() if response.text else {}
        except Exception as e:
            self.logger.error(f"Error in POST request to {url}: {e}")
            return None
