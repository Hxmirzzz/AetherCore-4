import requests
import logging
import urllib3
import json
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

class ExternalApiClient:
    """
    Client for external API integrations.
    Extend this class for specific external service integrations.
    """
    
    def __init__(self, config):
        self.base_url = config.external_api.url.rstrip('/')
        self.user = config.external_api.user
        self.password = config.external_api.password
        self.session = requests.Session()
        self.session.verify = True
        self.token = None

    def authenticate(self):
        """Authenticate with the external API and store the token."""
        url = f"{self.base_url}/auth/login/"
        payload = {
            "login": self.user,
            "password": self.password
        }

        try:
            response = self.session.post(url, json=payload, timeout=10)
            response.raise_for_status()
            data = response.json()
            self.token = data.get("token") or data.get("access")

            if self.token:
                self.session.headers.update({"Authorization": f"Bearer {self.token}"})
                logger.info("Authentication successful")
            else:
                logger.warning("Authentication failed: no token received")
                raise

        except requests.exceptions.RequestException as e:
            logger.error(f"Error in POST request to {url}: {e}")
            raise

    def create_service_order(self, order_data: Dict[str, Any]) -> Dict[str, Any]:
        """Crea una orden de servicio en la API externa."""
        if not self.token:
            self.authenticate()

        url = f"{self.base_url}/service-orders/"
        try:
            response = self.session.post(url, json=order_data, timeout=15)
            if response.status_code == 400:
                logger.error(f"Error in POST request to {url}: {response.status_code}")
                req = response.request

                cuerpo_enviado = req.body.decode('utf-8') if isinstance(req.body, bytes) else req.body
                
                logger.error("=" * 60)
                logger.error(f"Method and URL: {req.method} {req.url}")
                logger.error(f"Headers: {dict(req.headers)}")
                logger.error(f"Body:\n{cuerpo_enviado}")
                logger.error("-" * 60)
                logger.error(f"Response:\n{response.text}")
                logger.error("=" * 60)
            response.raise_for_status()
            return {"status": "success", "data": response.json(), "status_code": response.status_code}
        except requests.exceptions.RequestException as e:
            logger.error(f"Error in POST request to {url}: {e}")
            return {"status": "error", "message": str(e), "details": response.text if response else None}
            
    def create_bulk_orders(self, order_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Crea múltiples órdenes de servicio en la API externa."""
        if not self.token:
            self.authenticate()

        url = f"{self.base_url}/service-orders/bulk/"
        payload = {"orders": order_list}

        logger.info("=" * 60)
        logger.info("📦 PREVIEW DEL PAYLOAD BULK QUE SE VA A ENVIAR:")
        logger.info(json.dumps(payload, indent=4, default=str))
        logger.info("=" * 60)

        try:
            response = self.session.post(url, json=payload, timeout=30)

            if response.status_code >= 400:
                logger.error(f"Error in POST request to {url}: {response.status_code}")
                req = response.request

                cuerpo_enviado = req.body.decode('utf-8') if isinstance(req.body, bytes) else req.body
                
                logger.error("=" * 60)
                logger.error(f"Method and URL: {req.method} {req.url}")
                logger.error(f"Headers: {dict(req.headers)}")
                logger.error(f"Body Real Enviado:\n{cuerpo_enviado}")
                logger.error("-" * 60)
                logger.error(f"Response:\n{response.text}")
                logger.error("=" * 60)

            response.raise_for_status()
            return {"status": "success", "data": response.json(), "status_code": response.status_code}
        except requests.exceptions.RequestException as e:
            logger.error(f"Error in POST request to {url}: {e}")
            try:
                error_data = response.json() if 'response' in locals() and response else None
            except:
                error_data = response.text if 'response' in locals() and response else "Error desconocido"

            return {"status": "error", "message": str(e), "details": error_data}

    def get_mapping_clients(self) -> Dict[str, Any]:
        """Obtiene el mapeo de clientes de la API externa."""
        if not self.token:
            self.authenticate()

        url = f"{self.base_url}/clients/"
        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            clients_data = response.json()

            mapping = {}
            for client in clients_data:
                nit = str(client.get("tax_identification", "")).strip()
                code = client.get("client_code", "")
                name = client.get("commercial_name", "") or client.get("business_name", "")
                if nit and code:
                    mapping[nit] = {"code": code, "name": name}
            return mapping
        except Exception as e:
            logger.error(f"Error in GET request to {url}: {e}")
            return {}

    def get_service_types_mapping(self) -> Dict[str, str]:
        """Obtiene el mapeo de tipos de servicio de la API externa."""
        if not self.token:
            self.authenticate()

        url = f"{self.base_url}/service-types/"
        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            service_types_data = response.json()

            mapping = {}
            for service_type in service_types_data:
                code = service_type.get("code", "")
                name = service_type.get("name", "")
                if code and name:
                    mapping[name] = code
            return mapping
        except Exception as e:
            logger.error(f"Error in GET request to {url}: {e}")
            return {}
