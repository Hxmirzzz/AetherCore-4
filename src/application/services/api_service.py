import time
import requests
import logging
from typing import List, Optional
from src.application.dto.servicio_dto import AetherServiceImportDto

class ApiService:
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.logger = logging.getLogger(__name__)
        self.is_authenticated = False
        
    
    def login(self) -> bool:
        """
        Se autentica en VCashApp para obtener la cookie de sesión.
        Ajustado al endpoint de login estándar de ASP.NET Identity.
        """
        login_url = f"{self.base_url}/Account/Login"
        payload = {
            "Email": self.username,
            "Password": self.password,
            "RememberMe": False
        }
        
        try:
            self.logger.info(f"Intentando login para el usuario: {self.username}")
            response = self.session.post(login_url, data=payload, timeout=20)
            
            if response.status_code in [200, 302]:
                self.logger.info("Autenticación exitosa")
                self.is_authenticated = True
                return True
            else:
                self.logger.error(f"Error en el login: {response.status_code}")
                return False
        except Exception as e:
            self.logger.error(f"Error en el login: {e}")
            return False

    def upload_services(self, services: List[AetherServiceImportDto]) -> Optional[dict]:
        """
        Envía la lista de servicios al endpoint masivo en C#.
        """
        if not self.is_authenticated:
            if not self.login():
                return None
        
        endpoint = f"{self.base_url}/api/AetherCore/upload-services"
        payload = [s.to_dict() for s in services]
        
        try:
            self.logger.info(f"Enviando {len(payload)} servicios al microservicio...")
            response = self.session.post(endpoint, json=payload, timeout=60)
            
            if response.status_code == 200:
                result = response.json()
                self.logger.info(f"Carga finalizada. Resumen: {result.get('summary')}")
                return result
            elif response.status_code == 401:
                self.logger.warning("Sesión expirada. Reautenticando...")
                self.is_authenticated = False
                return self.upload_services(services)
            else:
                self.logger.error(f"Error en el endpoint de carga: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error al subir servicios: {e}")
            return None
