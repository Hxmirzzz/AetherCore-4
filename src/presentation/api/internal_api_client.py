import requests
import logging
from typing import List, Optional
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from src.application.dto.servicio_dto import AetherServiceImportDto

class ApiService:
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.logger = logging.getLogger(__name__)
        self.is_authenticated = False
        self.token = False
        
    
    def login(self) -> bool:
        """
        Se autentica en VCashApp para obtener la cookie de sesión.
        Ajustado al endpoint de login estándar de ASP.NET Identity.
        """
        login_url = f"{self.base_url}/Auth/Login"
        payload = {
            "userName": self.username,
            "passWord": self.password,
        }
        
        try:
            self.logger.info(f"Intentando login para el usuario: {self.username}")
            response = self.session.post(login_url, json=payload, timeout=20, verify=False)
            
            if response.status_code in [200, 302]:
                data = response.json()
                self.token = data.get('token')

                if self.token:
                    self.session.headers.update({"Authorization": f"Bearer {self.token}"})
                    self.is_authenticated = True
                    self.logger.info("Login exitoso")
                    return True
                else:
                    self.logger.error("Login devolvió 200, pero no se encontró el Token en el JSON.")
                    return False
            else:
                self.logger.error(f"Fallo de login. Código: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            self.logger.error(f"Error de conexión durante el login: {str(e)}")
            return False

    def upload_services(self, services: List[AetherServiceImportDto]) -> Optional[dict]:
        """
        Envía la lista de servicios al endpoint masivo asegurado con JWT.
        """
        if not self.is_authenticated:
            if not self.login():
                return None
        
        endpoint = f"{self.base_url}/AetherCore/upload-services"
        payload = [s.to_dict() for s in services]
        
        try:
            self.logger.info(f"Enviando {len(payload)} servicios al microservicio...")
            response = self.session.post(endpoint, json=payload, timeout=60, verify=False)
            
            if response.status_code == 200:
                try:
                    result = response.json()
                    summary = result.get('summary') or result.get('Summary', 'Sin resumen devuelto')
                    self.logger.info(f"✅ Carga finalizada. Resumen: {summary}")
                    return result
                except Exception as e:
                    self.logger.error("❌ El servidor devolvió 200 OK, pero el contenido NO es un JSON.")
                    self.logger.error(f"Respuesta cruda de C#: '{response.text}'")
                    return None
            elif response.status_code == 401:
                self.logger.warning("Token expirado (401). Reintentando login...")
                self.is_authenticated = False
                return self.upload_services(services)
            elif response.status_code in [301, 302, 307]:
                self.logger.error(f"❌ El servidor intentó redireccionar a: {response.headers.get('Location')}")
                return None
            else:
                self.logger.error(f"Error en el endpoint de carga: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error al subir servicios: {e}")
            return None

    def get_clients(self) -> list:
        """
        Consulta la API de VCash para obtener los clientes autorizados de AetherCore.
        """
        if not self.is_authenticated:
            if not self.login():
                raise Exception("No se pudo iniciar sesión en la API para obtener clientes.")
        
        endpoint = f"{self.base_url}/AetherCore/clients"
        
        try:
            self.logger.info("Solicitando lista de clientes al microservicio...")
            
            response = self.session.get(endpoint, timeout=20, verify=False)
            
            if response.status_code == 200:
                return response.json()
                
            elif response.status_code == 401:
                self.logger.warning("Token expirado (401). Reintentando login...")
                self.is_authenticated = False
                return self.get_clients()
                
            else:
                self.logger.error(f"Error en el endpoint de clientes: {response.status_code} - {response.text}")
                response.raise_for_status()

        except Exception as e:
            self.logger.error(f"Error al obtener clientes: {e}")
            raise

    def register_event(self, log_data: dict) -> Optional[dict]:
        """
        Registra un evento en la API de VCash.
        """
        if not self.is_authenticated:
            if not self.login():
                return None
        
        endpoint = f"{self.base_url}/AetherCore/log"
        
        try:
            self.logger.info("Registrando evento en la API...")
            
            response = self.session.post(endpoint, json=log_data, timeout=15, verify=False)
            
            if response.status_code in [200, 201]:
                return response.json()
                
            elif response.status_code == 401:
                self.logger.warning("Token expirado (401). Reintentando login...")
                self.is_authenticated = False
                return self.register_event(log_data)

            elif response.status_code == 400:
                self.logger.error(f"Error en el endpoint de registro de eventos: {response.status_code} - {response.text}")
                return None
                
            else:
                self.logger.error(f"Error en el endpoint de registro de eventos: {response.status_code} - {response.text}")
                return None

        except Exception as e:
            self.logger.error(f"Error al registrar evento: {e}")
            return None

    def update_event(self, log_id: int, status_data: dict) -> Optional[dict]:
        """
        Actualiza un evento en la API de VCash.
        """
        if not self.is_authenticated:
            if not self.login():
                return None
        
        endpoint = f"{self.base_url}/AetherCore/log/{log_id}"
        
        try:
            self.logger.info(f"Actualizando evento en la API... ID: {log_id}")
            
            response = self.session.put(endpoint, json=status_data, timeout=10, verify=False)
            
            if response.status_code in [200, 204]:
                return response.json() if response.text else { "status": "success" }
                
            elif response.status_code == 401:
                self.logger.warning("Token expirado (401). Reintentando login...")
                self.is_authenticated = False
                return self.update_event(log_id, status_data)

            elif response.status_code == 400:
                self.logger.error(f"Error en el endpoint de actualización de eventos: {response.status_code} - {response.text}")
                return None
                
            else:
                self.logger.error(f"Error en el endpoint de actualización de eventos: {response.status_code} - {response.text}")
                return None

        except Exception as e:
            self.logger.error(f"Error al actualizar evento: {e}")
            return None
