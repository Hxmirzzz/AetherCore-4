from pathlib import Path
from src.infrastructure.config.settings import get_config

class PathManager:
    def __init__(self):
        self.config = get_config()
        self.base_dir = Path(self.config.paths.base_dir)
    
    def get_solicitudes_dir(self) -> Path:
        r"""Retorna C:\AetherCore\solicitudes"""
        return self.base_dir / "solicitudes"

    def get_client_folder(self, client_name: str) -> Path:
        r"""Retorna C:\AetherCore\solicitudes\{cliente}"""
        return self.get_solicitudes_dir() / client_name

    def get_gestionado_path(self, client_name: str) -> Path:
        r"""Retorna C:\AetherCore\solicitudes\{cliente}\{solicitud}\gestionado"""
        return self.get_client_folder(client_name) / "gestionado"
        
    def get_errores_path(self, client_name: str) -> Path:
        r"""Retorna C:\AetherCore\solicitudes\{cliente}\{solicitud}\errores"""
        return self.get_client_folder(client_name) / "errores"
        
    def get_novedades_path(self, client_name: str) -> Path:
        r"""Retorna C:\AetherCore\solicitudes\{cliente}\{solicitud}\novedades"""
        return self.get_client_folder(client_name) / "novedades"

    def create_request_structure(self, client_name: str):
        """
        Llama a este método justo antes de procesar un Excel.
        Se encargará de crear toda la ruta de carpetas en Windows si no existían.
        """
        self.get_gestionado_path(client_name).mkdir(parents=True, exist_ok=True)
        self.get_errores_path(client_name).mkdir(parents=True, exist_ok=True)
        self.get_novedades_path(client_name).mkdir(parents=True, exist_ok=True)