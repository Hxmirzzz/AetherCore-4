from __future__ import annotations
from typing import Optional

# Config
from src.infrastructure.config.settings import get_config

# API
from src.presentation.api.internal_api_client import ApiService
from src.presentation.api.external_api_client import ExternalApiClient

# Excel Processing
from src.application.processors.excel.excel_file_reader import ExcelFileReader
from src.application.processors.excel.excel_processor import ExcelProcessor
from src.infrastructure.excel.excel_styler import ExcelStyler

# Infrastructure Data
from src.infrastructure.file_system.path_manager import PathManager
from src.infrastructure.file_system.file_watcher import DirectoryWatcher
from src.application.orchestrators.processing_orchestrator import ProcessingOrchestrator

class ApplicationContainer:
    """
    Contenedor de Dependencias optimizado para AetherCore 4 (API Client).
    """

    _config = None
    _api_service: Optional[ApiService] = None
    _external_api: Optional[ExternalApiClient] = None

    # ---------- Config ----------
    def config(self):
        """Singleton de configuración."""
        if self._config is None:
            self._config = get_config()
        return self._config

    # ---------- API Service ----------
    def api_service(self) -> ApiService:
        """
        Singleton del servicio de comunicación con VCashApp.
        Mantiene la sesión (cookies) activa.
        """
        if self._api_service is None:
            conf = self.config()
            self._api_service = ApiService(
                base_url=conf.api.base_url, 
                username=conf.api.auth_user,
                password=conf.api.auth_password
            )
        return self._api_service

    def external_api(self) -> ExternalApiClient:
        """
        Singleton del servicio de comunicación con la API Externa.
        """
        if self._external_api is None:
            conf = self.config()
            self._external_api = ExternalApiClient(conf)
        return self._external_api

    # ====== XLSX PROCESSORS ======
    def excel_file_reader(self) -> ExcelFileReader:
        """
        Factory para lector de archivos Excel.
        
        Returns:
            ExcelFileReader configurado
        """
        return ExcelFileReader()

    def excel_styler(self) -> ExcelStyler:
        """Factory simple; la clase es estática pero lo exponemos para mantener patrón uniforme."""
        return ExcelStyler()

    def excel_processor(self) -> ExcelProcessor:
        """
        Factory para el procesador de Excel.
        """
        return ExcelProcessor(
            reader=self.excel_file_reader(),
            api_service=self.api_service(),
            external_api=self.external_api(),
            path_manager=self.path_manager(),
        )
        
    # ====== FILE SYSTEM ======
    def path_manager(self) -> PathManager:
        return PathManager()

    def watcher_factory(self):
        """
        Devuelve la clase DirectoryWatcher como factory para inyectarla al orquestador.
        Útil si luego quieres cambiar a watchdog u otra implementación.
        """
        return DirectoryWatcher

    # ====== ORCHESTRATORS ======
    def orchestrator(self) -> ProcessingOrchestrator:
        """
        Orquestador simplificado: Solo maneja Excel a través de la API.
        """
        return ProcessingOrchestrator(
            excel_processor=self.excel_processor(),
            path_manager=self.path_manager(),
            watcher_factory=self.watcher_factory(),
            debounce_ms=800,
        )