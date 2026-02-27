from __future__ import annotations
from typing import Optional

# Config y DB
from src.infrastructure.config.settings import get_config

# API
from src.application.services.api_service import ApiService

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
                base_url=conf.api_base_url,
                username=conf.api_username,
                password=conf.api_password,
            )
        return self._api_service

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
        conf = self.config()
        base_dir = conf.paths.base_dir / 'data' / 'SOLICITUDES'
        
        return ExcelProcessor(
            reader=self.excel_file_reader(),
            api_service=self.api_service(),
            base_solicitudes_dir=base_dir,
            styler=self.excel_styler()
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