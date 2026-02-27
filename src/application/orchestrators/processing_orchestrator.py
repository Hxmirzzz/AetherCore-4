import logging
from pathlib import Path
from src.domain.value_objects.cliente_folder import ClienteFolder
from src.infrastructure.file_system.path_manager import PathManager
from typing import Optional, Any
import threading
import time

logger = logging.getLogger(__name__)

class ProcessingOrchestrator:
    def __init__(
        self,
        excel_processor,
        path_manager: PathManager,
        watcher_factory,
        debounce_ms=800
    ):
        self.excel_processor = excel_processor
        self.path_manager = path_manager
        self.watcher_factory = watcher_factory
        self.debounce_ms = debounce_ms

    def start(self):
        """Inicia el monitoreo de la carpeta de entrada configurada."""
        input_path = self.path_manager.get_input_path()
        logger.info(f"🚀 AetherCore 4 iniciado. Monitoreando: {input_path}")

        watcher = self.watcher_factory(
            path=input_path,
            callback=self.on_file_detected,
            patterns=["*.xlsx", "*.xlsm"],
        )
        watcher.start()

    def on_file_detected(self, file_path: str):
        """
        Evento disparado cuando se detecta un nuevo archivo Excel.
        """
        ruta = Path(file_path)
        logger.info(f"📂 Archivo detectado: {ruta.name}")

        try:
            folder_name = ruta.parent.name
            cliente_folder = ClienteFolder.from_folder_name(folder_name)

            exito = self.excel_processor.procesar_archivo_excel(ruta, cliente_folder)
            if exito:
                logger.info(f"✅ Procesamiento exitoso: {ruta.name}")
            else:
                logger.warning(f"⚠️ El proceso de {ruta.name} terminó con novedades.")

        except Exception as e:
            logger.error(f"❌ Error crítico en el orquestador: {e}")
            self._mover_a_emergencia(ruta)

    def _mover_a_emergencia(self, ruta: Path):
        """Mueve el archivo a una carpeta de error genérica si falla la lógica inicial."""
        try:
            error_dir = ruta.parent / "ERRORES_SISTEMA"
            error_dir.mkdir(parents=True, exist_ok=True)
            shutil.move(str(ruta), str(error_dir / ruta.name))
        except:
            pass
