import logging
import shutil
import time
from pathlib import Path
from src.domain.value_objects.cliente_folder import ClienteFolder
from src.infrastructure.file_system.path_manager import PathManager

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
        input_path = self.path_manager.get_solicitudes_dir()
        logger.info(f"🚀 AetherCore 4 iniciado. Monitoreando: {input_path}")

        watcher = self.watcher_factory(
            path=input_path,
            callback=self.on_file_detected,
            patterns=["*.xlsx", "*.xlsm"],
        )
        watcher.start()

    def on_file_detected(self, file_path: str):
        ruta = Path(file_path)

        intentos = 0
        while intentos < 10:
            try:
                ruta.rename(ruta)
                break
            except PermissionError:
                logger.info(f"⚠️ Archivo {ruta.name} bloqueado. Reintentando en 1 segundo...")
                time.sleep(1)
                intentos += 1

        logger.info(f"✅ Archivo {ruta.name} listo para procesar")
        
        try:
            solicitud_name = ruta.parent.name
            cliente_name = ruta.parent.parent.name
            
            cliente_folder = ClienteFolder.from_folder_name(cliente_name)

            exito = self.excel_processor.procesar_archivo_excel(ruta, cliente_folder, solicitud_name)
            if exito:
                logger.info(f"✅ Procesamiento exitoso: {ruta.name}")
            else:
                logger.warning(f"⚠️ El proceso de {ruta.name} terminó con novedades.")

        except Exception as e:
            logger.error(f"❌ Error crítico en el orquestador: {e}")
            self._mover_a_emergencia(ruta)

    def _mover_a_emergencia(self, ruta: Path):
        try:
            error_dir = ruta.parent / "errores"
            error_dir.mkdir(parents=True, exist_ok=True)
            shutil.move(str(ruta), str(error_dir / ruta.name))
        except:
            pass
