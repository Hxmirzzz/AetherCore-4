"""
Orquestador de procesamiento (XML).
- run_once(): procesa todos los XML pendientes en la carpeta de entrada.
- run_watch(): observa la carpeta y procesa a medida que lleguen archivos (usa debounce).
"""
from __future__ import annotations
import logging
from re import T
from typing import Dict, Any, Optional, Callable
from pathlib import Path
import time
import threading

from src.application.processors.xml.xml_processor import XMLProcessor
from src.application.processors.txt.txt_processor import TXTProcessor
from src.application.processors.excel.excel_processor import ExcelProcessor
from src.domain.value_objects.cliente_folder import ClienteFolder
from src.infrastructure.file_system.path_manager import PathManager

logger = logging.getLogger(__name__)

class ProcessingOrchestrator:
    def __init__(
        self,
        xml_processor: XMLProcessor,
        path_manager: PathManager,
        watcher_factory: Callable,
        debounce_ms: int = 800,
        txt_processor: TXTProcessor | None = None,
        excel_processor: ExcelProcessor | None = None,
    ):
        self._xml = xml_processor
        self._txt = txt_processor
        self._excel = excel_processor
        self._paths = path_manager
        self._Watcher = watcher_factory
        self._debounce_ms = debounce_ms

    # ===== XML =====
    def run_once_xml(self, puntos_info: Dict[str, Dict[str, str]], conn: Any):
        entrada = self._paths.input_xml_dir()
        logger.info("Procesando XML (once) en: %s", str(entrada))
        if not entrada.exists():
            logger.warning("Carpeta de entrada XML no existe: %s", str(entrada))
            return

        for xml_file in sorted(Path(entrada).glob("*.xml")):
            out_xlsx = self._paths.build_output_excel_path(xml_file)
            try:
                self._xml.procesar_archivo_xml(xml_file, out_xlsx, puntos_info, conn)
            except Exception as e:
                logger.error("Error procesando XML %s: %s", xml_file, e)

    def run_watch_xml(self, puntos_info: Dict[str, Dict[str, str]], conn: Any):
        entrada = self._paths.input_xml_dir()
        logger.info("Observando carpeta XML: %s", str(entrada))
        
        def on_file_callback(file_path: Path):
            if file_path.suffix.lower() == '.xml':
                self._xml.procesar_archivo_xml(
                    file_path, 
                    self._paths.build_output_excel_path(file_path), 
                    puntos_info, 
                    conn
                )
        
        watcher = self._Watcher(entrada, on_new_file=on_file_callback, debounce_ms=self._debounce_ms)
        watcher.start()

    # ===== TXT =====
    def run_once_txt(self, conn):
        if self._txt is None: return

        entrada = self._paths.input_txt_dir()
        logger.info("Procesando TXT (once) en: %s", str(entrada))
        if not entrada.exists(): return

        for txt in sorted(Path(entrada).glob("*.txt")):
            try:
                self._txt.procesar_archivo_txt(txt, conn)
            except Exception as e:
                logger.error(f"Error procesando TXT {txt.name}:: {e}")
            
    def run_watch_txt(self, conn):
        if self._txt is None: return

        entrada = self._paths.input_txt_dir()
        logger.info("Observando carpeta TXT: %s", str(entrada))
        
        def on_file_callback(file_path: Path):
            if file_path.suffix.lower() == '.txt':
                self._txt.procesar_archivo_txt(file_path, conn)
        
        watcher = self._Watcher(entrada, on_new_file=on_file_callback, debounce_ms=self._debounce_ms)
        watcher.start()

    # ===== EXCEL =====
    def run_once_excel(self):
        if self._excel is None:
            logger.warning("ExcelProcessor no configurado. Saltando")
            return

        base_solicitudes = self._paths.base_dir / 'data' / 'SOLICITUDES'
        logger.info("Procesando Excel (once) en: %s", str(base_solicitudes))
        if not base_solicitudes.exists(): return

        for carpeta_cliente in base_solicitudes.iterdir():
            if not carpeta_cliente.is_dir(): continue

            try:
                cliente_folder = ClienteFolder.from_folder_name(carpeta_cliente.name)

                for archivo in carpeta_cliente.glob("*.*"):
                    if archivo.suffix.lower() in ['.xlsx', '.xls', '.xlsm'] and not archivo.name.startswith('~$'):
                        logger.info(f"   -> Encontrado: {archivo.name}")
                        self._excel.procesar_archivo_excel(archivo, cliente_folder)
            except ValueError:
                pass
            except Exception as e:
                logger.error(f"Error explorando carpeta {carpeta_cliente.name}: {e}")

    def run_watch_excel(self):
        if self._excel is None: return

        base_solicitudes = self._paths.base_dir / 'data' / 'SOLICITUDES'
        logger.info(f"Observando arbol EXCEL: {base_solicitudes}")

        def on_file_callback(file_path: Path):
            if file_path.suffix.lower() not in ['.xlsx', '.xls', '.xlsm']:
                return
            if file_path.name.startswith('~$'):
                return
            try:
                carpeta_padre = file_path.parent

                if carpeta_padre.parent.resolve() == base_solicitudes.resolve():
                    cliente_folder = ClienteFolder.from_folder_name(carpeta_padre.name)
                    logger.info(f"Nuevo excel detectado: {file_path.name}")
                    time.sleep(0.5)
                    self._excel.procesar_archivo_excel(file_path, cliente_folder)
            except Exception as e:
                logger.error(f"Error en watcher Excel para {file_path.name}: {e}")
        
        watcher = self._Watcher(
            base_solicitudes,
            on_new_file=on_file_callback,
            debounce_ms=self._debounce_ms,
            recursive=True
        )
        watcher.start()
                    
    # ===== ALL =====
    def run_once_all(self, puntos_info: Dict[str, Dict[str, str]], conn: Any, only: Optional[str] = None):
        if only is None or only == "xml":
            self.run_once_xml(puntos_info, conn)
        if only is None or only == "txt":
            self.run_once_txt(puntos_info, conn)
        if only is None or only == "excel":
            self.run_once_excel()

    def run_watch_all(self, puntos_info: Dict[str, Dict[str, str]], conn: Any, only: Optional[str] = None):
        threads = []

        def _t(fn, name):
            t = threading.Thread(target=fn, name=name, daemon=True)
            threads.append(t)
            t.start()

        if only is None or only == "xml":
            _t(lambda: self.run_watch_xml(puntos_info, conn), "watch-xml")

        if only is None or only == "txt":
            _t(lambda: self.run_watch_txt(puntos_info, conn), "watch-txt")

        if only is None or only == "excel":
            _t(lambda: self.run_watch_excel(), "watch-excel")

        if not threads:
            logger.warning("Nose iniciaron watchers (verifique argumentos --only)")
            return

        logger.info(f"Monitoreando {len(threads)} servicios. Presione Ctrl+C para salir.")

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Deteniendo watchers…")