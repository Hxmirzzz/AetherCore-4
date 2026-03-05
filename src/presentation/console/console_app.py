"""
Console Runner para AetherCore 4 (API Client).

Uso:
    # Procesar todos los archivos Excel pendientes (una vez)
    python -m src.presentation.console.console_app --once
    
    # Monitorear carpetas y procesar nuevos archivos
    python -m src.presentation.console.console_app --watch
    
    # Procesar solo un cliente específico
    python -m src.presentation.console.console_app --once --cliente 4
    
    # Procesar archivo específico
    python -m src.presentation.console.console_app --file /path/to/solicitud.xlsx --cliente 4
"""
import argparse
import logging
import sys
import io
from pathlib import Path
from typing import List, Dict
import time

from src.infrastructure.di.container import ApplicationContainer
from src.domain.value_objects.cliente_folder import ClienteFolder
from src.application.processors.excel.excel_processor_factory import ExcelProcessorFactory

if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('logs/process_excel.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

class ExcelConsoleRunner:
    """Runner de consola para procesamiento de Excel vía API"""

    def __init__(self, container: ApplicationContainer):
        self.container = container
        self.config = container.config()
        self.base_dir = self.config.paths.base_dir / 'data' / 'SOLICITUDES'

        if not self.base_dir.exists():
            logger.warning(f"Directorio SOLICITUDES no existe: {self.base_dir}")
            logger.warning("Por favor crea la estructura de carpetas en data/SOLICITUDES/")

    def run_once(self, cod_cliente: str | None = None) -> Dict[str, int]:
        logger.info("=" * 60)
        logger.info("AETHERCORE 4 - PROCESAMIENTO DE EXCEL (MODO ONCE)")
        logger.info("=" * 60)
        
        stats = {
            'total_archivos': 0,
            'procesados_exitosos': 0,
            'procesados_fallidos': 0,
            'clientes_procesados': 0
        }

        carpetas_clientes = self._obtener_carpetas_clientes(cod_cliente)

        if not carpetas_clientes:
            logger.warning("No se encontraron carpetas de clientes válidas")
            return stats

        for cliente_folder in carpetas_clientes:
            archivos = self._obtener_archivos_pendientes(cliente_folder)

            if not archivos:
                continue

            stats['clientes_procesados'] += 1

            for archivo in archivos:
                stats['total_archivos'] += 1
                if self._procesar_archivo(archivo, cliente_folder):
                    stats['procesados_exitosos'] += 1
                else:
                    stats['procesados_fallidos'] += 1

        self._imprimir_resumen(stats)
        return stats

    def run_watch(self, cod_cliente: str | None = None, interval: int = 10):
        logger.info("=" * 60)
        logger.info("AETHERCORE 4 - PROCESAMIENTO CONTINUO (MODO WATCH)")
        logger.info(f"Intervalo: {interval} segundos")
        logger.info("Presione Ctrl+C para detener")
        logger.info("=" * 60)

        carpetas_clientes = self._obtener_carpetas_clientes(cod_cliente)

        if not carpetas_clientes:
            logger.error("No hay carpetas de clientes para monitorear")
            return

        archivos_procesados = set()

        try:
            while True:
                for cliente_folder in carpetas_clientes:
                    archivos = self._obtener_archivos_pendientes(cliente_folder)
                    archivos_nuevos = [a for a in archivos if a not in archivos_procesados]

                    for archivo in archivos_nuevos:
                        self._procesar_archivo(archivo, cliente_folder)
                        archivos_procesados.add(archivo)

                time.sleep(interval)

        except KeyboardInterrupt:
            logger.info("\nDeteniendo monitoreo. Proceso finalizado por el usuario.")

    def run_file(self, ruta_archivo: Path, cod_cliente: str) -> bool:
        logger.info("=" * 60)
        logger.info(f"PROCESANDO ARCHIVO ESPECÍFICO: {ruta_archivo.name}")
        logger.info("=" * 60)

        if not ruta_archivo.exists():
            logger.error(f"El archivo no existe: {ruta_archivo}")
            return False

        cliente_folder = ClienteFolder.from_folder_name(cod_cliente)
        return self._procesar_archivo(ruta_archivo, cliente_folder)

    def _obtener_carpetas_clientes(self, cod_cliente: str | None = None) -> List[ClienteFolder]:
        carpetas = []
        if not self.base_dir.exists(): return carpetas
        
        for item in self.base_dir.iterdir():
            if not item.is_dir() or item.name.startswith('.') or item.name.startswith('_'):
                continue
            try:
                cliente_folder = ClienteFolder.from_folder_name(item.name)
                
                if cod_cliente and str(cliente_folder.cod_cliente) != str(cod_cliente):
                    continue

                if str(cliente_folder.cod_cliente) not in self.config.clientes_permitidos:
                    logger.debug(f"Se omite la carpeta {item.name} porque el cliente {cliente_folder.cod_cliente} no está en clientes_permitidos.")
                    continue

                carpetas.append(cliente_folder)
            except Exception as e:
                logger.error(f"Error procesando el nombre de la carpeta '{item.name}': {e}")
                continue

        return carpetas

    def _obtener_archivos_pendientes(self, cliente_folder: ClienteFolder) -> List[Path]:
        carpeta_cliente = cliente_folder.to_path(self.base_dir)
        if not carpeta_cliente.exists(): return []

        extensiones_validas = ['.xlsx', '.xls', '.xlsm']
        archivos = []
        
        for item in carpeta_cliente.iterdir():
            if item.is_file() and item.suffix.lower() in extensiones_validas and not item.name.startswith('~$'):
                archivos.append(item)
                
        return sorted(archivos)

    def _procesar_archivo(self, ruta_archivo: Path, cliente_folder: ClienteFolder) -> bool:
        try:
            excel_processor = self.container.excel_processor()
            return excel_processor.procesar_archivo_excel(ruta_archivo, cliente_folder)
        except Exception as e:
            logger.error(f"Error procesando archivo {ruta_archivo.name}: {e}", exc_info=True)
            return False

    def _imprimir_resumen(self, stats: Dict[str, int]):
        logger.info("\n" + "=" * 60)
        logger.info("RESUMEN DE PROCESAMIENTO MODO ONCE")
        logger.info("=" * 60)
        logger.info(f"Total archivos procesados: {stats['total_archivos']}")
        logger.info(f"Exitosos: {stats['procesados_exitosos']}")
        logger.info(f"Fallidos: {stats['procesados_fallidos']}")
        logger.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(description='AetherCore 4 API Client')
    
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--once', action='store_true', help='Procesa todos los archivos pendientes una vez')
    mode.add_argument('--watch', action='store_true', help='Monitorea carpetas')
    mode.add_argument('--file', type=Path, help='Procesa un archivo específico')
    
    parser.add_argument('--cliente', type=str, help='Filtrar por código de cliente')
    parser.add_argument('--interval', type=int, default=10, help='Intervalo en segundos')
    
    args = parser.parse_args()
    
    if args.file and not args.cliente:
        parser.error("--file requiere usar también --cliente para saber qué formato aplicar")
    
    container = ApplicationContainer()
    
    try:
        runner = ExcelConsoleRunner(container)
        
        if args.once:
            stats = runner.run_once(cod_cliente=args.cliente)
            return 0 if stats['procesados_fallidos'] == 0 else 1
            
        elif args.watch:
            runner.run_watch(cod_cliente=args.cliente, interval=args.interval)
            return 0
            
        elif args.file:
            return 0 if runner.run_file(args.file, args.cliente) else 1
            
    except Exception as e:
        logger.error(f"Error crítico en la aplicación: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())