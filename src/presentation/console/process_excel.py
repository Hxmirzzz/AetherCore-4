"""
Console Runner para procesamiento de archivos Excel de solicitudes.

Uso:
    # Procesar todos los archivos Excel pendientes (una vez)
    python -m src.presentation.console.process_excel --once
    
    # Monitorear carpetas y procesar nuevos archivos
    python -m src.presentation.console.process_excel --watch
    
    # Procesar solo un cliente específico
    python -m src.presentation.console.process_excel --once --cliente 45
    
    # Procesar archivo específico
    python -m src.presentation.console.process_excel --file /path/to/solicitud.xlsx --cliente 45
"""
import argparse
import logging
import sys
import io
from pathlib import Path
from typing import List, Dict
import time

from src.infrastructure.di.container import ApplicationContainer
from src.infrastructure.config.settings import get_config
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
        logging.FileHandler('process_excel.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

class ExcelConsoleRunner:
    """Runner de consola para procesamiento de Excel"""

    def __init__(self, container: ApplicationContainer):
        """
        Inicializa el runner.
        
        Args:
            container: Contenedor de dependencias
        """
        self.container = container
        self.config = container.config()
        self.base_dir = self.config.paths.base_dir / 'data' / 'SOLICITUDES'

        if not self.base_dir.exists():
            logger.warning(f"Directorio SOLICITUDES no existe: {self.base_dir}")
            logger.warning("Ejecute setup_client_folders.py primero")


    def run_once(self, cod_cliente: str | None = None) -> Dict[str, int]:
        """
        Procesa todos los archivos Excel pendientes una sola vez.
        
        Args:
            cod_cliente: Código de cliente específico (None = todos)
            
        Returns:
            Estadísticas de procesamiento
        """
        logger.info("=" * 60)
        logger.info("PROCESAMIENTO DE EXCEL - MODO ONCE")
        logger.info("=" * 60)
        
        stats = {
            'total_archivos': 0,
            'procesados_exitosos': 0,
            'procesados_fallidos': 0,
            'clientes_procesados': 0
        }

        carpetas_clientes = self._obtener_carpetas_clientes(cod_cliente)

        if not carpetas_clientes:
            logger.warning("No se encontraron carpetas de clientes")
            return stats

        logger.info(f"Se procesarán {len(carpetas_clientes)} carpetas de clientes")

        for cliente_folder in carpetas_clientes:
            logger.info(f"\n{'─' * 60}")
            logger.info(f"Procesando cliente: {cliente_folder}")
            logger.info(f"{'─' * 60}")

            archivos = self._obtener_archivos_pendientes(cliente_folder)

            if not archivos:
                logger.info(f" No hay archivos pendientes para {cliente_folder}")
                continue

            logger.info(f" Encontrados {len(archivos)} archivos pendientes")
            stats['clientes_procesados'] += 1

            for archivo in archivos:
                stats['total_archivos'] += 1

                logger.info(f"\n Procesando: {archivo.name}")

                if self._procesar_archivo(archivo, cliente_folder):
                    stats['procesados_exitosos'] += 1
                else:
                    stats['procesados_fallidos'] += 1

        self._imprimir_resumen(stats)
        return stats

    def run_watch(self, cod_cliente: str | None = None, interval: int = 10):
        """
        Monitorea carpetas y procesa nuevos archivos.
        
        Args:
            cod_cliente: Código de cliente específico (None = todos)
            interval: Intervalo de escaneo en segundos
        """
        logger.info("=" * 60)
        logger.info("PROCESAMIENTO DE EXCEL - MODO WATCH")
        logger.info(f"Intervalo: {interval} segundos")
        logger.info("Presione Ctrl+C para detener")
        logger.info("=" * 60)

        carpetas_clientes = self._obtener_carpetas_clientes(cod_cliente)

        if not carpetas_clientes:
            logger.error("No hay carpetas de clientes para monitorear")
            return

        logger.info(f"Monitoreando {len(carpetas_clientes)} carpetas:")
        for cf in carpetas_clientes:
            logger.info(f"  - {cf}")

        archivos_procesados = set()

        try:
            while True:
                for cliente_folder in carpetas_clientes:
                    archivos = self._obtener_archivos_pendientes(cliente_folder)

                    archivos_nuevos = [
                        a for a in archivos
                        if a not in archivos_procesados
                    ]

                    if archivos_nuevos:
                        logger.info(f"\n Nuevos archivos detectados para {cliente_folder}: {len(archivos_nuevos)}")
                        
                        for archivo in archivos_nuevos:
                            logger.info(f"\n Procesando: {archivo.name}")
                            
                            if self._procesar_archivo(archivo, cliente_folder):
                                logger.info(f"Procesado exitosamente")
                            else:
                                logger.error(f"Error en procesamiento")
                            
                            archivos_procesados.add(archivo)

                time.sleep(interval)

        except KeyboardInterrupt:
            logger.info("\n\n Deteniendo monitoreo...")
            logger.info("Proceso finalizado por el usuario")

    def run_file(self, ruta_archivo: Path, cod_cliente: str) -> bool:
        """
        Procesa un archivo específico.
        
        Args:
            ruta_archivo: Path al archivo Excel
            cod_cliente: Código del cliente
            
        Returns:
            True si procesó exitosamente
        """
        logger.info("=" * 60)
        logger.info("PROCESAMIENTO DE ARCHIVO ESPECÍFICO")
        logger.info(f"Archivo: {ruta_archivo}")
        logger.info(f"Cliente: {cod_cliente}")
        logger.info("=" * 60)

        if not ruta_archivo.exists():
            logger.error(f"Archivo no existe: {ruta_archivo}")

        try:
            cliente_repo = self.container.cliente_repository()
            clientes = cliente_repo.obtener_todos()

            if cod_cliente not in clientes:
                logger.error(f"Cliente {cod_cliente} no encontrado en BD")
                return False

            cliente_info = clientes[cod_cliente]
            cliente_folder = ClienteFolder.from_database(
                cod_cliente=cod_cliente,
                nombre_cliente=cliente_info['cliente']
            )

        except Exception as e:
            logger.error(f"Error obteniendo info del cliente: {e}")
            return False

        return self._procesar_archivo(ruta_archivo, cliente_folder)

    def _obtener_carpetas_clientes(
        self,
        cod_cliente: str | None = None
    ) -> List[ClienteFolder]:
        """
        Obtiene las carpetas de clientes a procesar.
        
        Args:
            cod_cliente: Filtrar por cliente (None = todos)
            
        Returns:
            Lista de ClienteFolder
        """
        carpetas = []

        if not self.base_dir.exists():
            return carpetas
        
        for item in self.base_dir.iterdir():
            if not item.is_dir():
                continue

            if item.name.startswith('.') or item.name.startswith('_'):
                continue

            try:
                cliente_folder = ClienteFolder.from_folder_name(item.name)
                
                if cod_cliente and cliente_folder.cod_cliente != cod_cliente:
                    continue

                if not ExcelProcessorFactory.tiene_mapper(cliente_folder.cod_cliente):
                    logger.warning(
                        f"No existe mapper para {cliente_folder}, se omite"
                    )
                    continue

                carpetas.append(cliente_folder)

            except Exception as e:
                logger.warning(f"Error parseando carpeta '{item.name}': {e}")
                continue

        return carpetas

    def _obtener_archivos_pendientes(
        self,
        cliente_folder: ClienteFolder
    ) -> List[Path]:
        """
        Obtiene archivos Excel pendientes en una carpeta de cliente.
        
        Args:
            cliente_folder: ClienteFolder del cliente
            
        Returns:
            Lista de Paths a archivos Excel
        """
        carpeta_cliente = cliente_folder.to_path(self.base_dir)

        if not carpeta_cliente.exists():
            return []

        archivos = []
        extensiones_validas = ['.xlsx', '.xls', '.xlsm']

        for item in carpeta_cliente.iterdir():
            if not item.is_file():
                continue

            if item.suffix.lower() not in extensiones_validas:
                continue

            if item.name.startswith('~$') or item.name.startswith('.'):
                continue

            archivos.append(item)

        return sorted(archivos)

    def _procesar_archivo(
        self,
        ruta_archivo: Path,
        cliente_folder: ClienteFolder
    ) -> bool:
        """
        Procesa un archivo Excel específico.
        
        Args:
            ruta_archivo: Path al archivo
            cliente_folder: ClienteFolder del cliente
            
        Returns:
            True si procesó exitosamente
        """
        try:
            excel_processor = self.container.excel_processor()

            return excel_processor.procesar_archivo_excel(
                ruta_archivo,
                cliente_folder
            )

        except Exception as e:
            logger.error(f"Error procesando archivo: {e}", exc_info=True)
            return False

    def _imprimir_resumen(self, stats: Dict[str, int]):
        """Imprime resumen de estadísticas"""
        logger.info("\n" + "=" * 60)
        logger.info("RESUMEN DE PROCESAMIENTO")
        logger.info("=" * 60)
        logger.info(f"Total archivos: {stats['total_archivos']}")
        logger.info(f"Exitosos: {stats['procesados_exitosos']}")
        logger.info(f"Fallidos: {stats['procesados_fallidos']}")
        logger.info(f"Clientes procesados: {stats['clientes_procesados']}")
        logger.info("=" * 60)

def main():
    """Función principal"""
    parser = argparse.ArgumentParser(
        description='Procesador de archivos Excel de solicitudes'
    )
    
    # Modo de operación
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        '--once',
        action='store_true',
        help='Procesa todos los archivos Excel pendientes una sola vez'
    )
    mode.add_argument(
        '--watch',
        action='store_true',
        help='Monitorea carpetas y procesa nuevos archivos'
    )
    mode.add_argument(
        '--file',
        type=Path,
        help='Procesa un archivo específico'
    )
    
    # Filtros opcionales
    parser.add_argument(
        '--cliente',
        type=str,
        help='Procesa solo archivos del cliente especificado (código)'
    )
    parser.add_argument(
        '--interval',
        type=int,
        default=10,
        help='Intervalo de escaneo en modo watch (segundos, default: 10)'
    )
    
    args = parser.parse_args()
    
    # Validar argumentos
    if args.file and not args.cliente:
        parser.error("--file requiere --cliente")
    
    # Crear container
    container = ApplicationContainer()
    
    try:
        # Crear runner
        runner = ExcelConsoleRunner(container)
        
        # Ejecutar según modo
        if args.once:
            stats = runner.run_once(cod_cliente=args.cliente)
            exit_code = 0 if stats['procesados_fallidos'] == 0 else 1
            
        elif args.watch:
            runner.run_watch(
                cod_cliente=args.cliente,
                interval=args.interval
            )
            exit_code = 0
            
        elif args.file:
            exitoso = runner.run_file(args.file, args.cliente)
            exit_code = 0 if exitoso else 1
        
        return exit_code
        
    except Exception as e:
        logger.error(f"Error crítico: {e}", exc_info=True)
        return 1
    finally:
        container.close_all_connections()


if __name__ == "__main__":
    sys.exit(main())