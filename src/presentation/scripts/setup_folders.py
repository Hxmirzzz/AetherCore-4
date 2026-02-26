"""
Script para crear/actualizar la estructura de carpetas por cliente.

Uso:
    python setup_client_folders.py [--base-dir PATH]
    
Este script:
1. Consulta los clientes en la BD
2. Crea carpetas por cliente ({cod_cliente}_{nombre_cliente})
3. Crea subcarpetas GESTIONADOS
4. Genera un archivo de mapeo JSON
"""
import argparse
import json
import logging
import sys
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime

from src.infrastructure.di.container import ApplicationContainer
from src.domain.value_objects.cliente_folder import ClienteFolder
from src.infrastructure.config.settings import get_config

if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('setup_folders.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

class FolderSetup:
    """Gestor de creación de estructura de carpetas"""
    
    def __init__(self, base_dir: Path):
        """
        Inicializa el setup.
        
        Args:
            base_dir: Directorio base (ej: data/SOLICITUDES)
        """
        self.base_dir = base_dir
        self.mapping_file = base_dir / "clientes_mapping.json"

    def setup_all(self, clientes: List[Dict[str, str]]) -> Dict[str, Any]:
        """
        Configura todas las carpetas de clientes.
        
        Args:
            clientes: Lista de dicts con 'cod_cliente' y 'nombre_cliente'
            
        Returns:
            Diccionario con estadísticas de la operación
        """
        logger.info("=" * 60)
        logger.info("INICIANDO SETUP DE CARPETAS DE CLIENTES")
        logger.info("=" * 60)

        stats = {
            'total_clientes': len(clientes),
            'carpetas_creadas': 0,
            'carpetas_existentes': 0,
            'errores': [],
            'mapeo': {}
        }

        self.base_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Directorio base: {self.base_dir}")

        for cliente_data in clientes:
            try:
                resultado = self._setup_cliente(cliente_data)

                if resultado['creada']:
                    stats['carpetas_creadas'] += 1
                    logger.info(f"✅ Creada: {resultado['folder_name']}")
                else:
                    stats['carpetas_existentes'] += 1
                    logger.info(f"ℹ️  Ya existe: {resultado['folder_name']}")

                stats['mapeo'][resultado['cod_cliente']] = {
                    'folder_name': resultado['folder_name'],
                    'path_relativo': resultado['path_relativo'],
                    'nombre_cliente': resultado['nombre_cliente']
                }

            except Exception as e:
                cod_cliente = cliente_data.get('cod_cliente', 'DESCONOCIDO')
                logger.error(f"❌ Error con cliente {cod_cliente}: {e}")
                stats['errores'].append({
                    'cod_cliente': cod_cliente,
                    'error': str(e)
                })
        
        self._save_mapping(stats['mapeo'])

        logger.info("=" * 60)
        logger.info("RESUMEN DEL SETUP")
        logger.info(f"  Total clientes: {stats['total_clientes']}")
        logger.info(f"  Carpetas creadas: {stats['carpetas_creadas']}")
        logger.info(f"  Ya existentes: {stats['carpetas_existentes']}")
        logger.info(f"  Errores: {len(stats['errores'])}")
        logger.info("=" * 60)
        
        return stats

    def _setup_cliente(self, cliente_data: Dict[str, str]) -> Dict[str, Any]:
        """
        Configura carpetas para un cliente específico.
        
        Args:
            cliente_data: Dict con 'cod_cliente' y 'nombre_cliente'
            
        Returns:
            Dict con resultado de la operación
        """
        cliente_folder = ClienteFolder.from_database(
            cod_cliente=cliente_data['cod_cliente'],
            nombre_cliente=cliente_data['nombre_cliente']
        )

        cliente_path = cliente_folder.to_path(self.base_dir)
        gestionados_path = cliente_folder.gestionados_path(self.base_dir)

        ya_existia = cliente_path.exists()

        cliente_path.mkdir(exist_ok=True)
        gestionados_path.mkdir(exist_ok=True)

        readme_path = cliente_path / "README.txt"
        if not readme_path.exists():
            self._create_readme(readme_path, cliente_folder)

        return {
            'cod_cliente': cliente_folder.cod_cliente,
            'nombre_cliente': cliente_folder.nombre_cliente,
            'folder_name': cliente_folder.folder_name,
            'path_relativo': str(cliente_path.relative_to(self.base_dir)),
            'creada': not ya_existia
        }

    def _create_readme(self, readme_path: Path, cliente_folder: ClienteFolder):
        """Crea un archivo README informativo en la carpeta"""
        contenido = f"""
        CARPETA DE SOLICITUDES - {cliente_folder.nombre_cliente}
        ========================================

        Cliente: {cliente_folder.nombre_cliente}
        Código: {cliente_folder.cod_cliente}
        Fecha creación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

        INSTRUCCIONES:
        --------------
        1. Coloque los archivos Excel de solicitud en esta carpeta
        2. Nombre sugerido: solicitud_YYYYMMDD_HHMMSS.xlsx
        3. Los archivos procesados se moverán a GESTIONADOS/

        FORMATO ESPERADO:
        -----------------
        El archivo debe contener las columnas según el mapper configurado
        para este cliente (Cliente{cliente_folder.cod_cliente}Mapper).

        CONTACTO:
        ---------
        Para soporte, contacte al equipo de desarrollo.
        """.strip()
        
        with open(readme_path, 'w', encoding='utf-8') as f:
            f.write(contenido)
    
    def _save_mapping(self, mapeo: Dict[str, Any]):
        """
        Guarda el mapeo de carpetas a JSON.
        
        Args:
            mapeo: Diccionario con el mapeo
        """
        with open(self.mapping_file, 'w', encoding='utf-8') as f:
            json.dump(
                {
                    'generado': datetime.now().isoformat(),
                    'base_dir': str(self.base_dir),
                    'clientes': mapeo
                },
                f,
                indent=2,
                ensure_ascii=False
            )
        
        logger.info(f"Mapeo guardado en: {self.mapping_file}")

def main():
    """Función principal"""
    parser = argparse.ArgumentParser(
        description='Setup de carpetas de clientes para procesamiento de Excel'
    )
    parser.add_argument(
        '--base-dir',
        type=Path,
        help='Directorio base (default: data/SOLICITUDES)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Solo simula, no crea carpetas'
    )

    args = parser.parse_args()

    config = get_config()

    if args.base_dir:
        base_dir = args.base_dir
    else:
        base_dir = config.paths.base_dir / 'data' / 'SOLICITUDES'

    logger.info(f"Directorio base: {base_dir}")

    container = ApplicationContainer()

    try:
        logger.info("Consultando clientes en base de datos...")
        cliente_repo = container.cliente_repository()
        clientes_dict = cliente_repo.obtener_todos()

        clientes = [
            {
                'cod_cliente': cod,
                'nombre_cliente': info['cliente']
            }
            for cod, info in clientes_dict.items()
        ]

        logger.info(f"Se encontraron {len(clientes)} clientes")

        if not clientes:
            logger.warning("No se encontraron clientes en la BD")
            return 1

        # Mostrar lista
        logger.info("\nClientes a procesar:")
        for c in clientes:
            logger.info(f"  - {c['cod_cliente']}: {c['nombre_cliente']}")
        
        # Confirmación
        if not args.dry_run:
            respuesta = input("\n¿Continuar con la creación? (si/no): ").strip().lower()
            if respuesta != 'si':
                logger.info("Operación cancelada")
                return 0
        
        # Ejecutar setup
        if args.dry_run:
            logger.info("\n[DRY RUN] No se crearán carpetas reales\n")
            return 0

        setup = FolderSetup(base_dir)
        stats = setup.setup_all(clientes)
        
        # Verificar errores
        if stats['errores']:
            logger.error("\n⚠️  Hubo errores durante el setup:")
            for error in stats['errores']:
                logger.error(f"  - {error['cod_cliente']}: {error['error']}")
            return 1
        
        logger.info("\n✅ Setup completado exitosamente")
        return 0
        
    except Exception as e:
        logger.error(f"❌ Error crítico: {e}", exc_info=True)
        return 1
    finally:
        container.close_all_connections()

if __name__ == "__main__":
    sys.exit(main())