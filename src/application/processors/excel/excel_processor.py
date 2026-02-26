
"""
Procesador principal de archivos Excel de solicitudes.

Orquesta el flujo completo:
1. Lectura del archivo Excel
2. Validación de estructura
3. Mapeo a DTOs
4. Inserción en BD
5. Generación de respuesta
6. Movimiento a carpeta GESTIONADOS
"""
from __future__ import annotations
from pathlib import Path
import logging
import os
import shutil
from datetime import datetime
from dataclasses import replace
import openpyxl

from src.application.processors.excel.excel_file_reader import ExcelFileReader
from src.application.processors.excel.excel_processor_factory import ExcelProcessorFactory
from src.application.services.insertion_service import InsertionService, ResultadoInsercion
from src.domain.value_objects.cliente_folder import ClienteFolder
from src.domain.value_objects.codigo_punto import CodigoPunto
from src.infrastructure.config.settings import get_config
from src.application.services.data_mapper_service import DataMapperService
from src.application.dto.servicio_dto import ServicioDTO
from src.application.dto.transaccion_dto import TransaccionDTO

logger = logging.getLogger(__name__)
Config = get_config()

class ExcelProcessor:
    """
    Procesador principal de archivos Excel de solicitudes.
    
    Coordina el flujo completo de procesamiento similar a XMLProcessor y TXTProcessor.
    """

    def __init__(
        self,
        reader: ExcelFileReader | None = None,
        insertion_service: InsertionService | None = None,
        data_mapper_service: DataMapperService | None = None,
        base_solicitudes_dir: Path | None = None
    ):
        """
        Inicializa el procesador.
        
        Args:
            reader: Lector de archivos Excel
            insertion_service: Servicio de inserción en BD
            data_mapper_service: Servicio de mapeo de datos
            base_solicitudes_dir: Directorio base SOLICITUDES
        """
        self._reader = reader or ExcelFileReader()
        self._insertion = insertion_service
        self._data_mapper = data_mapper_service

        if base_solicitudes_dir is None:
            self._base_dir = Config.paths.base_dir / 'data' / 'SOLICITUDES'
        else:
            self._base_dir = base_solicitudes_dir

    def procesar_archivo_excel(
        self,
        ruta_excel: Path,
        cliente_folder: ClienteFolder
    ) -> bool:
        """
        Procesa un archivo Excel de solicitudes.
        
        Args:
            ruta_excel: Path al archivo Excel
            cliente_folder: ClienteFolder del cliente
            
        Returns:
            True si procesó exitosamente, False en caso contrario
        """
        try:
            logger.info(f"═══════════════════════════════════════════════════════")
            logger.info(f"Procesando Excel: {ruta_excel.name}")
            logger.info(f"Cliente: {cliente_folder}")
            logger.info(f"═══════════════════════════════════════════════════════")
            
            info = self._reader.read_multiple_sheets(ruta_excel)
            
            if not info:
                logger.error(f"❌ El archivo está vacío o no se pudo leer ninguna hoja.")
                self._manejar_excel_fallido(ruta_excel, cliente_folder, "Archivo vacío o ilegible")
                return False
            
            try:
                mapper = ExcelProcessorFactory.get_mapper(cliente_folder.cod_cliente)
            except ValueError as e:
                self._manejar_excel_fallido(ruta_excel, cliente_folder, str(e))
                return False

            nombre_hoja_params = next((k for k in info.keys() if "PARAMETRO" in k.upper()), None)

            if nombre_hoja_params:
                logger.info(f"Hoja PARAMETRO encontrada: {nombre_hoja_params}")
                df_params = info.pop(nombre_hoja_params)
                
                if hasattr(mapper, 'actualizar_parametros'):
                    mapper.actualizar_parametros(df_params)

            dtos_list = []
            hojas_ok = 0
            
            for nombre_hoja, df in info.items():
                logger.info(f"Analizando hoja: '{nombre_hoja}' ({len(df)} filas)")

                valido, error_msg = mapper.validar_estructura(df)
                if not valido:
                    logger.warning(f"Saltando hoja '{nombre_hoja}' - {error_msg}")
                    continue
                
                try:
                    datos_hoja = mapper.mapear_a_dtos(df, f"{ruta_excel.name} [{nombre_hoja}]")
                    if datos_hoja:
                        for item in datos_hoja:
                            dtos_list.append((item[0], item[1], item[2], nombre_hoja))
                        hojas_ok += 1
                        logger.info(f"✅ Hoja '{nombre_hoja}' procesada: {len(datos_hoja)} DTOs generados")

                except Exception as e:
                    logger.error(f"❌ Error procesando hoja '{nombre_hoja}': {e}")

            if hojas_ok == 0:
                self._manejar_excel_fallido(ruta_excel, cliente_folder, "No se procesó ninguna hoja válida")
                return False

            logger.info(f"Total acumulado: {len(dtos_list)} servicios listos para procesar")

            exitosas_map = {}
            fallidas_map = {}
            errores = []
            
            if self._insertion is not None and dtos_list:  
                if self._data_mapper is None:
                    raise ValueError("DataMapperService no está configurado para validar puntos en Excel")

                for servicio_dto, transaccion_dto, idx_fila, nombre_hoja in dtos_list:
                    if nombre_hoja not in exitosas_map: exitosas_map[nombre_hoja] = set()
                    if nombre_hoja not in fallidas_map: fallidas_map[nombre_hoja] = set()
                    
                    try:
                        es_recoleccion = servicio_dto.cod_concepto == 1
                        codigo_punto_real = (
                            servicio_dto.cod_punto_origen if es_recoleccion 
                            else servicio_dto.cod_punto_destino
                        )

                        codigo_punto_real = CodigoPunto.from_raw(str(codigo_punto_real)).parte_numerica.strip()
                        
                        punto_info = self._data_mapper.obtener_info_completa_punto(
                            codigo_punto_real,
                            servicio_dto.cod_cliente
                        )
                        
                        if not punto_info:
                            error_msg = f"Punto '{codigo_punto_real}' no existe en BD para cliente {servicio_dto.cod_cliente}."
                            logger.warning(f"⚠️ Pedido {servicio_dto.numero_pedido}: {error_msg}")
                            errores.append(f"Hoja '{nombre_hoja}', Fila {idx_fila + 1 + 5}: {error_msg}")
                            fallidas_map[nombre_hoja].add(idx_fila)
                            continue
                        
                        cod_sucursal_bd = punto_info['cod_sucursal']
                        pk_punto_bd = punto_info['cod_punto_pk']
                        pk_fondo_bd = punto_info['cod_fondo']
                        cambios_servicio = {'cod_sucursal': cod_sucursal_bd}
                        cambios_transaccion = {'cod_sucursal': cod_sucursal_bd}
                        
                        if es_recoleccion:
                            cambios_servicio.update({
                                'cod_punto_origen': pk_punto_bd,
                                'indicador_tipo_origen': 'P',
                                'cod_punto_destino': pk_fondo_bd if pk_fondo_bd else pk_punto_bd,
                                'indicador_tipo_destino': 'F' if pk_fondo_bd else 'P'
                            })
                        else:
                            cambios_servicio.update({
                                'cod_punto_origen': pk_fondo_bd if pk_fondo_bd else pk_punto_bd,
                                'indicador_tipo_origen': 'F' if pk_fondo_bd else 'P',
                                'cod_punto_destino': pk_punto_bd,
                                'indicador_tipo_destino': 'P'
                            })

                        servicio_final = replace(servicio_dto, **cambios_servicio)
                        transaccion_final = replace(transaccion_dto, **cambios_transaccion)
                        
                        res = self._insertion.insertar_servicio_con_transaccion(
                            servicio_final,
                            transaccion_final
                        )
                        
                        if res.exitoso:
                            exitosas_map[nombre_hoja].add(idx_fila)
                        else:
                            errores.append(f"Pedido {servicio_dto.numero_pedido}: {res.error}")
                            fallidas_map[nombre_hoja].add(idx_fila)
                        
                    except Exception as e:
                        logger.error(f"Error inesperado: {e}")
                        errores.append(f"Error critico en pedido {servicio_dto.numero_pedido}: {e}")
                        fallidas_map[nombre_hoja].add(idx_fila)
                
                total_exitosos = sum(len(v) for v in exitosas_map.values())
                total_fallidos = sum(len(v) for v in fallidas_map.values())
                logger.info(f"Resultado Inserción: {total_exitosos} exitosos, {total_fallidos} fallidos")

                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                nombre_base_limpio = ruta_excel.stem.replace('_NOVEDADES', '').replace('_OK', '')
                nombre_base_limpio = nombre_base_limpio.split('_202')[0]

                if total_exitosos > 0:
                    gestionados_dir = cliente_folder.gestionados_path(self._base_dir)
                    gestionados_dir.mkdir(parents=True, exist_ok=True)
                    nombre_ok = f"{nombre_base_limpio}_{timestamp}{ruta_excel.suffix}"
                    ruta_ok = gestionados_dir / nombre_ok

                    if total_fallidos == 0:
                        shutil.move(ruta_excel, ruta_ok)
                        logger.info(f"✅ Archivo original movido a GESTIONADOS: {nombre_ok}")
                        return True
                    else:
                        self._generar_copia_filtrada(ruta_excel, ruta_ok, exitosas_map)
                        logger.info(f"Gestionados guardados en: {nombre_ok}")
                    
                if total_fallidos > 0:
                    novedades_dir = cliente_folder.to_path(self._base_dir) / "NOVEDADES"
                    novedades_dir.mkdir(parents=True, exist_ok=True)
                    nombre_novedades = f"{nombre_base_limpio}_NOVEDADES_{timestamp}{ruta_excel.suffix}"
                    ruta_novedades = novedades_dir / nombre_novedades
                    ruta_txt = novedades_dir / f"{nombre_base_limpio}_NOVEDADES_{timestamp}.txt"

                    self._generar_copia_filtrada(ruta_excel, ruta_novedades, fallidas_map, borrar_hojas_vacias=True)

                    with open(ruta_txt, 'w', encoding='utf-8') as f:
                        f.write(f"REPORTE DE NOVEDADES\nArchivo: {ruta_excel.name}\nFecha: {datetime.now()}\n")
                        f.write("-" * 50 + "\n")
                        f.write(f"Se generó un archivo Excel adjunto que contiene SOLO las filas con errores.\n")
                        f.write(f"Los registros exitosos ({total_exitosos}) se movieron a la carpeta GESTIONADOS.\n\n")
                        f.write("Detalle de errores:\n")
                        for err in errores:
                            f.write(f"- {err}\n")
                    
                    logger.warning(f"⚠️ Novedades guardadas en: {nombre_novedades}")

                if ruta_excel.exists():
                    try:
                        os.remove(ruta_excel)
                        logger.info(f"Archivo original eliminado: {ruta_excel.name}")
                    except Exception as e:
                        logger.error(f"Error al eliminar archivo original: {e}")
            
            else:
                self._mover_a_gestionados(ruta_excel, cliente_folder)

            logger.info(f"═══════════════════════════════════════════════════════")
            logger.info(f"✅ PROCESAMIENTO COMPLETADO: {ruta_excel.name}")
            logger.info(f"═══════════════════════════════════════════════════════")
            return True
            
        except Exception as e:
            logger.exception(f"❌ Error crítico procesando Excel '{ruta_excel.name}'")
            self._manejar_excel_fallido(ruta_excel, cliente_folder, f"Error inesperado: {e}")
            return False

    def _generar_copia_filtrada(self, ruta_origen: Path, ruta_destino: Path, filas_a_mantener: dict, borrar_hojas_vacias: bool = True):
        """
        Genera una copia del archivo Excel filtrando solo las filas especificadas.
        """
        try:
            shutil.copy2(ruta_origen, ruta_destino)
            wb = openpyxl.load_workbook(ruta_destino)
            KEYWORDS = ExcelFileReader.KEYWORDS_HEADER

            hojas_a_borrar = []
            
            for sheet_name in wb.sheetnames:
                if "PARAMETRO" in sheet_name.upper():
                    continue

                indices_a_mantener = filas_a_mantener.get(sheet_name, set())
                ws = wb[sheet_name]
                
                if not indices_a_mantener:
                    if borrar_hojas_vacias and len(wb.sheetnames) > 1:
                        hojas_a_borrar.append(sheet_name)
                    else:
                        self._limpiar_datos_hoja(ws, KEYWORDS, set())
                    continue
                    
                self._limpiar_datos_hoja(ws, KEYWORDS, indices_a_mantener)
            
            for sh_name in hojas_a_borrar:
                if len(wb.sheetnames) > 1:
                    del wb[sh_name]
            
            wb.save(ruta_destino)
            wb.close()
            
        except Exception as e:
            logger.error(f"Error generando copia filtrada {ruta_destino.name}: {e}")

    def _limpiar_datos_hoja(self, ws, keywords: list, indices_a_mantener: set):
        """
        Limpia los datos de una hoja, manteniendo solo las filas especificadas.
        """
        header_row_idx = None
        for i, row in enumerate(ws.iter_rows(max_row=15, values_only=True), start=1):
            row_str = [str(c).upper().strip() if c else "" for c in row]
            if any(k in row_str for k in keywords):
                header_row_idx = i
                break
        
        if not header_row_idx: return

        start_data_row = header_row_idx + 1
        max_row = ws.max_row
        filas_a_mantener = {start_data_row + idx for idx in indices_a_mantener}

        for r in range(start_data_row, max_row + 1):
            if r not in filas_a_mantener:
                for cell in ws[r]:
                    cell.value = None

                ws.row_dimensions[r].hidden = True

    def _mover_a_gestionados(self, ruta_excel, cliente_folder):
        """
        Mueve el archivo Excel a la carpeta GESTIONADOS.
        """
        try:
            gestionados_dir = cliente_folder.gestionados_path(self._base_dir)
            gestionados_dir.mkdir(parents=True, exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            nombre_nuevo = f"{ruta_excel.stem}_{timestamp}{ruta_excel.suffix}"
            destino = gestionados_dir / nombre_nuevo
            
            os.rename(ruta_excel, destino)
            logger.info(f"Archivo movido a: {destino.name}")
        except Exception as e:
            logger.warning(f"No se pudo mover archivo: {e}")

    def _manejar_excel_fallido(self, ruta_excel, cliente_folder, razon_error):
        """
        Maneja archivos Excel que fallaron en el procesamiento.
        """
        logger.error(f"   Razón: {razon_error}")
        try:
            errores_dir = cliente_folder.to_path(self._base_dir) / "ERRORES"
            errores_dir.mkdir(exist_ok=True)

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            nombre = f"{ruta_excel.stem}_ERROR_{timestamp}{ruta_excel.suffix}"
            destino = errores_dir / nombre

            os.rename(ruta_excel, destino)

            log_path = destino.with_suffix('.txt')
            with open(log_path, 'w', encoding='utf-8') as f:
                f.write(f"Error: {razon_error}\nFecha: {datetime.now()}")
            
            logger.info(f"Log de error creado: {destino.name}")
        except Exception as e:
            logger.error(f"Error moviendo archivo fallido: {e}")