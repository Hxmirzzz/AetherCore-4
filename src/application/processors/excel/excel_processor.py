from __future__ import annotations
from pathlib import Path
import logging
import os
import shutil
from datetime import datetime

from src.application.processors.excel.excel_file_reader import ExcelFileReader
from src.application.processors.excel.excel_processor_factory import ExcelProcessorFactory
from src.application.services.api_service import ApiService
from src.application.dto.servicio_dto import AetherServiceImportDto
from src.domain.value_objects.cliente_folder import ClienteFolder
from src.domain.value_objects.codigo_punto import CodigoPunto

logger = logging.getLogger(__name__)

class ExcelProcessor:
    """
    Procesador principal de archivos Excel de solicitudes.
    """

    def __init__(
        self,
        reader: ExcelFileReader | None = None,
        api_service: ApiService | None = None,
        base_solicitudes_dir: Path | None = None
    ):
        """
        Inicializa el procesador.
        """
        self._reader = reader or ExcelFileReader()
        self._api_service = api_service
        self._base_dir = base_solicitudes_dir or Path("data/SOLICITUDES")

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
            logger.info(f"== Procesando: {ruta_excel.name} para Cliente: {cliente_folder} ==")
        
            info = self._reader.read_multiple_sheets(ruta_excel)    
            if not info:
                self._manejar_excel_fallido(ruta_excel, cliente_folder, "Archivo vacío o ilegible")
                return False
            
            try:
                mapper = ExcelProcessorFactory.get_mapper(cliente_folder.cod_cliente)
            except ValueError as e:
                self._manejar_excel_fallido(ruta_excel, cliente_folder, str(e))
                return False

            nombre_hoja_params = next((k for k in info.keys() if "PARAMETRO" in k.upper()), None)
            if nombre_hoja_params and hasattr(mapper, 'actualizar_parametros'):
                mapper.actualizar_parametros(info.pop(nombre_hoja_params))

            dtos_a_enviar = []
            mapeo_filas_origen = {}
            
            for nombre_hoja, df in info.items():
                valido, error_msg = mapper.validar_estructura(df)
                if not valido:
                    logger.warning(f"Saltando hoja '{nombre_hoja}' - {error_msg}")
                    continue

                datos_hoja = mapper.mapear_a_dtos(df, f"{ruta_excel.name} [{nombre_hoja}]")
            
                for s_old, t_old, idx_fila in datos_hoja:
                    punto_limpio = CodigoPunto.from_raw(str(s_old.cod_punto_origen)).parte_numerica.strip()

                    dto = AetherServiceImportDto(
                        # --- Datos del Servicio ---
                        numero_pedido=s_old.numero_pedido,
                        cod_os_cliente=s_old.cod_os_cliente,
                        cod_cliente=s_old.cod_cliente,
                        cod_sucursal=s_old.cod_sucursal,
                        fecha_solicitud=str(s_old.fecha_solicitud),
                        hora_solicitud=str(s_old.hora_solicitud),
                        cod_concepto=s_old.cod_concepto,
                        tipo_traslado=s_old.tipo_traslado,
                        modalidad_servicio=s_old.modalidad_servicio,
                        observaciones=s_old.observaciones,
                        cod_punto_origen=punto_limpio,
                        indicador_tipo_origen=s_old.indicador_tipo_origen,
                        cod_punto_destino=s_old.cod_punto_destino,
                        indicador_tipo_destino=s_old.indicador_tipo_destino,
                        valor_billete=s_old.valor_billete,
                        valor_moneda=s_old.valor_moneda,
                        numero_kits_cambio=s_old.numero_kits_cambio,
                        # --- Datos de Transacción ---
                        cef_numero_planilla=t_old.numero_planilla,
                        cantidad_bolsas_declaradas=t_old.cantidad_bolsas_declaradas,
                        cantidad_sobres_declarados=t_old.cantidad_sobres_declarados,
                        cantidad_cheques_declarados=t_old.cantidad_cheques_declarados,
                        cantidad_documentos_declarados=t_old.cantidad_documentos_declarados,
                        valor_billetes_declarado=t_old.valor_billetes_declarado,
                        valor_monedas_declarado=t_old.valor_monedas_declarado,
                        valor_total_declarado=t_old.valor_total_declarado,
                        valor_total_declarado_letras=t_old.valor_total_declarado_letras,
                        cef_es_custodia=t_old.es_custodia,
                        cef_es_punto_a_punto=t_old.es_punto_a_punto
                    )

                    dtos_a_enviar.append(dto)
                    key = len(dtos_a_enviar) - 1
                    mapeo_filas_origen[key] = (nombre_archivo, idx_fila, s_old.numero_pedido)
                
            if not dtos_a_enviar:
                self._manejar_excel_fallido(ruta_excel, cliente_folder, "No se encontraron registros válidos en el archivo")
                return False

            logger.info(f"🚀 Enviando {len(dtos_a_enviar)} servicios a VCashApp vía API...")
            respuesta = self._api_service.upload_services(dtos_a_enviar)

            if respuesta:
                return self._gestionar_finalizacion(ruta_excel, cliente_folder, respuesta, mapeo_filas_origen)
            else:
                self._manejar_excel_fallido(ruta_excel, cliente_folder, "Error al enviar servicios a VCashApp")
                return False
            
        except Exception as e:
            logger.exception(f"❌ Error crítico: {e}")
            self._manejar_excel_fallido(ruta_excel, cliente_folder, str(e))
            return False

    def _gestionar_finalizacion(self, ruta_excel: Path, cliente_folder: ClienteFolder, respuesta: dict, mapeo: dict) -> bool:
        """
        Gestiona la finalización del procesamiento del archivo Excel.
        """
        exitosos_indices = {}
        fallidos_indices = {}
        errores_log = []
        
        for idx, result in enumerate(respuesta.get('details', [])):
            hoja, fila_idx, pedido = mapeo[idx]

            if result['success']:
                if hoja not in exitosos_indices: exitosos_indices[hoja] = set()
                exitosos_indices[hoja].add(fila_idx)
            else:
                if hoja not in fallidos_indices: fallidos_indices[hoja] = set()
                fallidos_indices[hoja].add(fila_idx)
                errores_log.append(f"Fila {fila_idx + 6} (Hoja {hoja}): Pedido {pedido} -> {result['message']}")

        total_exitosos = sum(len(v) for v in exitosos_indices.values())
        total_fallidos = sum(len(v) for v in fallidos_indices.values())

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        nombre_base = ruta_excel.stem.replace('_NOVEDADES', '').replace('_OK', '').split('_202')[0]

        if total_exitosos > 0:
            gestionados_dir =  cliente_folder.gestionados_path(self._base_dir)
            gestionados_dir.mkdir(parents=True, exist_ok=True)
            ruta_ok = gestionados_dir / f"{nombre_base}_OK_{timestamp}{ruta_excel.suffix}"

            if total_fallidos == 1:
                shutil.move(str(ruta_excel), str(ruta_ok))
                logger.info(f"✅ Archivo completo movido a Gestionados")
            else:
                self._generar_copia_filtrada(ruta_excel, ruta_ok, exitosos_indices)
                logger.info(f"📁 Copia de exitosos guardada en GESTIONADOS")

        if total_fallidos > 0:
            novedades_dir = cliente_folder.to_path(self._base_dir) / "NOVEDADES"
            novedades_dir.mkdir(parents=True, exist_ok=True)
            ruta_novedades = novedades_dir / f"{nombre_base}_NOVEDADES_{timestamp}{ruta_excel.suffix}"
            self._generar_copia_filtrada(ruta_excel, ruta_novedades, fallidos_indices, borrar_hojas_vacias=True)
            
            ruta_txt = novedades_dir / f"{nombre_base}_NOVEDADES_{timestamp}.txt"
            with open(ruta_txt, 'w', encoding='utf-8') as f:
                f.write(f"REPORTE DE NOVEDADES - AETHERCORE 4\n")
                f.write(f"Archivo: {ruta_excel.name}\n")
                f.write(f"Resultado API: {respuesta.get('summary')}\n")
                f.write("-" * 60 + "\n")
                for err in errores_log:
                    f.write(f"- {err}\n")
            
            logger.warning(f"⚠️ Se generó archivo de NOVEDADES con {total_fallidos} errores")


        if ruta_excel.exists():
            try:
                os.remove(ruta_excel)
            except Exception as e:
                logger.error(f"❌ Error al eliminar archivo original: {e}")  

        logger.info(f"=== PROCESO FINALIZADO: {total_exitosos} OK / {total_fallidos} ERROR ===")
        return True          

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