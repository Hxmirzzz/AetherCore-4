from __future__ import annotations
from typing import List, Tuple
from datetime import datetime, date
from decimal import Decimal
import pandas as pd
import logging

from src.application.interfaces.i_excel_mapper import BaseExcelMapper
from src.application.dto.servicio_dto import AetherServiceImportDto

logger = logging.getLogger(__name__)

class Cash4uExcelMapper(BaseExcelMapper):
    """
    Mapper especializado para formato (Cash4U).
    """

    DEFAULT_USER_ID = "dda6d2ea-0a02-4f77-a063-04d940572a1a"

    def __init__(self, cod_cliente: str):
        self._cod_cliente = cod_cliente
        self.col_pedido = None
        self.col_codigo = None
        self.col_calidad = None
        self.col_valor_rec = None

    @property
    def cod_cliente(self) -> str:
        return self._cod_cliente

    @property
    def nombre_cliente(self) -> str:
        return "CASH4U_CLIENTE"

    def validar_estructura(self, df: pd.DataFrame) -> tuple[bool, str]:
        cols_upper = [str(c).upper().strip() for c in df.columns]

        self.col_pedido = next((c for c in cols_upper if "NRO" in c and "SERVICIO" in c), None)
        punto_especifico = next((c for c in cols_upper if "PUNTO" in c and ("CODIGO" in c or "CÓDIGO" in c or "COD" in c)), None)
        codigo_generico = next((c for c in cols_upper if "CODIGO" in c or "CÓDIGO" in c or "COD" in c), None)
        self.col_codigo = punto_especifico if punto_especifico else codigo_generico
        self.col_calidad = next((c for c in cols_upper if "CALIDAD" in c), None)
        self.col_valor_rec = next((c for c in cols_upper if "VALOR" in c and "RECOLECCI" in c), None)
        
        if not self.col_pedido or not self.col_codigo:
            return False, "Columnas requeridas no encontradas"
        
        return True, "Estructura válida"

    def mapear_a_dtos(self, df: pd.DataFrame, nombre_archivo: str) -> List[Tuple[AetherServiceImportDto, int]]:
        dtos = []

        logger.info(f"Procesando archivo Cash4U: {nombre_archivo}")

        metadata = getattr(df, 'attrs', {}).get('metadata', {})
        raw_f_solicitud = metadata.get('FECHA SOLICITUD') or metadata.get('FECHA DE SOLICITUD')
        fecha_sol = self._parsear_fecha(raw_f_solicitud) or date.today()

        cols_denominacion = []
        for col, col_name in enumerate(df.columns):
            raw_col = str(col_name).replace('$', '').strip()

            try:
                clean_col = raw_col.replace('.0', '').replace('.', '').replace(',', '')
                if clean_col.isdigit() and int(clean_col) >= 50:
                    cols_denominacion.append((col, int(clean_col)))
            except: continue

        cols_denominacion.sort(key=lambda x: x[1], reverse=True)

        map_cols = {str(c).upper().strip(): i for i, c in enumerate(df.columns)}
        idx_pedido = map_cols.get(self.col_pedido, -1)
        idx_codigo = map_cols.get(self.col_codigo, -1)
        idx_calidad = map_cols.get(self.col_calidad, -1)
        idx_valor_rec = map_cols.get(self.col_valor_rec, -1)
        
        for idx, row in df.iterrows():
            try:
                val_pedido = str(row.iloc[idx_pedido]).strip() if idx_pedido != -1 else ''

                if "RESUMEN" in val_pedido.upper() or "DENOMINACION" in val_pedido.upper(): break
                
                pedido_clean = val_pedido.replace('_', '').replace('.', '').strip()
                cod_punto = str(row.iloc[idx_codigo]).strip() if idx_codigo != -1 else ''
                punto_clean = cod_punto.replace('_', '').replace('.', '').strip()

                if not pedido_clean or not punto_clean or punto_clean == '0':
                    continue
                
                calidad = str(row.iloc[idx_calidad]).upper().strip() if idx_calidad != -1 else ""
                es_recoleccion = (idx_valor_rec != -1 or "RECOLECCION" in calidad)
                es_atm = "CAJERO" in calidad or "ATM" in calidad
                es_moneda = "MONEDA" in calidad

                valor_billete = Decimal(0)
                valor_moneda = Decimal(0)
                detalle_tecnico = []

                if es_recoleccion:
                    val_rec = row.iloc[idx_valor_rec] if idx_valor_rec != -1 else 0
                    valor_billete = self._parse_valor_monetario(val_rec)
                else:
                    for col_idx, deno_val in cols_denominacion:
                        cantidad = self._parse_entero(row.iloc[col_idx])
                        if cantidad > 0:
                            deno = deno_val
                            
                            if es_moneda and deno >= 10000:
                                deno = int(deno / 100)

                            subtotal = Decimal(cantidad) * Decimal(deno)
                            
                            if deno >= 2000:
                                valor_billete += subtotal
                            else:
                                valor_moneda += subtotal

                            detalle_tecnico.append(f"{deno}:{cantidad}")
                    
                valor_total = valor_billete + valor_moneda
                cod_con = 1 if es_recoleccion else (3 if es_atm else 2)

                dto = AetherServiceImportDto(
                    cod_cliente=int(self.cod_cliente),
                    cod_sucursal=1, 
                    fecha_solicitud=str(fecha_sol),
                    hora_solicitud=datetime.now().strftime("%H:%M:%S"),
                    cod_concepto=cod_con,
                    cod_punto_origen=punto_clean,
                    cod_punto_destino="",
                    numero_pedido=pedido_clean,
                    cod_os_cliente=val_pedido,
                    observaciones=f"{calidad} | {'; '.join(detalle_tecnico)}"[:450],
                    valor_billete=valor_billete,
                    valor_moneda=valor_moneda,
                    valor_servicio=valor_total,
                    cef_numero_planilla=0,
                    valor_total_declarado=valor_total,
                    cef_tipo_transaccion="RC" if es_recoleccion else "PV"
                )

                dtos.append((dto, idx))

            except Exception as e:
                logger.error(f"Error al procesar fila {idx} de Cash4U: {e}")
        
        return dtos

    def _parsear_fecha(self, fecha_str: str | None) -> date | None:
        """
        Parsea fechas como: 'Enero 28 de 2026' o '28 de Enero de 2026'
        """
        if not fecha_str or pd.isna(fecha_str):
            return None
            
        texto = str(fecha_str).upper().strip()
        meses = {
            'ENERO': 1,
            'FEBRERO': 2,
            'MARZO': 3,
            'ABRIL': 4,
            'MAYO': 5,
            'JUNIO': 6,
            'JULIO': 7,
            'AGOSTO': 8,
            'SEPTIEMBRE': 9,
            'OCTUBRE': 10,
            'NOVIEMBRE': 11,
            'DICIEMBRE': 12
        }
        
        try:
            partes = texto.replace('DE', '').replace(',', '').strip().split()
            dia, mes, anio = None, None, None

            for p in partes:
                if p in meses:
                    mes = meses[p]
                elif p.isdigit():
                    val = int(p)
                    if val > 31:
                        anio = val
                    else:
                        dia = val

            if dia and mes and anio:
                return date(anio, mes, dia)

        except Exception as e:
            logger.error(f"Error al parsear fecha: '{texto}' - {e}")

        return None

    def _parse_valor_monetario(self, val) -> Decimal:
        if pd.isna(val):
            return Decimal('0')
        s = str(val).replace('_', '').replace('$','').replace(' ','').strip()
        if not s:
            return Decimal('0')
        
        s = s.replace('.', '')
        s = s.replace(',', '.')

        try:
            return Decimal(s)
        except:
            return Decimal('0')

    def _parse_entero(self, val) -> int:
        if pd.isna(val):
            return 0

        s = str(val).replace('_', '').replace(' ','').strip()
        if not s:
            return 0
        
        s = s.replace('.', '')
        
        if ',' in s:
            s = s.split(',')[0]

        try:
            return int(s)
        except:
            return 0