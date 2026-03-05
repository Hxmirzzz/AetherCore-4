from __future__ import annotations
from typing import List, Tuple, Dict, Any
from datetime import datetime, date, time
from decimal import Decimal
import pandas as pd
import logging

from src.application.interfaces.i_excel_mapper import BaseExcelMapper
from src.application.dto.servicio_dto import AetherServiceImportDto

logger = logging.getLogger(__name__)

class EmergencyMapper(BaseExcelMapper):
    """
    Mapper para archivos Excel de emergencia.
    """

    def __init__(self, cod_cliente: str):
        self._cod_cliente = cod_cliente
        self.col_pedido = None
        self.col_codigo = None
        self.col_kit_moneda = None
        self.col_kit_billete = None
        self.col_fecha = None

    @property
    def cod_cliente(self) -> str:
        return self._cod_cliente

    @property
    def nombre_cliente(self) -> str:
        return "EMERGENCY_CLIENTE"

    def actualizar_parametros(self, df_params: pd.DataFrame) -> None:
        pass

    def validar_estructura(self, df: pd.DataFrame) -> tuple[bool, str]:
        df.columns = [str(c).upper().strip() for c in df.columns]

        self.col_pedido = next((c for c in df.columns if "ID" in c and "BCT" in c), None)
        self.col_codigo = next((c for c in df.columns if "COD" in c and "UNICO" in c), None)
        self.col_kit_moneda = next((c for c in df.columns if "KITS" in c and "MONEDA" in c), None)
        self.col_kit_billete = next((c for c in df.columns if "KITS" in c and "BILLETE" in c), None)
        self.col_fecha = next((c for c in df.columns if "FECHA" in c), None)
        
        if not self.col_pedido or not self.col_codigo:
            return False, "Faltan columnas clave (ID BCT, COD. UNICO)"
        
        return True, "Estructura válida"

    def mapear_a_dtos(self, df: pd.DataFrame, nombre_archivo: str) -> List[Tuple[AetherServiceImportDto, int]]:
        dtos = []
        logger.info(f"Procesando archivo Emergency: {nombre_archivo}")

        headers = getattr(df, 'attrs', {}).get('header_rows', pd.DataFrame())
        info_kit = self._extaer_info_kits(headers)
        
        unidad_moneda = info_kit['moneda']['valor']
        detalle_moneda = info_kit['moneda']['detalle']
        unidad_billete = info_kit['billete']['valor']
        detalle_billete = info_kit['billete']['detalle']

        for idx, row in df.iterrows():
            try:
                pedido = str(row.get(self.col_pedido, '')).strip().replace('.0', '')
                codigo = str(row.get(self.col_codigo, '')).strip().replace('.0', '').replace(' ', '')

                if not pedido or not codigo or pedido.upper() == 'NAN':
                    continue
                
                qty_moneda = self._parse_entero(row.get(self.col_kit_moneda, 0))
                qty_billete = self._parse_entero(row.get(self.col_kit_billete, 0))
                
                total_moneda = Decimal(qty_moneda) * unidad_moneda
                total_billete = Decimal(qty_billete) * unidad_billete
                valor_servicio = total_moneda + total_billete
                
                fecha_serv = self._parsear_fecha(row.get(self.col_fecha))
                now = datetime.now()

                obs = f"Kits Moneda: {qty_moneda} [{detalle_moneda}], Kits Billete: {qty_billete} [{detalle_billete}]"[:450]

                dto = AetherServiceImportDto(
                    cod_cliente=int(self.cod_cliente),
                    cod_sucursal=1,
                    fecha_solicitud=str(now.date()),
                    hora_solicitud=now.strftime("%H:%M:%S"),
                    fecha_programacion=str(fecha_serv),
                    hora_programacion="00:00:00",
                    cod_concepto=2,
                    cod_punto_origen=codigo, 
                    cod_punto_destino="",
                    numero_pedido=f"{self.cod_cliente}{pedido}",
                    cod_os_cliente=pedido,
                    observaciones=obs,
                    valor_billete=total_billete,
                    valor_moneda=total_moneda,
                    valor_servicio=valor_servicio,
                    numero_kits_cambio=qty_moneda + qty_billete,
                    cef_numero_planilla=0,
                    valor_total_declarado=valor_servicio,
                    cef_divisa="COP",
                    cef_tipo_transaccion="PV",
                    cef_estado_transaccion="ProvisionEnProceso"
                )

                dtos.append((dto, idx))

            except Exception as e:
                logger.error(f"Error procesando fila {idx}: {e}")

        return dtos

    def _extaer_info_kits(self, raw_df: pd.DataFrame) -> dict:
        """
        Analiza la cabecera para extraer el contenido y valor de los kits.
        Retorna: {'moneda': {'valor': X, 'detalle': '...'}, 'billete': ...}
        """
        info = {
            'moneda': {'valor': Decimal('0'), 'detalle': ''},
            'billete': {'valor': Decimal('0'), 'detalle': ''}
        }

        if raw_df.empty:
            return info

        indices = []

        start_row_idx = -1
        for i in range(min(5, len(raw_df))):
            row_vals = [str(x).upper().strip() for x in raw_df.iloc[i].values]
            for col_idx, val in enumerate(row_vals):
                if 'DENOMINACIÓN' in val or "DENOMINACION" in val:
                    indices.append(col_idx)
                    start_row_idx = i
            if indices:
                break
        
        if start_row_idx == -1:
            return info

        for table_idx in indices:
            tipo_kit = 'moneda' if table_idx < 5 else 'billete'
            items = []
            valor_total = Decimal('0')
            
            for r in range(start_row_idx + 1, len(raw_df)):
                try:
                    denom = str(raw_df.iloc[r, table_idx]).strip()
                    cant_raw = raw_df.iloc[r, table_idx + 1]
                    valor_raw = raw_df.iloc[r, table_idx + 2]
                    
                    if not denom or denom.lower() == 'nan': continue

                    if "TOTAL" in denom.upper():
                        valor_total = self._parse_valor_monetario(valor_raw)
                        break

                    cant = self._parse_entero(cant_raw)
                    if cant > 0:
                        items.append(f"{denom}:{cant}")
                except:
                    break

            info[tipo_kit]['valor'] = valor_total
            info[tipo_kit]['detalle'] = "; ".join(items)

        return info

    def _parse_valor_monetario(self, val) -> Decimal:
        if pd.isna(val) : return Decimal('0')
        s = str(val).replace('$', '').replace(' ', '').replace('_', '').strip()
        try:
            return Decimal(s)
        except:
            return Decimal('0')

    def _parse_entero(self, val) -> int:
        try:
            return int(float(str(val)))
        except:
            return 0

    def _parsear_fecha(self, val) -> date:
        if pd.isna(val) or not str(val).strip() : return date.today()

        if isinstance(val, (datetime, pd.Timestamp)):
            return val.date()

        val_str = str(val).split()[0].strip()

        formatos = [ 
            '%Y-%m-%d', # 2026-03-28
            '%d-%m-%Y', # 28-03-2026
            '%d/%m/%Y', # 28/03/2026
            '%Y/%m/%d'  # 2026/03/28
        ]

        for fmt in formatos:
            try:
                return datetime.strptime(val_str, fmt).date()
            except ValueError:
                continue
        
        logger.warning(f"⚠️ No se pudo entender el formato de fecha '{val_str}'. Usando la de hoy.")
        return date.today()
        
        