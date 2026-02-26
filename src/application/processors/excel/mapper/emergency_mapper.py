from __future__ import annotations
from typing import List, Tuple, Dict, Any
from datetime import datetime, date, time
from decimal import Decimal
import pandas as pd
import logging

from src.application.interfaces.i_excel_mapper import BaseExcelMapper
from src.application.dto.servicio_dto import ServicioDTO
from src.application.dto.transaccion_dto import TransaccionDTO

logger = logging.getLogger(__name__)

class EmergencyMapper(BaseExcelMapper):
    """
    Mapper para archivos Excel de emergencia.
    """

    DEFAULT_USER_ID = "e5926e18-33b1-468c-a979-e4e839a86f30"

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

    def obtener_resumen(self, df: pd.DataFrame) -> Dict[str, Any]:
        return {
            'total_servicios': len(df),
            'archivo_valido': True
        }

    def mapear_a_dtos(self, df: pd.DataFrame, nombre_archivo: str) -> List[Tuple[ServicioDTO, TransaccionDTO, int]]:
        dtos = []
        logger.info(f"Procesando archivo Emergency: {nombre_archivo}")

        headers = getattr(df, 'attrs', {}).get('header_rows', pd.DataFrame())
        info_kit = self._extaer_info_kits(headers)
        
        unidad_moneda = info_kit['moneda']['valor']
        detalle_moneda = info_kit['moneda']['detalle']
        unidad_billete = info_kit['billete']['valor']
        detalle_billete = info_kit['billete']['detalle']

        logger.info(f"Unidad Moneda: {detalle_moneda}, Unidad Billete: {detalle_billete}")
        logger.info(f"Valores de kits: Moneda={unidad_moneda}, Billete={unidad_billete}")

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
                
                fecha_raw = row.get(self.col_fecha)
                fecha = self._parsear_fecha(fecha_raw)
                
                now = datetime.now()
                numero_pedido = f"{self.cod_cliente}{pedido}"
                obs_parts = []
                if qty_moneda > 0:
                    obs_parts.append(f"Kits Moneda: {qty_moneda} [{detalle_moneda}]")
                if qty_billete > 0:
                    obs_parts.append(f"Kits Billete: {qty_billete} [{detalle_billete}]")
                obs = ", ".join(obs_parts)
                if not obs:
                    obs = "No se especificaron kits"

                servicio = ServicioDTO(
                    numero_pedido=numero_pedido,
                    cod_cliente=int(self.cod_cliente),
                    cod_sucursal=1,
                    cod_concepto=2,
                    tipo_traslado='N',
                    fecha_solicitud=now.date(),
                    hora_solicitud=now.time(),
                    fecha_programacion=fecha,
                    hora_programacion=time(8,0),
                    cod_estado=0,
                    cod_cliente_origen=int(self.cod_cliente),
                    cod_punto_origen="FONDO", indicador_tipo_origen='F',
                    cod_cliente_destino=int(self.cod_cliente),
                    cod_punto_destino=codigo, indicador_tipo_destino='P',
                    fallido=False,
                    valor_billete=total_billete,
                    valor_moneda=total_moneda,
                    valor_servicio=valor_servicio,
                    cod_os_cliente=pedido,
                    numero_kits_cambio=qty_moneda + qty_billete,
                    modalidad_servicio='2',
                    observaciones=obs[:455],
                    archivo_detalle=nombre_archivo
                )

                transaccion = TransaccionDTO(
                    cod_sucursal=1,
                    fecha_registro=now,
                    usuario_registro_id=self.DEFAULT_USER_ID,
                    tipo_transaccion='PV',
                    divisa='COP',
                    valor_billetes_declarado=total_billete,
                    valor_monedas_declarado=total_moneda,
                    valor_total_declarado=valor_servicio,
                    estado_transaccion='ProvisionEnProceso',
                    novedad_informativa=obs[:455]
                )

                dtos.append((servicio, transaccion, idx))

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
        if pd.isna(val) : return date.today()

        if isinstance(val, (datetime, pd.Timestamp)):
            return val.date()
        try:
            val_str = str(val).split()[0]
            if '-' in val_str:
                return datetime.strptime(val_str, '%Y-%m-%d').date()
            if '/' in val_str:
                return datetime.strptime(val_str, '%d/%m/%Y').date()
        except:
            pass
        
        return date.today()
        
        