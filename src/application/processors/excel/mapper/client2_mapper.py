from __future__ import annotations
from typing import List, Tuple
from datetime import datetime, date
from decimal import Decimal
import pandas as pd
import logging
import re

from src.application.interfaces.i_excel_mapper import BaseExcelMapper
from src.application.dto.servicio_dto import AetherServiceImportDto

logger = logging.getLogger(__name__)

class Client2Mapper(BaseExcelMapper):
    """
    Mapper para archivos Excel correspondientes al Cliente 2.
    """

    def __init__(self, cod_cliente: str):
        self._cod_cliente = cod_cliente
        self.col_codigo = None
        self.col_punto = None
        self.col_modalidad = None
        self.col_saldo = None
        self.col_fecha = None
        self.col_rango = None
        self.col_observacion = None

    @property
    def cod_cliente(self) -> str:
        return self._cod_cliente

    @property
    def nombre_cliente(self) -> str:
        return f"CLIENTE_{self._cod_cliente}"

    def actualizar_parametros(self, df_params: pd.DataFrame) -> None:
        pass

    def validar_estructura(self, df: pd.DataFrame) -> tuple[bool, str]:
        df.columns = [str(c).upper().strip() for c in df.columns]

        self.col_codigo = next((c for c in df.columns if c == "CODIGO" or "CÓDIGO" in c), None)
        self.col_punto = next((c for c in df.columns if "PUNTO DE ATENCION" in c or "ATENCIÓN" in c), None)
        self.col_modalidad = next((c for c in df.columns if "MODALIDAD" in c), None)
        self.col_saldo = next((c for c in df.columns if "SALDO PROGRAMADO" in c or "SALDO" in c), None)
        self.col_fecha = next((c for c in df.columns if "FECHA DEL SERVICIO" in c and "FECHA" in c), None)
        self.col_rango = next((c for c in df.columns if "RANGO" in c), None)
        self.col_observacion = next((c for c in df.columns if "OBSERVACION" in c or "OBSERVACIÓN" in c), None)
        
        if not self.col_codigo or not self.col_saldo or not self.col_fecha:
            return False, "Faltan columnas clave (CODIGO, SALDO PROGRAMADO, FECHA DEL SERVICIO)"
        
        return True, "Estructura válida"

    def mapear_a_dtos(self, df: pd.DataFrame, nombre_archivo: str) -> List[Tuple[AetherServiceImportDto, int]]:
        dtos = []
        logger.info(f"Procesando archivo Client2: {nombre_archivo}")

        for idx, row in df.iterrows():
            try:
                codigo = str(row.get(self.col_codigo, '')).strip().replace('.0', '').replace(' ', '')

                if not codigo or codigo.upper() == 'NAN':
                    continue

                saldo_str = row.get(self.col_saldo, 0)
                valor_total = self._parse_valor_monetario(saldo_str)

                if valor_total == Decimal('0'):
                    logger.debug(f"Fila {idx} omitida: Saldo programado en 0 o inválido.")
                    continue

                fecha_serv = self._parsear_fecha(row.get(self.col_fecha))
                rango_hora_str = str(row.get(self.col_rango, '')).strip()
                hora_inicio = self._parsear_rango_a_hora(rango_hora_str)
                now = datetime.now()

                modalidad = str(row.get(self.col_modalidad, '')).strip()
                obs_usuario = str(row.get(self.col_observacion, '')).strip()

                obs_usuario = obs_usuario.replace('\n', ' ').replace('\r', ' ')

                observaciones_list = []
                if modalidad and modalidad.upper() != 'NAN':
                    observaciones_list.append(f"Mod: {modalidad}")
                if rango_hora_str and rango_hora_str.upper() != 'NAN':
                    observaciones_list.append(f"Rango: {rango_hora_str}")
                if obs_usuario and obs_usuario.upper() != 'NAN':
                    observaciones_list.append(f"Nota: {obs_usuario}")

                obs = " | ".join(observaciones_list)[:450]

                numero_pedido_sintetico = f"{self.cod_cliente}-{fecha_serv.strftime('%Y%m%d')}-{codigo}"

                if "RECOLECCION BLINDADA" in modalidad:
                    service_type = "RC"
                else:
                    service_type = "PV"

                dto = AetherServiceImportDto(
                    cod_cliente=int(self.cod_cliente),
                    cod_sucursal=1,
                    fecha_solicitud=str(now.date()),
                    hora_solicitud=now.strftime("%H:%M:%S"),
                    fecha_programacion=str(fecha_serv),
                    hora_programacion=hora_inicio,
                    cod_concepto=1,
                    cod_punto_origen=codigo, 
                    cod_punto_destino="",
                    numero_pedido=numero_pedido_sintetico,
                    cod_os_cliente=numero_pedido_sintetico,
                    observaciones=obs,
                    valor_billete=0,
                    valor_moneda=0,
                    valor_servicio=valor_total,
                    numero_kits_cambio=0,
                    cef_numero_planilla=0,
                    valor_total_declarado=valor_total,
                    cef_divisa="COP",
                    cef_tipo_transaccion=service_type,
                    cef_estado_transaccion="RegistroTesoreria"
                )

                dtos.append((dto, idx))

            except Exception as e:
                logger.error(f"Error procesando fila {idx} en Cliente 2: {e}")

        return dtos

    def _parse_valor_monetario(self, val) -> Decimal:
        if pd.isna(val): 
            return Decimal('0')
        s = str(val).replace('$', '').replace(' ', '').replace('_', '').strip()

        if '.' in s and ',' not in s:
            s = s.replace('.', '')
        elif ',' in s and '.' in s:
            if s.rfind(',') > s.rfind('.'):
                s = s.replace('.', '').replace(',', '.')
            else:
                s = s.replace(',', '')
        elif ',' in s:
            if len(s) - s.rfind(',') - 1 <= 2:
                s = s.replace(',', '.')
            else:
                s = s.replace(',', '')

        try:
            return Decimal(s)
        except:
            return Decimal('0')

    def _parsear_rango_a_hora(self, rango: str) -> str:
        """
        Intenta extraer la hora de inicio de un rango como '15PM - 18PM' o '9AM - 18PM'.
        Retorna formato 'HH:MM:SS'.
        """
        if not rango or rango.upper() == 'NAN':
            return "00:00:00"

        try:
            match = re.search(r'\d+', rango)
            if match:
                hora = int(match.group())

                if "PM" in rango.upper().split('-')[0] and hora < 12:
                    hora += 12
                elif hora > 23:
                    hora = 0

                return f"{hora:02d}:00:00"
        except Exception:
            pass

        return "00:00:00"

    def _parsear_fecha(self, val) -> date:
        if pd.isna(val) or not str(val).strip() : return date.today()

        if isinstance(val, (datetime, pd.Timestamp)):
            if val.day > 12:
                return val.date()
            val = val.strftime('%d/%m/%Y')

        val_str = str(val).split()[0].strip()

        formatos = [ 
            '%d/%m/%Y', # 09/05/2026
            '%d-%m-%Y', # 09-05-2026
            '%Y-%m-%d', # 2026-05-09
            '%Y/%m/%d'  # 2026/05/09
        ]

        for fmt in formatos:
            try:
                return datetime.strptime(val_str, fmt).date()
            except ValueError:
                continue
        
        logger.warning(f"⚠️ No se pudo entender el formato de fecha '{val_str}'. Usando la de hoy.")
        return date.today()