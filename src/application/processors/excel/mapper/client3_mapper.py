from __future__ import annotations

from typing import List, Tuple, Dict, Any
from datetime import datetime, date
from decimal import Decimal
import pandas as pd
import logging

from src.application.interfaces.i_excel_mapper import BaseExcelMapper
from src.application.dto.servicio_dto import AetherServiceImportDto

logger = logging.getLogger(__name__)

class Client3Mapper(BaseExcelMapper):
    """
    Mapper para archivos Excel correspondientes al Cliente 3.
    Soporta dos formatos dinámicamente:
    1. RECAMBIOS (SUPPLY_CHANGE): Usa Cod.Int., Val. Total, Texto breve, Cantidad.
    2. ATM (ATM_REPLENISHMENT): Usa Código Maquina, Fecha Servicio.
    """

    def __init__(self, cod_cliente: str):
        self._cod_cliente = cod_cliente
        self.tipo_archivo = None
        self.col_codigo = None
        self.col_fecha = None
        self.col_cantidad = None
        self.col_valor_total = None
        self.col_observacion = None

    @property
    def cod_cliente(self) -> str:
        return self._cod_cliente

    @property
    def nombre_cliente(self) -> str:
        return f"CLIENTE_{self.cod_cliente}"

    def actualizar_parametros(self, df_params: pd.DataFrame) -> None:
        pass

    def validar_estructura(self, df: pd.DataFrame) -> Tuple[bool, str]:
        if df.empty: return False, "Estructura non valida"

        def clean_text(text):
            if pd.isna(text): return ""
            t = str(text).upper()
            t = t.replace('Á', 'A').replace('É', 'E').replace('Í', 'I').replace('Ó', 'O').replace('Ú', 'U')
            return "".join(t.split())

        super_headers = []
        filas_a_escanear = min(10, len(df))

        for col_idx in range(len(df.columns)):
            col_parts = [str(df.columns[col_idx])]
            for row_idx in range(filas_a_escanear):
                val = df.iloc[row_idx, col_idx]
                if not pd.isna(val):
                    col_parts.append(str(val))

            super_headers.append(clean_text(" ".join(col_parts)))

        logger.info(
            f"🔎 Radiografía detectada (ejemplo de la col 2): {super_headers[2] if len(super_headers) > 2 else 'N/A'}")

        idx_atm_codigo = next((i for i, h in enumerate(super_headers) if "CODIGOMAQUINA" in h), None)
        idx_rec_codigo = next((i for i, h in enumerate(super_headers) if "CODINT" in h), None)

        if idx_atm_codigo is not None:
            self.tipo_archivo = 'ATM'
            self.col_codigo = df.columns[idx_atm_codigo]

            idx_fecha = next((i for i, h in enumerate(super_headers) if "FECHASERVICIO" in h or "FECHASOLICITUD" in h), None)
            idx_valor = next((i for i, h in reversed(list(enumerate(super_headers))) if "TOTAL" in h or "MAQUINA" in h), None)

            if idx_valor is None or idx_fecha is None:
                return False, "Estructura ATM inválida: Faltan columnas críticas"

            self.col_fecha = df.columns[idx_fecha]
            self.col_valor_total = df.columns[idx_valor]

            return True, "Estructura válida (ATM)"

        elif idx_rec_codigo is not None:
            self.tipo_archivo = 'RECAMBIO'
            self.col_codigo = df.columns[idx_rec_codigo]

            idx_fecha = next((i for i, h in enumerate(super_headers) if "FECHA" in h and "ENTREGA" in h), None)
            idx_valor = next((i for i, h in enumerate(super_headers) if "VALTOTAL" in h or "VALORTOTAL" in h), None)
            idx_obs = next((i for i, h in enumerate(super_headers) if "TEXTOBREVE" in h or "OBSERVACION" in h), None)
            idx_cant = next((i for i, h in enumerate(super_headers) if "CANTIDAD" in h or "CANTIDA" in h), None)

            if idx_valor is None or idx_cant is None:
                return False, "Estructura Recambio inválida: Faltan columnas críticas"

            self.col_fecha = df.columns[idx_fecha] if idx_fecha is not None else None
            self.col_valor_total = df.columns[idx_valor]
            self.col_observacion = df.columns[idx_obs] if idx_obs is not None else None
            self.col_cantidad = df.columns[idx_cant]
            return True, "Estructura válida (RECAMBIO)"

        return False, "No se reconoció la estructura del archivo (Ni ATM ni Recambio)"

    def mapear_a_dtos(
        self,
        df: pd.DataFrame,
        source_name: str
    ) -> List[Tuple[AetherServiceImportDto, int]]:
        logger.info(f"Procesando archivo Cliente 3 [{self.tipo_archivo}]: {source_name}")

        if self.tipo_archivo == 'ATM':
            return self._mapear_atm(df)
        elif self.tipo_archivo == 'RECAMBIO':
            return self._mapear_recambios(df)

        return []

    def _mapear_atm(self, df: pd.DataFrame) -> List[Tuple[AetherServiceImportDto, int]]:
        dtos = []
        for idx, row in df.iterrows():
            try:
                code = str(row.get(self.col_codigo, '')).strip().replace('.0', '').replace(' ', '')
                upper_code = code.upper()

                if not code or code.upper() == 'NAN' or "CODIGO" in upper_code or "MAQUINA" in upper_code or "TOTAL" in upper_code or "FECHA" in upper_code:
                    logger.debug(f"Saltando fila {idx} por considerarse encabezado residual: {code}")
                    continue

                service_date = self._parsear_fecha(row.get(self.col_fecha))
                now = datetime.now()
                order_number = f"{self.cod_cliente}-{service_date.strftime('%Y%m%d')}-{code}-ATM"
                total_value = self._parse_valor_monetario(row.get(self.col_valor_total, 0))

                dto = AetherServiceImportDto(
                    cod_cliente=int(self.cod_cliente),
                    cod_sucursal=1,
                    fecha_solicitud=str(now.date()),
                    hora_solicitud=now.strftime("%H:%M:%S"),
                    fecha_programacion=str(service_date),
                    hora_programacion="00:00:00",
                    cod_concepto=3,
                    cod_punto_origen=code,
                    cod_punto_destino="",
                    numero_pedido=order_number,
                    cod_os_cliente=order_number,
                    observaciones="",
                    valor_billete=0,
                    valor_moneda=0,
                    valor_servicio=total_value,
                    numero_kits_cambio=0,
                    cef_numero_planilla=0,
                    valor_total_declarado=total_value,
                    cef_divisa="COP",
                    cef_tipo_transaccion="PA",
                    cef_estado_transaccion="Programado"
                )
                dtos.append((dto, idx))
            except Exception as e:
                logger.error(f"Error procesando fila {idx} (ATM): {e}")
        return dtos

    def _mapear_recambios(self, df: pd.DataFrame) -> List[Tuple[AetherServiceImportDto, int]]:
        dtos = []
        for idx, row in df.iterrows():
            try:
                code = str(row.get(self.col_codigo, '')).strip().replace('.0', '').replace(' ', '')
                if not code or code.upper() == 'NAN':
                    continue

                raw_date = row.get(self.col_fecha)
                if isinstance(raw_date, str) and any(c.isalpha() for c in raw_date):
                    raw_date = row.get(f"{self.col_fecha}.1", raw_date)

                service_date = self._parsear_fecha(raw_date)
                now = datetime.now()

                total_value = self._parse_valor_monetario(row.get(self.col_valor_total, 0))
                quantity = self._parse_entero(row.get(self.col_cantidad, 1))

                obs = str(row.get(self.col_observacion, '')).strip().replace('\n', ' ').replace('\r', ' ')
                order_number = f"{self.cod_cliente}-{service_date.strftime('%Y%m%d')}-{code}-REC"

                combo_lines = [{
                    "combo_code": "COMBO_A",
                    "quantity": quantity if quantity > 0 else 1
                }]

                dto = AetherServiceImportDto(
                    cod_cliente=int(self.cod_cliente),
                    cod_sucursal=1,
                    fecha_solicitud=str(now.date()),
                    hora_solicitud=now.strftime("%H:%M:%S"),
                    fecha_programacion=str(service_date),
                    hora_programacion="00:00:00",
                    cod_concepto=2,
                    cod_punto_origen=code,
                    cod_punto_destino="",
                    numero_pedido=order_number,
                    cod_os_cliente=order_number,
                    observaciones=obs,
                    valor_billete=0,
                    valor_moneda=0,
                    valor_servicio=total_value,
                    numero_kits_cambio=quantity,
                    cef_numero_planilla=0,
                    valor_total_declarado=total_value,
                    cef_divisa="COP",
                    cef_tipo_transaccion="SC",
                    cef_estado_transaccion="Programado",
                    combo_lines=combo_lines
                )
                dtos.append((dto, idx))
            except Exception as e:
                logger.error(f"Error procesando fila {idx} (Recambio): {e}")
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
            if len(s) - s.rfind(',') - 1  <= 2:
                s = s.replace(',', '.')
            else:
                s = s.replace(',', '')

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
        if pd.isna(val) or not str(val).strip():
            return date.today()

        if isinstance(val, (datetime, pd.Timestamp)):
            return val.date()

        val_str = str(val).split()[0].strip()

        formats = [
            '%Y%m%d',  # Formato específico detectado en cliente: 20260523
            '%d/%m/%Y',  # 23/05/2026
            '%d-%m-%Y',  # 23-05-2026
            '%Y-%m-%d',  # 2026-05-23
            '%Y/%m/%d'  # 2026/05/23
        ]

        for fmt in formats:
            try:
                return datetime.strptime(val_str, fmt).date()
            except ValueError:
                continue

        logger.warning(f"No se pudo entender el formato de fecha '{val_str}'. Usando la de hoy.")
        return date.today()