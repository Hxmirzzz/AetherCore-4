from __future__ import annotations
from typing import List, Tuple, Dict, Any
from datetime import datetime, date, time
from decimal import Decimal
import pandas as pd
import logging
import re

from src.application.interfaces.i_excel_mapper import BaseExcelMapper
from src.application.dto.servicio_dto import AetherServiceImportDto

logger = logging.getLogger(__name__)

class StandardExcelMapper(BaseExcelMapper):
    """
    Mapper UNIVERSAL para el formato estándar.
    """
    UMBRAL_BILLETE = 2000

    COLUMNAS_BASE = [
        'FECHA_SOLICITUD',
        'FECHA_SERVICIO',
        'CODIGO',
        'MODALIDAD',
        'VALOR_TOTAL'
    ]

    COLUMNAS_OPCIONALES = {
        'NUMERO_PEDIDO': None,
        'OBSERVACION': None
    }

    DEFAULT_USER_ID = "dda6d2ea-0a02-4f77-a063-04d940572a1a"

    def __init__(self, cod_cliente: str):
        self._cod_cliente = cod_cliente
        self._nombre_cliente = ""
        self.config_kits: Dict[str, Any] = {}
    
    @property
    def cod_cliente(self) -> str:
        return self._cod_cliente

    @property
    def nombre_cliente(self) -> str:
        return self._nombre_cliente

    def actualizar_parametros(self, df_params: pd.DataFrame) -> None:
        """Actualiza los parámetros del mapper con los valores del DataFrame de parámetros."""
        try:
            if len(df_params.columns) < 2: return
            df_params.columns = [str(c).upper().strip() for c in df_params.columns]
            tiene_tipo = 'TIPO' in df_params.columns

            for _, row in df_params.iterrows():
                clave = str(row.iloc[0]).upper().strip()
                valor = row.iloc[1]

                match = re.search(r'KIT_(\d+)', clave)
                if match:
                    num_kit = match.group(1)
                    valor = self._parse_valor_monetario(valor)
                    tipo = "MIXTO"
                    if tiene_tipo:
                        val_tipo = row['TIPO']
                        if pd.notna(val_tipo):
                            tipo_raw = str(val_tipo).upper().strip()
                            if tipo_raw in ["MIXTO", "MONEDA", "BILLETE"]:
                                tipo = tipo_raw

                    self.config_kits[num_kit] = {
                        "VALOR": valor,
                        "TIPO": tipo
                    }
                    logger.info(f"🔧 Configurado KIT_{num_kit}: ${valor:,.0f} ({tipo})")
            
        except Exception as e:
            logger.error(f"Error al actualizar parámetros: {e}")

    def validar_estructura(self, df: pd.DataFrame) -> tuple[bool, str]:
        """
        Valida que el DataFrame tenga las columnas obligatorias y las columnas opcionales.
        """
        valido, error = self._validar_no_vacio(df)
        if not valido: return (False, error)

        df.columns = [str(col).upper().strip() for col in df.columns]

        faltantes = [col for col in self.COLUMNAS_BASE if col not in df.columns]
        if faltantes:
            if 'VALOR_TOTAL' in faltantes and any('KIT' in c for c in df.columns):
                pass
            else:
                return (False, f"Faltan columnas obligatorias: {', '.join(faltantes)}")

        return (True, "")

    def obtener_resumen(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Genera estadísticas rápidas del archivo."""
        try:
            total_valor = 0
            if 'VALOR_TOTAL' in df.columns:
                total_valor = sum(self._parse_valor_monetario(v) for v in df['VALOR_TOTAL'])

            return {
                'total_servicios': len(df),
                'puntos_unicos': df['CODIGO'].nunique(),
                'total_valor': Decimal(str(total_valor)),
                'archivo_valido': True,
            }
        except Exception as e:
            return {'error': str(e), 'archivo_valido': False}

    def mapear_a_dtos(self, df: pd.DataFrame, nombre_archivo: str) -> List[Tuple[AetherServiceImportDto, int]]:
        """
        Detecta el tipo de formato (ATM vs Oficina) fila a fila o por estructura
        y convierte a DTOs.
        """
        dtos = []
        cols = df.columns
        es_formato_atm = 'GAVETA_1' in cols
        es_formato_kits = any('KIT' in col for col in cols)

        for idx, row in df.iterrows():
            raw_codigo = str(row.get('CODIGO') or row.get('COD. UNICO', '')).strip().lower()
            if not raw_codigo or raw_codigo == "nan": continue

            try:
                if es_formato_kits:
                    dto = self._procesar_fila_kits(row, nombre_archivo, idx)
                elif es_formato_atm:
                    dto = self._procesar_fila_atm(row, nombre_archivo, idx)
                else:
                    dto = self._procesar_fila_oficina(row, nombre_archivo, idx)
                
                dtos.append((dto, idx))

            except Exception as e:
                logger.error(f"Error al procesar fila {idx}: {str(e)}")

        return dtos

    def _procesar_fila_kits(self, row: pd.Series, nombre_archivo: str, idx: int) -> AetherServiceImportDto:
        """
        Procesa una fila del formato de kits (paquetes)
        """
        valor_moneda_total = Decimal(0)
        valor_billete_total = Decimal(0)
        total_kits_count = 0
        detalle_kits = []

        for i in range(1, 21):
            col_encontrada = next((c for c in row.index if f"KIT_{i}" in str(c).upper() and "CANT" in str(c).upper()), None)
            
            if col_encontrada:
                cantidad = self._parse_entero(row[col_encontrada])
                if cantidad > 0:
                    config = self.config_kits.get(str(i))
                    if config:
                        valor_unitario = config['VALOR']
                        tipo = config['TIPO']

                        subtotal = Decimal(cantidad) * valor_unitario
                        total_kits_count += cantidad

                        if tipo == "MONEDA":
                            valor_moneda_total += subtotal
                        elif tipo == "BILLETE":
                            valor_billete_total += subtotal
                        else:
                            valor_billete_total += subtotal

                        detalle_kits.append(f"K{i}({tipo}):{cantidad}")
                    else:
                        logger.warning(f"Fila {idx}: Kit {i} tiene cantidad {cantidad} pero no está definido en PARAMETROS.")

        detalle = " | ".join(detalle_kits)

        return self._crear_dtos(
            row=row,
            valor_billete=valor_billete_total,
            valor_moneda=valor_moneda_total,
            cant_kits=total_kits_count,
            es_atm=False,
            archivo=nombre_archivo,
            idx=idx,
            detalle_tecnico=detalle,
            forzar_provision=True
        )

    # ATM
    def _procesar_fila_atm(self, row: pd.Series, nombre_archivo: str, idx: int) -> AetherServiceImportDto:
        valor_calculado = Decimal(0)
        detalle_str = []
        
        for i in range(1, 11):
            col_gaveta = f"GAVETA_{i}"
            col_deno = f"DENO_{i}"
            col_tipo = f"TIPO_{i}"
            
            if col_gaveta in row and col_deno in row:
                try:
                    cant = self._parse_entero(row[col_gaveta])
                    deno = self._parse_entero(row[col_deno])
                    tipo = ""
                    
                    if col_tipo in row.index and pd.notna(row[col_tipo]):
                        tipo = str(row[col_tipo]).upper().strip()
                    
                    if cant > 0 and deno > 0:
                        subtotal = Decimal(cant) * Decimal(deno)
                        valor_calculado += subtotal

                        info_extra = f"({tipo})" if tipo else ""
                        detalle_str.append(f"G{i}{info_extra}: {cant}x{deno}")
                except:
                    continue
                
        return self._crear_dtos(
            row=row,
            valor_billete=valor_calculado,
            valor_moneda=Decimal('0'),
            es_atm=True,
            archivo=nombre_archivo,
            idx=idx,
            detalle_tecnico=" | ".join(detalle_str)
        )

    # Oficina
    def _procesar_fila_oficina(self, row: pd.Series, archivo: str, idx: int) -> AetherServiceImportDto:
        valor_billete = Decimal('0')
        valor_moneda = Decimal('0')
        patron_billete = re.compile(r'^(\d+)(NF|AF|NUEVA|ANTIGUA)?$')

        for col_name in row.index:
            col_str = str(col_name).upper().strip()
            
            if col_str in self.COLUMNAS_BASE or col_str in self.COLUMNAS_OPCIONALES:
                continue
            
            match = patron_billete.match(col_str)
            if match:
                try:
                    deno = int(match.group(1))
                    valor = self._parse_valor_monetario(row[col_name])

                    if valor > 0:
                        if deno >= self.UMBRAL_BILLETE:
                            valor_billete += valor
                        else:
                            valor_moneda += valor
                except:
                    continue
        
        return self._crear_dtos(
            row=row,
            valor_billete=valor_billete,
            valor_moneda=valor_moneda,
            es_atm=False,
            archivo=archivo,
            idx=idx,
        )

    def _crear_dtos(
        self, row, valor_billete: Decimal, valor_moneda: Decimal, 
        es_atm: bool, archivo: str, idx: int, detalle_tecnico: str = "",
        forzar_provision: bool = False, cant_kits: int = 0
    ) -> AetherServiceImportDto:

        valor_final = valor_billete + valor_moneda
        if valor_final == 0:
            valor_final = self._parse_valor_monetario(row['VALOR_MONETARIO'])
            valor_billete = valor_final
        
        fecha_serv = self._parse_fecha(row.get('FECHA_SERVICIO') or row.get('FECHA'))
        fecha_sol = self._parse_fecha(row.get('FECHA_SOLICITUD'))

        modalidad_raw = str(row.get('MODALIDAD', '')).upper().strip()

        if forzar_provision:
            cod_con = 2
        else:
            if "RECOLECCION" in modalidad_raw or "RECOLECCIÓN" in modalidad_raw:
                cod_con = 1
            else:
                cod_con = 3 if es_atm else 2

        codigo_punto = str(row['CODIGO']).strip()

        return AetherServiceImportDto(
            cod_cliente=int(self.cod_cliente),
            cod_sucursal=1,
            fecha_solicitud=str(fecha_sol),
            hora_solicitud=datetime.now().strftime("%H:%M:%S"),
            cod_concepto=cod_con,
            cod_punto_origen=codigo_punto,
            cod_punto_destino="",
            numero_pedido=str(row.get('NUMERO_PEDIDO', '')),
            cod_os_cliente=str(row.get('NUMERO_PEDIDO', '')),
            observaciones=f"{row.get('OBSERVACION', '')} || {detalle_tecnico}".strip()[:450],
            valor_billete=valor_billete,
            valor_moneda=valor_moneda,
            valor_servicio=valor_final,
            numero_kits_cambio=cant_kits,

            cef_numero_planilla=0,
            valor_total_declarado=valor_final,
            cef_divisa="COP",
            cef_tipo_transaccion="RC" if cod_con == 1 else "PV"
        )

    def _parse_fecha(self, val):
        """Intenta parsear fecha dd/mm/yyyy o yyyy-mm-dd"""
        if pd.isna(val) or str(val).strip() == '':
            raise ValueError("Fecha inválida")

        val_str = str(val).strip()

        if isinstance(val, (datetime, pd.Timestamp)):
            return val.date()
        
        formatos = ['%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y', '%Y/%m/%d']

        for fmt in formatos:
            try:
                fecha_limpia = val_str.split(' ')[0]
                return datetime.strptime(fecha_limpia, fmt).date()
            except ValueError:
                continue
        
        raise ValueError(f"Fecha inválida: {val_str}")

    def _parse_valor_monetario(self, val) -> Decimal:
        """Intenta parsear valor monetario"""
        if pd.isna(val): return Decimal('0')
        
        s = str(val).strip()
        s = s.replace('$', '').replace(' ', '')
        s = s.replace('.', '').replace(',', '.')

        try:
            return Decimal(s)
        except:
            return Decimal('0')

    def _parse_entero(self, val) -> int:
        """Parsea cantidades enteras de forma segura"""
        if pd.isna(val): return 0

        try:
            val_limpio = str(val).replace(',', '').replace('.', '')
            return int(val_limpio)
        except:
            return 0
        