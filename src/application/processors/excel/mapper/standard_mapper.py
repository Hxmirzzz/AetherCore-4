from __future__ import annotations
from typing import List, Tuple, Dict, Any
from datetime import datetime, date, time
from decimal import Decimal
import pandas as pd
import logging
import re

from src.application.interfaces.i_excel_mapper import BaseExcelMapper
from src.application.dto.servicio_dto import ServicioDTO
from src.application.dto.transaccion_dto import TransaccionDTO

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

    DEFAULT_USER_ID = "e5926e18-33b1-468c-a979-e4e839a86f30"

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

    def mapear_a_dtos(self, df: pd.DataFrame, nombre_archivo: str) -> List[Tuple[ServicioDTO, TransaccionDTO, int]]:
        """
        Detecta el tipo de formato (ATM vs Oficina) fila a fila o por estructura
        y convierte a DTOs.
        """
        dtos = []
        errores = []
        
        cols = df.columns
        es_formato_atm = 'GAVETA_1' in cols
        es_formato_kits = any('KIT' in col for col in cols)

        if es_formato_kits:
            tipo_log = "Kits (Paquetes)"
        elif es_formato_atm:
            tipo_log = "ATM (Gavetas)"
        else:
            tipo_log = "Oficina (Denominaciones)"

        logger.info(f"Procesando archivo {nombre_archivo} ({tipo_log})")

        for idx, row in df.iterrows():
            raw_codigo = str(row.get('CODIGO') or row.get('COD. UNICO', '')).strip().lower()
            if not raw_codigo or raw_codigo == "nan" or raw_codigo == "none": continue
            try:
                if es_formato_kits:
                    resultado = self._procesar_fila_kits(row, nombre_archivo, idx)
                elif es_formato_atm:
                    resultado = self._procesar_fila_atm(row, nombre_archivo, idx)
                else:
                    resultado = self._procesar_fila_oficina(row, nombre_archivo, idx)
                
                dtos.append((resultado[0], resultado[1], idx))

            except Exception as e:
                error_msg = f"Error al procesar fila {idx}: {str(e)}"
                errores.append((idx, error_msg))
                logger.error(error_msg)

        if errores:
            logger.warning(f"Se encontraron {len(errores)} errores en el archivo {nombre_archivo}")

        return dtos

    def _procesar_fila_kits(self, row: pd.Series, nombre_archivo: str, idx: int) -> Tuple[ServicioDTO, TransaccionDTO]:
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
    def _procesar_fila_atm(self, row: pd.Series, nombre_archivo: str, idx: int) -> Tuple[ServicioDTO, TransaccionDTO]:
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
    def _procesar_fila_oficina(self, row: pd.Series, archivo: str, idx: int) -> Tuple[ServicioDTO, TransaccionDTO]:
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
    ) -> Tuple[ServicioDTO, TransaccionDTO]:
        valor_calculado = valor_billete + valor_moneda
        valor_declarado = self._parse_valor_monetario(row['VALOR_TOTAL'])

        if valor_calculado > 0:
            valor_final = valor_calculado
        else:
            valor_final = valor_declarado
            if valor_declarado > 0 and valor_billete == 0 and valor_moneda == 0:
                valor_billete = valor_declarado

        try:
            f_raw = row.get('FECHA_SERVICIO') or row.get('FECHA')
            fecha_servicio = self._parse_fecha(f_raw)
        except:
            fecha_servicio = date.today()

        try:
            fecha_solicitud = self._parse_fecha(row.get('FECHA_SOLICITUD'))
        except:
            fecha_solicitud = date.today()

        cod_os_cliente_raw = str(row.get('NUMERO_PEDIDO', '')).upper().strip()
        cod_os_cliente = None
        if cod_os_cliente_raw and cod_os_cliente_raw.lower() != 'nan' and cod_os_cliente_raw.lower() != 'none':
            cod_os_cliente = cod_os_cliente_raw.split('.')[0]

        modalidad_raw = str(row.get('MODALIDAD', '')).upper().strip()

        if forzar_provision:
            es_recoleccion = False
            cod_concepto = 2
            tipo_transaccion = 'PV'
        else:
            if "RECOLECCION" in modalidad_raw or "RECOLECCIÓN" in modalidad_raw:
                es_recoleccion = True
                cod_concepto = 1
                tipo_transaccion = 'RC'
            elif "PROVISION" in modalidad_raw or "PROVISIÓN" in modalidad_raw:
                es_recoleccion = False
                cod_concepto = 3 if es_atm else 2
                tipo_transaccion = 'PV'
            else:
                es_recoleccion = False
                cod_concepto = 2
                tipo_transaccion = 'PV'

        obs = str(row.get('OBSERVACION', '')).strip()
        if detalle_tecnico:
            obs = f"{obs} || Detalle: {detalle_tecnico}".strip()
            obs = obs[:500]

        codigo_punto = str(row['CODIGO']).strip()
        
        if es_recoleccion:
            origen = codigo_punto
            tipo_origen = 'P'
            destino = "FONDO"
            tipo_destino = 'F'
        else:
            origen = "FONDO"
            tipo_origen = 'F'
            destino = codigo_punto
            tipo_destino = 'P'

        ahora = datetime.now()
        timestamp = ahora.strftime('%H%M%S%f')
        sufijo = f"{idx:03d}"
        numero_pedido_sistema = f"{self.cod_cliente}{ahora.strftime('%Y%m%d')}{timestamp}{sufijo}"

        servicio = ServicioDTO(
            numero_pedido=numero_pedido_sistema,
            cod_cliente=int(self.cod_cliente),
            cod_sucursal=1,
            cod_concepto=cod_concepto,
            tipo_traslado='N',
            fecha_solicitud=fecha_solicitud,
            hora_solicitud=datetime.now().time(),
            fecha_programacion=fecha_servicio,
            hora_programacion=time(8, 0),
            cod_estado=0,

            cod_cliente_origen=int(self.cod_cliente),
            cod_punto_origen=origen,
            indicador_tipo_origen=tipo_origen,

            cod_cliente_destino=int(self.cod_cliente),
            cod_punto_destino=destino,
            indicador_tipo_destino=tipo_destino,
            
            fallido=False,
            valor_billete=valor_billete,
            valor_moneda=valor_moneda,
            valor_servicio=valor_final,
            cod_os_cliente=cod_os_cliente,
            numero_kits_cambio=cant_kits,
            modalidad_servicio='2',
            observaciones=obs,
            archivo_detalle=archivo
        )

        transaccion = TransaccionDTO(
            cod_sucursal=1,
            fecha_registro=datetime.now(),
            usuario_registro_id=self.DEFAULT_USER_ID,
            tipo_transaccion=tipo_transaccion,
            divisa="COP",
            valor_billetes_declarado=servicio.valor_billete,
            valor_monedas_declarado=servicio.valor_moneda,
            valor_total_declarado= servicio.valor_servicio,
            estado_transaccion='RegistroTesoreria'
        )

        return (servicio, transaccion)

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
        