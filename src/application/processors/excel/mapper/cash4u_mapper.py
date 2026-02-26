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

class Cash4uExcelMapper(BaseExcelMapper):
    """
    Mapper especializado para formato (Cash4U).
    Maneja:
    1. Oficinas Provision (Cantidades por denominación)
    2. Oficinas Recolección (Valor total)
    3. Cajeros (Gavetas dinámicas)
    """

    DEFAULT_USER_ID = "e5926e18-33b1-468c-a979-e4e839a86f30"

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

    def actualizar_parametros(self, df_params: pd.DataFrame) -> None:
        pass

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

    def obtener_resumen(self, df: pd.DataFrame) -> Dict[str, Any]:
        return {
            'total_servicios': len(df),
            'archivo_valido': True
        }

    def mapear_a_dtos(self, df: pd.DataFrame, nombre_archivo: str) -> List[Tuple[ServicioDTO, TransaccionDTO, int]]:
        dtos = []

        logger.info(f"Procesando archivo Cash4U: {nombre_archivo}")

        metadata = getattr(df, 'attrs', {}).get('metadata', {})
        raw_f_solicitud = metadata.get('FECHA SOLICITUD') or metadata.get('FECHA DE SOLICITUD')
        raw_f_servicio = metadata.get('FECHA SERVICIO') or metadata.get('FECHA DE SERVICIO')
        fecha_sol = self._parsear_fecha(raw_f_solicitud) or date.today()
        fecha_serv = self._parsear_fecha(raw_f_servicio) or date.today()

        if raw_f_solicitud: logger.info(f"📅 Fecha Solicitud detectada en cabecera: {fecha_sol}")
        else: logger.warning("⚠️ No se encontró fecha solicitud en cabecera, usando HOY.")

        cols_denominacion = []
        for col, col_name in enumerate(df.columns):
            raw_col = str(col_name).replace('$', '').strip()

            try:
                if raw_col.endswith('.0'):
                    raw_col = raw_col[:-2]
                elif raw_col.endswith('.00'):
                    raw_col = raw_col[:-3]

                clean_col = raw_col.replace('.', '').replace(',', '')

                if clean_col.isdigit():
                    valor = int(clean_col)
                    if valor >= 50:
                        cols_denominacion.append((col, valor))
            except:
                continue

        cols_denominacion.sort(key=lambda x: x[1], reverse=True)
        logger.info(f"Denominaciones detectadas: {[d[1] for d in cols_denominacion]}")

        map_cols = {str(c).upper().strip(): i for i, c in enumerate(df.columns)}
        idx_pedido = map_cols.get(self.col_pedido) if self.col_pedido else -1
        idx_codigo = map_cols.get(self.col_codigo) if self.col_codigo else -1
        idx_calidad = map_cols.get(self.col_calidad) if self.col_calidad else -1
        idx_valor_rec = map_cols.get(self.col_valor_rec) if self.col_valor_rec else -1
        
        for idx, row in df.iterrows():
            try:
                val_pedido = str(row.iloc[idx_pedido]).strip() if idx_pedido != -1 else ''

                if "RESUMEN" in val_pedido.upper() or "DENOMINACION" in val_pedido.upper():
                    logger.info(f"Fin de archivo detectado")
                    break
                
                pedido_clean = val_pedido.replace('_', '').replace('.', '').strip()
                cod_punto = str(row.iloc[idx_codigo]).strip() if idx_codigo != -1 else ''
                punto_clean = cod_punto.replace('_', '').replace('.', '').strip()

                KEYWORDS_BASURA = [
                    'TOTAL', 'RESUMEN', 'NAN', 'NONE', 'OBSERVACIONES',
                    'FIRMA', 'AUTORIZADA', 'BANCO', 'CLIENTE', 'NOMBRE'
                ]

                if not pedido_clean or not punto_clean:
                    continue

                if any(k in val_pedido.upper() for k in KEYWORDS_BASURA):
                    continue
                if any(k in cod_punto.upper() for k in KEYWORDS_BASURA):
                    continue

                if val_pedido.upper() == self.col_pedido:
                    continue
                if cod_punto.upper() == self.col_codigo:
                    continue
                
                if (punto_clean == '0' or
                    cod_punto.upper() in ['CODIGO', 'CÓDIGO', 'CODIGO PUNTO', 'CÓDIGO PUNTO']):
                    continue
                
                calidad = str(row.iloc[idx_calidad]).upper().strip() if idx_calidad != -1 else ""
                es_recoleccion = False
                es_atm = "CAJERO" in calidad or "ATM" in calidad
                es_moneda = "MONEDA" in calidad

                if idx_valor_rec != -1:
                    es_recoleccion = True

                if "RECOLECCION" in calidad:
                    es_recoleccion = True

                valor_billete = Decimal(0)
                valor_moneda = Decimal(0)
                detalle_tecnico = []
                valor_servicio = Decimal(0)

                if es_recoleccion:
                    val_rec = row.iloc[idx_valor_rec] if idx_valor_rec != -1 else 0
                    valor_servicio = self._parse_valor_monetario(val_rec)
                    if valor_servicio > 0:
                        valor_billete = valor_servicio
                else:
                    for col_name, deno_val in cols_denominacion:
                        cantidad = self._parse_entero(row.iloc[col_name])
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
                    
                    valor_servicio = valor_billete + valor_moneda

                punto = punto_clean
                
                if es_recoleccion:
                    origen, t_orig = punto, 'P'
                    destino, t_dest = "FONDO", 'F'
                    cod_con, tipo_trn = 1, 'RC'
                else:
                    origen, t_orig = "FONDO", 'F'
                    destino, t_dest = punto, 'P'
                    cod_con = 3 if es_atm else 2
                    tipo_trn = 'PV'

                now = datetime.now()
                numero_pedido = f"{self.cod_cliente}{now.strftime('%Y%m%d%H%M%S%f')}{idx:03d}"
                obs = f"{calidad} | {'; '.join(detalle_tecnico)}"[:500]

                servicio = ServicioDTO(
                    numero_pedido=numero_pedido,
                    cod_cliente=int(self.cod_cliente),
                    cod_sucursal=1,
                    cod_concepto=cod_con,
                    tipo_traslado='N',
                    fecha_solicitud=fecha_sol,
                    hora_solicitud=datetime.now().time(),
                    fecha_programacion=fecha_serv,
                    hora_programacion=time(8, 0),
                    cod_estado=0,
                    cod_cliente_origen=int(self.cod_cliente),
                    cod_punto_origen=origen,
                    indicador_tipo_origen=t_orig,
                    cod_cliente_destino=int(self.cod_cliente),
                    cod_punto_destino=destino,
                    indicador_tipo_destino=t_dest,
                    fallido=False,
                    valor_billete=valor_billete,
                    valor_moneda=valor_moneda,
                    valor_servicio=valor_servicio,
                    cod_os_cliente=val_pedido,
                    numero_kits_cambio=0,
                    modalidad_servicio='2',
                    observaciones=obs,
                    archivo_detalle=nombre_archivo
                )

                transaccion = TransaccionDTO(
                    cod_sucursal = 1,
                    fecha_registro=now,
                    usuario_registro_id=self.DEFAULT_USER_ID,
                    tipo_transaccion=tipo_trn,
                    divisa='COP',
                    valor_billetes_declarado=valor_billete,
                    valor_monedas_declarado=valor_moneda,
                    valor_total_declarado=valor_servicio,
                    estado_transaccion='RegistroTesoreria'
                )

                dtos.append((servicio, transaccion, idx))

            except Exception as e:
                logger.error(f"Error al procesar fila {idx}: {e}")
        
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