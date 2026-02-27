from dataclasses import dataclass
from datetime import date, time
from typing import Optional
from decimal import Decimal

@dataclass
class AetherServiceImportDto:
    """
    Data Transfer Object para datos de servicio.
    
    Representa los datos necesarios para insertar un servicio mediante
    el stored procedure AddServiceTransaction.
    """
    # --- Datos del Servicio (CgsServicios) ---
    cod_cliente: int
    cod_sucursal: int
    fecha_solicitud: str
    hora_solicitud: str
    cod_concepto: int
    cod_punto_origen: str
    cod_punto_destino: str
    
    numero_pedido: Optional[str] = None
    cod_os_cliente: Optional[str] = None
    tipo_traslado: str = 'P'
    indicador_tipo_origen: str = 'P'
    indicador_tipo_destino: str = 'P'
    modalidad_servicio: Optional[str] = None
    observaciones: str = ""
    
    valor_billete: Decimal = Decimal('0')
    valor_moneda: Decimal = Decimal('0')
    valor_servicio: Decimal = Decimal('0')
    numero_kits_cambio: int = 0
    numero_bolsas_moneda: int = 0

    # --- Datos de Transacción (CefTransacciones) ---
    cef_cod_ruta: Optional[str] = None
    cef_numero_planilla: int = 0
    cef_divisa: str = "COP"
    cef_tipo_transaccion: str = "Checkin"
    
    cantidad_bolsas_declaradas: int = 0
    cantidad_sobres_declarados: int = 0
    cantidad_cheques_declarados: int = 0
    cantidad_documentos_declarados: int = 0
    
    valor_billetes_declarado: Decimal = Decimal('0')
    valor_monedas_declarado: Decimal = Decimal('0')
    valor_total_declarado: Decimal = Decimal('0')
    valor_total_declarado_letras: Optional[str] = None
    
    cef_es_custodia: bool = False
    cef_es_punto_a_punto: bool = False
    
    def to_dict(self):
        """Convierte el DTO a un diccionario listo para enviar como JSON"""
        return {
            "NumeroPedido": self.numero_pedido,
            "CodCliente": self.cod_cliente,
            "CodOsCliente": self.cod_os_cliente,
            "CodSucursal": self.cod_sucursal,
            "FechaSolicitud": self.fecha_solicitud,
            "HoraSolicitud": self.hora_solicitud,
            "CodConcepto": self.cod_concepto,
            "TipoTraslado": self.tipo_traslado,
            "CodPuntoOrigen": self.cod_punto_origen,
            "IndicadorTipoOrigen": self.indicador_tipo_origen,
            "CodPuntoDestino": self.cod_punto_destino,
            "IndicadorTipoDestino": self.indicador_tipo_destino,
            "ModalidadServicio": self.modalidad_servicio,
            "Observaciones": self.observaciones,
            "ValorBillete": float(self.valor_billete),
            "ValorMoneda": float(self.valor_moneda),
            "ValorServicio": float(self.valor_servicio),
            "NumeroKitsCambio": self.numero_kits_cambio,
            "NumeroBolsasMoneda": self.numero_bolsas_moneda,
            "CefCodRuta": self.cef_cod_ruta,
            "CefNumeroPlanilla": self.cef_numero_planilla,
            "CefDivisa": self.cef_divisa,
            "CefTipoTransaccion": self.cef_tipo_transaccion,
            "CantidadBolsasDeclaradas": self.cantidad_bolsas_declaradas,
            "CantidadSobresDeclarados": self.cantidad_sobres_declarados,
            "CantidadChequesDeclarados": self.cantidad_cheques_declarados,
            "CantidadDocumentosDeclarados": self.cantidad_documentos_declarados,
            "ValorBilletesDeclarado": float(self.valor_billetes_declarado),
            "ValorMonedasDeclarado": float(self.valor_monedas_declarado),
            "ValorTotalDeclarado": float(self.valor_total_declarado),
            "ValorTotalDeclaradoLetras": self.valor_total_declarado_letras,
            "CefEsCustodia": self.cef_es_custodia,
            "CefEsPuntoAPunto": self.cef_es_punto_a_punto
        }