"""
Interfaz para mappers de Excel.

Define el contrato que el StandardMapper (y futuros mappers) deben cumplir
para interactuar con el ExcelProcessor.
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Tuple
import pandas as pd

from src.application.dto.servicio_dto import ServicioDTO
from src.application.dto.transaccion_dto import TransaccionDTO

class IExcelMapper(ABC):
    """
    Contrato base para el procesamiento de Excel.
    """
    
    @property
    @abstractmethod
    def cod_cliente(self) -> str:
        """Código del cliente asignado al mapper."""
        pass
    
    @property
    @abstractmethod
    def nombre_cliente(self) -> str:
        """Nombre descriptivo del cliente o del estándar."""
        pass
    
    @abstractmethod
    def validar_estructura(self, df: pd.DataFrame) -> tuple[bool, str]:
        """
        Valida que el DataFrame tenga las columnas del Estándar.
        """
        pass
    
    @abstractmethod
    def mapear_a_dtos(
        self,
        df: pd.DataFrame,
        nombre_archivo: str
    ) -> List[tuple[ServicioDTO, TransaccionDTO]]:
        """
        Convierte las filas del Excel en objetos de negocio (DTOs).
        """
        pass
    
    @abstractmethod
    def obtener_resumen(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Obtiene estadísticas rápidas para logs (total filas, suma valores).
        """
        pass


class BaseExcelMapper(IExcelMapper):
    """
    Clase base con utilidades de limpieza de datos.
    Estas herramientas las usa el StandardExcelMapper internamente.
    """
    
    def _validar_no_vacio(self, df: pd.DataFrame) -> tuple[bool, str]:
        """Valida que el DataFrame no esté vacío"""
        if df.empty:
            return (False, "El archivo Excel está vacío")
        return (True, "")
    
    def _limpiar_codigo_punto(self, codigo_raw: str) -> str:
        """
        Limpia y normaliza un código de punto.
        
        Args:
            codigo_raw: Código como viene del Excel
            
        Returns:
            Código limpio (solo parte numérica)
        """
        codigo = str(codigo_raw).strip().upper()
        
        if '-' in codigo:
            codigo = codigo.split('-')[-1]

        for prefijo in ['SUC-', 'PUNTO-', 'P-', 'OF-']:
            if codigo.startswith(prefijo):
                codigo = codigo.replace(prefijo, '')
        
        return codigo.strip()
    
    def _construir_codigo_punto_completo(self, codigo_punto: str) -> str:
        """
        Construye el código completo combinando cliente + punto.
        
        Args:
            codigo_punto: Parte numérica del punto
            
        Returns:
            Código completo "45-0033"
        """
        return f"{self.cod_cliente}-{codigo_punto}"
    
    def _parse_valor_monetario(self, valor: Any) -> float:
        """
        Convierte strings de dinero colombianos a float.
        Soporta: "$ 1.500.000,00", "1500000", etc.
        """
        if pd.isna(valor):
            return 0.0
        
        valor_str = str(valor).strip()
        if not valor_str:
            return 0.0
        
        valor_str = valor_str.replace('$', '').replace(' ', '')
        valor_str = valor_str.replace('.', '').replace(',', '.')
        
        try:
            return float(valor_str)
        except ValueError:
            return 0.0
    
    @property
    def cod_cliente(self) -> str:
        return "BASE"

    @property
    def nombre_cliente(self) -> str:
        return "BASE_MAPPER"

    def validar_estructura(self, df: pd.DataFrame) -> tuple[bool, str]:
        return (True, "")

    def mapear_a_dtos(self, df: pd.DataFrame, nombre_archivo: str) -> List[Tuple[ServicioDTO, TransaccionDTO]]:
        return []

    def obtener_resumen(self, df: pd.DataFrame) -> Dict[str, Any]:
        return {}