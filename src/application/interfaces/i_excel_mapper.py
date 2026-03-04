from __future__ import annotations
from abc import ABC, abstractmethod
from typing import List,Tuple
import pandas as pd

from src.application.dto.servicio_dto import AetherServiceImportDto

class BaseExcelMapper(ABC):
    """
    Contrato base para el procesamiento de Excel.
    """
    
    @abstractmethod
    def validar_estructura(self, df: pd.DataFrame) -> Tuple[bool, str]:
        """
        Valida que el DataFrame tenga las columnas del Estándar.
        """
        pass
    
    @abstractmethod
    def mapear_a_dtos(
        self,
        df: pd.DataFrame,
        source_name: str
    ) -> List[Tuple[AetherServiceImportDto, int]]:
        """
        Retorna una lista de tuplas (DTO_UNIFICADO, indice_fila)
        """
        pass