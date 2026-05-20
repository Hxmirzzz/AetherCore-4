from __future__ import annotations
from src.application.interfaces.i_excel_mapper import BaseExcelMapper
from src.application.processors.excel.mapper.standard_mapper import StandardExcelMapper
from src.application.processors.excel.mapper.cash4u_mapper import Cash4uExcelMapper
from src.application.processors.excel.mapper.emergency_mapper import EmergencyMapper
from src.application.processors.excel.mapper.client2_mapper import Client2Mapper

class ExcelProcessorFactory:
    """
    Factory simplificada: Siempre entrega el StandardExcelMapper.
    """
    @staticmethod
    def get_mapper(cod_cliente: int | str) -> BaseExcelMapper:
        cod = str(cod_cliente)

        if cod in ['2']:
            return Client2Mapper(cod)

        if cod in ['4']:
            return EmergencyMapper(cod)

        if cod in ['58']:
            return Cash4uExcelMapper(cod)

        return StandardExcelMapper(cod)