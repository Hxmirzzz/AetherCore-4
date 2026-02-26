from __future__ import annotations
from src.application.interfaces.i_excel_mapper import BaseExcelMapper
from src.application.processors.excel.mapper.standard_mapper import StandardExcelMapper
from src.application.processors.excel.mapper.cash4u_mapper import Cash4uExcelMapper
from src.application.processors.excel.mapper.emergency_mapper import EmergencyMapper

class ExcelProcessorFactory:
    """
    Factory simplificada: Siempre entrega el StandardExcelMapper.
    Se mantiene el patrón Factory por si en el futuro lejano 
    necesitas una excepción, pero por ahora es directo.
    """
    @staticmethod
    def get_mapper(cod_cliente: int | str) -> BaseExcelMapper:
        cod = str(cod_cliente)

        if cod in ['4']:
            return EmergencyMapper(cod)

        if cod in ['58']:
            return Cash4uExcelMapper(cod)

        return StandardExcelMapper(cod)