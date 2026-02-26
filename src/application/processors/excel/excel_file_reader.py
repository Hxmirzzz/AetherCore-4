"""
Lector de archivos Excel para procesamiento de solicitudes.
"""
from __future__ import annotations
import keyword
from pathlib import Path
from typing import Dict, Any, Optional
import pandas as pd
import logging
import warnings

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

logger = logging.getLogger(__name__)

class ExcelFileReader:
    """
    Lector robusto de archivos Excel.
    """

    KEYWORDS_HEADER = [
        'FECHA_SOLICITUD', 
        'FECHA_SERVICIO', 
        'CODIGO', 
        'NUMERO_PEDIDO', 
        'PARAMETRO', 
        'GAVETA_1',
        #'100000',

        # Cash4U
        'NRO SERVICIO',
        'CODIGO PUNTO',
        'CÓDIGO PUNTO',
        'VALOR RECOLECCION',

        # Kits D1
        'ID BCT',
        'COD. UNICO',
        'NUMERO KITS',
        'NÚMERO KITS'
    ]
    
    def __init__(self):
        """Inicializa el lector"""
        self._encodings = ['utf-8', 'latin-1', 'iso-8859-1']

    def read(
        self,
        ruta_excel: Path,
        hoja: Optional[str | int] = None
    ) -> Dict[str, Any]:
        """
        Lee un archivo Excel y retorna información procesada.
        
        Args:
            ruta_excel: Path al archivo Excel
            hoja: Nombre o índice de la hoja (None = primera hoja)
            
        Returns:
            Diccionario con:
            {
                'empty': bool,           # Si está vacío
                'df': pd.DataFrame,      # DataFrame con datos
                'sheet_name': str,       # Nombre de la hoja leída
                'file_name': str,        # Nombre del archivo
                'total_rows': int,       # Total de filas con datos
                'total_cols': int,       # Total de columnas
                'error': str | None      # Mensaje de error (si aplica)
            }
            
        Raises:
            FileNotFoundError: Si el archivo no existe
        """
        if not ruta_excel.exists():
            raise FileNotFoundError(f"Archivo Excel no existe: {ruta_excel}")
        
        if ruta_excel.stat().st_size == 0:
            logger.warning(f"Archivo Excel vacío: {ruta_excel.name}")
            return self._build_empty_result(ruta_excel)
        
        if ruta_excel.suffix.lower() not in ['.xlsx', '.xls', '.xlsm']:
            error_msg = f"Extensión no soportada: {ruta_excel.suffix}"
            logger.error(error_msg)
            return self._build_error_result(ruta_excel, error_msg)

        try:
            logger.info(f"Leyendo archivo Excel: {ruta_excel.name}")

            extension = ruta_excel.suffix.lower()
            engine = 'openpyxl' if extension == '.xlsx' else 'xlrd'

            if hoja is not None:
                df = pd.read_excel(
                    ruta_excel,
                    sheet_name=hoja,
                    engine=engine,
                    dtype=str,
                    na_filter=False
                )
                sheet_name = str(hoja)
            else:
                with pd.ExcelFile(ruta_excel, engine=engine) as excel_file:
                    first_sheet = excel_file.sheet_names[0]
                    df = pd.read_excel(
                        excel_file,
                        sheet_name=first_sheet,
                        dtype=str,
                        na_filter=False
                    )
                    sheet_name = first_sheet

            df = self._encontrar_y_ajustar_header(df)
            df.columns = [self._limpiar_header(col) for col in df.columns]
            df = df.dropna(how='all')

            if df.empty:
                logger.warning(f"Excel sin datos después de limpieza: {ruta_excel.name}")
                return self._build_empty_result(ruta_excel, sheet_name)
            
            logger.info(
                f"Excel leído exitosamente: {len(df)} filas, "
                f"{len(df.columns)} columnas (hoja: {sheet_name})"
            )

            return {
                'empty': False,
                'df': df,
                'sheet_name': sheet_name,
                'file_name': ruta_excel.name,
                'total_rows': len(df),
                'total_cols': len(df.columns),
                'error': None
            }
        
        except pd.errors.EmptyDataError:
            logger.warning(f"Archivo Excel vacío o sin datos: {ruta_excel.name}")
            return self._build_empty_result(ruta_excel)
        
        except Exception as e:
            error_msg = f"Error leyendo Excel: {e}"
            logger.error(error_msg, exc_info=True)
            return self._build_error_result(ruta_excel, error_msg)

    def list_sheets(self, ruta_excel: Path) -> list[str]:
        """
        Lista todas las hojas disponibles en el Excel.
        
        Args:
            ruta_excel: Path al archivo Excel
            
        Returns:
            Lista de nombres de hojas
        """
        try:
            engine = 'openpyxl' if ruta_excel.suffix == '.xlsx' else 'xlrd'
            excel_file = pd.ExcelFile(ruta_excel, engine=engine)
            return excel_file.sheet_names
        except Exception as e:
            logger.error(f"Error listando hojas de {ruta_excel.name}: {e}")
            return []

    def read_multiple_sheets(
        self,
        ruta_excel: Path,
        hojas: Optional[list[str | int]] = None
    ) -> Dict[str, pd.DataFrame]:
        """
        Lee múltiples hojas de un Excel.
        
        Args:
            ruta_excel: Path al archivo Excel
            hojas: Lista de hojas a leer (None = todas)
            
        Returns:
            Diccionario {nombre_hoja: DataFrame}
        """
        try:
            engine = 'openpyxl' if ruta_excel.suffix == '.xlsx' else 'xlrd'
            processed_sheets = {}

            with pd.ExcelFile(ruta_excel, engine=engine) as xls:
                hojas_a_leer = hojas if hojas is not None else xls.sheet_names

                for sheet_name in hojas_a_leer:
                    sheet_name_str = str(sheet_name)

                    try:
                        df = pd.read_excel(
                            xls,
                            sheet_name=sheet_name,
                            dtype=str,
                            header=None
                        )

                        df = self._encontrar_y_ajustar_header(df)
                        
                        if not df.empty:
                            df.columns = [self._limpiar_header(col) for col in df.columns]
                            df = df.dropna(how='all')
                            processed_sheets[sheet_name_str] = df
                    
                    except Exception as ex:
                        logger.error(f"Error leyendo hoja {sheet_name_str}: {ex}")

                return processed_sheets

        except Exception as e:
            logger.error(f"Error leyendo múltiples hojas: {e}", exc_info=True)
            return {}

    def _encontrar_y_ajustar_header(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Busca en las primeras filas si encuentra las palabras clave de los títulos.
        Si las encuentra, promueve esa fila a encabezado y descarta las anteriores (logos, títulos, vacíos).
        """
        if df.empty: return df
        total_filas = len(df)
        limit = min(25, total_filas)
        metadata_content = {}
        
        for i in range(limit):
            row_values_raw = df.iloc[i].values
            row_values_upper = [str(x).upper().strip() for x in row_values_raw]

            for idx_col, val in enumerate(row_values_raw):
                val_str = str(val).strip()
                if ':' in val_str and len(val_str) < 50:
                    key = val_str.replace(':', '').strip().upper()

                    if idx_col + 1 < len(row_values_raw):
                        next_val = str(row_values_raw[idx_col + 1]).strip()
                        if next_val and next_val.lower() != 'nan':
                            metadata_content[key] = next_val

            match = any(keyword in row_values_upper for keyword in self.KEYWORDS_HEADER)
            if match:
                if i > 0:
                    logger.info(f"🔎 Encabezados detectados en fila {i+1}. Ajustando tabla...")
                    nuevos_headers = df.iloc[i]
                    header_rows = df.iloc[:i].copy()
                    df_nuevo = df.iloc[i+1:].reset_index(drop=True)
                    df_nuevo.columns = nuevos_headers
                    df_nuevo.attrs['metadata'] = metadata_content
                    df_nuevo.attrs['header_rows'] = header_rows

                    return df_nuevo
                else:
                    df.columns = df.iloc[i]
                    df_final = df.iloc[i+1:].reset_index(drop=True)
                    df_final.attrs['metadata'] = metadata_content
                    df_final.attrs['header_rows'] = pd.DataFrame()
                    return df_final

        return df

    def _limpiar_header(self, header: str) -> str:
        """
        Limpia y normaliza un header de columna.
        
        Args:
            header: Header original
            
        Returns:
            Header limpio y normalizado
        """
        if not isinstance(header, str):
            header = str(header)
        return ' '.join(header.strip().upper().split())

    def _build_empty_result(
        self,
        ruta_excel: Path,
        sheet_name: str = "Unknown"
    ) -> Dict[str, Any]:
        """Construye resultado para tabla vacía"""
        return {
            'empty': True,
            'df': pd.DataFrame(),
            'sheet_name': sheet_name,
            'file_name': ruta_excel.name,
            'total_rows': 0,
            'total_cols': 0,
            'error': None
        }

    def _build_error_result(
        self,
        ruta_excel: Path,
        error_msg: str
    ) -> Dict[str, Any]:
        """Construye resultado para error"""
        return {
            'empty': True,
            'df': pd.DataFrame(),
            'sheet_name': "Error",
            'file_name': ruta_excel.name,
            'total_rows': 0,
            'total_cols': 0,
            'error': error_msg
        }