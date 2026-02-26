"""
Value Object para representar una carpeta de cliente.

Formato: {cod_cliente}_{nombre_cliente}
Ejemplo: 45_BANCO_EJEMPLO
"""
from __future__ import annotations
from dataclasses import dataclass
from multiprocessing import Value
from os import name
from pathlib import Path
from typing import Optional

@dataclass(frozen=True)
class ClienteFolder:
    """
    Value Object que representa una carpeta de cliente.
    
    Attributes:
        cod_cliente: Código del cliente (ej: "45")
        nombre_cliente: Nombre del cliente (ej: "BANCO_EJEMPLO")
    """
    cod_cliente: str
    nombre_cliente: str

    def __post_init__(self):
        """Validaciones de integridad"""
        if not self.cod_cliente or not self.cod_cliente.strip():
            raise ValueError("cod_cliente no puede estar vacio")

        if not self.nombre_cliente or not self.nombre_cliente.strip():
            raise ValueError("nombre_cliente no puede estar vacio")

        invalid_chars = ['/', '\\', ':', '*', '?', '"', '<', '>', '|']
        if any(char in self.nombre_cliente for char in invalid_chars):
            raise ValueError(
                f"nombre_cliente contiene caracteres inválidos: {invalid_chars}"
            )

    @classmethod
    def from_folder_name(cls, folder_name: str) -> ClienteFolder:
        """
        Crea un ClienteFolder desde el nombre de carpeta.
        
        Args:
            folder_name: Nombre en formato "45_BANCO_EJEMPLO"
            
        Returns:
            ClienteFolder
            
        Raises:
            ValueError: Si el formato es inválido
        """
        if '_' not in folder_name:
            raise ValueError(
                f"Formato inválido: '{folder_name}."
                f"Esperado: 'CODIGO_NOMBRE'"
            )
        
        parts = folder_name.split('_', 1)
        if len(parts) != 2:
            raise ValueError(f"No se pudo parsear el nombre: '{folder_names}'")

        cod_cliente = parts[0].strip()
        nombre_cliente = parts[1].strip()

        return cls(cod_cliente=cod_cliente, nombre_cliente=nombre_cliente)

    @classmethod
    def from_database(cls, cod_cliente: str, nombre_cliente: str) -> ClienteFolder:
        """
        Crea un ClienteFolder desde datos de base de datos.
        
        Args:
            cod_cliente: Código del cliente (requerido)
            nombre_cliente: Nombre del cliente (requerido, se normalizará)
            
        Returns:
            ClienteFolder con nombre normalizado
            
        Raises:
            ValueError: Si cod_cliente o nombre_cliente están vacíos o contienen caracteres inválidos
        """
        if not nombre_cliente or not nombre_cliente.strip():
            raise ValueError("nombre_cliente no puede estar vacío")
            
        nombre_normalizado = cls._normalizar_nombre(nombre_cliente)
        return cls(cod_cliente=cod_cliente, nombre_cliente=nombre_normalizado)

    
    @staticmethod
    def _normalizar_nombre(nombre: str) -> str:
        """
        Normaliza un nombre para ser usado como carpeta.
        
        - Convierte a mayúsculas
        - Reemplaza espacios por guiones bajos
        - Elimina caracteres especiales
        """
        nombre = nombre.upper()
        nombre = nombre.replace(' ', '_')
        nombre = ''.join(
            char for char in nombre
            if char.isalnum() or char == '_'
        )
        max_length = 50
        if len(nombre) > max_length:
            nombre = nombre[:max_length]

        return nombre

    @property
    def folder_name(self) -> str:
        """
        Retorna el nombre de carpeta completo.
        
        Returns:
            String en formato "45_BANCO_EJEMPLO"
        """
        return f"{self.cod_cliente}_{self.nombre_cliente}"

    def to_path(self, base_dir: Path) -> Path:
        """
        Convierte a Path absoluto.
        
        Args:
            base_dir: Directorio base (ej: data/SOLICITUDES)
            
        Returns:
            Path completo a la carpeta
        """
        return base_dir / self.folder_name

    def gestionados_path(self, base_dir: Path) -> Path:
        """
        Retorna el path a la carpeta GESTIONADOS.
        
        Args:
            base_dir: Directorio base
            
        Returns:
            Path a {base_dir}/{folder_name}/GESTIONADOS
        """
        return self.to_path(base_dir) / "GESTIONADOS"

    def __str__(self) -> str:
        return self.folder_name

    def __eq__(self, other) -> bool:
        if not isinstance(other, ClienteFolder):
            return False
        return self.cod_cliente == other.cod_cliente

    def __hash__(self) -> init:
        return hash(self.cod_cliente)