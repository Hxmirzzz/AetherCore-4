from pathlib import Path
from typing import List, Any
import os

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic.functional_validators import field_validator


class ApiConfig(BaseSettings):
    """
    Configuración para la comunicación con el microservicio C#.
    """
    base_url: str = Field(..., alias='VCASH_API_URL')
    auth_user: str = Field(..., alias='AC4_AUTH_USER')
    auth_password: str = Field(..., alias='AC4_AUTH_PASSWORD')

    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore',
    )

class PathConfig(BaseSettings):
    """
    Configuración de rutas/carpetas de la aplicación.
    """
    base_dir: Path = Field(default=Path("C:/AetherCore"), alias='BASE_DIR')
    logs_dir: Path | None = None

    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore',
    )
    
    def __init__(self, **data: Any):
        super().__init__(**data)
        if self.logs_dir is None:
            logs_path = self.base_dir / 'logs'
            logs_path.mkdir(exist_ok=True)
            object.__setattr__(self, 'logs_dir', logs_path)

    @property
    def log_file(self) -> Path:
        return self.logs_dir / 'AetherCore4_API.log'


class MonitoringConfig(BaseSettings):
    """
    Configuración de monitoreo y temporización.
    """
    tiempo_espera_segundos: int = Field(default=10, alias='TIEMPO_ESPERA_MONITOREO_GENERAL')

    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore',
    )

class AppConfig(BaseSettings):
    """
    Configuración general de la aplicación (Pydantic v2).
    """
    environment: str = Field(default='DEV', alias='APP_ENV')

    api: ApiConfig = Field(default_factory=ApiConfig)
    paths: PathConfig = Field(default_factory=PathConfig)
    monitoring: MonitoringConfig = Field(default_factory=MonitoringConfig)

    clientes_permitidos: List[str] = Field(default=['4', '45', '46', '47', '48'])

    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore',
    )

# Singleton
_config_instance: AppConfig | None = None

def get_config() -> AppConfig:
    global _config_instance
    if _config_instance is None:
        _config_instance = AppConfig()
    return _config_instance