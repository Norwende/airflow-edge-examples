"""Configuration de l'application, lue depuis les variables d'environnement (12-factor)."""

from typing import Literal

from pydantic import SecretStr, ValidationInfo, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

SERVICE_NAME = "notes-api"

_DEFAULTS_IF_BLANK = {"app_version": "0.0.0", "app_commit": "unknown"}


class Settings(BaseSettings):
    """Paramètres de l'application.

    Chaque champ correspond à une variable d'environnement du même nom en majuscules
    (``DATABASE_URL``, ``APP_ENV``…). Aucun fichier ``.env`` n'est lu : en local on exporte
    les variables, dans Kubernetes elles viennent du Deployment.
    """

    model_config = SettingsConfigDict(case_sensitive=False, extra="ignore", frozen=True)

    # URI PostgreSQL (Secret CNPG ``notes-pg-app``, clé ``uri``), jamais affichée (SecretStr).
    database_url: SecretStr | None = None
    app_env: Literal["dev", "prod", "local"] = "local"
    # Version exposée par /version : c'est le tag de l'image, injecté par le chart Helm.
    app_version: str = "0.0.0"
    app_commit: str = "unknown"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    log_format: Literal["json", "console"] = "json"

    @field_validator("log_level", mode="before")
    @classmethod
    def _upper(cls, value: object) -> object:
        """Tolère ``info``, ``Warning``… (normalisé avant la validation du Literal)."""
        return value.strip().upper() if isinstance(value, str) else value

    @field_validator("app_env", "log_format", mode="before")
    @classmethod
    def _lower(cls, value: object) -> object:
        """Tolère ``DEV``, ``JSON``… (normalisé avant la validation du Literal)."""
        return value.strip().lower() if isinstance(value, str) else value

    @field_validator("app_version", "app_commit", mode="before")
    @classmethod
    def _default_if_blank(cls, value: object, info: ValidationInfo) -> object:
        """Une variable définie mais vide (``APP_COMMIT=``) retombe sur la valeur par défaut."""
        if isinstance(value, str) and not value.strip():
            return _DEFAULTS_IF_BLANK[info.field_name or ""]
        return value
