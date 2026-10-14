"""Chargement de la configuration non sensible du projet."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml


@dataclass(frozen=True)
class Settings:
    project_id: str
    dataset_id: str
    table_id: str
    bucket_name: str
    region: str
    prefix: str = ""


def load_settings(
    config_path: str | Path = "config.yaml",
    overrides: Mapping[str, str] | None = None,
) -> Settings:
    """Charge config.yaml puis applique les remplacements définis par variables d'environnement."""
    path = Path(config_path)
    if not path.is_file():
        raise FileNotFoundError(f"Fichier de configuration introuvable : {path}")

    with path.open(encoding="utf-8") as config_file:
        raw_config: Any = yaml.safe_load(config_file)
    if not isinstance(raw_config, dict):
        raise ValueError(f"La configuration {path} doit contenir un objet YAML.")

    values = {
        "project_id": raw_config.get("project_id"),
        "dataset_id": raw_config.get("dataset_id"),
        "table_id": raw_config.get("table_id"),
        "bucket_name": raw_config.get("bucket_name"),
        "region": raw_config.get("region"),
        "prefix": raw_config.get("prefix", ""),
    }
    env_names = {
        "project_id": "GCP_PROJECT_ID",
        "dataset_id": "BIGQUERY_DATASET",
        "table_id": "BIGQUERY_TABLE",
        "bucket_name": "GCS_BUCKET_NAME",
        "region": "GCP_REGION",
        "prefix": "GCS_PREFIX",
    }
    for key, env_name in env_names.items():
        if os.getenv(env_name) is not None:
            values[key] = os.environ[env_name]
    if overrides:
        for key, value in overrides.items():
            if key in values and value is not None:
                values[key] = str(value)

    missing = [
        key
        for key in ("project_id", "dataset_id", "table_id", "bucket_name", "region")
        if not isinstance(values[key], str)
        or not values[key].strip()
        or values[key].startswith("your-")
    ]
    if missing:
        raise ValueError(
            "Configuration incomplète ou valeur exemple non remplacée : "
            + ", ".join(missing)
        )

    return Settings(**values)
