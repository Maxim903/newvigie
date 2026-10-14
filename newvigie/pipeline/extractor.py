"""Découverte récursive et lecture des CSV stockés dans Google Cloud Storage."""

from __future__ import annotations

import logging
from io import BytesIO
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from google.cloud.storage import Blob
    from google.cloud.storage.client import Client as StorageClient

logger = logging.getLogger(__name__)


def discover_csv_files(
    storage_client: StorageClient,
    bucket_name: str,
    prefix: str = "",
) -> list[Blob]:
    """Liste les CSV du préfixe, y compris ceux de tous ses sous-dossiers."""
    bucket = storage_client.bucket(bucket_name)
    blobs = [
        blob
        for blob in storage_client.list_blobs(bucket, prefix=prefix)
        if blob.name.lower().endswith(".csv")
    ]
    blobs.sort(key=lambda blob: blob.name.casefold())
    logger.info(
        "%d fichier(s) CSV découvert(s) dans gs://%s/%s",
        len(blobs),
        bucket_name,
        prefix,
    )
    return blobs


def read_csv_blob(blob: Blob) -> pd.DataFrame:
    """Lit un CSV avec détection du séparateur et replis d'encodage explicites."""
    content = blob.download_as_bytes()
    failures: list[tuple[str, Exception]] = []
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return pd.read_csv(
                BytesIO(content),
                sep=None,
                engine="python",
                encoding=encoding,
                dtype="string",
                keep_default_na=False,
                on_bad_lines="error",
            )
        except (UnicodeDecodeError, pd.errors.ParserError, ValueError) as error:
            failures.append((encoding, error))

    details = "; ".join(f"{encoding}: {error}" for encoding, error in failures)
    raise ValueError(f"Lecture impossible pour gs://{blob.bucket.name}/{blob.name}: {details}")


def extract_reports(
    storage_client: StorageClient,
    bucket_name: str,
    prefix: str = "",
) -> list[tuple[pd.DataFrame, str]]:
    """Retourne les DataFrames et leurs URI GCS, sans ignorer de fichier illisible."""
    reports: list[tuple[pd.DataFrame, str]] = []
    for blob in discover_csv_files(storage_client, bucket_name, prefix):
        source_uri = f"gs://{bucket_name}/{blob.name}"
        logger.info("Lecture du rapport %s", source_uri)
        reports.append((read_csv_blob(blob), source_uri))
    return reports
