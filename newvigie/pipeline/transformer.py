"""Normalisation des rapports et contrôles de qualité des données."""

from __future__ import annotations

import logging
import re
import unicodedata
from datetime import datetime, timezone
from typing import Iterable

import pandas as pd

logger = logging.getLogger(__name__)

_ALIASES = {
    "date": {
        "date", "date_rapport", "date_demande", "date_reception", "date_depot",
        "date_creation", "date_de_la_demande", "date_de_publication",
        "date_du_rapport", "created_at", "timestamp",
    },
    "organisme": {
        "organisme", "organisation", "organization", "entite", "entity",
        "administration", "ministere", "service", "institution",
    },
    "type_demande": {
        "type_demande", "type_de_demande", "type_request", "categorie_demande",
        "category", "categorie",
    },
    "source_file": {"source_file", "fichier_source"},
    "ingested_at": {"ingested_at", "date_ingestion"},
}
_ALIAS_TO_CANONICAL = {
    alias: canonical
    for canonical, aliases in _ALIASES.items()
    for alias in aliases | {canonical}
}
_IDENTIFIER_TOKENS = {
    "id", "code", "ref", "numero", "num", "telephone", "tel", "postal",
    "siret", "siren", "identifiant",
}
_CATEGORY_TOKENS = {
    "organisme", "organisation", "organization", "entite", "entity",
    "administration", "ministere", "service", "institution", "type",
    "categorie", "category", "nom", "name", "libelle", "label",
    "commune", "region", "departement", "ville", "pays",
}
_NUMERIC_HINTS = (
    "montant", "somme", "total", "nombre", "nb_", "volume", "effectif",
    "quantite", "taux", "pourcentage", "delai", "duree",
    "count", "amount", "value", "score", "ratio", "annee", "year",
)
_DATE_HINTS = ("date", "datetime", "timestamp", "_at")


def normalize_column_name(name: object) -> str:
    """Convertit un nom en minuscules ASCII séparées par des underscores."""
    value = unicodedata.normalize("NFKD", str(name))
    value = value.encode("ascii", "ignore").decode("ascii").lower()
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", value)).strip("_")


def _merge_duplicate_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Fusionne les colonnes rendues identiques par la normalisation."""
    if not frame.columns.duplicated().any():
        return frame

    merged: dict[str, pd.Series] = {}
    for name in dict.fromkeys(frame.columns):
        columns = frame.loc[:, frame.columns == name]
        series = columns.iloc[:, 0]
        for index in range(1, columns.shape[1]):
            series = series.combine_first(columns.iloc[:, index])
        merged[name] = series
    return pd.DataFrame(merged, index=frame.index)


def _parse_numeric(values: pd.Series) -> pd.Series:
    text = values.astype("string").str.strip()
    missing = text.isna() | text.isin(["", "NA", "N/A", "null", "None"])
    dash_zero = text.isin(["-", "–", "—", "--"])
    negative = text.str.match(r"^\(.*\)$", na=False)
    text = text.str.replace(r"^\((.*)\)$", r"\1", regex=True)
    text = text.str.replace(
        r"(?i)\b(?:EUR|USD|GBP|CAD|AUD|CHF|JPY)\b|"
        r"(?<=\d)(?:EUR|USD|GBP|CAD|AUD|CHF|JPY)\b|"
        r"\b(?:EUR|USD|GBP|CAD|AUD|CHF|JPY)(?=\d)",
        "",
        regex=True,
    )
    text = text.str.replace(r"[\s\u00a0\u202f]", "", regex=True)
    text = text.str.replace(r"[’']", "", regex=True)
    text = text.str.replace(r"[€$£¥%]", "", regex=True)

    comma_decimal = text.str.contains(",", na=False)
    dot_decimal = text.str.contains(r"\.", na=False)
    both = comma_decimal & dot_decimal
    european = both & (text.str.rfind(",") > text.str.rfind("."))
    text.loc[european] = (
        text.loc[european].str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    )
    us = both & ~european
    text.loc[us] = text.loc[us].str.replace(",", "", regex=False)
    comma_only = comma_decimal & ~dot_decimal
    comma_grouping = comma_only & text.str.match(r"^[+-]?\d{1,3}(,\d{3})+$", na=False)
    text.loc[comma_grouping] = text.loc[comma_grouping].str.replace(",", "", regex=False)
    text.loc[comma_only & ~comma_grouping] = text.loc[
        comma_only & ~comma_grouping
    ].str.replace(",", ".", regex=False)
    dot_grouping = (
        dot_decimal
        & ~comma_decimal
        & text.str.match(r"^[+-]?\d{1,3}(\.\d{3})+$", na=False)
    )
    text.loc[dot_grouping] = text.loc[dot_grouping].str.replace(".", "", regex=False)
    parsed = pd.to_numeric(text, errors="coerce")
    parsed.loc[negative] = -parsed.loc[negative].abs()
    parsed.loc[dash_zero] = 0
    parsed.loc[missing] = pd.NA
    return parsed


def _should_parse_numeric(name: str, values: pd.Series) -> bool:
    tokens = set(name.split("_"))
    if tokens & (_IDENTIFIER_TOKENS | _CATEGORY_TOKENS):
        return False
    non_empty = values.dropna().astype("string").str.strip()
    non_empty = non_empty[~non_empty.isin(["", "NA", "N/A", "null", "None"])]
    if non_empty.empty:
        return False
    parsed = _parse_numeric(non_empty)
    ratio = parsed.notna().mean()
    hinted = any(hint in name for hint in _NUMERIC_HINTS)
    return ratio >= (0.5 if hinted else 0.95)


def _normalize_report(
    frame: pd.DataFrame,
    source_uri: str,
    ingested_at: datetime,
) -> pd.DataFrame:
    normalized = frame.copy()
    normalized.columns = [
        _ALIAS_TO_CANONICAL.get(normalize_column_name(column), normalize_column_name(column))
        for column in normalized.columns
    ]
    normalized = _merge_duplicate_columns(normalized)
    normalized = normalized.replace(r"^\s*$", pd.NA, regex=True)

    for column in normalized.columns:
        name = str(column)
        if any(hint in name for hint in _DATE_HINTS):
            normalized[column] = pd.to_datetime(
                normalized[column], errors="coerce", dayfirst=True, utc=True
            )
        elif _should_parse_numeric(name, normalized[column]):
            normalized[column] = _parse_numeric(normalized[column])

    normalized["source_file"] = source_uri
    normalized["ingested_at"] = pd.Timestamp(ingested_at)
    return normalized


def business_key_columns(columns: Iterable[str]) -> list[str]:
    """Détermine une clé métier uniquement si date et organisme sont disponibles."""
    available = set(columns)
    date_column = next(
        (name for name in ("date", "date_rapport", "annee", "year") if name in available),
        None,
    )
    entity_column = next(
        (name for name in ("organisme", "entite", "administration") if name in available),
        None,
    )
    if date_column is None or entity_column is None:
        return []
    keys = [date_column, entity_column]
    if "type_demande" in available:
        keys.append("type_demande")
    return keys


def transform_reports(
    reports: Iterable[tuple[pd.DataFrame, str]],
) -> pd.DataFrame:
    """Harmonise l'union des schémas, trace chaque ligne et supprime les doublons."""
    normalized_reports = [
        _normalize_report(frame, source_uri, datetime.now(timezone.utc))
        for frame, source_uri in reports
    ]
    if not normalized_reports:
        raise ValueError("Aucun rapport CSV à transformer.")

    unified = pd.concat(normalized_reports, ignore_index=True, sort=False)
    content_columns = [
        column for column in unified.columns if column not in {"source_file", "ingested_at"}
    ]
    before_strict = len(unified)
    if content_columns:
        unified = unified.sort_values("ingested_at", kind="stable")
        unified = unified.drop_duplicates(subset=content_columns, keep="last")
    logger.info("Doublons stricts supprimés : %d", before_strict - len(unified))

    keys = business_key_columns(unified.columns)
    before_business = len(unified)
    if keys:
        complete_key = unified[keys].notna().all(axis=1)
        keyed = unified.loc[complete_key].sort_values("ingested_at", kind="stable")
        keyed = keyed.drop_duplicates(subset=keys, keep="last")
        unified = pd.concat([unified.loc[~complete_key], keyed], ignore_index=True, sort=False)
    logger.info("Doublons métier supprimés : %d (clé : %s)", before_business - len(unified), keys)

    return unified.reset_index(drop=True)
