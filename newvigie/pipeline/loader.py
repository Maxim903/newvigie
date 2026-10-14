"""Chargement du pipeline dans BigQuery et orchestration de bout en bout."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Iterable

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage
from google.cloud.bigquery import SchemaField

from pipeline.extractor import extract_reports
from pipeline.settings import Settings, load_settings
from pipeline.transformer import business_key_columns, transform_reports

logger = logging.getLogger(__name__)

_TYPE_MAP = {
    "STRING": "STRING",
    "BOOL": "BOOL",
    "BOOLEAN": "BOOL",
    "INTEGER": "INT64",
    "INT64": "INT64",
    "FLOAT": "FLOAT64",
    "FLOAT64": "FLOAT64",
    "NUMERIC": "NUMERIC",
    "BIGNUMERIC": "BIGNUMERIC",
    "DATE": "DATE",
    "DATETIME": "DATETIME",
    "TIME": "TIME",
    "TIMESTAMP": "TIMESTAMP",
    "GEOGRAPHY": "GEOGRAPHY",
    "JSON": "JSON",
}


def _table_id(settings: Settings, table_name: str | None = None) -> str:
    return (
        f"{settings.project_id}.{settings.dataset_id}."
        f"{table_name or settings.table_id}"
    )


def _quoted_table(settings: Settings, table_name: str | None = None) -> str:
    return f"`{_table_id(settings, table_name)}`"


def _quoted_column(name: str) -> str:
    return f"`{name.replace('`', '')}`"


def _table_row_count(client: bigquery.Client, table_id: str) -> int:
    row_count = client.get_table(table_id).num_rows
    if row_count is not None:
        return row_count
    row = next(iter(client.query(f"SELECT COUNT(*) AS row_count FROM `{table_id}`").result()))
    return int(row.row_count)


def ensure_dataset(client: bigquery.Client, settings: Settings) -> None:
    """Vérifie le dataset BigQuery et le crée s'il n'existe pas."""
    dataset_id = f"{settings.project_id}.{settings.dataset_id}"
    try:
        client.get_dataset(dataset_id)
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = settings.region
        client.create_dataset(dataset, exists_ok=True)
        logger.info("Dataset BigQuery créé : %s (%s)", dataset_id, settings.region)


def _add_missing_columns(
    client: bigquery.Client,
    settings: Settings,
    target_name: str,
    target_schema: Iterable[SchemaField],
    incoming_schema: Iterable[SchemaField],
) -> list[SchemaField]:
    """Ajoute au schéma final les colonnes apparues dans les nouveaux rapports."""
    fields = list(target_schema)
    existing_names = {field.name for field in fields}
    for field in incoming_schema:
        if field.name in existing_names:
            continue
        field_type = _TYPE_MAP.get(field.field_type.upper())
        if field_type is None or field.mode == "REPEATED":
            raise ValueError(
                f"Type BigQuery non pris en charge pour {field.name}: "
                f"{field.field_type} ({field.mode})"
            )
        sql = (
            f"ALTER TABLE {_quoted_table(settings, target_name)} "
            f"ADD COLUMN IF NOT EXISTS {_quoted_column(field.name)} {field_type}"
        )
        client.query(sql).result()
        fields.append(SchemaField(field.name, field_type, mode="NULLABLE"))
        existing_names.add(field.name)
        logger.info("Colonne ajoutée à la table finale : %s", field.name)
    return fields


def _projection(
    target_fields: list[SchemaField],
    source_fields: Iterable[SchemaField],
) -> str:
    source_types: dict[str, str] = {}
    for source_field in source_fields:
        source_type = _TYPE_MAP.get(source_field.field_type.upper())
        if source_type is None or source_field.mode == "REPEATED":
            raise ValueError(
                f"Type BigQuery non pris en charge pour {source_field.name}: "
                f"{source_field.field_type} ({source_field.mode})"
            )
        source_types[source_field.name] = source_type
    expressions = []
    for field in target_fields:
        column = _quoted_column(field.name)
        target_type = _TYPE_MAP.get(field.field_type.upper())
        if target_type is None:
            raise ValueError(
                f"Type BigQuery non pris en charge pour {field.name}: "
                f"{field.field_type}"
            )
        if field.name not in source_types:
            expressions.append(f"CAST(NULL AS {target_type}) AS {column}")
        elif source_types[field.name] == target_type:
            expressions.append(f"{column} AS {column}")
        else:
            expressions.append(f"SAFE_CAST({column} AS {target_type}) AS {column}")
    return ", ".join(expressions)


def _deduplicated_query(
    union_sql: str,
    target_fields: list[SchemaField],
) -> str:
    names = [field.name for field in target_fields]
    content_columns = [
        name for name in names if name not in {"source_file", "ingested_at"}
    ]
    keys = business_key_columns(names)
    if not content_columns:
        return f"SELECT * FROM ({union_sql})"

    partition = ", ".join(_quoted_column(name) for name in content_columns)
    order = (
        "ingested_at DESC, source_file DESC"
        if {"ingested_at", "source_file"}.issubset(names)
        else ", ".join(_quoted_column(name) for name in names)
    )
    strict_query = (
        "SELECT * EXCEPT (_strict_rank) FROM ("
        f"SELECT *, ROW_NUMBER() OVER (PARTITION BY {partition} "
        f"ORDER BY {order}) AS _strict_rank FROM ({union_sql})"
        ") WHERE _strict_rank = 1"
    )
    if not keys:
        return strict_query

    key_list = ", ".join(_quoted_column(name) for name in keys)
    complete_condition = " AND ".join(
        f"{_quoted_column(name)} IS NOT NULL" for name in keys
    )
    business_order = (
        "ingested_at DESC, source_file DESC"
        if {"ingested_at", "source_file"}.issubset(names)
        else ", ".join(_quoted_column(name) for name in names)
    )
    return f"""
        SELECT * EXCEPT (_business_rank)
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY {key_list} ORDER BY {business_order}
            ) AS _business_rank
            FROM ({strict_query})
            WHERE {complete_condition}
        )
        WHERE _business_rank = 1
        UNION ALL
        SELECT * FROM ({strict_query})
        WHERE NOT ({complete_condition})
    """


def load_to_bigquery(
    client: bigquery.Client,
    frame: pd.DataFrame,
    settings: Settings,
) -> tuple[str, str]:
    """Charge un staging de run puis consolide la table finale en préservant son schéma."""
    if frame.empty:
        raise ValueError("Le DataFrame consolidé ne contient aucune ligne.")
    ensure_dataset(client, settings)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8]
    staging_name = f"stg_{settings.table_id}_{run_id}"
    work_name = f"wrk_{settings.table_id}_{run_id}"
    staging_id = _table_id(settings, staging_name)
    final_id = _table_id(settings)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_dataframe(frame, staging_id, job_config=job_config)
    job.result()
    logger.info("%d ligne(s) chargée(s) dans %s", job.output_rows or len(frame), staging_id)

    staging_table = client.get_table(staging_id)
    staging_schema = list(staging_table.schema)
    try:
        final_table = client.get_table(final_id)
    except NotFound:
        client.query(
            f"CREATE TABLE {_quoted_table(settings)} AS "
            f"SELECT * FROM {_quoted_table(settings, staging_name)}"
        ).result()
        logger.info("Table finale créée : %s", final_id)
        return staging_id, final_id

    target_fields = _add_missing_columns(
        client,
        settings,
        settings.table_id,
        final_table.schema,
        staging_schema,
    )
    final_projection = _projection(target_fields, target_fields)
    staging_projection = _projection(target_fields, staging_schema)
    union_sql = (
        f"SELECT {final_projection} FROM {_quoted_table(settings)} "
        f"UNION ALL "
        f"SELECT {staging_projection} FROM {_quoted_table(settings, staging_name)}"
    )
    consolidate_sql = _deduplicated_query(union_sql, target_fields)
    work_id = _table_id(settings, work_name)
    client.query(f"CREATE OR REPLACE TABLE `{work_id}` AS {consolidate_sql}").result()
    consolidated_rows = _table_row_count(client, work_id)
    input_rows = _table_row_count(client, final_id) + _table_row_count(client, staging_id)
    client.query(
        f"CREATE OR REPLACE TABLE {_quoted_table(settings)} "
        f"AS SELECT * FROM `{work_id}`"
    ).result()
    client.delete_table(work_id, not_found_ok=True)

    logger.info(
        "Doublons supprimés pendant la consolidation BigQuery : %d",
        input_rows - consolidated_rows,
    )
    logger.info("Table finale consolidée : %s", final_id)
    return staging_id, final_id


def run_pipeline(config_path: str = "config.yaml") -> tuple[str, str]:
    """Exécute la découverte GCS, la transformation et le chargement BigQuery."""
    settings = load_settings(config_path)
    storage_client = storage.Client(project=settings.project_id)
    bigquery_client = bigquery.Client(
        project=settings.project_id,
        location=settings.region,
    )
    reports = extract_reports(
        storage_client,
        settings.bucket_name,
        settings.prefix,
    )
    unified = transform_reports(reports)
    return load_to_bigquery(bigquery_client, unified, settings)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    staging_table, final_table = run_pipeline()
    logger.info("Pipeline terminé. Staging : %s ; table finale : %s", staging_table, final_table)
