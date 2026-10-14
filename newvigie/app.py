"""Tableau de bord Streamlit pour les rapports de transparence BigQuery."""

from __future__ import annotations

import json
from datetime import date
from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st
from google.auth.exceptions import GoogleAuthError
from google.api_core.exceptions import GoogleAPICallError
from google.cloud import bigquery
from google.oauth2 import service_account

from pipeline.settings import Settings, load_settings

st.set_page_config(page_title="Transparence — Analytics", page_icon="📊", layout="wide")

_NUMERIC_TYPES = {"INTEGER", "INT64", "FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC"}
_TEMPORAL_TYPES = {"DATE", "DATETIME", "TIMESTAMP"}
_TABLE_PREVIEW_LIMIT = 10_000


def _secret_mapping(key: str) -> dict[str, Any]:
    value = st.secrets.get(key, {})
    return dict(value) if hasattr(value, "items") else {}


def _settings_from_secrets() -> tuple[Settings, dict[str, Any]]:
    gcp_secrets = _secret_mapping("gcp")
    config_keys = {
        "project_id",
        "dataset_id",
        "table_id",
        "bucket_name",
        "region",
        "prefix",
    }
    overrides = {key: str(value) for key, value in gcp_secrets.items() if key in config_keys}
    settings = load_settings(overrides=overrides)
    service_account_info = _secret_mapping("gcp_service_account")
    return settings, service_account_info


@st.cache_resource
def _create_client(
    project_id: str,
    region: str,
    serialized_credentials: str | None,
) -> bigquery.Client:
    credentials = None
    if serialized_credentials:
        credentials = service_account.Credentials.from_service_account_info(
            json.loads(serialized_credentials),
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
    return bigquery.Client(
        project=project_id,
        location=region,
        credentials=credentials,
    )


def _client_for(
    settings: Settings,
    service_account_info: dict[str, Any],
) -> bigquery.Client:
    serialized = (
        json.dumps(service_account_info, sort_keys=True)
        if service_account_info
        else None
    )
    return _create_client(settings.project_id, settings.region, serialized)


def _table_name(settings: Settings) -> str:
    return f"`{settings.project_id}.{settings.dataset_id}.{settings.table_id}`"


def _quoted_column(name: str) -> str:
    return f"`{name.replace('`', '')}`"


def _query(
    client: bigquery.Client,
    sql: str,
    parameters: list[bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter],
) -> bigquery.table.RowIterator:
    return client.query(
        sql,
        job_config=bigquery.QueryJobConfig(query_parameters=parameters),
    ).result()


def _distinct_values(
    client: bigquery.Client,
    table: str,
    column: str,
) -> list[str]:
    sql = (
        f"SELECT DISTINCT CAST({_quoted_column(column)} AS STRING) AS value "
        f"FROM {table} WHERE {_quoted_column(column)} IS NOT NULL "
        "ORDER BY value LIMIT 1000"
    )
    return [row.value for row in client.query(sql).result()]


def _date_bounds(
    client: bigquery.Client,
    table: str,
    column: str,
) -> tuple[date, date] | None:
    row = next(
        iter(
            client.query(
                f"SELECT MIN(DATE({_quoted_column(column)})) AS min_date, "
                f"MAX(DATE({_quoted_column(column)})) AS max_date FROM {table}"
            ).result()
        )
    )
    if row.min_date is None or row.max_date is None:
        return None
    return row.min_date, row.max_date


def _year_bounds(
    client: bigquery.Client,
    table: str,
    column: str,
) -> tuple[int, int] | None:
    row = next(
        iter(
            client.query(
                f"SELECT MIN(SAFE_CAST({_quoted_column(column)} AS INT64)) AS min_year, "
                f"MAX(SAFE_CAST({_quoted_column(column)} AS INT64)) AS max_year "
                f"FROM {table}"
            ).result()
        )
    )
    if row.min_year is None or row.max_year is None:
        return None
    return int(row.min_year), int(row.max_year)


def _filters(
    client: bigquery.Client,
    table: str,
    schema: list[bigquery.SchemaField],
) -> tuple[str, list[bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter]]:
    names = {field.name for field in schema}
    source_column = "source_file" if "source_file" in names else None
    entity_column = next(
        (name for name in ("organisme", "entite", "administration") if name in names),
        None,
    )
    temporal_column = next(
        (
            field.name
            for field in schema
            if field.field_type.upper() in _TEMPORAL_TYPES
        ),
        None,
    )
    year_column = next(
        (
            name
            for name in ("annee", "year")
            if name in names
        ),
        None,
    )

    clauses: list[str] = []
    parameters: list[
        bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter
    ] = []
    with st.sidebar:
        st.header("Filtres")
        if source_column:
            source_values = _distinct_values(client, table, source_column)
            selected_sources = st.multiselect("Source du rapport", source_values)
            if selected_sources:
                clauses.append(
                    f"{_quoted_column(source_column)} IN UNNEST(@sources)"
                )
                parameters.append(
                    bigquery.ArrayQueryParameter("sources", "STRING", selected_sources)
                )
            if len(source_values) == 1000:
                st.caption("La liste des sources est limitée aux 1 000 premières.")

        if entity_column:
            entity_values = _distinct_values(client, table, entity_column)
            selected_entities = st.multiselect("Organisme / entité", entity_values)
            if selected_entities:
                clauses.append(
                    f"CAST({_quoted_column(entity_column)} AS STRING) IN UNNEST(@entities)"
                )
                parameters.append(
                    bigquery.ArrayQueryParameter("entities", "STRING", selected_entities)
                )

        if temporal_column:
            bounds = _date_bounds(client, table, temporal_column)
            if bounds:
                chosen = st.date_input(
                    "Période",
                    value=bounds,
                    min_value=bounds[0],
                    max_value=bounds[1],
                )
                if isinstance(chosen, tuple) and len(chosen) == 2:
                    clauses.append(
                        f"DATE({_quoted_column(temporal_column)}) "
                        "BETWEEN @start_date AND @end_date"
                    )
                    parameters.extend(
                        [
                            bigquery.ScalarQueryParameter("start_date", "DATE", chosen[0]),
                            bigquery.ScalarQueryParameter("end_date", "DATE", chosen[1]),
                        ]
                    )
        elif year_column:
            bounds = _year_bounds(client, table, year_column)
            if bounds:
                chosen_years = st.slider(
                    "Année",
                    min_value=bounds[0],
                    max_value=bounds[1],
                    value=bounds,
                )
                clauses.append(
                    f"SAFE_CAST({_quoted_column(year_column)} AS INT64) "
                    "BETWEEN @start_year AND @end_year"
                )
                parameters.extend(
                    [
                        bigquery.ScalarQueryParameter("start_year", "INT64", chosen_years[0]),
                        bigquery.ScalarQueryParameter("end_year", "INT64", chosen_years[1]),
                    ]
                )

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return where, parameters


def _format_number(value: Any) -> str:
    if value is None or pd.isna(value):
        return "0"
    return f"{float(value):,.2f}".rstrip("0").rstrip(".").replace(",", " ")


def _render_dashboard(client: bigquery.Client, settings: Settings) -> None:
    table = _table_name(settings)
    table_metadata = client.get_table(
        f"{settings.project_id}.{settings.dataset_id}.{settings.table_id}"
    )
    schema = list(table_metadata.schema)

    st.caption(f"Table BigQuery : `{settings.dataset_id}.{settings.table_id}`")

    where, parameters = _filters(client, table, schema)
    aggregate_columns = ["COUNT(*) AS row_count"]
    if any(field.name == "source_file" for field in schema):
        aggregate_columns.append(
            "COUNT(DISTINCT source_file) AS file_count"
        )
        aggregate_columns.append(
            "SAFE_DIVIDE(COUNTIF(source_file IS NOT NULL), COUNT(*)) AS traceability"
        )
    entity_column = next(
        (
            name
            for name in ("organisme", "entite", "administration")
            if any(field.name == name for field in schema)
        ),
        None,
    )
    if entity_column:
        aggregate_columns.append(
            f"COUNT(DISTINCT {_quoted_column(entity_column)}) AS entity_count"
        )
    aggregates = next(
        iter(
            _query(
                client,
                f"SELECT {', '.join(aggregate_columns)} FROM {table} {where}",
                parameters,
            )
        )
    )

    kpi_columns = st.columns(4)
    kpi_columns[0].metric("Volume total filtré", _format_number(aggregates.row_count))
    kpi_columns[1].metric(
        "Fichiers unifiés",
        _format_number(getattr(aggregates, "file_count", 0)),
    )
    kpi_columns[2].metric(
        "Organismes",
        _format_number(getattr(aggregates, "entity_count", 0)),
    )
    kpi_columns[3].metric(
        "Lignes avec source",
        f"{float(getattr(aggregates, 'traceability', 0) or 0) * 100:.1f}%",
    )

    numeric_fields = [
        field
        for field in schema
        if field.field_type.upper() in _NUMERIC_TYPES
        and field.name not in {"ingested_at"}
    ]
    temporal_fields = [
        field for field in schema if field.field_type.upper() in _TEMPORAL_TYPES
    ]
    if numeric_fields:
        metric_fields = numeric_fields[:3]
        metric_expressions = [
            f"SUM({_quoted_column(field.name)}) AS {_quoted_column('metric_' + str(index))}"
            for index, field in enumerate(metric_fields)
        ]
        metric_row = next(
            iter(
                _query(
                    client,
                    f"SELECT {', '.join(metric_expressions)} FROM {table} {where}",
                    parameters,
                )
            )
        )
        metric_columns = st.columns(len(metric_fields))
        for index, field in enumerate(metric_fields):
            metric_columns[index].metric(
                f"Total · {field.name}",
                _format_number(getattr(metric_row, f"metric_{index}")),
            )

    st.subheader("Analyse interactive")
    if numeric_fields and temporal_fields:
        left, right = st.columns([1, 3])
        metric_name = left.selectbox(
            "Indicateur numérique",
            [field.name for field in numeric_fields],
        )
        temporal_name = temporal_fields[0].name
        chart_sql = (
            f"SELECT DATE_TRUNC(DATE({_quoted_column(temporal_name)}), MONTH) AS periode, "
            f"SUM({_quoted_column(metric_name)}) AS valeur "
            f"FROM {table} {where} "
            "GROUP BY periode ORDER BY periode"
        )
        chart_data = client.query(
            chart_sql,
            job_config=bigquery.QueryJobConfig(query_parameters=parameters),
        ).to_dataframe()
        if not chart_data.empty:
            figure = px.line(
                chart_data,
                x="periode",
                y="valeur",
                markers=True,
                title=f"{metric_name} par mois",
            )
            right.plotly_chart(figure, use_container_width=True)
        else:
            right.info("Aucune donnée ne correspond aux filtres sélectionnés.")
    elif numeric_fields:
        metric_name = st.selectbox(
            "Indicateur numérique",
            [field.name for field in numeric_fields],
        )
        chart_sql = (
            f"SELECT {_quoted_column(metric_name)} AS valeur FROM {table} {where} "
            "LIMIT 20000"
        )
        chart_data = client.query(
            chart_sql,
            job_config=bigquery.QueryJobConfig(query_parameters=parameters),
        ).to_dataframe()
        st.plotly_chart(
            px.histogram(chart_data, x="valeur", title=f"Distribution · {metric_name}"),
            use_container_width=True,
        )
    elif temporal_fields:
        temporal_name = temporal_fields[0].name
        chart_sql = (
            f"SELECT DATE_TRUNC(DATE({_quoted_column(temporal_name)}), MONTH) AS periode, "
            f"COUNT(*) AS nombre FROM {table} {where} "
            "GROUP BY periode ORDER BY periode"
        )
        chart_data = client.query(
            chart_sql,
            job_config=bigquery.QueryJobConfig(query_parameters=parameters),
        ).to_dataframe()
        st.plotly_chart(
            px.bar(chart_data, x="periode", y="nombre", title="Volume par mois"),
            use_container_width=True,
        )
    else:
        st.info("Aucune colonne numérique ou temporelle détectée pour produire un graphique.")

    st.subheader("Inspection des données")
    preview_sql = f"SELECT * FROM {table} {where} LIMIT {_TABLE_PREVIEW_LIMIT}"
    preview = client.query(
        preview_sql,
        job_config=bigquery.QueryJobConfig(query_parameters=parameters),
    ).to_dataframe()
    st.caption(
        f"Aperçu de {len(preview):,} ligne(s), limité à {_TABLE_PREVIEW_LIMIT:,}. "
        "Le téléchargement inclut toutes les lignes correspondant aux filtres."
    )
    st.dataframe(preview, use_container_width=True, hide_index=True)
    if st.button("Préparer le téléchargement CSV"):
        with st.spinner("Lecture de toutes les lignes filtrées dans BigQuery..."):
            full_result = client.query(
                f"SELECT * FROM {table} {where}",
                job_config=bigquery.QueryJobConfig(query_parameters=parameters),
            ).to_dataframe()
        st.download_button(
            "Télécharger les données filtrées",
            data=full_result.to_csv(index=False).encode("utf-8-sig"),
            file_name="rapports_transparence_filtres.csv",
            mime="text/csv",
        )


def main() -> None:
    st.title("Rapports de transparence")
    try:
        settings, service_account_info = _settings_from_secrets()
        client = _client_for(settings, service_account_info)
        _render_dashboard(client, settings)
    except (GoogleAPICallError, GoogleAuthError, FileNotFoundError, ValueError) as error:
        st.error(f"Impossible de charger le tableau de bord : {error}")
        st.info(
            "Vérifiez config.yaml ou les secrets Streamlit, les permissions GCP, "
            "ainsi que l'existence de la table consolidée."
        )


if __name__ == "__main__":
    main()
