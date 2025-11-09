"""
Nightly BigQuery archival workflow for property and job listings.
This DAG copies 30-day-old partitions from the hot/raw dataset to an
archive dataset, optionally exports them to Cloud Storage, and can drop
the original partitions after a successful archive.
"""

from __future__ import annotations

import datetime
import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List

from airflow.decorators import dag
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


DEFAULT_CONFIG_FILE = Path("/opt/airflow/include/configs/archive_config.json")
CONFIG_ENV_VAR = "ARCHIVE_CONFIG_FILE"


def load_archive_config(config_path: str) -> Dict[str, Any]:
    """Load archive configuration from JSON file."""
    if not config_path:
        raise ValueError("config_file parameter is required.")

    resolved_path = Path(config_path)
    if not resolved_path.exists():
        raise FileNotFoundError(f"Archive config file not found: {config_path}")

    with resolved_path.open("r", encoding="utf-8") as config_file:
        config = json.load(config_file)

    if "tables" not in config or not isinstance(config["tables"], list):
        raise ValueError("Archive config must contain a 'tables' array.")

    return config


def sanitize_task_id(base: str) -> str:
    """Sanitize strings to be valid Airflow task_ids."""
    sanitized = re.sub(r"[^a-zA-Z0-9_]+", "_", base.strip())
    sanitized = re.sub(r"_+", "_", sanitized)
    sanitized = sanitized.lower().strip("_")
    if not sanitized:
        sanitized = "archive_task"
    return sanitized


def build_procedure_query(
    procedure_path: str,
    table: str,
    archive_key: str,
    archive_bucket: str,
    export_format: str,
    export_suffix: str,
    archive_lag_days: int,
    export_enabled: bool,
) -> str:
    """Create the SQL statement that calls the archive stored procedure."""
    archive_date_expr = f"{{{{ macros.ds_add(ds, -{archive_lag_days}) }}}}"

    if not export_enabled:
        return f"""SELECT "Export disabled for {table}" AS status;"""

    return f"""
DECLARE archive_date DATE DEFAULT DATE('{archive_date_expr}');

CALL `{procedure_path}`(
  '{table}',
  CURRENT_DATE(), -- archive_date
  '{archive_bucket}',
  '{archive_key}',
  '{export_format}',
  '{export_suffix}'
);
"""


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.datetime(2025, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=10),
}


@dag(
    dag_id="archive_hot_data",
    default_args=default_args,
    description="Archive 30-day-old partitions from raw tables to archive storage",
    # schedule_interval="0 2 * * *",
    catchup=False,
    tags=["archival", "bigquery", "maintenance"],
)
def archive_hot_data():
    """
    Trigger stored procedure exports based on configuration file.
    """

    config_path = Path(os.environ.get(CONFIG_ENV_VAR, str(DEFAULT_CONFIG_FILE))).resolve()
    config = load_archive_config(str(config_path))

    procedure_path = config.get(
        "procedure_path", "niblr-agentic-service.raw_layer.archive_table_partition"
    )
    default_archive_bucket = config.get("archive_bucket", "gs://niblr-archive-layer-dev")
    default_export_format = config.get("export_format", "PARQUET")
    default_export_suffix = config.get("export_suffix", "parquet")
    default_archive_lag_days = config.get("archive_lag_days", 30)
    default_export_enabled = config.get("export_enabled", True)

    tasks: List[BigQueryInsertJobOperator] = []
    for table_cfg in config["tables"]:
        table = table_cfg.get("table")
        archive_key = table_cfg.get("archive_key")
        if not table or not archive_key:
            raise ValueError(
                f"Each table config must include 'table' and 'archive_key'. Got: {table_cfg}"
            )

        archive_bucket = table_cfg.get("archive_bucket", default_archive_bucket)
        export_format = table_cfg.get("export_format", default_export_format)
        export_suffix = table_cfg.get("export_suffix", default_export_suffix)
        archive_lag_days = int(
            table_cfg.get("archive_lag_days", default_archive_lag_days)
        )
        export_enabled = bool(
            table_cfg.get("export_enabled", default_export_enabled)
        )
        task_id = sanitize_task_id(
            table_cfg.get("task_id")
            or f"archive_{archive_key}"
            or f"archive_{table.split('.')[-1]}"
        )

        query = build_procedure_query(
            procedure_path=procedure_path,
            table=table,
            archive_key=archive_key,
            archive_bucket=archive_bucket,
            export_format=export_format,
            export_suffix=export_suffix,
            archive_lag_days=archive_lag_days,
            export_enabled=export_enabled,
        )

        task = BigQueryInsertJobOperator(
            task_id=task_id,
            gcp_conn_id="google_cloud_default",
            configuration={
                "query": {
                    "query": query,
                    "useLegacySql": False,
                }
            },
        )
        tasks.append(task)

    # Optionally chain tasks sequentially to prevent concurrent exports to same bucket
    for upstream, downstream in zip(tasks, tasks[1:]):
        upstream >> downstream


dag = archive_hot_data()


