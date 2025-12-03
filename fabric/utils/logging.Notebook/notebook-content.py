# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# ================================================
# Logging helpers for all notebooks
# ================================================
# Requires:
# - %run config in the caller notebook
#   (environment, run_id, DEBUG, local_timezone,
#    ETL_STEP_LOG_TABLE, PIPELINE_NAME)
# ================================================

from datetime import datetime, timezone
from pyspark.sql.types import (
    StructType, StructField, StringType,
    LongType, TimestampType, DoubleType
)
from pyspark.sql import functions as F

# Try to use local timezone if provided in config
try:
    from zoneinfo import ZoneInfo
    _local_tz = ZoneInfo(local_timezone) if "local_timezone" in globals() and local_timezone else None
except Exception:
    _local_tz = None


def _timestamp_str():
    """Return formatted timestamp string(s) depending on timezone availability."""
    ts_utc = datetime.now(timezone.utc)

    if _local_tz is not None:
        ts_local = ts_utc.astimezone(_local_tz)
        return ts_utc.isoformat(timespec="seconds"), ts_local.isoformat(timespec="seconds")

    # No local timezone configured - return only UTC
    return ts_utc.isoformat(timespec="seconds"), None


def log_stage(message: str) -> None:
    """Log high level ETL stage with UTC and optional local timestamp."""
    ts_utc, ts_local = _timestamp_str()

    if ts_local:
        print(f"[STAGE][UTC {ts_utc}][LOCAL {ts_local}][{environment}][{run_id}] {message}")
    else:
        print(f"[STAGE][UTC {ts_utc}][{environment}][{run_id}] {message}")


def log(message: str) -> None:
    """Log detailed messages when DEBUG is enabled."""
    if DEBUG:
        ts_utc, ts_local = _timestamp_str()

        if ts_local:
            print(f"[LOG][UTC {ts_utc}][LOCAL {ts_local}][{environment}][{run_id}] {message}")
        else:
            print(f"[LOG][UTC {ts_utc}][{environment}][{run_id}] {message}")


# Optional helper to write ETL step metrics
def write_etl_step_log(
    step_name: str,
    source_table: str,
    target_table: str,
    rows_in: int,
    rows_out: int,
    rows_rejected: int,
    rows_duplicates: int,
    load_date: str,
    status: str,
    error_message: str,
    started_at,
    finished_at
) -> None:
    """Append ETL step metrics to ETL_STEP_LOG_TABLE."""

    # Compute execution duration in seconds (float)
    duration_seconds = None
    if started_at is not None and finished_at is not None:
        try:
            duration_seconds = float((finished_at - started_at).total_seconds())
        except Exception:
            duration_seconds = None

    step_log_schema = StructType([
        StructField("run_id", StringType(), False),
        StructField("pipeline_name", StringType(), False),
        StructField("step_name", StringType(), False),
        StructField("environment", StringType(), False),
        StructField("source_table", StringType(), True),
        StructField("target_table", StringType(), True),
        StructField("rows_in", LongType(), True),
        StructField("rows_out", LongType(), True),
        StructField("rows_rejected", LongType(), True),
        StructField("rows_duplicates", LongType(), True),
        StructField("load_date", StringType(), True),
        StructField("status", StringType(), False),
        StructField("error_message", StringType(), True),
        StructField("started_at", TimestampType(), True),
        StructField("finished_at", TimestampType(), True),
        StructField("duration_seconds", DoubleType(), True),
    ])

    step_log_row = [(
        run_id,
        PIPELINE_NAME,
        step_name,
        environment,
        source_table,
        target_table,
        int(rows_in) if rows_in is not None else None,
        int(rows_out) if rows_out is not None else None,
        int(rows_rejected) if rows_rejected is not None else None,
        int(rows_duplicates) if rows_duplicates is not None else None,
        load_date,
        status,
        error_message,
        started_at,
        finished_at,
        duration_seconds,
    )]

    df_step_log = spark.createDataFrame(step_log_row, schema=step_log_schema)

    (
        df_step_log
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(ETL_STEP_LOG_TABLE)
    )

    # Extra log line with duration for convenience
    if duration_seconds is not None:
        log(f"ETL step '{step_name}' finished in {duration_seconds:.2f} seconds and appended to {ETL_STEP_LOG_TABLE}")
    else:
        log(f"ETL step metrics for '{step_name}' appended to {ETL_STEP_LOG_TABLE}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
