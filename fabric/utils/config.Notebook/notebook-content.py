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
# Global configuration for all Fabric notebooks
# ================================================
# Contains:
# - Medallion schemas
# - Table names (bronze, silver, gold, meta)
# - Raw file paths
# - Constants (NULL_LITERAL, timezone)
# ================================================

# Common literal for representing NULL values
NULL_LITERAL = "<Empty>"

# Local timezone for logging
local_timezone = "America/Edmonton"

# ================================================
# Schemas (medallion layers)
# ================================================

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA   = "gold"

# Separate schema for logging and meta tables
META_SCHEMA = "silver_meta"

def tbl(schema: str, name: str) -> str:
    """Build fully qualified table name like 'schema.table'."""
    return f"{schema}.{name}"

# ================================================
# Bronze tables & raw sources
# ================================================

BRONZE_RETAIL_TRANSACTIONS = tbl(BRONZE_SCHEMA, "retail_transactions")

# Raw file path
RAW_RETAIL_FILE_PATH = "Files/raw/new_retail_data.csv"

# ================================================
# Silver-level fact & dimensions
# ================================================

SILVER_FACT_RETAIL   = tbl(SILVER_SCHEMA, "retail_clean")
SILVER_DIM_DATE      = tbl(SILVER_SCHEMA, "dim_date")
SILVER_DIM_CUSTOMER  = tbl(SILVER_SCHEMA, "dim_customer")
SILVER_DIM_PRODUCT   = tbl(SILVER_SCHEMA, "dim_product")

# ================================================
# Gold-level fact & dimensions
# ================================================

GOLD_FACT_SALES      = tbl(GOLD_SCHEMA, "fact_sales")
GOLD_DIM_DATE        = tbl(GOLD_SCHEMA, "dim_date")
GOLD_DIM_CUSTOMER    = tbl(GOLD_SCHEMA, "dim_customer")
GOLD_DIM_PRODUCT     = tbl(GOLD_SCHEMA, "dim_product")

# Aggregates
GOLD_AGG_SALES_MONTHLY      = tbl(GOLD_SCHEMA, "agg_sales_monthly")
GOLD_AGG_SALES_BY_CUSTOMER  = tbl(GOLD_SCHEMA, "agg_sales_by_customer")

# ================================================
# Meta tables (DQ, logging)
# ================================================

ETL_STEP_LOG_TABLE       = tbl(META_SCHEMA, "etl_step_log")
DQ_REJECTED_ROWS_TABLE   = tbl(META_SCHEMA, "dq_rejected_rows")
DQ_DUPLICATES_TABLE      = tbl(META_SCHEMA, "dq_duplicates_facts")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
