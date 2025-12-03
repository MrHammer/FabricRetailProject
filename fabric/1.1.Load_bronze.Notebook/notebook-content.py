# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e2ba261a-d3b7-4d14-b030-9bca99106276",
# META       "default_lakehouse_name": "lh_retail_project",
# META       "default_lakehouse_workspace_id": "9e9ce652-4028-4da6-bc43-cc864552cecf",
# META       "known_lakehouses": [
# META         {
# META           "id": "e2ba261a-d3b7-4d14-b030-9bca99106276"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 1.0 Bronze - Ingest Retail Transactions (Raw Layer)
# 
# **Objective**
# 
# This notebook implements the Bronze layer ingestion for the `new_retail_data.csv` file:
# - Land raw retail transactions into the `bronze.retail_transactions` Delta table.
# - Preserve the file structure as-is (all columns as strings).
# - Add ingestion metadata for lineage and auditing.
# 
# **Design principles (Medallion / Microsoft Fabric best practices)**
# 
# - **Schema-on-read but fixed schema definition:**  
#   We explicitly define the schema as `STRING` for all business columns to avoid unexpected type inference issues.
# - **No business transformations:**  
#   Bronze is a *raw landing zone*; no cleansing, no type casting, no business rules.
# - **Metadata enrichment only:**  
#   We only add:
#   - `ingestion_timestamp` - when the data landed in Bronze.
#   - `source_file` - which raw file was the source.
# - **Idempotent load for demo:**  
#   For this lab, we use `mode("overwrite")` to keep the table in a known good state.  
#   In production, this would typically be an **append-only** pattern with partitioning and incremental loads.


# PARAMETERS CELL ********************

# Notebook parameters - values here can be overridden by Fabric Pipeline

environment = "dev"                  # dev, test, prod
run_mode = "Full"                    # Full or Incremental
load_date = None                     # 'YYYY-MM-DD' or None
DEBUG = True                         # True for verbose logging
CONFIG_VERSION = "1.0.0"
run_id = "local_manual_run"          # pipeline will override this
PIPELINE_NAME = "pl_retail_daily_refresh"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run logging

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
from datetime import datetime, timezone

STEP_NAME = "bronze_retail_load"

log_stage("Notebook start - Bronze retail load")
log(f"Environment: {environment}, DEBUG={DEBUG}, CONFIG_VERSION={CONFIG_VERSION}, run_id={run_id}")

started_at = datetime.now(timezone.utc)

# ============================================================
# 1. Configuration
# ============================================================

log_stage("Configuration")

RAW_FILE_PATH = RAW_RETAIL_FILE_PATH
BRONZE_TABLE_NAME = BRONZE_RETAIL_TRANSACTIONS

log(f"Raw file path       : {RAW_FILE_PATH}")
log(f"Bronze target table : {BRONZE_TABLE_NAME}")

# Explicit Bronze schema: all business fields as STRING
bronze_schema = StructType([
    StructField("Transaction_ID",   StringType()),
    StructField("Customer_ID",      StringType()),
    StructField("Name",             StringType()),
    StructField("Email",            StringType()),
    StructField("Phone",            StringType()),
    StructField("Address",          StringType()),
    StructField("City",             StringType()),
    StructField("State",            StringType()),
    StructField("Zipcode",          StringType()),
    StructField("Country",          StringType()),
    StructField("Age",              StringType()),
    StructField("Gender",           StringType()),
    StructField("Income",           StringType()),
    StructField("Customer_Segment", StringType()),
    StructField("Date",             StringType()),
    StructField("Year",             StringType()),
    StructField("Month",            StringType()),
    StructField("Time",             StringType()),
    StructField("Total_Purchases",  StringType()),
    StructField("Amount",           StringType()),
    StructField("Total_Amount",     StringType()),
    StructField("Product_Category", StringType()),
    StructField("Product_Brand",    StringType()),
    StructField("Product_Type",     StringType()),
    StructField("Feedback",         StringType()),
    StructField("Shipping_Method",  StringType()),
    StructField("Payment_Method",   StringType()),
    StructField("Order_Status",     StringType()),
    StructField("Ratings",          StringType()),
    StructField("products",         StringType())
])

log("Bronze schema (all STRING columns) defined.")

# ============================================================
# 2. Ensure schemas exist
# ============================================================

log_stage("Ensure schemas exist")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {META_SCHEMA}")

log(f"Schema '{BRONZE_SCHEMA}' ensured.")
log(f"Schema '{META_SCHEMA}' ensured.")

# ============================================================
# 3. Read raw CSV into DataFrame (Bronze landing)
# ============================================================

log_stage("Read raw CSV into Bronze DataFrame")

df_raw = (
    spark.read
        .option("header", "true")
        .schema(bronze_schema)       # enforce all columns as STRING
        .csv(RAW_FILE_PATH)
)

rows_in = df_raw.count()
log(f"Raw DataFrame loaded from CSV. Row count: {rows_in}")
log(f"Raw DataFrame columns: {df_raw.columns}")

# ============================================================
# 4. Enrich with ingestion metadata
# ============================================================

log_stage("Enrich with ingestion metadata")

df_bronze = (
    df_raw
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("source_file", F.input_file_name())
)

rows_out = df_bronze.count()
log("Metadata columns added: 'ingestion_timestamp', 'source_file'.")
log(f"Bronze DataFrame row count (should match raw): {rows_out}")

rows_rejected = 0       # no validation at Bronze
rows_duplicates = 0     # deduplication will be handled in Silver fact

# ============================================================
# 5. Write Bronze Delta table
# ============================================================

log_stage("Write Bronze Delta table")

(
    df_bronze
    .write
    .format("delta")
    .mode("overwrite")            # overwrite for lab/demo; append in real pipelines
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE_TABLE_NAME)
)

log(f"Bronze Delta table written: {BRONZE_TABLE_NAME}")

df_bronze_table = spark.table(BRONZE_TABLE_NAME)
bronze_table_count = df_bronze_table.count()
log(f"Rows in Bronze table '{BRONZE_TABLE_NAME}': {bronze_table_count}")

# ============================================================
# 6. Log ETL step metrics
# ============================================================

log_stage("Log ETL step metrics")

finished_at = datetime.now(timezone.utc)

write_etl_step_log(
    step_name=STEP_NAME,
    source_table=None,              # file based source
    target_table=BRONZE_TABLE_NAME,
    rows_in=rows_in,
    rows_out=rows_out,
    rows_rejected=rows_rejected,
    rows_duplicates=rows_duplicates,
    load_date=load_date,
    status="Success",
    error_message=None,
    started_at=started_at,
    finished_at=finished_at
)

log_stage("Bronze ingestion notebook finished successfully.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
