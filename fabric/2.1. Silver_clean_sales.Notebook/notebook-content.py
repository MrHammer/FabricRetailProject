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

# # 2.1 Silver - Clean Retail Sales
# 
# **Objective**
# 
# This notebook builds the Silver `retail_clean` table from the raw Bronze data:
# 
# - Loads raw transactions from `bronze.retail_transactions`.
# - Applies data quality, cleansing, and normalization steps:
#   - Trims and standardizes string fields.
#   - Converts empty or invalid values to NULL.
#   - Reconstructs a valid `OrderDate` using raw components (`Date`, `Year`, `Month`).
#   - Normalizes identifiers (`Customer_ID`, `Transaction_ID`) and removes formatting artifacts such as ".0".
#   - Builds `OrderDateTime` from `OrderDate` plus `Time`.
#   - Casts numeric fields (Amount, Total_Amount, Age, etc.) to proper numeric types.
# - Detects and removes duplicate transactions and writes them to the duplicate log table.
# - Produces a curated, business-ready Delta table: `silver.retail_clean`.
# 
# **Design principles (Medallion Architecture and Microsoft Fabric best practices)**
# 
# - Bronze stores raw data exactly as delivered (schema-on-read, no transformations).
# - Silver provides validated, standardized, row-level clean data.
# - No aggregations or dimensional modeling here. Only cleansing, validation, and structure corrections.
# - All anomalies such as invalid rows or duplicates are logged for traceability.
# - Idempotent demo pattern: the Silver table is overwritten on each run.


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

from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, trim, when, to_date, sha2, concat_ws, lit
)
from pyspark.sql.window import Window
from datetime import datetime, timezone

STEP_NAME = "silver_clean_retail"

log_stage("Notebook start - Silver clean retail")
log(f"Environment: {environment}, DEBUG={DEBUG}, version={CONFIG_VERSION}, run_id={run_id}")

started_at = datetime.now(timezone.utc)

# ============================================================
# 1. Configuration and schema
# ============================================================

log_stage("Ensure Silver and meta schemas exist")

BRONZE_TABLE_NAME = BRONZE_RETAIL_TRANSACTIONS
SILVER_TABLE_NAME = SILVER_FACT_RETAIL

log(f"Source Bronze table       : {BRONZE_TABLE_NAME}")
log(f"Target Silver fact table  : {SILVER_TABLE_NAME}")
log(f"Duplicates log table      : {DQ_DUPLICATES_TABLE}")
log(f"Rejected rows log table   : {DQ_REJECTED_ROWS_TABLE}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {META_SCHEMA}")
log("Schemas ensured.")

# ============================================================
# 2. Read Bronze
# ============================================================

log_stage("Read Bronze table")

df_bronze = spark.table(BRONZE_TABLE_NAME)
rows_in = df_bronze.count()
log(f"Bronze rows: {rows_in}")

# ============================================================
# 3. Basic cleansing: trim and empty to NULL
# ============================================================

log_stage("Trim strings and convert empty strings to NULL")

df_clean = df_bronze

# Trim all string columns
for cname, ctype in df_bronze.dtypes:
    if ctype == "string":
        df_clean = df_clean.withColumn(cname, trim(col(cname)))

cols_to_null = [
    "Name","Email","Address","City","State","Country",
    "Customer_Segment","Income",
    "Product_Category","Product_Brand","Product_Type",
    "Feedback","Shipping_Method","Payment_Method",
    "Order_Status","products","source_file"
]

for cname in cols_to_null:
    if cname in df_clean.columns:
        df_clean = df_clean.withColumn(
            cname,
            when(col(cname) == "", None).otherwise(col(cname))
        )

log("String cleanup complete.")

# ============================================================
# 4. Parse and reconstruct OrderDate
# ============================================================

log_stage("Parse and reconstruct OrderDate")

# Try to parse from raw Date column
df_clean = df_clean.withColumn(
    "OrderDate_raw",
    to_date(col("Date"), "M/d/yyyy")
)

# Fallback from Year and Month text
df_clean = df_clean.withColumn(
    "OrderDate",
    when(col("OrderDate_raw").isNotNull(), col("OrderDate_raw"))
    .otherwise(
        when(
            col("Year").isNotNull() & col("Month").isNotNull(),
            to_date(
                concat_ws(" ", lit(1), col("Month"), col("Year").cast("int")),
                "d MMMM yyyy"
            )
        )
    )
)

# Recover NULL OrderDate using neighbor rows by Transaction_ID ordering
w_orderdate = Window.orderBy("Transaction_ID")
df_clean = df_clean.withColumn(
    "OrderDate",
    F.when(
        col("OrderDate").isNull(),
        F.coalesce(
            F.lag("OrderDate", 1).over(w_orderdate),
            F.lead("OrderDate", 1).over(w_orderdate)
        )
    ).otherwise(col("OrderDate"))
)

log("OrderDate reconstructed using Date, Year/Month and neighbor rows.")

# ============================================================
# 5. Normalize IDs and phone/zipcode
# ============================================================

log_stage("Normalize ID, phone, and zipcode fields")

def clean_float_str(cname: str):
    # Remove .0 suffixes from values that came as floats
    return F.regexp_replace(col(cname).cast("string"), r"\.0+$", "")

for cname in ["Transaction_ID", "Customer_ID", "Phone", "Zipcode"]:
    if cname in df_clean.columns:
        df_clean = df_clean.withColumn(cname, clean_float_str(cname))

log("ID and contact fields cleaned.")

# ============================================================
# 6. Generate synthetic TransactionID and CustomerID
# ============================================================

log_stage("Generate TransactionID and CustomerID")

df_clean = df_clean.withColumn(
    "TransactionID",
    when(
        col("Transaction_ID").isNull(),
        sha2(
            concat_ws(
                "|",
                col("Customer_ID"),
                col("OrderDate").cast("string"),
                col("products"),
                col("Amount"),
                col("Total_Amount")
            ),
            256
        )
    ).otherwise(col("Transaction_ID"))
)

df_clean = df_clean.withColumn(
    "CustomerID",
    when(
        col("Customer_ID").isNull(),
        sha2(
            concat_ws(
                "|",
                col("Name"), col("Email"), col("Phone"),
                col("Address"), col("City"), col("State"),
                col("Zipcode"), col("Country")
            ),
            256
        )
    ).otherwise(col("Customer_ID"))
)

log("TransactionID and CustomerID generated.")

# ============================================================
# 7. Build OrderDateTime and derived fields
# ============================================================

log_stage("Build OrderDateTime and OrderHour")

df_clean = df_clean.withColumn(
    "Time_clean",
    when((col("Time") == "") | col("Time").isNull(), lit("00:00:00"))
    .otherwise(col("Time"))
)

df_clean = df_clean.withColumn(
    "OrderDateTime",
    F.to_timestamp(
        F.concat_ws(" ", col("OrderDate").cast("string"), col("Time_clean")),
        "yyyy-MM-dd H:mm:ss"
    )
)

df_clean = df_clean.withColumn("OrderHour", F.hour("OrderDateTime"))

log("OrderDateTime and OrderHour created.")

# ============================================================
# 8. Cast numeric fields
# ============================================================

log_stage("Cast numeric columns")

df_clean = (
    df_clean
        .withColumn("Age", col("Age").cast("int"))
        .withColumn("Year", col("Year").cast("int"))
        .withColumn("Total_Purchases", col("Total_Purchases").cast("int"))
        .withColumn("Amount", col("Amount").cast("double"))
        .withColumn("Total_Amount", col("Total_Amount").cast("double"))
        .withColumn("Ratings", col("Ratings").cast("double"))
        .withColumn("Income", F.regexp_replace("Income", ",", "").cast("double"))
)

log("Numeric conversion done.")

# ============================================================
# 9. Normalize NULLs for product attributes using NULL_LITERAL
# ============================================================

log_stage("Normalize NULLs for product attributes with NULL_LITERAL")

df_clean = (
    df_clean
        .withColumn(
            "products",
            F.coalesce(col("products"), F.lit(NULL_LITERAL))
        )
        .withColumn(
            "Product_Category",
            F.coalesce(col("Product_Category"), F.lit(NULL_LITERAL))
        )
        .withColumn(
            "Product_Brand",
            F.coalesce(col("Product_Brand"), F.lit(NULL_LITERAL))
        )
        .withColumn(
            "Product_Type",
            F.coalesce(col("Product_Type"), F.lit(NULL_LITERAL))
        )
)

log("Product related attributes normalized with NULL_LITERAL.")

# ============================================================
# 10. Data quality rules and rejected rows
# ============================================================

log_stage("Apply data quality rules and capture rejected rows")

# At this point we already:
# - tried to parse OrderDate from Date or Year/Month
# - tried to recover missing OrderDate from neighbor rows
# - generated TransactionID and CustomerID
# - built OrderDateTime using OrderDate and Time_clean
#
# So the only rows that we consider "hard failed" are those
# where OrderDate is still NULL, which means we had no way
# to reconstruct a valid date at all.

reject_condition = col("OrderDate").isNull()

df_rejected = df_clean.filter(reject_condition)
df_valid = df_clean.filter(~reject_condition)

rows_rejected = df_rejected.count()
log(f"Rejected rows after all reconstruction logic (OrderDate still NULL): {rows_rejected}")

if rows_rejected > 0:
    log_stage("Write rejected rows to DQ_REJECTED_ROWS_TABLE")

    rej_row_json = F.to_json(F.struct([col(c) for c in df_rejected.columns]))

    df_rej_log = (
        df_rejected
        .withColumn("run_id", lit(run_id))
        .withColumn("pipeline_name", lit(PIPELINE_NAME))
        .withColumn("step_name", lit(STEP_NAME))
        .withColumn("environment", lit(environment))
        .withColumn("source_table", lit(BRONZE_TABLE_NAME))
        .withColumn("target_table", lit(SILVER_TABLE_NAME))
        .withColumn("rule_name", lit("OrderDate_not_recoverable"))
        .withColumn(
            "business_key",
            F.coalesce(col("TransactionID").cast("string"), col("CustomerID").cast("string"))
        )
        .withColumn(
            "error_message",
            lit("OrderDate could not be reconstructed from Date, Year/Month or neighbor rows")
        )
        .withColumn("row_json", rej_row_json)
        .withColumn("load_date", lit(load_date))
        .withColumn("created_at", F.current_timestamp())
        .select(
            "run_id",
            "pipeline_name",
            "step_name",
            "environment",
            "source_table",
            "target_table",
            "rule_name",
            "business_key",
            "error_message",
            "row_json",
            "load_date",
            "created_at"
        )
    )

    (
        df_rej_log
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(DQ_REJECTED_ROWS_TABLE)
    )

    log(f"Rejected rows appended to {DQ_REJECTED_ROWS_TABLE}")

# ============================================================
# 11. Detect and store duplicates, then filter them out
# ============================================================

log_stage("Detect and store exact business duplicates")

# Business columns - all columns except technical ingestion metadata
technical_cols = {"ingestion_timestamp", "source_file"}
business_cols = [c for c in df_valid.columns if c not in technical_cols]

log(f"Business columns used for duplicate detection: {business_cols}")

# Choose ordering inside duplicate groups
order_col = "ingestion_timestamp" if "ingestion_timestamp" in df_valid.columns else "OrderDateTime"

# Partition by full business row, not only TransactionID
w_dup = Window.partitionBy(*business_cols).orderBy(col(order_col).asc())

df_with_rank = df_valid.withColumn("dup_rank", F.row_number().over(w_dup))

# All exact duplicates (same business row, later rank)
df_duplicates = df_with_rank.filter(col("dup_rank") > 1)
# Keep only first occurrence of each exact business row
df_silver = df_with_rank.filter(col("dup_rank") == 1).drop("dup_rank")

rows_duplicates = df_duplicates.count()
log(f"Exact business duplicate rows detected: {rows_duplicates}")

if rows_duplicates > 0:
    log_stage("Write exact duplicates to DQ_DUPLICATES_TABLE")

    # Build JSON snapshot of each duplicate row
    dup_row_json = F.to_json(F.struct([col(c) for c in df_duplicates.columns]))

    # Build a stable hash of the business row as business_key
    business_key_hash = sha2(
        concat_ws("|", *[col(c).cast("string") for c in business_cols]),
        256
    )

    df_dup_log = (
        df_duplicates
        .withColumn("run_id", lit(run_id))
        .withColumn("pipeline_name", lit(PIPELINE_NAME))
        .withColumn("step_name", lit(STEP_NAME))
        .withColumn("environment", lit(environment))
        .withColumn("source_table", lit(BRONZE_TABLE_NAME))
        .withColumn("target_table", lit(SILVER_TABLE_NAME))
        .withColumn("rule_name", lit("Exact_business_duplicate"))
        .withColumn("business_key", business_key_hash.cast("string"))
        .withColumn("duplicate_rank", col("dup_rank"))
        .withColumn("row_json", dup_row_json)
        .withColumn("load_date", lit(load_date))
        .withColumn("created_at", F.current_timestamp())
        .select(
            "run_id",
            "pipeline_name",
            "step_name",
            "environment",
            "source_table",
            "target_table",
            "rule_name",
            "business_key",
            "duplicate_rank",
            "row_json",
            "load_date",
            "created_at"
        )
    )

    (
        df_dup_log
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(DQ_DUPLICATES_TABLE)
    )

    log(f"Exact duplicates appended to {DQ_DUPLICATES_TABLE}")

# ============================================================
# 12. Select final Silver schema
# ============================================================

log_stage("Select final Silver columns")

cols_final = [
    "TransactionID", "CustomerID",
    "Name","Email","Phone","Address","City","State","Zipcode","Country",
    "Age","Gender","Income","Customer_Segment",
    "Year","Month","Time",
    "OrderDate","OrderDateTime","OrderHour",
    "Total_Purchases","Amount","Total_Amount",
    "Product_Category","Product_Brand","Product_Type",
    "Feedback","Shipping_Method","Payment_Method",
    "Order_Status","Ratings",
    "products",
    "ingestion_timestamp","source_file"
]

df_silver = df_silver.select(cols_final)
rows_out = df_silver.count()

log(f"Final Silver projection built. Rows after cleansing, rejections, and deduplication: {rows_out}")

# ============================================================
# 13. Write Silver table
# ============================================================

log_stage("Write Silver table")

(
    df_silver
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE_NAME)
)

log(f"Silver table '{SILVER_TABLE_NAME}' written successfully.")

# ============================================================
# 14. Quality checks
# ============================================================

log_stage("Quality checks on final Silver table")

log(f"NULL OrderDate: {df_silver.filter(col('OrderDate').isNull()).count()}")
log(f"NULL CustomerID: {df_silver.filter(col('CustomerID').isNull()).count()}")
log(f"NULL TransactionID: {df_silver.filter(col('TransactionID').isNull()).count()}")
log(f"NULL OrderDateTime: {df_silver.filter(col('OrderDateTime').isNull()).count()}")

# ============================================================
# 15. Log ETL step metrics
# ============================================================

log_stage("Log ETL step metrics")

finished_at = datetime.now(timezone.utc)

write_etl_step_log(
    step_name=STEP_NAME,
    source_table=BRONZE_TABLE_NAME,
    target_table=SILVER_TABLE_NAME,
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

log_stage("Silver clean notebook completed successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
