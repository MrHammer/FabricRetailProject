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

# # 3.2 Gold - Build Fact Sales
# 
# This notebook builds the Gold fact table for retail sales:
# 
# - Reads transactional data from `silver.retail_clean`.
# - Joins Gold dimensions:
#   - `gold.dim_date`
#   - `gold.dim_customer`
#   - `gold.dim_product`
# - Produces a star-schema-friendly fact table:
#   - `gold.fact_sales`
#   - One row per transaction (TransactionID)
#   - Numeric foreign keys: DateKey, CustomerKey, ProductKey
#   - Measures: Total_Purchases, Amount, Total_Amount, Discount (if available)
#   - Degenerate attributes from the sales order (order status, payment, shipping, feedback).

# CELL ********************

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
from pyspark.sql.functions import col, lit
from datetime import datetime, timezone

STEP_NAME = "gold_fact_sales"

log_stage("Notebook start - Gold fact sales creation")
log(f"Environment: {environment}, DEBUG={DEBUG}, version={CONFIG_VERSION}, run_id={run_id}")

# Table names from config
FACT_SOURCE_TABLE = SILVER_FACT_RETAIL
GOLD_DIM_DATE_TABLE = GOLD_DIM_DATE
GOLD_DIM_CUSTOMER_TABLE = GOLD_DIM_CUSTOMER
GOLD_DIM_PRODUCT_TABLE = GOLD_DIM_PRODUCT
GOLD_FACT_TABLE = GOLD_FACT_SALES

log(f"Source Silver fact        : {FACT_SOURCE_TABLE}")
log(f"Gold dim_date table       : {GOLD_DIM_DATE_TABLE}")
log(f"Gold dim_customer table   : {GOLD_DIM_CUSTOMER_TABLE}")
log(f"Gold dim_product table    : {GOLD_DIM_PRODUCT_TABLE}")
log(f"Target Gold fact table    : {GOLD_FACT_TABLE}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {META_SCHEMA}")

started_at = datetime.now(timezone.utc)

# ============================================================
# 1. Load Silver fact and Gold dimensions
# ============================================================

log_stage("Read Silver fact and Gold dimensions")

df_fact = spark.read.table(FACT_SOURCE_TABLE)
rows_in = df_fact.count()
log(f"Rows in Silver fact: {rows_in}")

if rows_in == 0:
    raise ValueError(f"Fact source table {FACT_SOURCE_TABLE} is empty. Cannot build Gold fact.")

df_dim_date = spark.read.table(GOLD_DIM_DATE_TABLE)
df_dim_cust = spark.read.table(GOLD_DIM_CUSTOMER_TABLE)
df_dim_prod = spark.read.table(GOLD_DIM_PRODUCT_TABLE)

log(f"Rows in gold.dim_date     : {df_dim_date.count()}")
log(f"Rows in gold.dim_customer : {df_dim_cust.count()}")
log(f"Rows in gold.dim_product  : {df_dim_prod.count()}")

# ============================================================
# 2. Join with dim_date to get DateKey
# ============================================================

log_stage("Join fact with gold.dim_date")

# Expecting Date column in dim_date, OrderDate in fact
df_fact_with_date = (
    df_fact.alias("f")
    .join(
        df_dim_date.select("DateKey", "Date").alias("d"),
        on=col("f.OrderDate") == col("d.Date"),
        how="left"
    )
)

# ============================================================
# 3. Join with dim_customer to get CustomerKey
# ============================================================

log_stage("Join fact with gold.dim_customer")

df_fact_with_cust = (
    df_fact_with_date
    .join(
        df_dim_cust.select("CustomerKey", "CustomerID").alias("c"),
        on=col("f.CustomerID") == col("c.CustomerID"),
        how="left"
    )
)

# ============================================================
# 4. Join with dim_product to get ProductKey
# ============================================================

log_stage("Join fact with gold.dim_product")

df_fact_with_prod = (
    df_fact_with_cust
    .join(
        df_dim_prod.select(
            "ProductKey",
            "products",
            "Product_Category",
            "Product_Brand",
            "Product_Type"
        ).alias("p"),
        on=(
            (col("f.products") == col("p.products")) &
            (col("f.Product_Category") == col("p.Product_Category")) &
            (col("f.Product_Brand") == col("p.Product_Brand")) &
            (col("f.Product_Type") == col("p.Product_Type"))
        ),
        how="left"
    )
)

log("Joins with all dimensions completed.")

# ============================================================
# 5. Derive measures (including Discount if possible)
# ============================================================

log_stage("Derive measures for Gold fact")

df_enriched = df_fact_with_prod

# If there is a Discount column already - just use it,
# otherwise compute a simple placeholder (0.0)
if "Discount" in df_enriched.columns:
    df_enriched = df_enriched.withColumn(
        "Discount",
        col("Discount").cast("double")
    )
else:
    df_enriched = df_enriched.withColumn(
        "Discount",
        F.lit(0.0)
    )

# ============================================================
# 6. Build final Gold fact projection
# ============================================================

log_stage("Select final Gold fact columns")

# Count rows with missing foreign keys for monitoring
rows_missing_datekey = df_enriched.filter(col("DateKey").isNull()).count()
rows_missing_custkey = df_enriched.filter(col("CustomerKey").isNull()).count()
rows_missing_prodkey = df_enriched.filter(col("ProductKey").isNull()).count()

log(f"Rows with NULL DateKey     : {rows_missing_datekey}")
log(f"Rows with NULL CustomerKey : {rows_missing_custkey}")
log(f"Rows with NULL ProductKey  : {rows_missing_prodkey}")

# We keep all rows for now, even with missing keys, but log the counts above
df_fact_gold = df_enriched.select(
    # Foreign keys
    col("DateKey").cast("int"),
    col("CustomerKey").cast("int"),
    col("ProductKey").cast("int"),

    # Degenerate and business keys
    col("f.TransactionID").alias("TransactionID"),
    col("f.CustomerID").alias("CustomerID"),
    col("f.products").alias("ProductNameRaw"),

    # Measures
    col("f.Total_Purchases").cast("int").alias("Total_Purchases"),
    col("f.Amount").cast("double").alias("Amount"),
    col("f.Total_Amount").cast("double").alias("Total_Amount"),
    col("Discount").alias("Discount"),

    # Optional additional measures
    col("f.Ratings").cast("double").alias("Ratings"),

    # Degenerate attributes from the order
    col("f.OrderDate"),
    col("f.OrderDateTime"),
    col("f.OrderHour"),
    col("f.Order_Status").alias("OrderStatus"),
    col("f.Payment_Method").alias("PaymentMethod"),
    col("f.Shipping_Method").alias("ShippingMethod"),
    col("f.Feedback").alias("FeedbackText"),

    # Product context
    col("f.Product_Category"),
    col("f.Product_Brand"),
    col("f.Product_Type"),

    # Customer context (denormalized for convenience)
    col("f.City"),
    col("f.State"),
    col("f.Country"),
    col("f.Customer_Segment"),

    # Lineage
    col("f.ingestion_timestamp"),
    col("f.source_file")
)

rows_out = df_fact_gold.count()
log(f"Rows in Gold fact after joins: {rows_out}")

# ============================================================
# 7. Write Gold fact table
# ============================================================

log_stage("Write Gold fact table")

(
    df_fact_gold
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_FACT_TABLE)
)

log(f"Gold fact table '{GOLD_FACT_TABLE}' written successfully.")

# ============================================================
# 8. Log ETL step metrics
# ============================================================

log_stage("Log ETL step metrics for Gold fact")

finished_at = datetime.now(timezone.utc)

# Here we treat all rows as kept (no explicit rejected or duplicates on Gold fact)
rows_rejected = 0
rows_duplicates = 0

write_etl_step_log(
    step_name=STEP_NAME,
    source_table=FACT_SOURCE_TABLE,
    target_table=GOLD_FACT_TABLE,
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

log_stage("Gold fact sales notebook completed successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
