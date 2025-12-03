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

# # 3.3 Gold - Build Sales Aggregates
# 
# This notebook builds monthly aggregate fact tables on top of `gold.fact_sales`:
# 
# - `gold.fact_sales_monthly`
# - `gold.fact_sales_by_customer_month`
# - `gold.fact_sales_by_product_month`
# 
# Each table provides:
# 
# - Monthly grain based on `OrderDate`.
# - Measures:
#   - `TotalQuantity`       - sum of `Total_Purchases`
#   - `TotalSalesAmount`    - sum of `Total_Amount`
#   - `TotalDiscount`       - sum of `Discount`
#   - `NetSalesAmount`      - `TotalSalesAmount` minus `TotalDiscount`
#   - `OrdersCount`         - count of distinct transactions
#   - `AvgOrderValue`       - `NetSalesAmount` divided by `OrdersCount`


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

STEP_NAME = "gold_sales_aggregates"

log_stage("Notebook start - Gold sales aggregates")
log(f"Environment: {environment}, DEBUG={DEBUG}, version={CONFIG_VERSION}, run_id={run_id}")

# Source and target tables
FACT_SALES_TABLE = GOLD_FACT_SALES

AGG_MONTHLY_TABLE = f"{GOLD_SCHEMA}.fact_sales_monthly"
AGG_CUST_MONTHLY_TABLE = f"{GOLD_SCHEMA}.fact_sales_by_customer_month"
AGG_PROD_MONTHLY_TABLE = f"{GOLD_SCHEMA}.fact_sales_by_product_month"

log(f"Source Gold fact table             : {FACT_SALES_TABLE}")
log(f"Target gold.fact_sales_monthly     : {AGG_MONTHLY_TABLE}")
log(f"Target gold.fact_sales_by_customer : {AGG_CUST_MONTHLY_TABLE}")
log(f"Target gold.fact_sales_by_product  : {AGG_PROD_MONTHLY_TABLE}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {META_SCHEMA}")

started_at = datetime.now(timezone.utc)

# ============================================================
# 1. Load Gold fact
# ============================================================

log_stage("Read gold.fact_sales")

df_fact = spark.read.table(FACT_SALES_TABLE)
rows_in = df_fact.count()
log(f"Rows in gold.fact_sales: {rows_in}")

if rows_in == 0:
    raise ValueError(f"Gold fact table {FACT_SALES_TABLE} is empty. Cannot build aggregates.")

# Make sure numeric fields are correctly typed
df_fact = (
    df_fact
    .withColumn("Total_Purchases", col("Total_Purchases").cast("int"))
    .withColumn("Total_Amount", col("Total_Amount").cast("double"))
    .withColumn("Discount", col("Discount").cast("double"))
)

# Enrich with Year, Month, YearMonth for grouping
df_fact_enriched = (
    df_fact
    .withColumn("Year", F.year("OrderDate"))
    .withColumn("Month", F.month("OrderDate"))
    .withColumn("YearMonth", F.date_format(col("OrderDate"), "yyyyMM").cast("int"))
)

# ============================================================
# 2. Helper functions
# ============================================================

log_stage("Prepare helper functions for aggregates")

def add_standard_measures(grouped):
    """
    Given a GroupedData object, calculate standard sales measures:
    - TotalQuantity
    - TotalSalesAmount
    - TotalDiscount
    - NetSalesAmount
    - OrdersCount
    - AvgOrderValue (NetSalesAmount per order)
    """
    df_agg = (
        grouped
        .agg(
            F.sum("Total_Purchases").alias("TotalQuantity"),
            F.sum("Total_Amount").alias("TotalSalesAmount"),
            F.sum("Discount").alias("TotalDiscount"),
            F.countDistinct("TransactionID").alias("OrdersCount")
        )
    )

    df_agg = (
        df_agg
        .withColumn(
            "NetSalesAmount",
            col("TotalSalesAmount") - col("TotalDiscount")
        )
        .withColumn(
            "AvgOrderValue",
            F.when(col("OrdersCount") > 0,
                   col("NetSalesAmount") / col("OrdersCount"))
             .otherwise(F.lit(0.0))
        )
    )

    return df_agg


def log_agg_step(step_name: str, target_table: str, rows_out: int, started_at, finished_at):
    """
    Log aggregated step into ETL_STEP_LOG_TABLE.
    Note: rows_in is taken from parent fact table row count.
    """
    write_etl_step_log(
        step_name=step_name,
        source_table=FACT_SALES_TABLE,
        target_table=target_table,
        rows_in=rows_in,
        rows_out=rows_out,
        rows_rejected=0,
        rows_duplicates=0,
        load_date=load_date,
        status="Success",
        error_message=None,
        started_at=started_at,
        finished_at=finished_at
    )

# ============================================================
# 3. Build gold.fact_sales_monthly (overall by month)
# ============================================================

log_stage("Build gold.fact_sales_monthly")

started_monthly = datetime.now(timezone.utc)

grouped_monthly = df_fact_enriched.groupBy("Year", "Month", "YearMonth")
df_monthly = add_standard_measures(grouped_monthly)

rows_monthly = df_monthly.count()
log(f"Rows in gold.fact_sales_monthly: {rows_monthly}")

(
    df_monthly
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(AGG_MONTHLY_TABLE)
)

finished_monthly = datetime.now(timezone.utc)
log(f"gold.fact_sales_monthly written to {AGG_MONTHLY_TABLE}")

log_agg_step(
    step_name="gold_fact_sales_monthly",
    target_table=AGG_MONTHLY_TABLE,
    rows_out=rows_monthly,
    started_at=started_monthly,
    finished_at=finished_monthly
)

# ============================================================
# 4. Build gold.fact_sales_by_customer_month
# ============================================================

log_stage("Build gold.fact_sales_by_customer_month")

started_cust_month = datetime.now(timezone.utc)

grouped_cust_month = df_fact_enriched.groupBy(
    "Year",
    "Month",
    "YearMonth",
    "CustomerKey",
    "CustomerID",
    "Customer_Segment",
    "City",
    "State",
    "Country"
)

df_cust_month = add_standard_measures(grouped_cust_month)

rows_cust_month = df_cust_month.count()
log(f"Rows in gold.fact_sales_by_customer_month: {rows_cust_month}")

(
    df_cust_month
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(AGG_CUST_MONTHLY_TABLE)
)

finished_cust_month = datetime.now(timezone.utc)
log(f"gold.fact_sales_by_customer_month written to {AGG_CUST_MONTHLY_TABLE}")

log_agg_step(
    step_name="gold_fact_sales_by_customer_month",
    target_table=AGG_CUST_MONTHLY_TABLE,
    rows_out=rows_cust_month,
    started_at=started_cust_month,
    finished_at=finished_cust_month
)

# ============================================================
# 5. Build gold.fact_sales_by_product_month
# ============================================================

log_stage("Build gold.fact_sales_by_product_month")

started_prod_month = datetime.now(timezone.utc)

grouped_prod_month = df_fact_enriched.groupBy(
    "Year",
    "Month",
    "YearMonth",
    "ProductKey",
    "Product_Category",
    "Product_Brand",
    "Product_Type"
)

df_prod_month = add_standard_measures(grouped_prod_month)

rows_prod_month = df_prod_month.count()
log(f"Rows in gold.fact_sales_by_product_month: {rows_prod_month}")

(
    df_prod_month
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(AGG_PROD_MONTHLY_TABLE)
)

finished_prod_month = datetime.now(timezone.utc)
log(f"gold.fact_sales_by_product_month written to {AGG_PROD_MONTHLY_TABLE}")

log_agg_step(
    step_name="gold_fact_sales_by_product_month",
    target_table=AGG_PROD_MONTHLY_TABLE,
    rows_out=rows_prod_month,
    started_at=started_prod_month,
    finished_at=finished_prod_month
)

# ============================================================
# 6. Final log
# ============================================================

log_stage("Gold sales aggregates completed successfully")
log(f"fact_sales_monthly rows          : {rows_monthly}")
log(f"fact_sales_by_customer_month rows: {rows_cust_month}")
log(f"fact_sales_by_product_month rows : {rows_prod_month}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
