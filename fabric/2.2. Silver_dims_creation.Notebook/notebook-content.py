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

# # 2.2 Silver - Dimension Tables Creation
# 
# This notebook builds Silver-level dimension tables from `silver.retail_clean`:
# 
# - `dim_date`: continuous calendar based on `OrderDate`.
# - `dim_customer`: unique customers with enriched attributes.
# - `dim_product`: unique products by product attributes.
# 
# All dimensions are written into the `silver` schema:
# 
# - `silver.dim_date`
# - `silver.dim_customer`
# - `silver.dim_product`
# 
# Design notes:
# 
# - Source is the cleaned Silver fact table `silver.retail_clean`.
# - No aggregations for facts here. Only dimension-style rollups and attributes.
# - Each dimension is fully rebuilt on every run using `mode("overwrite")` for this demo.
# - Row counts and data quality metrics are logged to the meta tables.


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
from pyspark.sql.window import Window
from datetime import datetime, timezone

log_stage("Notebook start - Silver dimensions creation")
log(f"Environment: {environment}, DEBUG={DEBUG}, version={CONFIG_VERSION}, run_id={run_id}")

FACT_TABLE_NAME = SILVER_FACT_RETAIL
DIM_CUSTOMER_TABLE_NAME = SILVER_DIM_CUSTOMER
DIM_PRODUCT_TABLE_NAME = SILVER_DIM_PRODUCT

log(f"Source fact table      : {FACT_TABLE_NAME}")
log(f"Target dim_customer    : {DIM_CUSTOMER_TABLE_NAME}")
log(f"Target dim_product     : {DIM_PRODUCT_TABLE_NAME}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {META_SCHEMA}")

# ============================================================
# 1. Load Silver fact
# ============================================================

log_stage("Read Silver fact table")

df_fact = spark.read.table(FACT_TABLE_NAME)
fact_rows = df_fact.count()
log(f"Rows in Silver fact: {fact_rows}")

if fact_rows == 0:
    raise ValueError(f"Fact table {FACT_TABLE_NAME} is empty. Cannot build dimensions.")

# Helper to log per dimension step
def log_dim_step(step_name: str, target_table: str, rows_out: int, started_at, finished_at):
    write_etl_step_log(
        step_name=step_name,
        source_table=FACT_TABLE_NAME,
        target_table=target_table,
        rows_in=fact_rows,
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
# 2. Build dim_customer
# ============================================================

log_stage("Build dim_customer")

started_dim_customer = datetime.now(timezone.utc)

customer_cols = [
    "CustomerID",
    "Name",
    "Email",
    "Phone",
    "Address",
    "City",
    "State",
    "Zipcode",
    "Country",
    "Age",
    "Gender",
    "Income",
    "Customer_Segment",
    "OrderDate",
    "Total_Amount"
]

df_cust_src = df_fact.select(*[c for c in customer_cols if c in df_fact.columns])

# Aggregate metrics per customer
df_cust_agg = (
    df_cust_src
    .groupBy("CustomerID")
    .agg(
        F.min("OrderDate").alias("FirstOrderDate"),
        F.max("OrderDate").alias("LastOrderDate"),
        F.count(F.lit(1)).alias("OrderCount"),
        F.sum("Total_Amount").alias("TotalRevenue")
    )
)

# Latest non null attributes per customer based on last order date
w_cust = Window.partitionBy("CustomerID").orderBy(col("OrderDate").desc())

df_cust_latest = (
    df_cust_src
    .withColumn("rk", F.row_number().over(w_cust))
    .filter(col("rk") == 1)
    .drop("rk", "Total_Amount")
)

df_dim_customer = (
    df_cust_latest
    .join(df_cust_agg, on="CustomerID", how="left")
)

# Normalize NULLs for CustomerID if any, as a safety net
df_dim_customer = df_dim_customer.withColumn(
    "CustomerID",
    F.coalesce(col("CustomerID"), F.lit(NULL_LITERAL))
)

dim_customer_rows = df_dim_customer.count()
log(f"Rows in dim_customer: {dim_customer_rows}")

(
    df_dim_customer
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(DIM_CUSTOMER_TABLE_NAME)
)

finished_dim_customer = datetime.now(timezone.utc)
log(f"dim_customer written to {DIM_CUSTOMER_TABLE_NAME}")

log_dim_step(
    step_name="silver_dim_customer",
    target_table=DIM_CUSTOMER_TABLE_NAME,
    rows_out=dim_customer_rows,
    started_at=started_dim_customer,
    finished_at=finished_dim_customer
)

# ============================================================
# 3. Build dim_product
# ============================================================

log_stage("Build dim_product")

started_dim_product = datetime.now(timezone.utc)

product_cols = [
    "products",
    "Product_Category",
    "Product_Brand",
    "Product_Type",
    "Total_Amount"
]

df_prod_src = df_fact.select(*[c for c in product_cols if c in df_fact.columns])

# Replace NULLs with NULL_LITERAL for key product attributes
df_prod_src = (
    df_prod_src
    .withColumn("products", F.coalesce(col("products"), F.lit(NULL_LITERAL)))
    .withColumn("Product_Category", F.coalesce(col("Product_Category"), F.lit(NULL_LITERAL)))
    .withColumn("Product_Brand", F.coalesce(col("Product_Brand"), F.lit(NULL_LITERAL)))
    .withColumn("Product_Type", F.coalesce(col("Product_Type"), F.lit(NULL_LITERAL)))
)

df_dim_product = (
    df_prod_src
    .groupBy("products", "Product_Category", "Product_Brand", "Product_Type")
    .agg(
        F.count(F.lit(1)).alias("OrderCount"),
        F.sum("Total_Amount").alias("TotalRevenue")
    )
)

dim_product_rows = df_dim_product.count()
log(f"Rows in dim_product: {dim_product_rows}")

(
    df_dim_product
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(DIM_PRODUCT_TABLE_NAME)
)

finished_dim_product = datetime.now(timezone.utc)
log(f"dim_product written to {DIM_PRODUCT_TABLE_NAME}")

log_dim_step(
    step_name="silver_dim_product",
    target_table=DIM_PRODUCT_TABLE_NAME,
    rows_out=dim_product_rows,
    started_at=started_dim_product,
    finished_at=finished_dim_product
)

# ============================================================
# 4. Final log
# ============================================================

log_stage("Silver dimensions creation completed successfully")
log(f"dim_customer rows : {dim_customer_rows}")
log(f"dim_product rows  : {dim_product_rows}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
