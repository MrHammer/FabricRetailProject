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

# # 3.1 Gold - Build Dimensions
# 
# This notebook builds Gold-level dimension tables from the Silver layer:
# 
# - `gold.dim_date`     - enterprise calendar dimension
# - `gold.dim_customer` - conformed customer dimension with surrogate keys
# - `gold.dim_product`  - conformed product dimension with surrogate keys
# 
# **Source tables:**
# 
# - `silver.retail_clean`     (fact, for date range and metrics)
# - `silver.dim_customer`     (base customer attributes and metrics)
# - `silver.dim_product`      (base product attributes and metrics)
# 
# **Target tables:**
# 
# - `gold.dim_date`
# - `gold.dim_customer`
# - `gold.dim_product`


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

log_stage("Notebook start - Gold dimensions creation")
log(f"Environment: {environment}, DEBUG={DEBUG}, version={CONFIG_VERSION}, run_id={run_id}")

FACT_TABLE_NAME = SILVER_FACT_RETAIL
SILVER_DIM_CUSTOMER_TABLE = SILVER_DIM_CUSTOMER
SILVER_DIM_PRODUCT_TABLE = SILVER_DIM_PRODUCT

GOLD_DIM_DATE_TABLE = GOLD_DIM_DATE
GOLD_DIM_CUSTOMER_TABLE = GOLD_DIM_CUSTOMER
GOLD_DIM_PRODUCT_TABLE = GOLD_DIM_PRODUCT

log(f"Source fact table          : {FACT_TABLE_NAME}")
log(f"Source silver dim_customer : {SILVER_DIM_CUSTOMER_TABLE}")
log(f"Source silver dim_product  : {SILVER_DIM_PRODUCT_TABLE}")
log(f"Target gold dim_date       : {GOLD_DIM_DATE_TABLE}")
log(f"Target gold dim_customer   : {GOLD_DIM_CUSTOMER_TABLE}")
log(f"Target gold dim_product    : {GOLD_DIM_PRODUCT_TABLE}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {META_SCHEMA}")

# ============================================================
# 1. Load Silver fact and Silver dimensions
# ============================================================

log_stage("Read Silver fact and Silver dimensions")

df_fact = spark.read.table(FACT_TABLE_NAME)
fact_rows = df_fact.count()
log(f"Rows in Silver fact: {fact_rows}")

if fact_rows == 0:
    raise ValueError(f"Fact table {FACT_TABLE_NAME} is empty. Cannot build Gold dimensions.")

df_silver_cust = spark.read.table(SILVER_DIM_CUSTOMER_TABLE)
df_silver_prod = spark.read.table(SILVER_DIM_PRODUCT_TABLE)

log(f"Rows in silver.dim_customer: {df_silver_cust.count()}")
log(f"Rows in silver.dim_product : {df_silver_prod.count()}")

# Helper to log per dimension step
def log_dim_step(step_name: str, source_table: str, target_table: str, rows_out: int, started_at, finished_at):
    write_etl_step_log(
        step_name=step_name,
        source_table=source_table,
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
# 2. Build gold.dim_date
# ============================================================

log_stage("Build gold.dim_date")

started_dim_date = datetime.now(timezone.utc)

bounds = (
    df_fact
    .select(
        F.min("OrderDate").alias("min_date"),
        F.max("OrderDate").alias("max_date")
    )
    .collect()[0]
)

min_date = bounds["min_date"]
max_date = bounds["max_date"]

if min_date is None or max_date is None:
    raise ValueError("No OrderDate found in Silver fact. Cannot build gold.dim_date.")

log(f"Date bounds from fact: {min_date} to {max_date}")

days_between = (max_date - min_date).days

df_date = (
    spark
    .range(0, days_between + 1)
    .withColumn(
        "Date",
        F.expr(f"date_add(to_date('{min_date}'), cast(id as int))")
    )
    .drop("id")
)

df_dim_date = (
    df_date
    .withColumn("DateKey", F.date_format(col("Date"), "yyyyMMdd").cast("int"))
    .withColumn("Year", F.year("Date"))
    .withColumn("Month", F.month("Date"))
    .withColumn("Day", F.dayofmonth("Date"))
    .withColumn("Quarter", F.quarter("Date"))
    .withColumn("YearMonth", F.date_format(col("Date"), "yyyyMM").cast("int"))
    .withColumn("YearWeek", F.concat(F.year("Date"), F.lit("-"), F.weekofyear("Date")))
    .withColumn("MonthName", F.date_format(col("Date"), "MMMM"))
    .withColumn("DayOfWeek", F.date_format(col("Date"), "EEEE"))
    .withColumn("DayOfWeekNumber", F.dayofweek("Date"))
    .withColumn(
        "IsWeekend",
        col("DayOfWeekNumber").isin(1, 7).cast("boolean")
    )
)

dim_date_rows = df_dim_date.count()
log(f"Rows in gold.dim_date: {dim_date_rows}")

(
    df_dim_date
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_DIM_DATE_TABLE)
)

finished_dim_date = datetime.now(timezone.utc)
log(f"gold.dim_date written to {GOLD_DIM_DATE_TABLE}")

log_dim_step(
    step_name="gold_dim_date",
    source_table=FACT_TABLE_NAME,
    target_table=GOLD_DIM_DATE_TABLE,
    rows_out=dim_date_rows,
    started_at=started_dim_date,
    finished_at=finished_dim_date
)

# ============================================================
# 3. Build gold.dim_customer (add surrogate key)
# ============================================================

log_stage("Build gold.dim_customer")

started_dim_customer = datetime.now(timezone.utc)

# Make sure CustomerID is not NULL
df_cust = df_silver_cust.withColumn(
    "CustomerID",
    F.coalesce(col("CustomerID"), F.lit(NULL_LITERAL))
)

# Generate surrogate CustomerKey - stable within a full rebuild run
w_cust_key = Window.orderBy(col("CustomerID").asc())

df_dim_customer = (
    df_cust
    .withColumn("CustomerKey", F.row_number().over(w_cust_key))
)

# Reorder columns: key first, then CustomerID and attributes
cust_cols = [c for c in df_dim_customer.columns if c not in ["CustomerKey"]]
df_dim_customer = df_dim_customer.select(["CustomerKey"] + cust_cols)

dim_customer_rows = df_dim_customer.count()
log(f"Rows in gold.dim_customer: {dim_customer_rows}")

(
    df_dim_customer
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_DIM_CUSTOMER_TABLE)
)

finished_dim_customer = datetime.now(timezone.utc)
log(f"gold.dim_customer written to {GOLD_DIM_CUSTOMER_TABLE}")

log_dim_step(
    step_name="gold_dim_customer",
    source_table=SILVER_DIM_CUSTOMER_TABLE,
    target_table=GOLD_DIM_CUSTOMER_TABLE,
    rows_out=dim_customer_rows,
    started_at=started_dim_customer,
    finished_at=finished_dim_customer
)

# ============================================================
# 4. Build gold.dim_product (add surrogate key)
# ============================================================

log_stage("Build gold.dim_product")

started_dim_product = datetime.now(timezone.utc)

# Make sure product attributes are not NULL by using NULL_LITERAL
df_prod = (
    df_silver_prod
    .withColumn("products", F.coalesce(col("products"), F.lit(NULL_LITERAL)))
    .withColumn("Product_Category", F.coalesce(col("Product_Category"), F.lit(NULL_LITERAL)))
    .withColumn("Product_Brand", F.coalesce(col("Product_Brand"), F.lit(NULL_LITERAL)))
    .withColumn("Product_Type", F.coalesce(col("Product_Type"), F.lit(NULL_LITERAL)))
)

# Natural business key for ordering - combination of attributes
w_prod_key = Window.orderBy(
    col("products").asc(),
    col("Product_Category").asc(),
    col("Product_Brand").asc(),
    col("Product_Type").asc()
)

df_dim_product = (
    df_prod
    .withColumn("ProductKey", F.row_number().over(w_prod_key))
)

prod_cols = [c for c in df_dim_product.columns if c not in ["ProductKey"]]
df_dim_product = df_dim_product.select(["ProductKey"] + prod_cols)

dim_product_rows = df_dim_product.count()
log(f"Rows in gold.dim_product: {dim_product_rows}")

(
    df_dim_product
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_DIM_PRODUCT_TABLE)
)

finished_dim_product = datetime.now(timezone.utc)
log(f"gold.dim_product written to {GOLD_DIM_PRODUCT_TABLE}")

log_dim_step(
    step_name="gold_dim_product",
    source_table=SILVER_DIM_PRODUCT_TABLE,
    target_table=GOLD_DIM_PRODUCT_TABLE,
    rows_out=dim_product_rows,
    started_at=started_dim_product,
    finished_at=finished_dim_product
)

# ============================================================
# 5. Final log
# ============================================================

log_stage("Gold dimensions creation completed successfully")
log(f"gold.dim_date rows     : {dim_date_rows}")
log(f"gold.dim_customer rows : {dim_customer_rows}")
log(f"gold.dim_product rows  : {dim_product_rows}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
