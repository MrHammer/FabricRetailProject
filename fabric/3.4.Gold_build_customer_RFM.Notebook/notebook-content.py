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

# # 3.4 Gold - Build Customer RFM
# 
# This notebook builds the customer level RFM table on top of `gold.fact_sales`.
# 
# RFM metrics:
# 
# - **Recency**  - number of days since the last purchase.
# - **Frequency** - number of distinct orders.
# - **Monetary**  - total sales amount.
# 
# It also calculates R, F, and M scores (1-5) based on quintiles and an overall RFM score.
# 
# **Source:**
# 
# - `gold.fact_sales`
# 
# **Target:**
# 
# - `gold.fact_customer_rfm`


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

%run "./config"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run "./logging"

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

STEP_NAME = "gold_customer_rfm"

log_stage("Notebook start - Gold customer RFM")
log(f"Environment: {environment}, DEBUG={DEBUG}, version={CONFIG_VERSION}, run_id={run_id}")

FACT_SALES_TABLE = GOLD_FACT_SALES
RFM_TABLE = f"{GOLD_SCHEMA}.fact_customer_rfm"

log(f"Source Gold fact table : {FACT_SALES_TABLE}")
log(f"Target RFM table       : {RFM_TABLE}")

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
    raise ValueError(f"Gold fact table {FACT_SALES_TABLE} is empty. Cannot build RFM.")

# Ensure CustomerKey and CustomerID are not NULL
df_fact = (
    df_fact
    .withColumn("CustomerKey", col("CustomerKey").cast("int"))
    .withColumn(
        "CustomerID",
        F.coalesce(col("CustomerID"), F.lit(NULL_LITERAL))
    )
)

# ============================================================
# 2. Compute base RFM metrics per customer
# ============================================================

log_stage("Compute base RFM metrics per customer")

# Use max OrderDate in fact as reference date
bounds = (
    df_fact
    .select(F.max("OrderDate").alias("max_date"))
    .collect()[0]
)

reference_date = bounds["max_date"]
if reference_date is None:
    raise ValueError("OrderDate is NULL for all rows. Cannot compute Recency.")

log(f"Reference date for Recency: {reference_date}")

df_rfm_base = (
    df_fact
    .groupBy("CustomerKey", "CustomerID")
    .agg(
        F.min("OrderDate").alias("FirstOrderDate"),
        F.max("OrderDate").alias("LastOrderDate"),
        F.countDistinct("TransactionID").alias("Frequency"),
        F.sum("Total_Amount").alias("Monetary")
    )
    .withColumn(
        "RecencyDays",
        F.datediff(F.lit(reference_date), col("LastOrderDate"))
    )
)

rows_rfm_base = df_rfm_base.count()
log(f"Rows in base RFM table: {rows_rfm_base}")

# ============================================================
# 3. Calculate R, F, M scores using quintiles
# ============================================================

log_stage("Calculate R, F, M scores (quintiles)")

# Window definitions for ntile
w_recency = Window.orderBy(col("RecencyDays").asc())     # smaller RecencyDays is better
w_frequency = Window.orderBy(col("Frequency").asc())     # larger Frequency is better, will invert
w_monetary = Window.orderBy(col("Monetary").asc())       # larger Monetary is better, will invert

df_rfm_scored = (
    df_rfm_base
    # raw ntile scores where 1 is lowest, 5 is highest
    .withColumn("RecencyRankRaw", F.ntile(5).over(w_recency))
    .withColumn("FrequencyRankRaw", F.ntile(5).over(w_frequency))
    .withColumn("MonetaryRankRaw", F.ntile(5).over(w_monetary))
    # convert to classic RFM scores: 1 worst, 5 best
    # for Recency: smaller RecencyDays is better -> higher score
    .withColumn("R_Score", 6 - col("RecencyRankRaw"))
    # for Frequency and Monetary: higher is better -> invert same way
    .withColumn("F_Score", col("FrequencyRankRaw"))
    .withColumn("M_Score", col("MonetaryRankRaw"))
)

# RFM combined scores
df_rfm_scored = (
    df_rfm_scored
    .withColumn(
        "RFM_Score_Concat",
        F.concat(col("R_Score").cast("string"),
                 col("F_Score").cast("string"),
                 col("M_Score").cast("string"))
    )
    .withColumn(
        "RFM_Score_Sum",
        col("R_Score") + col("F_Score") + col("M_Score")
    )
)

# Optional: simple segmentation by RFM_Score_Sum
df_rfm_scored = (
    df_rfm_scored
    .withColumn(
        "Segment",
        F.when(col("RFM_Score_Sum") >= 13, lit("Champions"))
         .when(col("RFM_Score_Sum") >= 10, lit("Loyal"))
         .when(col("RFM_Score_Sum") >= 7, lit("Potential"))
         .otherwise(lit("Needs Attention"))
    )
)

# ============================================================
# 4. Final projection and write RFM table
# ============================================================

log_stage("Build final RFM projection")

df_rfm_final = df_rfm_scored.select(
    col("CustomerKey").cast("int"),
    col("CustomerID"),
    col("FirstOrderDate"),
    col("LastOrderDate"),
    col("RecencyDays").cast("int"),
    col("Frequency").cast("int"),
    col("Monetary").cast("double"),
    col("R_Score").cast("int"),
    col("F_Score").cast("int"),
    col("M_Score").cast("int"),
    col("RFM_Score_Concat"),
    col("RFM_Score_Sum").cast("int"),
    col("Segment")
)

rows_out = df_rfm_final.count()
log(f"Rows in final RFM table: {rows_out}")

log_stage("Write gold.fact_customer_rfm")

(
    df_rfm_final
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(RFM_TABLE)
)

log(f"RFM table '{RFM_TABLE}' written successfully.")

# ============================================================
# 5. Log ETL step metrics
# ============================================================

log_stage("Log ETL step metrics for RFM")

finished_at = datetime.now(timezone.utc)

write_etl_step_log(
    step_name=STEP_NAME,
    source_table=FACT_SALES_TABLE,
    target_table=RFM_TABLE,
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

log_stage("Gold customer RFM notebook completed successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
