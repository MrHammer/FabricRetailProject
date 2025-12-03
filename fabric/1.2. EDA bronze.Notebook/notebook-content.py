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

# CELL ********************

df_bronze = spark.table("bronze.retail_transactions")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_bronze.printSchema()
df_bronze.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, sum

null_counts = df_bronze.select([
    sum(col(c).isNull().cast("int")).alias(c) 
    for c in df_bronze.columns
])

display(null_counts)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cols = df_bronze.columns
df_duplicates = (
    df_bronze.groupBy(cols)
    .count()
    .filter("count > 1")
) 

df_duplicates.count() 
   

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_full_duplicates = df_bronze.exceptAll(df_bronze.dropDuplicates())
display(df_full_duplicates)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

numeric_cols = [c for c, t in df_bronze.dtypes if t in ("double", "int", "bigint", "float")]
numeric_cols

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_bronze.select(numeric_cols).describe())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_bronze.select(numeric_cols).summary())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_bronze.select("Amount", "Total_Amount").summary())

display(df_bronze.filter("Amount < 0 OR Total_Amount < 0"))
display(df_bronze.filter("Amount = 0 OR Total_Amount = 0"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Categorical Profiling

# CELL ********************

string_cols = [c for c, t in df_bronze.dtypes if t == "string"]
string_cols

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import approx_count_distinct, col

for c in string_cols:
    count_dist = df_bronze.select(approx_count_distinct(col(c))).first()[0]
    print(f"{c}: approx {count_dist} unique values")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_bronze.select("Date").limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_bronze.select("Date").distinct().limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import min, max

display(
    df_bronze.select(
        min("Date").alias("min_date"),
        max("Date").alias("max_date")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_bronze.select("Time").limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    df_bronze.select(
        min("Time").alias("min_time"),
        max("Time").alias("max_time")
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    df_bronze.filter(
        (df_bronze["Date"].isNull()) | (df_bronze["Date"] == "")
    ).select("Date").limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_bronze.filter((df_bronze["Date"].isNull()) | (df_bronze["Date"] == "")).count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    df_bronze.groupBy("Year")
    .count()
    .orderBy("Year")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    df_bronze.groupBy("Month")
    .count()
    .orderBy("Month")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
 df_bronze.select("Date")
  .distinct()
  .limit(50)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import to_date

display(
    df_bronze.select(
        "Date",
        to_date("Date", "M/d/yyyy").alias("parsed_date")
    ).limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

broken_dates = df_bronze.filter(
    to_date("Date", "M/d/yyyy").isNull()
).count()

print("Broken date records:", broken_dates)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import min, max

display(
    df_bronze.select(
        min("Amount").alias("min_amount"),
        max("Amount").alias("max_amount"),
        min("Total_Amount").alias("min_total_amount"),
        max("Total_Amount").alias("max_total_amount"),
        min("Ratings").alias("min_rating"),
        max("Ratings").alias("max_rating"),
        min("Age").alias("min_age"),
        max("Age").alias("max_age")
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

quantiles = df_bronze.approxQuantile(
    ["Amount", "Total_Amount", "Ratings", "Age"],
    probabilities=[0.25, 0.5, 0.75, 0.95, 0.99],
    relativeError=0.01
)

quantiles


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    df_bronze.orderBy(df_bronze.Amount.desc()).limit(20)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Q99_Amount = df_bronze.approxQuantile("Amount", [0.99], 0.01)[0]

display(
    df_bronze.filter(df_bronze.Amount > Q99_Amount).limit(50)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    df_bronze.select(
        min("Amount").alias("min_amount"),
        max("Amount").alias("max_amount"),
        min("Total_Amount").alias("min_total_amount"),
        max("Total_Amount").alias("max_total_amount"),
        min("Ratings").alias("min_rating"),
        max("Ratings").alias("max_rating"),
        min("Age").alias("min_age"),
        max("Age").alias("max_age")
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, expr

df_check_amount = df_bronze.withColumn(
    "calc_total",
    col("Amount") * col("Total_Purchases")
).withColumn(
    "difference",
    col("Total_Amount") - col("calc_total")
)

display(df_check_amount.select("Amount", "Total_Purchases", "Total_Amount", "calc_total", "difference").limit(20))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_inconsistent_amount = df_check_amount.filter("difference != 0")
df_inconsistent_amount.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import to_date, year, col

df_year_check = df_bronze.withColumn(
    "parsed_date",
    to_date("Date", "M/d/yyyy")
).withColumn(
    "derived_year",
    year(col("parsed_date"))
)

df_wrong_year = df_year_check.filter(col("derived_year") != col("Year"))

df_wrong_year_count = df_wrong_year.count()
df_wrong_year_count


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import month

df_month_check = df_bronze.withColumn(
    "parsed_date",
    to_date("Date", "M/d/yyyy")
).withColumn(
    "derived_month",
    month(col("parsed_date"))
)

df_wrong_month = df_month_check.filter(col("derived_month") != col("Month"))

df_wrong_month_count = df_wrong_month.count()
df_wrong_month_count


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
