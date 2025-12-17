# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ff331aa0-30ce-4f6a-bcb5-1fdb96ca679b",
# META       "default_lakehouse_name": "Real_timeLH",
# META       "default_lakehouse_workspace_id": "f8a8e7cb-a246-415d-a682-ccb1108926ba",
# META       "known_lakehouses": [
# META         {
# META           "id": "ff331aa0-30ce-4f6a-bcb5-1fdb96ca679b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import col, max
from pyspark.sql import SparkSession

GOLD_TABLE_NAME_NEW = "top_20_population_gold_v2" 

print(f"--- ABSOLUTE FINAL VERIFICATION: {GOLD_TABLE_NAME_NEW} ---")

# Run this check only if spark is not defined, otherwise assume it is
if 'spark' not in locals():
    spark = SparkSession.builder.appName("FinalCheck").getOrCreate()

latest_timestamp_df = spark.table(GOLD_TABLE_NAME_NEW).agg(max(col("Scrape_Timestamp")).alias("LatestTime"))
latest_time = latest_timestamp_df.collect()[0]['LatestTime']

print(f"\n✅ Latest Timestamp on NEW GOLD Table: {latest_time}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, max

GOLD_TABLE_NAME = "top_20_population_silver" 

print(f"--- FINAL VERIFICATION AFTER PIPELINE RUN ---")

latest_timestamp_df = spark.table(GOLD_TABLE_NAME).agg(max(col("Scrape_Timestamp")).alias("LatestTime"))
latest_time = latest_timestamp_df.collect()[0]['LatestTime']

print(f"\n✅ Latest Timestamp on GOLD Table: {latest_time}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, max
from pyspark.sql import SparkSession

# Ensure you are using the automatically created 'spark' object
# If you get a session error, open a brand new notebook for this check.

GOLD_TABLE_NAME_NEW = "top_20_population_gold_v2" 

print(f"--- VERIFYING NEW TABLE: {GOLD_TABLE_NAME_NEW} ---")

try:
    latest_timestamp_df = spark.table(GOLD_TABLE_NAME_NEW).agg(max(col("Scrape_Timestamp")).alias("LatestTime"))
    latest_time = latest_timestamp_df.collect()[0]['LatestTime']

    print(f"\n✅ Latest Timestamp on NEW GOLD Table: {latest_time}")
except Exception as e:
    print(f"ERROR: Could not read new table. Check the Lakehouse table list manually. Error: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, max
from pyspark.sql import SparkSession

# Ensure you are using the automatically created 'spark' object
# If you get a session error, open a brand new notebook for this check.

print(f"--- CHECKING SILVER SOURCE TABLE ---")

try:
    silver_df = spark.table("top_20_population_silver")
    
    # Find the Absolute Latest Timestamp in Silver
    latest_timestamp_df = silver_df.agg(max(col("Scrape_Timestamp")).alias("LatestTime"))
    latest_time = latest_timestamp_df.collect()[0]['LatestTime']

    print(f"\n✅ Latest Timestamp on SILVER Table: {latest_time}")

    # Check the count to ensure data is being appended
    total_count = silver_df.count()
    print(f"Total rows in Silver table: {total_count}")

except Exception as e:
    print(f"ERROR: Could not read Silver table. Error: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- This command will show the last 3 versions of the Gold_v2 table
# MAGIC DESCRIBE HISTORY top_20_population_gold_v2
# MAGIC LIMIT 30;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- This command will show the last 3 versions of the Gold_v2 table
# MAGIC DESCRIBE HISTORY top_20_population_silver
# MAGIC LIMIT 3;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

print(f"--- Examining Data in top_20_population_gold_v2 ---")

# Pull the last 5 rows, ordered by the problematic timestamp column
spark.table("top_20_population_gold_v2") \
    .select("Country_or_dependency", "Population_2025", "Net_Population_Change_Snapshot", "Scrape_Timestamp") \
    .orderBy(F.col("Scrape_Timestamp").desc()) \
    .limit(5) \
    .show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

print(f"--- Examining Data in top_20_population_gold_v2 ---")

# Pull the last 5 rows, ordered by the problematic timestamp column
spark.table("top_20_population_gold_v2") \
    .select('*') \
    .orderBy(F.col("Scrape_Timestamp").desc()) \
    .limit(5) \
    .show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

print("--- 1. SILVER CHECK ---")
try:
    spark.table("top_20_population_silver") \
        .select("Scrape_Timestamp") \
        .distinct() \
        .orderBy(col("Scrape_Timestamp").desc()) \
        .show(5)
except Exception as e:
    print(f"Silver Error: {e}")

print("--- 2. GOLD CHECK ---")
try:
    spark.table("top_20_population_gold_v2") \
        .select("Scrape_Timestamp") \
        .distinct() \
        .orderBy(col("Scrape_Timestamp").desc()) \
        .show(5)
except Exception as e:
    print(f"Gold Error: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("top_20_population_gold_v2").groupBy("Scrape_Timestamp").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
