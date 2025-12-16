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

#Gold Layer final set for Reports
#16/12/25

from pyspark.sql.functions import col, lag, when, lit
from pyspark.sql.window import Window

# --- 1. Load the history table (Silver Layer) ---
df = spark.table("top_20_population_silver")

# --- 2. Filter Out Duplicate Consecutive Population Values (Fixing the 'Same Value' Issue) ---
window_spec = Window.partitionBy("Country_or_dependency").orderBy("Scrape_Timestamp")

df_with_prev_val = df.withColumn(
    "Prev_Population_Raw", 
    lag(col("Population_2025"), 1).over(window_spec)
)

# Keep only rows where population has changed or it's the first row (NULL)
df_unique_population = df_with_prev_val.filter(
    (col("Population_2025") != col("Prev_Population_Raw")) | 
    col("Prev_Population_Raw").isNull()
).drop("Prev_Population_Raw")


# --- 3. Calculate Net Change on the Cleaned Data ---

# Re-run LAG on the unique values to find the actual change between updates
window_spec_clean = Window.partitionBy("Country_or_dependency").orderBy("Scrape_Timestamp")

df_calculated = df_unique_population.withColumn(
    "Previous_Population", 
    lag(col("Population_2025"), 1).over(window_spec_clean)
)

df_calculated = df_calculated.withColumn(
    "Net_Population_Change_Snapshot", 
    col("Population_2025") - col("Previous_Population")
)


# --- 4. Overwrite the Gold Layer Table for Power BI ---
df_calculated.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("top_20_population_gold")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, max

GOLD_TABLE_NAME = "top_20_population_gold" # Checking the SILVER table this time

print(f"--- Re-Checking Maximum Scrape Timestamp in {GOLD_TABLE_NAME} ---")

# Find the Absolute Latest Timestamp
latest_timestamp_df = spark.table(GOLD_TABLE_NAME).agg(max(col("Scrape_Timestamp")).alias("LatestTime"))
latest_time = latest_timestamp_df.collect()[0]['LatestTime']

print(f"\n✅ Latest Timestamp AFTER Run: {latest_time}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
