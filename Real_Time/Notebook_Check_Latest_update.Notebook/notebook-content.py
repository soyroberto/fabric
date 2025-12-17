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

#check silver timestamp
from pyspark.sql.functions import col, max

GOLD_TABLE_NAME = "top_20_population_silver" # Checking the SILVER table this time

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

# CELL ********************

#Check Gold timestamp
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
