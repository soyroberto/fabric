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
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

# Get Spark Session (if not already running)
spark = SparkSession.builder.appName("LatestDataCheck").getOrCreate()

# --- 1. Define the table to check ---
GOLD_TABLE_NAME = "top_20_population_silver"

print(f"--- Checking Maximum Scrape Timestamp in {GOLD_TABLE_NAME} ---")

# --- 2. Find the Absolute Latest Timestamp ---
latest_timestamp_df = spark.table(GOLD_TABLE_NAME).agg(max(col("Scrape_Timestamp")).alias("LatestTime"))

# Extract the value for clear display
latest_time = latest_timestamp_df.collect()[0]['LatestTime']

if latest_time:
    # --- 3. Display the Latest Time and the Data Associated with It ---
    print(f"\n✅ Absolute Latest Data Timestamp Found: {latest_time}")
    
    # Filter the table to show all rows recorded at that exact latest timestamp
    latest_data = spark.table(GOLD_TABLE_NAME).filter(col("Scrape_Timestamp") == latest_time)
    
    print("\n--- Latest Population Snapshot (All Countries) ---")
    latest_data.orderBy(col("Population_2025").desc()).show(20, truncate=False)
    
else:
    print("❌ Error: No data found in the table.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
