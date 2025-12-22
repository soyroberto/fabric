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

##Gold
import pandas as pd
from io import StringIO
import datetime
from pyspark.sql.window import Window
import pyspark.sql.functions as F

df_silver = spark.table("top_20_population_silver_v3")
window_spec = Window.partitionBy("Country_or_dependency").orderBy("Scrape_Timestamp")

# Calculate Delta and Cast to Whole Numbers (Long)
df_gold_v3 = df_silver.withColumn(
    "Population_2025_Live", F.col("Population_2025_Live").cast("long")
).withColumn(
    "Previous_Pop", F.lag("Population_2025_Live", 1).over(window_spec)
).withColumn(
    "Net_Growth_Since_Last_Run", (F.col("Population_2025_Live") - F.col("Previous_Pop")).cast("long")
)

# Append to build your history
(df_gold_v3.write
  .format("delta")
  .mode("append")
  .option("mergeSchema", "true") # Helps avoid schema errors
  .saveAsTable("top_20_population_gold_v3"))

print("✅ Gold v3 Updated: Numbers rounded and snapshot appended.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
