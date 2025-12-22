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

#only keep this code
import pandas as pd
from io import StringIO
import datetime
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType

# 1. LOAD BRONZE
df_bronze = spark.table("top_20_population_bronze_v3")

# 2. DEFINE 2025 GROWTH RATES (Yearly %)
# We use a dictionary to map the official 2025 rates to your countries
growth_rates = {
    "India": 0.89, "China": -0.23, "United States": 0.54, 
    "Indonesia": 0.79, "Pakistan": 1.57, "Nigeria": 2.08,
    "Brazil": 0.38, "Bangladesh": 1.22, "Russia": -0.57,
    "Ethiopia": 2.58, "Mexico": 0.83, "Japan": -0.52,
    "Egypt": 1.57, "Philippines": 0.81, "DR Congo": 3.25,
    "Vietnam": 0.6, "Iran": 0.93, "Turkey": 0.24,
    "Germany": -0.56, "Thailand": -0.07
}

# Convert dictionary to a Spark mapping
mapping_expr = F.create_map([F.lit(x) for x in sum(growth_rates.items(), ())])

# 3. CALCULATE LIVE POPULATION
# Formula: Base_Pop + (Base_Pop * (Rate/100) * (Seconds_Elapsed_Today / Total_Seconds_Year))
df_silver = df_bronze.withColumn("Annual_Rate", mapping_expr[F.col("Country_or_dependency")]) \
    .withColumn("Base_Pop", F.col("Population_2025").cast("double")) \
    .withColumn("Seconds_Today", F.hour(F.current_timestamp()) * 3600 + F.minute(F.current_timestamp()) * 60 + F.second(F.current_timestamp())) \
    .withColumn("Population_2025_Live", 
                F.col("Base_Pop") + (F.col("Base_Pop") * (F.col("Annual_Rate")/100) * (F.col("Seconds_Today")/31536000)))

# 4. SAVE TO SILVER
(df_silver.write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .saveAsTable("top_20_population_silver_v3"))

print("✅ Silver v3: Live population calculated and saved.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
