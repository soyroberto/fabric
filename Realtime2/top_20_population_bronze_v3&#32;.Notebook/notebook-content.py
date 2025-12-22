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


import requests
import pandas as pd
from io import StringIO
import datetime
from pyspark.sql import functions as F


url = 'https://www.worldometers.info/world-population/population-by-country/'
headers = {'User-Agent': 'Mozilla/5.0'}
response = requests.get(url, headers=headers)
response.encoding = 'utf-8'

# 1. SCRAPE & CLEAN (Pandas Level)
# Just use this scrapper to create the tables
tables = pd.read_html(StringIO(response.text))
df_raw = tables[0].head(20).copy()

# Fix Column Names immediately in Pandas before Spark sees them
# This replaces dots '.', spaces ' ', and brackets with underscores
df_raw.columns = [
    c.replace('.', '_')
     .replace(' ', '_')
     .replace('(', '')
     .replace(')', '') 
     for c in df_raw.columns
]

df_raw['Scrape_Timestamp'] = datetime.datetime.now()

# 2. LOAD TO SPARK
spark_df = spark.createDataFrame(df_raw)

# 3. REGEX CLEANING & TYPE CASTING
clean_regex = "[^a-zA-Z0-9\s\.\,\-\%]"

# Specify which columns should be numbers
numeric_cols = ["Population_2025", "Net_Change", "Land_Area_Km_2", "Density_P/Km_2"] 

for col_name in spark_df.columns:
    # Clean the "âˆ’" and other artifacts
    spark_df = spark_df.withColumn(
        col_name, 
        F.regexp_replace(F.col(col_name).cast("string"), clean_regex, "")
    )
    
    # NEW: Cast numbers back to Double/Long so math works in Silver
    if col_name in numeric_cols:
        spark_df = spark_df.withColumn(col_name, F.col(col_name).cast("double"))

# 4. SAVE
(spark_df.write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .saveAsTable("top_20_population_bronze_v3"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
