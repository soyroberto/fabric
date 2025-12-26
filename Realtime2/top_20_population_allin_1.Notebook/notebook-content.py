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
import datetime
from pyspark.sql import functions as F

# 1. SCRAPE DATA
url = "https://www.worldometers.info/world-population/population-by-country/"
response = requests.get(url)
df_list = pd.read_html(response.text)
df_raw = df_list[0]

# 2. CLEAN & TIMESTAMP IMMEDIATELY
# We use a standard format that Spark loves
now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Rename columns and keep top 20
df_raw = df_raw.iloc[:20, [1, 2]]
df_raw.columns = ['Country', 'Population_2025']

# 3. CONVERT TO SPARK & CALCULATE GROWTH
growth_rates = {
    "India": 0.89, "China": -0.23, "United States": 0.54, "Indonesia": 0.79, 
    "Pakistan": 1.57, "Nigeria": 2.08, "Brazil": 0.38, "Bangladesh": 1.22, 
    "Russia": -0.57, "Ethiopia": 2.58, "Mexico": 0.83, "Japan": -0.52, 
    "Egypt": 1.57, "Philippines": 0.81, "DR Congo": 3.25, "Vietnam": 0.6, 
    "Iran": 0.93, "Turkey": 0.24, "Germany": -0.56, "Thailand": -0.07
}

mapping_expr = F.create_map([F.lit(x) for x in sum(growth_rates.items(), ())])

# Convert pandas to spark
df_spark = spark.createDataFrame(df_raw)
df_final = df_spark.withColumn("Scrape_Timestamp", F.to_timestamp(F.lit(now), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("Annual_Rate", mapping_expr[F.col("Country")]) \
    .withColumn("Base_Pop", F.col("Population_2025").cast("double")) \
    .withColumn("Seconds_Today", F.hour(F.current_timestamp()) * 3600 + F.minute(F.current_timestamp()) * 60 + F.second(F.current_timestamp())) \
    .withColumn("Live_Population", 
                F.col("Base_Pop") + (F.col("Base_Pop") * (F.col("Annual_Rate")/100) * (F.col("Seconds_Today")/31536000))) \
    .withColumn("Growth_Per_Second", 
                (F.col("Base_Pop") * (F.col("Annual_Rate")/100)) / 31536000) # <--- NEW CALCULATION

# 4. APPEND TO HISTORY TABLE / Data model
(df_final.select("Country", "Scrape_Timestamp", "Live_Population", "Growth_Per_Second")
  .write
  .mode("append")
  .option("mergeSchema", "true") # This ensures the new column is added to your existing table
  .saveAsTable("population_growth_history"))

print(f"✅ Success! Velocity calculated for {now}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT Scrape_Timestamp, COUNT(*) 
# MAGIC FROM population_growth_history 
# MAGIC GROUP BY Scrape_Timestamp;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM population_growth_history ORDER BY Scrape_Timestamp DESC

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT 
# MAGIC     Country, 
# MAGIC     Scrape_Timestamp, 
# MAGIC     Live_Population,
# MAGIC     -- Subtract current population from the population of the previous timestamp
# MAGIC     Live_Population - LAG(Live_Population) OVER (PARTITION BY Country ORDER BY Scrape_Timestamp) AS Growth_Since_Last_Run
# MAGIC FROM population_growth_history
# MAGIC ORDER BY Scrape_Timestamp DESC, Country ASC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
