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

# MAGIC %%sql
# MAGIC SELECT 
# MAGIC     Country_or_dependency, 
# MAGIC     COUNT(*) as snapshot_count,
# MAGIC     MIN(Scrape_Timestamp) as first_recorded,
# MAGIC     MAX(Scrape_Timestamp) as last_recorded
# MAGIC FROM top_20_population_gold_v3
# MAGIC WHERE Country_or_dependency = 'Mexico'
# MAGIC GROUP BY Country_or_dependency;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT DISTINCT Scrape_Timestamp 
# MAGIC FROM top_20_population_bronze_v3 
# MAGIC ORDER BY Scrape_Timestamp DESC

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT DISTINCT Scrape_Timestamp 
# MAGIC FROM top_20_population_gold_v3 
# MAGIC ORDER BY Scrape_Timestamp DESC

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT COUNT(*) as snapshot_count FROM top_20_population_gold_v3 WHERE Country_or_dependency = 'Mexico'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT COUNT(*) as snapshot_count FROM top_20_population_silver_v3 WHERE Country_or_dependency = 'Mexico'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT DISTINCT Scrape_Timestamp FROM top_20_population_gold_v3

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT DISTINCT Scrape_Timestamp FROM top_20_population_silver_v3

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT DISTINCT Scrape_Timestamp FROM top_20_population_bronze_v3

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT COUNT(*) FROM top_20_population_gold_v3;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT COUNT(DISTINCT Scrape_Timestamp) AS Total_Pipeline_Runs
# MAGIC FROM top_20_population_gold_v3;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select Scrape_Timestamp from top_20_population_gold_v3

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- pulse growth 
# MAGIC SELECT 
# MAGIC     Scrape_Timestamp, 
# MAGIC     COUNT(*) as country_count,
# MAGIC     SUM(Population_2025_Live) as total_captured_pop
# MAGIC FROM top_20_population_gold_v3
# MAGIC GROUP BY Scrape_Timestamp
# MAGIC ORDER BY Scrape_Timestamp DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT DISTINCT Scrape_Timestamp 
# MAGIC FROM top_20_population_gold_v3 
# MAGIC ORDER BY Scrape_Timestamp DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT COUNT(*) FROM top_20_population_gold_v3

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT DISTINCT Scrape_Timestamp FROM top_20_population_gold_v3;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
