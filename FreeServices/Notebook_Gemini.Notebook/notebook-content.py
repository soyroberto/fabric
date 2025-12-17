# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "35d7d41a-389b-4dff-9883-7c781276d341",
# META       "default_lakehouse_name": "PopulationDataLake",
# META       "default_lakehouse_workspace_id": "f8a8e7cb-a246-415d-a682-ccb1108926ba",
# META       "known_lakehouses": [
# META         {
# META           "id": "35d7d41a-389b-4dff-9883-7c781276d341"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# borrar
#time track added
#Data Ingestion ETL
#main data gatherer
#added tracking table: top_20_population_table

# Real-time History Table Script
import requests
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
# Added current_timestamp to imports
from pyspark.sql.functions import col, regexp_replace, trim, current_timestamp 
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

def scrape_worldometers_top20():
    url = "https://www.worldometers.info/world-population/#top20"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching page: {e}")
        return None, None

    soup = BeautifulSoup(response.content, 'html.parser')
    
    target_table = None
    
    # Iterate through ALL tables and check ALL headers
    tables = soup.find_all("table")
    for table in tables:
        headers_refs = table.find_all("th")
        header_texts = [h.get_text(strip=True) for h in headers_refs]
        
        if any("Country" in h for h in header_texts) and any("Population" in h for h in header_texts):
            target_table = table
            break
            
    if not target_table:
        print("Could not locate the population table.")
        return None, None

    # Extract clean column names
    headers_html = target_table.find_all("th")
    columns = []
    for th in headers_html:
        col_text = th.get_text(strip=True)
        clean_col = col_text.replace(" ", "_").replace(".", "").replace("%", "Pct").replace("²", "2").replace("(", "").replace(")", "")
        columns.append(clean_col)

    # Extract Rows
    data = []
    rows_container = target_table.find("tbody") if target_table.find("tbody") else target_table
    rows = rows_container.find_all("tr")
    valid_rows = [r for r in rows if r.find_all("td")]

    # Limit to Top 20
    for row in valid_rows[:20]:
        cols = row.find_all("td")
        row_data = [col.get_text(strip=True) for col in cols]
        
        if len(row_data) == len(columns):
            data.append(row_data)

    return columns, data

def create_pyspark_df(spark, columns, data):
    if not data:
        return None

    df = spark.createDataFrame(data, schema=columns)
    
    for col_name in columns:
        if any(x in col_name for x in ["#", "Population", "Area", "Density", "Change", "Rate", "Age", "Migrants"]):
            df = df.withColumn(col_name, regexp_replace(col(col_name), "[^0-9.-]", ""))
            
            if "Population" in col_name or "Area" in col_name or "#" == col_name:
                 df = df.withColumn(col_name, col(col_name).cast(LongType()))
            else:
                 df = df.withColumn(col_name, col(col_name).cast(DoubleType()))

    return df

# --- Main Execution (Consolidated) ---

spark = SparkSession.builder \
    .appName("WorldometersRealTime") \
    .getOrCreate()

print("1. Scraping data...")
cols, raw_data = scrape_worldometers_top20()

if raw_data and cols:
    print(f"2. Found {len(raw_data)} rows. Creating DataFrame...")
    final_df = create_pyspark_df(spark, cols, raw_data)
    
    if final_df:
        # --- MODIFICATION: Add Timestamp for History --- for tendency and visualization
        print("3. Adding timestamp column...")
        final_df = final_df.withColumn("Scrape_Timestamp", current_timestamp())
        table_name = "top_20_population_history" # tracking history table
        
        print(f"4. Appending to Fabric Lakehouse table: '{table_name}'...")
        
        # --- MODIFICATION: Change to 'append' mode ---
        final_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(table_name)
            
        print("5. Success! Data appended.")
        
        # Verify by showing the latest 5 entries sorted by time
        print("--- Verifying latest data ---")
        saved_df = spark.table(table_name)
        saved_df.orderBy(col("Scrape_Timestamp").desc()).show(5, truncate=False)
        
    else:
        print("Failed to create DataFrame.")
else:
    print("Scraping returned no data.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Show latest updated Timestamp in the Silver table
from pyspark.sql.functions import col

# 1. Load the table
xt = spark.table("top_20_population_history")

# 2. Order by timestamp descending, select only the Scrape_TimeStamp column, and show the top 1 row
latest_time = (
    xt.orderBy(col("Scrape_TimeStamp").desc())
    .select("Scrape_TimeStamp")
    .limit(1)
    .show(truncate=False)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Show latest in the gold table
from pyspark.sql.functions import col

# 1. Load the table
xt = spark.table("gold_population_metrics")

# 2. Order by timestamp descending, select only the Scrape_TimeStamp column, and show the top 1 row
latest_time = (
    xt.orderBy(col("Scrape_TimeStamp").desc())
    .select("Scrape_TimeStamp")
    .limit(1)
    .show(truncate=False)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT  country_or_dependency,format_number(Population_2025, 0) AS population, Yearly_Change, format_number(Net_Change, 0) as Population_Change FROM top_20_population_liveg ORDER BY Yearly_Change DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Scheduled Graphic Run in Fabric

# CELL ********************

# First, load your historical data from the CORRECT table.
# This table is assumed to have been created by the 'append' script, 
# which added the 'Scrape_Timestamp' column.

# Load the PySpark Delta table into a Pandas DataFrame for Plotly 
df_history = spark.table("gold_population_metrics").toPandas()

# Verify that the timestamp column exists in the Pandas DataFrame before plotting
if 'Scrape_Timestamp' not in df_history.columns:
    print("Error: 'Scrape_Timestamp' column not found in the 'top_20_population_history' table.")
    print("Please ensure the data ingestion script (with .mode('append')) has run at least once.")
else:
    # Create the Animation
    import plotly.express as px

    # The column names are based on the output of your scraper's cleaning logic
    fig = px.bar(
        df_history, 
        x="Population_2025", 
        y="Country_or_dependency", 
        color="Country_or_dependency", 
        # This is the key that was missing from the old table
        animation_frame="Scrape_Timestamp", 
        animation_group="Country_or_dependency",
        # Use a dynamic range based on the data max value
        range_x=[0, df_history['Population_2025'].max() * 1.1], 
        orientation='h',
        title="🌎 Live Population Growth Race (Historical Data)"
    )

    # Sort the y-axis (Countries) to show the largest population at the top of each frame
    fig.update_layout(yaxis={'categoryorder':'total ascending'})
    fig.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Global Heatmap
import plotly.graph_objects as go

fig = go.Figure(data=go.Choropleth(
    locations=df_history['Country_or_dependency'], # Ensure these match standard country names
    locationmode='country names',
    z=df_history['Population_2025'],
    colorscale='Viridis',
    colorbar_title="Population"
))

fig.update_layout(title_text='World Population Density', geo=dict(projection_type='orthographic')) # 'orthographic' gives a 3D globe view
fig.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Time stamped Graphic no slice

# CELL ********************

import plotly.express as px
import pandas as pd
from pyspark.sql.functions import col # Need to import col for orderBy


df_history_spark = spark.table("gold_population_metrics") # Assuming this is the history table

# --- Step 1: Find the most recent timestamp ---
# Get the latest row based on the Scrape_Timestamp
latest_timestamp_row = df_history_spark.orderBy(col("Scrape_Timestamp").desc()).limit(1).collect()

# Extract the datetime object
if latest_timestamp_row:
    latest_timestamp = latest_timestamp_row[0]['Scrape_Timestamp']
    # Format the timestamp for a clean title display
    formatted_time = latest_timestamp.strftime("%Y-%m-%d %H:%M:%S %Z")
else:
    formatted_time = "Data Timestamp Unknown"

# --- Step 2: Convert to Pandas for Plotly ---
df_history = df_history_spark.toPandas()

# --- Step 3: Create the Static Bar Chart with the Time in the Title ---
fig = px.bar(
    df_history, 
    x="Population_2025", 
    y="Country_or_dependency", 
    color="Country_or_dependency", 
    orientation='h',
    # ADDED: Dynamic title using the latest timestamp
    title=f"Current Population (Static Snapshot) - Data as of: **{formatted_time}**"
)

fig.update_layout(yaxis={'categoryorder':'total ascending'})
fig.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Prep for PowerBI

# CELL ********************

from pyspark.sql.functions import date_trunc, datediff, lag, lit
from pyspark.sql.window import Window

# 1. Load the history table
df = spark.table("top_20_population_history")

# 2. Define a window specification to calculate daily change per country
# Partition by Country_or_dependency and order by Scrape_Timestamp
window_spec = Window.partitionBy("Country_or_dependency").orderBy("Scrape_Timestamp")

# 3. Calculate the population from the *previous* recorded snapshot (LAG)
df_calculated = df.withColumn(
    "Previous_Population", 
    lag(col("Population_2025"), 1).over(window_spec)
)

# 4. Calculate the 'Net_Population_Change_Snapshot'
# Use lit(None).cast("long") for the first row where previous population is null
df_calculated = df_calculated.withColumn(
    "Net_Population_Change_Snapshot", 
    col("Population_2025") - col("Previous_Population")
)

# 5. Create a clean "Gold" layer table for Power BI consumption
# We will use 'gold_population_metrics' for the Power BI reports
df_calculated.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("gold_population_metrics")

print("Gold layer table 'gold_population_metrics' created successfully for Power BI.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
