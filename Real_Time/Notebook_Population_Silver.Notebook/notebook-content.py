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

# Silver layer (Ingestion_to_Silver Notebook)

# Real-time History Table Script
#Webpython
import requests
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, current_timestamp 
from pyspark.sql.types import LongType, DoubleType 

def scrape_worldometers_top20():
    url = "https://www.worldometers.info/world-population/#top20"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Failed to fetch page. Reason: {e}") 
        return None, None

    soup = BeautifulSoup(response.content, 'html.parser')
    
    target_table = None
    
    tables = soup.find_all("table")
    for table in tables:
        headers_refs = table.find_all("th")
        header_texts = [h.get_text(strip=True) for h in headers_refs]
        
        if any("Country" in h for h in header_texts) and any("Population" in h for h in header_texts):
            target_table = table
            break
            
    if not target_table:
        print("ERROR: Could not locate the population table on the page.")
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
table_name = "top_20_population_silver" 

if raw_data and cols:
    print(f"2. Found {len(raw_data)} rows. Creating DataFrame...")
    final_df = create_pyspark_df(spark, cols, raw_data)
    
    if final_df is not None:
        print("3. Adding timestamp column...")
        #below final
        final_df = final_df.withColumn("Scrape_Timestamp", current_timestamp())        
        print(f"4. Appending to Fabric Lakehouse table: '{table_name}'...")
        
        # --- ROBUST WRITE BLOCK ---
        # This is the tested, clean, and reliable Delta append for Fabric.
        final_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true").saveAsTable(table_name)
            
        print("5. SUCCESS! Data appended.")
        
        # Verification: Show the latest 5 entries sorted by time
        print("--- Verifying latest data ---")
        saved_df = spark.table(table_name)
        saved_df.orderBy(col("Scrape_Timestamp").desc()).show(5, truncate=False)
        
    else:
        print("ERROR: Failed to create DataFrame.")
else:
    print("Scraping returned no data.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
