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

#No real time table

import requests
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, trim
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
    
    # IMPROVED LOGIC: Iterate through ALL tables and check ALL headers
    tables = soup.find_all("table")
    for table in tables:
        # Get all header cells for this table
        headers_refs = table.find_all("th")
        header_texts = [h.get_text(strip=True) for h in headers_refs]
        
        # We look for a table that definitely contains "Country" and "Population" in its headers
        # We use 'any' to check if "Country" exists in ANY of the headers, not just the first one
        if any("Country" in h for h in header_texts) and any("Population" in h for h in header_texts):
            target_table = table
            break
            
    if not target_table:
        print("Could not locate the population table. The website structure might have changed.")
        return None, None

    # Extract clean column names
    # Replace newlines, % signs, and spaces for Spark compatibility
    headers_html = target_table.find_all("th")
    columns = []
    for th in headers_html:
        col_text = th.get_text(strip=True)
        clean_col = col_text.replace(" ", "_").replace(".", "").replace("%", "Pct").replace("²", "2").replace("(", "").replace(")", "")
        columns.append(clean_col)

    # Extract Rows
    data = []
    # Try finding rows in tbody, if not, try direct children (some tables are malformed)
    rows_container = target_table.find("tbody") if target_table.find("tbody") else target_table
    rows = rows_container.find_all("tr")
    
    # Skip header row if it's inside tbody or if we selected the table directly
    # We filter for rows that actually have 'td' cells
    valid_rows = [r for r in rows if r.find_all("td")]

    # Limit to Top 20
    for row in valid_rows[:20]:
        cols = row.find_all("td")
        row_data = [col.get_text(strip=True) for col in cols]
        
        # Ensure row length matches column length to avoid index errors
        if len(row_data) == len(columns):
            data.append(row_data)

    return columns, data

def create_pyspark_df(spark, columns, data):
    if not data:
        return None

    # 1. Create Raw DataFrame
    # We create it as StringType first to handle the raw scraped text safely
    df = spark.createDataFrame(data, schema=columns)
    
    # 2. Data Cleaning & Type Casting
    # We will clean the common numeric columns:
    # - Remove commas (e.g., "1,234")
    # - Remove plus signs/percentages (e.g., "+0.5 %")
    
    # Identify potential numeric columns by name keywords
    for col_name in columns:
        if any(x in col_name for x in ["#", "Population", "Area", "Density", "Change", "Rate", "Age", "Migrants"]):
            
            # Remove non-numeric characters (keep digits, dots, and minus signs)
            # This regex removes anything that IS NOT a digit, a dot, or a minus.
            df = df.withColumn(col_name, regexp_replace(col(col_name), "[^0-9.-]", ""))
            
            # Handle empty strings that result from cleaning (turn them into nulls) or N/A
            # Then cast to Double (safest) or Long
            if "Population" in col_name or "Area" in col_name or "#" == col_name:
                 df = df.withColumn(col_name, col(col_name).cast(LongType()))
            else:
                 df = df.withColumn(col_name, col(col_name).cast(DoubleType()))

    return df

# --- Main Execution ---

spark = SparkSession.builder \
    .appName("WorldometersScraperFixed") \
    .getOrCreate()

print("Scraping data...")
cols, raw_data = scrape_worldometers_top20()

if raw_data and cols:
    print(f"Found {len(raw_data)} rows. Creating DataFrame...")
    
    final_df = create_pyspark_df(spark, cols, raw_data)
    
    if final_df:
        print("Top 20 Largest Countries by Population:")
        final_df.show(20, truncate=False)
        final_df.printSchema()
    else:
        print("Failed to create DataFrame.")
else:
    print("Scraping returned no data.")

    # --- Main Execution ---

spark = SparkSession.builder.getOrCreate()

print("1. Scraping data...")
cols, raw_data = scrape_worldometers_top20()

if raw_data and cols:
    print(f"2. Found {len(raw_data)} rows. Creating DataFrame...")
    final_df = create_pyspark_df(spark, cols, raw_data)
    
    if final_df:
        # Define your table name
        table_name = "top_20_population_liveG"
        
        print(f"3. Saving to Fabric Lakehouse table: '{table_name}'...")
        
        # Write to Delta Lake
        # mode("overwrite") ensures that if you run this tomorrow, it updates the data
        final_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .saveAsTable(table_name)
            
        print("4. Success! Data saved.")
        
        # --- VERIFICATION METHOD 1: PROGRAMMATIC ---
        print("--- Verifying by reading back from Lakehouse ---")
        saved_df = spark.table(table_name)
        saved_df.show(15, truncate=False)
        
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

spark.table("top_20_population_liveG").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM top_20_population_liveG 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

print("📅 CHECKING TABLE CREATION TIMES")
print("=" * 60)

# List all tables
print("\n📋 ALL TABLES IN LAKEHOUSE:")
spark.sql("SHOW TABLES").show(truncate=False)

# Get detailed info for specific tables
tables_to_check = ["Top_20_population_liveg", "Top_20_Population_liveG"]

for table_name in tables_to_check:
    print(f"\n🔍 TABLE: {table_name}")
    try:
        # Method A: DESCRIBE EXTENDED (shows everything)
        print("Method A: Using DESCRIBE EXTENDED")
        info_df = spark.sql(f"DESCRIBE EXTENDED {table_name}")
        
        # Filter for creation time info
        creation_info = info_df.filter(
            (info_df.col_name.contains("Created")) | 
            (info_df.col_name.contains("Time")) |
            (info_df.col_name.contains("Date"))
        ).collect()
        
        if creation_info:
            for row in creation_info:
                print(f"   {row['col_name']}: {row['data_type']}")
        else:
            print("   No creation time found in DESCRIBE EXTENDED")
        
        # Method B: Check file timestamps
        print("\nMethod B: Checking file system timestamps")
        try:
            location_df = info_df.filter(info_df.col_name.contains("Location")).collect()
            if location_df:
                location = location_df[0]['data_type']
                print(f"   Table location: {location}")
                
                # Try to list files and get their timestamps
                files_df = spark.sql(f"SHOW FILES IN `{location}`")
                if files_df.count() > 0:
                    oldest = files_df.agg({"modificationTime": "min"}).collect()[0][0]
                    newest = files_df.agg({"modificationTime": "max"}).collect()[0][0]
                    print(f"   Oldest file: {oldest}")
                    print(f"   Newest file: {newest}")
        except:
            print("   Could not check file timestamps")
            
    except Exception as e:
        print(f"   ❌ Error checking {table_name}: {e}")

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
