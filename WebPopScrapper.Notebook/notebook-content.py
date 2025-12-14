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

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""
Web Scraper for Worldometers Population Data - Fabric Version
Debugged and tested for Fabric integration
"""
#Files Loaded

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
import json

def scrape_population_data(url):
    """
    Scrapes population data from Worldometers website
    """
    try:
        print("Starting web scrape...")
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        print("Website fetched successfully")
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # DEBUG: Let's see what tables are available
        tables = soup.find_all('table')
        print(f"Found {len(tables)} tables on the page")
        
        # Try different table selectors
        table = None
        table_selectors = [
            {'id': 'example2'},
            {'class': 'table'},
            {'class': 'table-striped'},
            {'class': 'table-bordered'}
        ]
        
        for selector in table_selectors:
            table = soup.find('table', selector)
            if table:
                print(f"Found table with selector: {selector}")
                break
        
        if not table and len(tables) > 0:
            table = tables[0]
            print("Using first table found")
        
        if not table:
            print("No table found!")
            # Save HTML for debugging
            with open('debug_page.html', 'w', encoding='utf-8') as f:
                f.write(str(soup))
            return None
        
        # Extract table headers for debugging
        headers = []
        if table.find('thead'):
            header_row = table.find('thead').find('tr')
            if header_row:
                headers = [th.text.strip() for th in header_row.find_all('th')]
                print(f"Table headers: {headers}")
        
        # Extract data
        data = []
        rows = table.find_all('tr')
        print(f"Found {len(rows)} rows in table")
        
        for i, row in enumerate(rows[1:], 1):  # Skip header row
            cols = row.find_all(['td', 'th'])
            
            if len(cols) >= 3:  # At minimum we need rank, country, population
                try:
                    # Extract basic info
                    rank = cols[0].text.strip()
                    country = cols[1].text.strip()
                    
                    # Clean population (remove commas, non-numeric chars)
                    pop_text = cols[2].text.strip()
                    pop_cleaned = ''.join(filter(str.isdigit, pop_text))
                    
                    record = {
                        'Rank': int(rank) if rank.isdigit() else 0,
                        'Country': country,
                        'Population': int(pop_cleaned) if pop_cleaned else 0,
                        'Load_Timestamp': datetime.now()
                    }
                    
                    # Add additional columns if available
                    if len(cols) >= 4:
                        yearly_change = cols[3].text.strip().replace('%', '')
                        record['Yearly_Change'] = float(yearly_change) if yearly_change.replace('.', '').isdigit() else 0.0
                    
                    if len(cols) >= 5:
                        net_change = cols[4].text.strip().replace(',', '')
                        record['Net_Change'] = int(net_change) if net_change.isdigit() else 0
                    
                    if len(cols) >= 6:
                        density = cols[5].text.strip().replace(',', '')
                        record['Density_Per_Km2'] = int(density) if density.isdigit() else 0
                    
                    if len(cols) >= 7:
                        area = cols[6].text.strip().replace(',', '')
                        record['Land_Area_Km2'] = int(area) if area.isdigit() else 0
                    
                    data.append(record)
                    
                    # Print first few records for debugging
                    if i <= 5:
                        print(f"Sample record {i}: {record}")
                        
                except Exception as e:
                    print(f"Error processing row {i}: {str(e)}")
                    continue
        
        if not data:
            print("No data extracted!")
            return None
        
        df = pd.DataFrame(data)
        print(f"Successfully extracted {len(df)} records")
        print(f"Columns: {df.columns.tolist()}")
        print(f"First few countries: {df['Country'].head().tolist()}")
        
        return df
    
    except Exception as e:
        print(f"Error occurred while scraping: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def save_to_fabric_lakehouse(df, table_name="WorldPopulation"):
    """
    Save DataFrame to Fabric Lakehouse with better error handling
    """
    try:
        # Create Spark session
        spark = SparkSession.builder.getOrCreate()
        print("Spark session created")
        
        # Display DataFrame info
        print(f"DataFrame shape: {df.shape}")
        print(f"DataFrame columns: {df.columns.tolist()}")
        print(f"DataFrame dtypes:\n{df.dtypes}")
        
        # Check for empty DataFrame
        if df.empty:
            print("DataFrame is empty! Cannot save to Lakehouse.")
            return False
        
        # Ensure proper data types
        for col in df.columns:
            if df[col].dtype == 'object':
                # Check if it's a datetime column
                if 'timestamp' in col.lower() or 'date' in col.lower():
                    df[col] = pd.to_datetime(df[col])
        
        # Convert pandas DataFrame to Spark DataFrame
        print("Converting to Spark DataFrame...")
        spark_df = spark.createDataFrame(df)
        
        # Show schema
        print("Spark DataFrame Schema:")
        spark_df.printSchema()
        
        # Show first few rows
        print("First 5 rows:")
        spark_df.show(5)
        
        # Save to Lakehouse table
        print(f"Saving to table: {table_name}")
        spark_df.write.mode("overwrite").saveAsTable(table_name)
        
        print(f"✅ Data saved successfully to Lakehouse table: {table_name}")
        print(f"✅ Total records saved: {df.shape[0]}")
        
        # Verify the table was created
        try:
            verify_df = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}")
            count = verify_df.collect()[0]['count']
            print(f"✅ Verification: Table contains {count} records")
        except:
            print("⚠️ Could not verify table count")
        
        return True
        
    except Exception as e:
        print(f"❌ Error saving to Lakehouse: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    url = 'https://www.worldometers.info/world-population/#top20'
    print(f"Scraping data from: {url}")
    
    df = scrape_population_data(url)
    
    if df is not None and not df.empty:
        # Save to Fabric Lakehouse
        success = save_to_fabric_lakehouse(df, "WorldPopulation")
        
        if success:
            # Also save locally as backup (optional)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            local_filename = f'population_data_{timestamp}.csv'
            df.to_csv(local_filename, index=False, encoding='utf-8')
            print(f"📁 Local backup saved as: {local_filename}")
            
            # Display summary
            print("\n📊 Data Summary:")
            print(f"Total countries: {len(df)}")
            print(f"Total population: {df['Population'].sum():,}")
            print(f"Top 5 countries by population:")
            top5 = df.nlargest(5, 'Population')[['Country', 'Population']]
            for _, row in top5.iterrows():
                print(f"  {row['Country']}: {row['Population']:,}")
    else:
        print("❌ Failed to extract data")

if __name__ == "__main__":
    main()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Query 1: Show all data
df = spark.sql("SELECT * FROM worldpopulation")
display(df)

# Query 2: Show top 10 countries by population
top10 = spark.sql("""
    SELECT 
        Rank,
        Country,
        Population,
        FORMAT_NUMBER(Population, 0) as Population_Formatted
    FROM WorldPopulation
    WHERE Rank <= 10
    ORDER BY Rank
""")
display(top10)

# Query 3: Aggregate data
summary = spark.sql("""
    SELECT 
        COUNT(*) as Total_Countries,
        SUM(Population) as World_Population,
        AVG(Population) as Avg_Population,
        MIN(Population) as Min_Population,
        MAX(Population) as Max_Population
    FROM WorldPopulation
""")
display(summary)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#basic version
# Test in a separate notebook cell first
url = 'https://www.worldometers.info/world-population/#top20'
headers = {'User-Agent': 'Mozilla/5.0'}

response = requests.get(url, headers=headers)
soup = BeautifulSoup(response.content, 'html.parser')

# Find all tables
tables = soup.find_all('table')
print(f"Total tables found: {len(tables)}")

# Look for table with population data
for i, table in enumerate(tables):
    if 'rank' in table.text.lower() and 'population' in table.text.lower():
        print(f"\nTable {i} contains population data")
        # Try to read with pandas
        try:
            dfs = pd.read_html(str(table))
            if dfs:
                df = dfs[0]
                print(f"Successfully read table {i}")
                print(f"Shape: {df.shape}")
                print(f"Columns: {df.columns.tolist()}")
                display(df.head())
                break
        except:
            continue

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
