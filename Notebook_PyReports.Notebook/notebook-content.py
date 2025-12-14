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

"""
GENERATE ALL VISUALIZATIONS FOR POWER BI
This creates the data aggregations needed for each visual
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, max, min, count, when
import pandas as pd

spark = SparkSession.builder.getOrCreate()

def generate_visualization_data():
    """
    Create datasets for each visualization
    """
    print("📊 GENERATING VISUALIZATION DATA SETS")
    print("=" * 70)
    
    # Read your data
    df = spark.table("top_20_population_liveg")
    
    # 1. TOP 10 COUNTRIES DATA
    print("\n1️⃣ Top 10 Countries Data:")
    top10 = df.orderBy(col("Population").desc()).limit(10)
    top10_pd = top10.toPandas()
    print(top10_pd[['Country_or_dependency', 'Population_2025', 'Yearly_Change']])
    
    # Save for Power BI
    top10.write.mode("overwrite").saveAsTable("viz_top10_countries")
    
    # 2. GROWTH RATE COMPARISON
    print("\n2️⃣ Growth Rate Comparison Data:")
    growth_data = df.select(
        "Country", 
        "Yearly_Change_Percent",
        when(col("Yearly_Change") > 0, "Positive")
          .when(col("Yearly_Change") < 0, "Negative")
          .otherwise("Stable").alias("Growth_Status")
    ).orderBy("Yearly_Change_Percent", ascending=False)
    
    growth_data.write.mode("overwrite").saveAsTable("viz_growth_comparison")
    
    # 3. URBANIZATION VS DENSITY
    print("\n3️⃣ Urbanization vs Density Data:")
    scatter_data = df.select(
        "Country_or_Dependency",
        "Urban_Pop_Pct",
        "Density_P/Km2",
        "Population",
        when(col("Urban_Pop_Pct") > 70, "Highly Urbanized")
          .when(col("Urban_Pop_Pct") > 50, "Moderately Urbanized")
          .otherwise("Less Urbanized").alias("Urbanization_Level")
    )
    
    scatter_data.write.mode("overwrite").saveAsTable("viz_urban_density")
    
    # 4. FERTILITY RATE DISTRIBUTION
    print("\n4️⃣ Fertility Rate Distribution:")
    fertility_bins = df.select(
        "Country_or_dependency",
        "Fert_Rate",
        when(col("Fert_Rate") < 2.1, "Below Replacement")
          .when(col("Fert_Rate") <= 3.0, "Moderate")
          .otherwise("High").alias("Fertility_Level")
    )
    
    fertility_bins.write.mode("overwrite").saveAsTable("viz_fertility_distribution")
    
    # 5. WORLD SHARE DATA
    print("\n5️⃣ World Share Data:")
    world_share = df.select(
        "Country",
        "World_Share_Percent"
    ).orderBy(col("World_Share_Percent").desc())
    
    world_share.write.mode("overwrite").saveAsTable("viz_world_share")
    
    # 6. MIGRATION PATTERNS
    print("\n6️⃣ Migration Patterns:")
    migration = df.select(
        "Country",
        "Migrants_Net",
        when(col("Migrants_Net") > 0, "Net Inflow")
          .otherwise("Net Outflow").alias("Migration_Direction")
    ).orderBy("Migrants_Net", ascending=False)
    
    migration.write.mode("overwrite").saveAsTable("viz_migration")
    
    # 7. DENSITY CATEGORIES
    print("\n7️⃣ Density Categories:")
    density_cats = df.select(
        "Country",
        "Density_Per_Km2",
        when(col("Density_Per_Km2") > 500, "Very High (>500)")
          .when(col("Density_Per_Km2") > 250, "High (250-500)")
          .when(col("Density_Per_Km2") > 100, "Medium (100-250)")
          .when(col("Density_Per_Km2") > 50, "Low (50-100)")
          .otherwise("Very Low (<50)").alias("Density_Category")
    )
    
    density_cats.write.mode("overwrite").saveAsTable("viz_density_categories")
    
    # 8. KPI METRICS
    print("\n8️⃣ KPI Metrics:")
    kpi_data = df.agg(
        sum("Population").alias("Total_Population"),
        avg("Yearly_Change_Percent").alias("Average_Growth_Rate"),
        max("Population").alias("Max_Population"),
        max("Yearly_Change_Percent").alias("Max_Growth_Rate"),
        avg("Fertility_Rate").alias("Average_Fertility"),
        avg("Median_Age").alias("Average_Median_Age")
    )
    
    kpi_data.write.mode("overwrite").saveAsTable("viz_kpi_metrics")
    
    # 9. CORRELATION MATRIX DATA
    print("\n9️⃣ Correlation Matrix Data:")
    corr_data = df.select(
        "Population",
        "Yearly_Change_Percent",
        "Density_Per_Km2",
        "Fertility_Rate",
        "Urban_Pop_Percent",
        "Median_Age"
    )
    
    # Calculate correlations
    corr_pd = corr_data.toPandas()
    correlation_matrix = corr_pd.corr()
    
    # Convert to long format for heatmap
    corr_long = correlation_matrix.stack().reset_index()
    corr_long.columns = ['Variable1', 'Variable2', 'Correlation']
    
    # Save to Spark
    corr_spark = spark.createDataFrame(corr_long)
    corr_spark.write.mode("overwrite").saveAsTable("viz_correlation_matrix")
    
    print("\n✅ All visualization datasets created!")
    
    return {
        "top10": top10_pd,
        "correlation_matrix": correlation_matrix
    }

# Run the function
viz_data = generate_visualization_data()

# Show summary
print("\n📋 VISUALIZATION DATASETS CREATED:")
tables = spark.sql("SHOW TABLES LIKE 'viz_%'").collect()
for table in tables:
    count = spark.table(table.tableName).count()
    print(f"  • {table.tableName}: {count} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""
GENERATE ALL VISUALIZATIONS FOR POWER BI
This creates the data aggregations needed for each visual
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, max, min, count, when
import pandas as pd

spark = SparkSession.builder.getOrCreate()

def generate_visualization_data():
    """
    Create datasets for each visualization
    """
    print("📊 GENERATING VISUALIZATION DATA SETS")
    print("=" * 70)
    
    # Read your data
    df = spark.table("top_20_population_liveg")
    
    # 1. TOP 10 COUNTRIES DATA
    print("\n1️⃣ Top 10 Countries Data:")
    top10 = df.orderBy(col("Population").desc()).limit(10)
    top10_pd = top10.toPandas()
    print(top10_pd[['Country', 'Population', 'Yearly_Change_Percent']])
    
    # Save for Power BI
    top10.write.mode("overwrite").saveAsTable("viz_top10_countries")
    
    # 2. GROWTH RATE COMPARISON
    print("\n2️⃣ Growth Rate Comparison Data:")
    growth_data = df.select(
        "Country", 
        "Yearly_Change_Percent",
        when(col("Yearly_Change_Percent") > 0, "Positive")
          .when(col("Yearly_Change_Percent") < 0, "Negative")
          .otherwise("Stable").alias("Growth_Status")
    ).orderBy("Yearly_Change_Percent", ascending=False)
    
    growth_data.write.mode("overwrite").saveAsTable("viz_growth_comparison")
    
    # 3. URBANIZATION VS DENSITY
    print("\n3️⃣ Urbanization vs Density Data:")
    scatter_data = df.select(
        "Country",
        "Urban_Pop_Percent",
        "Density_Per_Km2",
        "Population",
        when(col("Urban_Pop_Percent") > 70, "Highly Urbanized")
          .when(col("Urban_Pop_Percent") > 50, "Moderately Urbanized")
          .otherwise("Less Urbanized").alias("Urbanization_Level")
    )
    
    scatter_data.write.mode("overwrite").saveAsTable("viz_urban_density")
    
    # 4. FERTILITY RATE DISTRIBUTION
    print("\n4️⃣ Fertility Rate Distribution:")
    fertility_bins = df.select(
        "Country",
        "Fertility_Rate",
        when(col("Fertility_Rate") < 2.1, "Below Replacement")
          .when(col("Fertility_Rate") <= 3.0, "Moderate")
          .otherwise("High").alias("Fertility_Level")
    )
    
    fertility_bins.write.mode("overwrite").saveAsTable("viz_fertility_distribution")
    
    # 5. WORLD SHARE DATA
    print("\n5️⃣ World Share Data:")
    world_share = df.select(
        "Country",
        "World_Share_Percent"
    ).orderBy(col("World_Share_Percent").desc())
    
    world_share.write.mode("overwrite").saveAsTable("viz_world_share")
    
    # 6. MIGRATION PATTERNS
    print("\n6️⃣ Migration Patterns:")
    migration = df.select(
        "Country",
        "Migrants_Net",
        when(col("Migrants_Net") > 0, "Net Inflow")
          .otherwise("Net Outflow").alias("Migration_Direction")
    ).orderBy("Migrants_Net", ascending=False)
    
    migration.write.mode("overwrite").saveAsTable("viz_migration")
    
    # 7. DENSITY CATEGORIES
    print("\n7️⃣ Density Categories:")
    density_cats = df.select(
        "Country",
        "Density_Per_Km2",
        when(col("Density_Per_Km2") > 500, "Very High (>500)")
          .when(col("Density_Per_Km2") > 250, "High (250-500)")
          .when(col("Density_Per_Km2") > 100, "Medium (100-250)")
          .when(col("Density_Per_Km2") > 50, "Low (50-100)")
          .otherwise("Very Low (<50)").alias("Density_Category")
    )
    
    density_cats.write.mode("overwrite").saveAsTable("viz_density_categories")
    
    # 8. KPI METRICS
    print("\n8️⃣ KPI Metrics:")
    kpi_data = df.agg(
        sum("Population").alias("Total_Population"),
        avg("Yearly_Change_Percent").alias("Average_Growth_Rate"),
        max("Population").alias("Max_Population"),
        max("Yearly_Change_Percent").alias("Max_Growth_Rate"),
        avg("Fertility_Rate").alias("Average_Fertility"),
        avg("Median_Age").alias("Average_Median_Age")
    )
    
    kpi_data.write.mode("overwrite").saveAsTable("viz_kpi_metrics")
    
    # 9. CORRELATION MATRIX DATA
    print("\n9️⃣ Correlation Matrix Data:")
    corr_data = df.select(
        "Population",
        "Yearly_Change_Percent",
        "Density_Per_Km2",
        "Fertility_Rate",
        "Urban_Pop_Percent",
        "Median_Age"
    )
    
    # Calculate correlations
    corr_pd = corr_data.toPandas()
    correlation_matrix = corr_pd.corr()
    
    # Convert to long format for heatmap
    corr_long = correlation_matrix.stack().reset_index()
    corr_long.columns = ['Variable1', 'Variable2', 'Correlation']
    
    # Save to Spark
    corr_spark = spark.createDataFrame(corr_long)
    corr_spark.write.mode("overwrite").saveAsTable("viz_correlation_matrix")
    
    print("\n✅ All visualization datasets created!")
    
    return {
        "top10": top10_pd,
        "correlation_matrix": correlation_matrix
    }

# Run the function
viz_data = generate_visualization_data()

# Show summary
print("\n📋 VISUALIZATION DATASETS CREATED:")
tables = spark.sql("SHOW TABLES LIKE 'viz_%'").collect()
for table in tables:
    count = spark.table(table.tableName).count()
    print(f"  • {table.tableName}: {count} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""
GENERATE ALL VISUALIZATIONS FOR POWER BI
This creates the data aggregations needed for each visual
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, max, min, count, when
import pandas as pd

spark = SparkSession.builder.getOrCreate()

def generate_visualization_data():
    """
    Create datasets for each visualization
    """
    print("📊 GENERATING VISUALIZATION DATA SETS")
    print("=" * 70)
    
    # Read your data
    df = spark.table("top_20_population_liveg")
    
    # 1. TOP 10 COUNTRIES DATA
    print("\n1️⃣ Top 10 Countries Data:")
    top10 = df.orderBy(col("Population").desc()).limit(10)
    top10_pd = top10.toPandas()
    print(top10_pd[['Country_or_dependency', 'Population_2025', 'Yearly_Change']])
    
    # Save for Power BI
    top10.write.mode("overwrite").saveAsTable("viz_top10_countries")
    
    # 2. GROWTH RATE COMPARISON
    print("\n2️⃣ Growth Rate Comparison Data:")
    growth_data = df.select(
        "Country", 
        "Yearly_Change_Percent",
        when(col("Yearly_Change") > 0, "Positive")
          .when(col("Yearly_Change") < 0, "Negative")
          .otherwise("Stable").alias("Growth_Status")
    ).orderBy("Yearly_Change_Percent", ascending=False)
    
    growth_data.write.mode("overwrite").saveAsTable("viz_growth_comparison")
    
    # 3. URBANIZATION VS DENSITY
    print("\n3️⃣ Urbanization vs Density Data:")
    scatter_data = df.select(
        "Country_or_Dependency",
        "Urban_Pop_Pct",
        "Density_P/Km2",
        "Population",
        when(col("Urban_Pop_Pct") > 70, "Highly Urbanized")
          .when(col("Urban_Pop_Pct") > 50, "Moderately Urbanized")
          .otherwise("Less Urbanized").alias("Urbanization_Level")
    )
    
    scatter_data.write.mode("overwrite").saveAsTable("viz_urban_density")
    
    # 4. FERTILITY RATE DISTRIBUTION
    print("\n4️⃣ Fertility Rate Distribution:")
    fertility_bins = df.select(
        "Country_or_dependency",
        "Fert_Rate",
        when(col("Fert_Rate") < 2.1, "Below Replacement")
          .when(col("Fert_Rate") <= 3.0, "Moderate")
          .otherwise("High").alias("Fertility_Level")
    )
    
    fertility_bins.write.mode("overwrite").saveAsTable("viz_fertility_distribution")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
