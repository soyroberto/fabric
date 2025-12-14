# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ad21b38c-85f1-42b8-ad11-db43446523be",
# META       "default_lakehouse_name": "copilot",
# META       "default_lakehouse_workspace_id": "4c071025-89dc-49cc-8031-06c564ed2f2d",
# META       "known_lakehouses": [
# META         {
# META           "id": "ad21b38c-85f1-42b8-ad11-db43446523be"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
df = spark.read.table("your_top_songs_2016_2024")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

# 1️⃣ Imports
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F

# 2️⃣ Sample data to avoid memory issues
# Adjust the number of rows as needed (e.g., 5000)
df_sample = df.limit(200).toPandas()

# 3️⃣ Identify numeric columns automatically
# Replace with your actual numeric columns from the dataset
numeric_cols = [
    "popularity", "danceability", "energy", "acousticness", 
    "instrumentalness", "liveness", "valence", "tempo"
]

# Convert numeric columns to float safely
for col in numeric_cols:
    if col in df_sample.columns:
        df_sample[col] = pd.to_numeric(df_sample[col], errors="coerce")

# 4️⃣ Plot: Average Popularity by Genre
if "genres" in df_sample.columns and "popularity" in df_sample.columns:
    genre_avg = df_sample.groupby("genres")["popularity"].mean().reset_index()

    plt.figure(figsize=(12,6))
    sns.barplot(x="genres", y="popularity", data=genre_avg)
    plt.xticks(rotation=45)
    plt.title("Average Popularity by Genre")
    plt.xlabel("Genre")
    plt.ylabel("Average Popularity")
    plt.show()

# 5️⃣ Plot: Scatter plots for numeric audio features
scatter_pairs = [
    ("danceability", "energy"),
    ("acousticness", "instrumentalness"),
    ("liveness", "valence")
]

for x_col, y_col in scatter_pairs:
    if x_col in df_sample.columns and y_col in df_sample.columns:
        plt.figure(figsize=(10,6))
        sns.scatterplot(x=x_col, y=y_col, data=df_sample, alpha=0.6)
        plt.title(f"{x_col.capitalize()} vs {y_col.capitalize()}")
        plt.xlabel(x_col.capitalize())
        plt.ylabel(y_col.capitalize())
        plt.show()

# 6️⃣ Optional: Additional scatter for tempo vs valence
if "tempo" in df_sample.columns and "valence" in df_sample.columns:
    plt.figure(figsize=(10,6))
    sns.scatterplot(x="tempo", y="valence", data=df_sample, alpha=0.6)
    plt.title("Tempo vs Valence")
    plt.xlabel("Tempo")
    plt.ylabel("Valence")
    plt.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
import matplotlib.pyplot as plt

# ---- Safety check: does `df` exist? ----
if "df" not in globals():
    raise ValueError("Your Spark DataFrame `df` is not defined in this environment. Please load it before running.")

# ---- Find numeric columns automatically ----
spark_df = df
pdf_preview = spark_df.limit(100).toPandas()
numeric_cols = pdf_preview.select_dtypes(include="number").columns.tolist()

if len(numeric_cols) == 0:
    raise ValueError("Your DataFrame has no numeric columns to plot. Please specify a column.")

default_col = numeric_cols[0]

# ---- Function to build sample and plot ----
def show_plot(limit=200, column=default_col):
    pdf = spark_df.limit(limit).toPandas()
    plt.figure(figsize=(8,4))
    plt.hist(pdf[column].dropna())
    plt.title(f"Distribution of '{column}' with limit={limit}")
    plt.xlabel(column)
    plt.ylabel("Frequency")
    plt.show()

# ---- UI Menu with ipywidgets ----
import ipywidgets as widgets
from IPython.display import display

limit_slider = widgets.IntSlider(
    value=200,
    min=50,
    max=5000,
    step=50,
    description="Rows:",
)

column_dropdown = widgets.Dropdown(
    options=numeric_cols,
    value=default_col,
    description="Column:",
)

ui = widgets.VBox([limit_slider, column_dropdown])
out = widgets.interactive_output(
    lambda limit, column: show_plot(limit, column),
    {"limit": limit_slider, "column": column_dropdown},
)

display(ui, out)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.table("your_top_songs_2016_2024")
df.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
