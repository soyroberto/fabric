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

df = spark.read.format("delta").load("Tables/your_top_songs_2016_2024")
df = df.toPandas()   # If dataset is small enough (< 1M rows), good for sklearn


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#3
from sklearn.model_selection import train_test_split

X = df[["danceability", "energy", "valence", "tempo", "popularity", "duration_ms"]]
y = df["popularity"]   # example prediction target

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#4
from sklearn.ensemble import RandomForestRegressor

model = RandomForestRegressor(n_estimators=200)
model.fit(X_train, y_train)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#5
from sklearn.metrics import mean_squared_error, r2_score

pred = model.predict(X_test)

mse = mean_squared_error(y_test, pred)
r2 = r2_score(y_test, pred)

print("MSE:", mse)
print("R2:", r2)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#6
import mlflow

mlflow.set_experiment("spotify_experiment")

with mlflow.start_run():
    mlflow.sklearn.log_model(model, "model")
    mlflow.log_metric("mse", mse)
    mlflow.log_metric("r2", r2)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
