# Databricks notebook source
# MAGIC %md
# MAGIC ## gold layer using delta table

# COMMAND ----------

# ==============================

spark.conf.set(
    "fs.azure.account.auth.type.pavanstoragenyc.dfs.core.windows.net",
    "OAuth"
)

spark.conf.set(
    "fs.azure.account.oauth.provider.type.pavanstoragenyc.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)

# Remove the extra space after the Client ID
spark.conf.set(
    "fs.azure.account.oauth2.client.id.pavanstoragenyc.dfs.core.windows.net",
    "0cd253df-a9f4-4899-9695-3640494a2d1d"
)

# Use your NEW client secret here
spark.conf.set(
    "fs.azure.account.oauth2.client.secret.pavanstoragenyc.dfs.core.windows.net",
    "MbI8Q~TjYv1tZZ8~~xyjQNpW6xinCW-N1s6Ama3n"
)

spark.conf.set(
    "fs.azure.account.oauth2.client.endpoint.pavanstoragenyc.dfs.core.windows.net",
    "https://login.microsoftonline.com/d02b1d1b-2795-4afa-83b9-499625071702/oauth2/token"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Database Creation

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE GOLD_LAYER;

# COMMAND ----------

silver="abfss://silver@pavanstoragenyc.dfs.core.windows.net"
gold="abfss://gold@pavanstoragenyc.dfs.core.windows.net"

# COMMAND ----------

df_zone = spark.read.format("parquet") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f'{silver}/trip_zone')

# COMMAND ----------

df_zone.display()

# COMMAND ----------

df_zone.write.format('delta') \
    .mode('append') \
    .option('path', f'{gold}/trip_zone') \
    .saveAsTable('GOLD_LAYER.trip_zone')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from GOLD_LAYER.trip_zone
# MAGIC where Borough="EWR";

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trip type

# COMMAND ----------

df_type = spark.read.format("parquet") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f'{silver}/trip_type')

# COMMAND ----------

df_type.write.format('delta') \
    .mode('append') \
    .option('path', f'{gold}/trip_type') \
    .saveAsTable('GOLD_LAYER.trip_type')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from GOLD_LAYER.trip_type;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trips 2023 data

# COMMAND ----------

df_trip = spark.read.format("parquet") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f'{silver}/trips2023_data')

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip.write.format('delta') \
    .mode('append') \
    .option('path', f'{gold}/trips2023_data') \
    .saveAsTable('GOLD_LAYER.trips2023_data')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from GOLD_LAYER.trips2023_data;