# Databricks notebook source
tenant_id ="d02b1d1b-2795-4afa-83b9-499625071702 " 
application_id ="0cd253df-a9f4-4899-9695-3640494a2d1d"  
service_secret ="MbI8Q~TjYv1tZZ8~~xyjQNpW6xinCW-N1s6Ama3n"  

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
    "<NEW_CLIENT_SECRET>"
)

spark.conf.set(
    "fs.azure.account.oauth2.client.endpoint.pavanstoragenyc.dfs.core.windows.net",
    "https://login.microsoftonline.com/d02b1d1b-2795-4afa-83b9-499625071702/oauth2/token"
)

# COMMAND ----------

spark.read.format("abfss").load("abfss://bronze@pavanstoragenyc.dfs.core.windows.net/")
dbutils.fs.ls("abfss://bronze@pavanstoragenyc.dfs.core.windows.net/")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_trip = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://bronze@pavanstoragenyc.dfs.core.windows.net/trip_type")




# COMMAND ----------

df_trip.display()



# COMMAND ----------

from pyspark.sql import SparkSession

# Create Spark Session (optional in Databricks)
spark = SparkSession.builder.appName("ReadCSV").getOrCreate()

storage_account = "pavanstoragenyc"
container = "bronze"

tenant_id = "d02b1d1b-2795-4afa-83b9-499625071702"
application_id = "0cd253df-a9f4-4899-9695-3640494a2d1d"
service_secret = "MbI8Q~TjYv1tZZ8~~xyjQNpW6xinCW-N1s6Ama3n"

spark.conf.set(
    "fs.azure.account.auth.type.pavanstoragenyc.dfs.core.windows.net",
    "OAuth"
)

spark.conf.set(
    "fs.azure.account.oauth.provider.type.pavanstoragenyc.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)

spark.conf.set(
    "fs.azure.account.oauth2.client.id.pavanstoragenyc.dfs.core.windows.net",
    application_id
)

spark.conf.set(
    "fs.azure.account.oauth2.client.secret.pavanstoragenyc.dfs.core.windows.net",
    service_secret
)

spark.conf.set(
    "fs.azure.account.oauth2.client.endpoint.pavanstoragenyc.dfs.core.windows.net",
    f"https://login.microsoftonline.com/d02b1d1b-2795-4afa-83b9-499625071702/oauth2/token"
)

df_trip_type = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"abfss://bronze@pavanstoragenyc.dfs.core.windows.net/trip_type")

df_trip_type.display()


# COMMAND ----------

df_trip_zone = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"abfss://bronze@pavanstoragenyc.dfs.core.windows.net/trip_zone")

df_trip_zone.display()

# COMMAND ----------

myschema = """
VendorID BIGINT,
lpep_pickup_datetime TIMESTAMP,
lpep_dropoff_datetime TIMESTAMP,
store_and_fwd_flag STRING,
RatecodeID BIGINT,
PULocationID BIGINT,
DOLocationID BIGINT,
passenger_count BIGINT,
trip_distance DOUBLE,
fare_amount DOUBLE,
extra DOUBLE,
mta_tax DOUBLE,
tip_amount DOUBLE,
tolls_amount DOUBLE,
ehail_fee DOUBLE,
improvement_surcharge DOUBLE,
total_amount DOUBLE,
payment_type BIGINT,
trip_type BIGINT,
congestion_surcharge DOUBLE
"""

# COMMAND ----------

# Read parquet files recursively
df_trip = spark.read.format("parquet") \
    .schema(myschema) \
    .option("header", "true") \
    .option("recursiveFileLookup", "true") \
    .load("abfss://bronze@pavanstoragenyc.dfs.core.windows.net/trip-data")

# COMMAND ----------

df_trip.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## transformation
# MAGIC

# COMMAND ----------

df_trip_type.display()

# COMMAND ----------

df_trip_type = df_trip_type.withColumnRenamed("description", "trip_description")
df_trip_type = df_trip_type.dropDuplicates()



# COMMAND ----------

from pyspark.sql.functions import trim

df_trip_type = df_trip_type.withColumn("trip_description",trim(col("trip_description")))
df_trip_type.display()


# COMMAND ----------

df_trip_type.write.mode("overwrite").parquet("abfss://silver@pavanstoragenyc.dfs.core.windows.net/trip_type")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation on trip zone

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import split, col, get, when

from pyspark.sql.functions import split, col, get, when

df_trip_zone = df_trip_zone.withColumn(
    "Zone1",
    get(split(col("Zone"), ","), 0)
).withColumn(
    "Zone2",
    when(
        get(split(col("Zone"), ","), 1).isNull(),
        "Not available"
    ).otherwise(get(split(col("Zone"), ","), 1))
)

df_trip_zone.display()



# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import split, col, get, when

df_trip_zone=df_trip_zone.withColumn("zone2",when(col("zone").isNull(),"Not available").otherwise(col("zone")))
df_trip_zone.display()

# COMMAND ----------

df_trip_zone.write.mode("overwrite").parquet("abfss://silver@pavanstoragenyc.dfs.core.windows.net/trip_zone")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation on trip type

# COMMAND ----------

df_trip.display()

# COMMAND ----------

from pyspark.sql.functions import col, to_date, month, year
df_trip=df_trip.withColumn('trip_date',to_date(col('lpep_pickup_datetime')))\
                .withColumn('trip_month',month(col('lpep_pickup_datetime')))\
                .withColumn('trip_year',year(col('lpep_pickup_datetime')))
df_trip.display()
                           

# COMMAND ----------

df_trip.printSchema()

# COMMAND ----------

df_trip.count()

# COMMAND ----------

df_trip.show(10)

# COMMAND ----------

from pyspark.sql.functions import col

df_trip = df_trip.withColumn(
    "ehail_fee",
    col("ehail_fee").cast("string")
)

df_trip = df_trip.fillna({"ehail_fee": "Not Available"})
df_trip.printSchema()

# COMMAND ----------

df_trip = df_trip.drop("ehail_fee")
df_trip.printSchema()

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip.write.mode("overwrite").parquet("abfss://silver@pavanstoragenyc.dfs.core.windows.net/trips2023_data")

# COMMAND ----------


df1 = spark.read.parquet("abfss://bronze@pavanstoragenyc.dfs.core.windows.net/trip-data/green_tripdata_2023-01.parquet")
df1.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

df_trip = df_trip.withColumn(
    "passenger_count",
    col("passenger_count").cast("long")
)

# COMMAND ----------

df_trip.printSchema()

# COMMAND ----------

df2 = spark.read.parquet("abfss://bronze@pavanstoragenyc.dfs.core.windows.net/trip-data/green_tripdata_2023-02.parquet")
df2.printSchema()

# COMMAND ----------

df3 = spark.read.parquet("abfss://bronze@pavanstoragenyc.dfs.core.windows.net/trip-data/green_tripdata_2023-03.parquet")
df3.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

df_trip = df_trip.withColumn(
    "PULocationID", col("PULocationID").cast("int")
).withColumn(
    "DOLocationID", col("DOLocationID").cast("int")
)

# COMMAND ----------

df_trip.printSchema()

# COMMAND ----------

df_trip.write.mode("overwrite").parquet("abfss://silver@pavanstoragenyc.dfs.core.windows.net/trips2023_data")

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, to_date, month, year

df_trip = df_trip \
    .withColumn("VendorID", col("VendorID").cast("long")) \
    .withColumn("PULocationID", col("PULocationID").cast("long")) \
    .withColumn("DOLocationID", col("DOLocationID").cast("long")) \
    .withColumn("lpep_pickup_datetime", col("lpep_pickup_datetime").cast("timestamp")) \
    .withColumn("lpep_dropoff_datetime", col("lpep_dropoff_datetime").cast("timestamp")) \
    .withColumn("trip_date", to_date(col("lpep_pickup_datetime"))) \
    .withColumn("trip_month", month(col("lpep_pickup_datetime"))) \
    .withColumn("trip_year", year(col("lpep_pickup_datetime")))

df_trip.printSchema()

# COMMAND ----------

df_trip.display()

# COMMAND ----------



# COMMAND ----------

df_trip.printSchema()

# COMMAND ----------

df = spark.read.parquet(
"abfss://bronze@pavanstoragenyc.dfs.core.windows.net/trip-data/green_tripdata_2023-01.parquet"
)

df.printSchema()

# COMMAND ----------

df_trip = spark.read \
    .option("mergeSchema","true") \
    .parquet("abfss://bronze@pavanstoragenyc.dfs.core.windows.net/trip-data")

# COMMAND ----------

from pyspark.sql.functions import col

df_trip = df_trip \
    .withColumn("VendorID", col("VendorID").cast("long")) \
    .withColumn("PULocationID", col("PULocationID").cast("long")) \
    .withColumn("DOLocationID", col("DOLocationID").cast("long")) \
    .withColumn("payment_type", col("payment_type").cast("long")) \
    .withColumn("trip_type", col("trip_type").cast("long"))