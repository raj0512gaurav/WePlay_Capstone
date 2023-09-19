# Databricks notebook source
spark.conf.set("stream_matches_mount","/mnt/weplaystreaming/")
source = spark.conf.get("stream_matches_mount")

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ##Ingest Data with Auto Loader

# COMMAND ----------

@dlt.table(
    name = "streaming_matches_bronze",
    comment = "Raw data from matches CDC feed"
)
def ingest_streaming_matches_cdc():
    df = spark.readStream\
        .format("cloudFiles")\
        .option("cloudFiles.format", "parquet")\
        .option("cloudFiles.schemaLocation", f"{source}/loader_stream/")\
        .option('inferSchema', 'true')\
        .load(f"{source}")
    df = df.select([col(c).alias(c.lower().replace(" ", "_")) for c in df.columns])
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ##Quality Enforcement

# COMMAND ----------

@dlt.view
@dlt.expect_or_drop("valid_Match_ID", "match_id IS NOT NULL")
@dlt.expect_or_drop("valid_match_date", "match_date IS NOT NULL")

def matches_bronze_clean():
    df = dlt.read_stream("streaming_matches_bronze")
    df = df.withColumn("match_date", to_timestamp(col("match_date"), "yyyy-MM-dd HH:mm:ss"))
    
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processing CDC Data with **`dlt.apply_changes()`**

# COMMAND ----------

dlt.create_target_table(
    name = "streaming_matches_silver")

dlt.apply_changes(
    target = "streaming_matches_silver",
    source = "matches_bronze_clean",
    keys = ["Match_ID"],
    sequence_by = col("eventprocessedutctime"),
    except_column_list = ["eventprocessedutctime", "partitionid", "eventenqueuedutctime", "_rescued_data"])
