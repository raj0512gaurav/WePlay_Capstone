# Databricks notebook source
# MAGIC %md
# MAGIC ## Accessing raw data from adls 

# COMMAND ----------

#setting configurations for raw data mountpoint
spark.conf.set("source_batch_data","/mnt/weplaybatchraw/")

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

source = spark.conf.get("source_batch_data")

# COMMAND ----------

@dlt.table
def ipl_batting_bronze():
    return (
        spark.read.format("csv").option("header", "true").option("inferSchema","true"). 
         load(f"{source}/ipl_batting.csv/")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Validating transforming and enriching data

# COMMAND ----------

@dlt.view(
    comment="cleaning batting data"
)
def ipl_batting_bronze_clean():
    return (
        dlt.read("batting_bronze")
        .withColumnRenamed("R", "runs_scored")
        .withColumnRenamed("B", "balls_faced")
    )

# COMMAND ----------

@dlt.table
@dlt.expect_or_fail("valid_match_key", "match_key IS NOT NULL")
@dlt.expect_or_drop("valid_match_no", "match_no IS NOT NULL")

def ipl_batting_silver():
    return (
        dlt.read("batting_bronze_clean")
    )
