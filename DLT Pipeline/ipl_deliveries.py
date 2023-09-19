# Databricks notebook source
# MAGIC %md
# MAGIC ###IMPORTING ALL PYSPARK FUNCTIONS AND DLT

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

#set spark config for raw data mount
spark.conf.set('raw_batch', '/mnt/weplaybatchraw')

source = spark.conf.get('raw_batch')

# COMMAND ----------

# MAGIC %md
# MAGIC ###INGESTING RAW DATA FROM ADLS AND CREATING TABLE ON TOP OF THAT

# COMMAND ----------

@dlt.create_table(
  comment="the raw deliveries data ingested from adls ",
  table_properties={
    "IPL_deliveries.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def ipl_deliveries_bronze():
    deliveries_df = spark.read.format("csv")\
        .option("header","true")\
        .option("inferschema","true").load(f"{source}/ipl_deliveries.csv")
    return deliveries_df

# COMMAND ----------

# MAGIC %md
# MAGIC ##APPLYING QUALITY CHECKS ON THE DATA 
# MAGIC

# COMMAND ----------

valids = {"valid_innings_order": "innings_order IS NOT NULL", "valid_playing_team":"playing_batsman_team IS NOT NULL", "valid_bowling":"bowling_over IS NOT NULL AND (bowling_over>0 AND bowling_over<=20)", "valid_delivery":"delivery IS NOT NULL AND (delivery>0 AND delivery <21)","valid_batsman":"batsman IS NOT NULL",
"valid_bowler":"bowler IS NOT NULL",
"Valid_non_striker":"non_striker IS NOT NULL"}

@dlt.table

@dlt.expect_or_fail("valid_id","match_key IS NOT NULL")
@dlt.expect_all_or_drop(valids)

def ipl_deliveries_silver():
    return (
        dlt.read("ipl_deliveries_bronze")
    )
