# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingesting data into Bronze Table

# COMMAND ----------

#set config for raw data mount point
spark.conf.set('raw_batch','/mnt/weplaybatchraw')

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

source = spark.conf.get('raw_batch')

# COMMAND ----------

@dlt.table(
    name = "ipl_matches_bronze",
    comment = "Ingesting Raw data of ipl_matches"
)
def ingest_ipl_matches():
    return (
        spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(f'{source}/ipl_matches.csv')
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Cleaning and Validating Bronze Data

# COMMAND ----------

valids = {'valid_match_date':'match_date IS NOT NULL', 'valid_ground':'ground IS NOT NULL', 'valid_teams':'(team_1 IS NOT NULL) AND (team_2 IS NOT NULL)'}
@dlt.view(
    name = 'ipl_matches_clean_view',
    comment = 'Cleaning ipl_matches_bronze data based on contraints'
)
@dlt.expect_or_fail('valid_match_key','match_key IS NOT NULL')
@dlt.expect_all_or_drop(valids)
def ipl_matches_expectations():
    return (
        dlt.read('ipl_matches_bronze')
    )

# COMMAND ----------

@dlt.table(
    comment = 'Handling Null Values'
)
def ipl_matches_silver():
    return (
        dlt.read('ipl_matches_clean_view')
        .dropDuplicates()
        .withColumn('winner', when(col('winner') == 'NONE', None).otherwise(col('winner')))
        .withColumn('toss_winner_code', when(col('toss_winner_code') == 'NONE', None).otherwise(col('winner')))
    )
