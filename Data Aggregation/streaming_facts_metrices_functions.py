# Databricks notebook source
# MAGIC %md
# MAGIC ##Maiden Overs

# COMMAND ----------

def maiden_overs_stream(stream_df):
    #Grouping
    df = stream_df.groupBy('match_id','inning','over','bowler').agg(sum('runs').alias('runs'))

    #Applying filter for overs having no runs
    df = df.filter(col('runs')==0).drop('runs')
    
    #Counting maiden overs per bowler
    maiden_overs = df.groupBy('bowler').agg(count('match_id').alias('maiden_overs')).orderBy(desc('maiden_overs'))

    return maiden_overs

# COMMAND ----------

# MAGIC %md
# MAGIC ##Boundaries Conceded

# COMMAND ----------

def boundaries_conceded_stream(stream_df):
    #Applying Filter for boundaries
    df = stream_df.filter((col('runs')==4) | (col('runs')==6))

    #Grouping by bowler
    boundaries_conceded = df.groupBy('bowler').agg(count('match_id').alias('boundaries_conceded')).orderBy(desc('boundaries_conceded'))

    return boundaries_conceded
