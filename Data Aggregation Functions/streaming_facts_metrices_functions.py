# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC ##Economy rate

# COMMAND ----------

def economy_rate_stream(stream_df):
    
    df=stream_df
    # Calculate the total runs conceded by each bowler

    total_runs_conceded = df.groupBy("bowler").agg(sum("runs").alias("total_runs_conceded"))

    # Calculate the total deliveries bowled by each bowler

    total_deliveries = df.groupBy("bowler").agg(count("ball").alias("total_deliveries"))

    # Join the two DataFrames to calculate the economy rate

    economy_df = total_runs_conceded.join(total_deliveries, "bowler")
    # Calculate the economy rate

    economy_df = economy_df.withColumn("economy_rate", round((economy_df["total_runs_conceded"] / economy_df["total_deliveries"]),2))

    # Select the relevant columns

    economy_df = economy_df.select("bowler", "economy_rate")

    return economy_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Number of extras

# COMMAND ----------

def Number_of_extras_stream(stream_df):

    df=stream_df
    #counting number of extras given by the bowler
    extras_by_bowler = df.groupBy("bowler") \
        .agg(sum("extras").alias("total_extras"))
    # Show the result
    return extras_by_bowler

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dot Ball Percentage

# COMMAND ----------

def dot_ball_percentage_stream(stream_df):
    # Filter for dot balls (runs_total = 0 and extras_type is null or empty)
    deliveries_df=stream_df
    dot_balls_df = deliveries_df.filter((deliveries_df.runs == 0) )

    # Group the dot ball DataFrame by the bowler's name and count the number of dot balls bowled by each bowler
    dot_balls_count_df = dot_balls_df.groupBy("bowler").agg(count("*").alias("dot_balls_count"))

    # Calculate the total number of balls bowled by each bowler in their career
    total_balls_df = deliveries_df.groupBy("bowler").agg(count("*").alias("total_balls_count"))

    # Join the dot balls count DataFrame with the total balls count DataFrame using the bowler's name as the key
    dot_balls_percentage_df = dot_balls_count_df.join(total_balls_df, "bowler", "inner")

    # Calculate the dot ball percentage for each bowler and round it to two decimal places
    dot_balls_percentage_df = dot_balls_percentage_df.withColumn("dot_ball_percentage",
        round((dot_balls_percentage_df.dot_balls_count / dot_balls_percentage_df.total_balls_count) * 100, 2))
    
    return dot_balls_percentage_df
