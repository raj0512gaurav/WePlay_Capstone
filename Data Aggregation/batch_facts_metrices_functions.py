# Databricks notebook source
#Importing modules
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Number of extras

# COMMAND ----------

def Number_of_extras(deliveries_df):

    df=deliveries_df
    #counting number of extras given by the bowler
    extras_by_bowler = df.groupBy("bowler") \
        .agg(sum("runs_extras").alias("total_extras"))

    # You can optionally order the results by the total_extras in descending order.
    extras_by_bowler = extras_by_bowler.orderBy(col("total_extras").desc())
    # Show the result
    return extras_by_bowler

# COMMAND ----------

# MAGIC %md
# MAGIC ##Dot ball percentage

# COMMAND ----------

def dot_ball_percentage(deliveries_df):
# Filter for dot balls (runs_total = 0 and extras_type is null or empty)
    dot_balls_df = deliveries_df.filter((deliveries_df.runs_total == 0) )

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

# COMMAND ----------

# MAGIC %md
# MAGIC ##Economy rate

# COMMAND ----------

def Economy_rate(deliveries_df):
    df=deliveries_df
    # Calculate the total runs conceded by each bowler

    total_runs_conceded = df.groupBy("bowler").agg(F.sum("runs_batsman").alias("total_runs_conceded"))

    # Calculate the total deliveries bowled by each bowler

    total_deliveries = df.groupBy("bowler").agg(F.count("delivery").alias("total_deliveries"))

    # Join the two DataFrames to calculate the economy rate

    economy_df = total_runs_conceded.join(total_deliveries, "bowler")
    # Calculate the economy rate
    economy_df = economy_df.withColumn("economy_rate", round((economy_df["total_runs_conceded"] / economy_df["total_deliveries"]),2))

    # Select the relevant columns

    economy_df = economy_df.select("bowler", "economy_rate")

    return economy_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bowling strike rate

# COMMAND ----------

def Bowling_strike_rate(deliveries_df):

    #counting number of wickets takes
    wickets_taken_df = deliveries_df \
            .filter((col("mode_of_dismissal") == "caught")) \
            .groupBy("bowler") \
            .agg((count("*")*6).alias("wickets_taken")) 

    # Calculate the total number of balls bowled by each bowler
    balls_bowled_df = deliveries_df \
            .groupBy("bowler") \
            .agg(count("*").alias("balls_bowled"))
    # Join the wickets_taken_df and balls_bowled_df DataFrames
    combined_df = balls_bowled_df.join(wickets_taken_df, "bowler", "left_outer")
    # Calculate the strike rate for each bowler and round it to two decimal places
    strike_rate_df = combined_df.withColumn(
            "strike_rate",
            round((combined_df["balls_bowled"] / (combined_df["wickets_taken"] + 1e-6)) * 6, 2))
    result_df=strike_rate_df.select("bowler","strike_rate")
    return result_df

# COMMAND ----------

# MAGIC %md
# MAGIC ##Best Bowling Figures

# COMMAND ----------

def best_bowling_figures(deliveries_df):

    #Filtering records by removing run outs
    df = deliveries_df.filter((col('mode_of_dismissal') != 'run out') | (col('mode_of_dismissal').isNull()))
    
    #Grouping
    df = df.groupBy('match_key', 'bowler').agg(count('player_out').alias('wickets'), sum('runs_batsman').alias('runs'))
    df = df.withColumn("best_bowling_figure", concat_ws("/", col("wickets"), col("runs")))

    # Defining a window specification to partition by "bowler_name" and rank by the criteria
    window_spec = Window.partitionBy("bowler").orderBy(col("wickets").desc(), col("runs").asc())

    # Calculate the ranking column based on wickets taken (you can adjust the criteria)
    df = df.withColumn("ranking", rank().over(window_spec))

    # Filter to keep only the rows where the ranking is 1 (best bowling figures for each bowler)
    best_bowling_figures = df.filter(col("ranking") == 1).drop('ranking','wickets', 'runs').orderBy(desc('best_bowling_figure')).drop('match_key')

    return best_bowling_figures

# COMMAND ----------

# MAGIC %md
# MAGIC ##Maiden Overs

# COMMAND ----------

def maiden_overs(deliveries_df):
    #Grouping
    df = deliveries_df.groupBy('match_key','innings_order','bowling_over','bowler').agg(sum('runs_total').alias('runs'))

    #Applying filter for overs having no runs
    df = df.filter(col('runs')==0).drop('runs')
    
    #Counting maiden overs per bowler
    maiden_overs = df.groupBy('bowler').agg(count('match_key').alias('maiden_overs')).orderBy(desc('maiden_overs'))

    return maiden_overs

# COMMAND ----------

# MAGIC %md
# MAGIC ##Boundaries Conceded

# COMMAND ----------

def boundaries_conceded(deliveries_df):
    #Applying Filter for boundaries
    df = deliveries_df.filter((col('runs_batsman')==4) | (col('runs_batsman')==6))

    #Grouping by bowler
    boundaries_conceded = df.groupBy('bowler').agg(count('match_key').alias('boundaries_conceded')).orderBy(desc('boundaries_conceded'))

    return boundaries_conceded
