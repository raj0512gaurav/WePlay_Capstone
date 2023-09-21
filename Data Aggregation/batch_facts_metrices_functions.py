# Databricks notebook source
#importing functions
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

location_ipl_deliveries_table ="dbfs:/pipelines/9ad3a5f0-6dbb-4ff7-a7e2-df19abdaac46/tables/ipl_deliveries_silver"



# COMMAND ----------

deliveries_df= spark.read.format("delta").option("header","true").load(location_ipl_deliveries_table)

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

    economy_df = economy_df.withColumn("economy_rate", (economy_df["total_runs_conceded"] / economy_df["total_deliveries"]))

    # Select the relevant columns

    economy_df = economy_df.select("bowler", "economy_rate")

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
