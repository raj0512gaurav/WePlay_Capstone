# Databricks notebook source
# Define the path to the cleaned silver data in your data lake
silver_data_path = "/pipelines/c8f60a52-2538-46ba-9a33-b67b68b89cca/tables/ipl_deliveries_silver"  

# Read the bronze data into a DataFrame
df = spark.read.format("delta").load(silver_data_path)

display(df)

# COMMAND ----------

from pyspark.sql.functions import *


# COMMAND ----------

#Bowling Averages: 
# Calculate the total runs conceded by each bowler
runs_conceded_by_bowler = df.groupBy("bowler") \
    .agg(
        sum("runs_total").alias("total_runs_conceded")
    )

# Calculate the bowling average for each bowler
bowler_average = runs_conceded_by_bowler.select("bowler", (col("total_runs_conceded") / 10).alias("Bowling_average"))

display(bowler_average)


# COMMAND ----------

#Bowling Strike rate: 
# Group the data by bowler and count deliveries and wickets
bowler_stats = df.groupBy("bowler").agg(
    count("delivery").alias("deliveries"),
    count(when(df["mode_of_dismissal"].isNotNull(), 1)).alias("wickets")
)

# Calculate the bowling strike rate
bowler_stats = bowler_stats.withColumn("strike_rate", round((bowler_stats["deliveries"] / bowler_stats["wickets"]).cast("double"),2))

# Show the results
display(bowler_stats)

# COMMAND ----------

#Mode of dismissal:
# Group the data by mode of dismissal and count the occurrences
dismissal_analysis = df.groupBy("mode_of_dismissal").agg(count("*").alias("dismissal_count"))

# Replace null values in the "mode_of_dismissal" column with 'not out'
dismissal_analysis = dismissal_analysis.withColumn(
    "mode_of_dismissal",
    when(dismissal_analysis["mode_of_dismissal"].isNull(), lit('not out')).otherwise(dismissal_analysis["mode_of_dismissal"])
)

# Show the results
display(dismissal_analysis)

# COMMAND ----------

#Bowling Performance:

# Filter rows where a wicket was taken
wickets_df = df.filter(df["mode_of_dismissal"].isNotNull())

# Group the data by bowler and count the wickets

bowler_wickets = wickets_df.groupBy("bowler").agg(count("*").alias("wickets_taken"))

# Order the results by the number of wickets taken in descending order

bowler_wickets = bowler_wickets.orderBy("wickets_taken", ascending=False)

# Show the results

display(bowler_wickets)

# COMMAND ----------

#Death over: 
# Define the range of death overs (e.g., overs 16 to 20)
death_overs_start = 16
death_overs_end = 20

# Filter the data for death overs
death_overs_data = df.filter((df["bowling_over"] >= death_overs_start) & (df["bowling_over"] <= death_overs_end))

# Calculate the total runs scored by each batsman in death overs
death_overs_runs = death_overs_data.groupBy("batsman") \
    .agg(
        sum("runs_batsman").alias("total_runs_in_death_overs"),
        count("delivery").alias("total_balls_faced_in_death_overs")
    )

# Calculate the death over strike rate for each batsman
death_over_strike_rate = death_overs_runs.withColumn(
    "death_over_strike_rate",
    round((col("total_runs_in_death_overs") / col("total_balls_faced_in_death_overs")) * 100, 2)
)

# Select only the desired columns for output
death_over_strike_rate = death_over_strike_rate.select("batsman", "death_over_strike_rate")

# Apply order by to sort the results by strike rate in descending order
death_over_strike_rate = death_over_strike_rate.orderBy(col("death_over_strike_rate").desc())

# Show the death over strike rate for each batsman
display(death_over_strike_rate)


# COMMAND ----------


