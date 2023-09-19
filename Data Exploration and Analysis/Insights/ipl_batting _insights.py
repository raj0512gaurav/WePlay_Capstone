# Databricks notebook source
# Define the path to the bronze data in your data lake
silver_data_path = "/pipelines/c8f60a52-2538-46ba-9a33-b67b68b89cca/tables/ipl_batting_silver"  

# Read the bronze data into a DataFrame
silver_df = spark.read.format("delta").load(silver_data_path)

display(silver_df)

# COMMAND ----------

from pyspark.sql.functions import *

# Extract the year from the date column
silver_df = silver_df.withColumn("year", year(col("match_date")))

# Calculate total runs and total matches played by each player year-wise
batting_stats = silver_df.groupBy("batsmen", "year") \
    .agg(sum("runs_scored").alias("total_runs"), count("*").alias("total_matches"))

# Calculate batting averages
batting_stats = batting_stats.withColumn("batting_average", round(batting_stats["total_runs"] / batting_stats["total_matches"], 2))

# Calculate overall batting average for each player
overall_batting_avg = batting_stats.groupBy("batsmen") \
    .agg(sum("total_runs").alias("career_runs"), sum("total_matches").alias("career_matches")) \
    .withColumn("career_batting_average", round(col("career_runs") / col("career_matches"), 2))
  
# Apply order by to sort the results by batting average in descending order
overall_batting_avg = overall_batting_avg.orderBy(col("career_batting_average").desc())

# Apply order by to sort the results by batting average in descending order
batting_stats = batting_stats.orderBy(col("batting_average").desc())

# Show the year-wise batting averages
display(batting_stats)

# Show the overall career batting averages
display(overall_batting_avg)

# COMMAND ----------

# Extract the year from the date column
silver_df = silver_df.withColumn("Year", year(col("match_date")))

# Calculate total runs, total balls faced, and total matches played by each player year-wise
batting_stats = silver_df.groupBy("batsmen", "Year") \
    .agg(
        sum("runs_scored").alias("total_runs"),
        sum("balls_faced").alias("total_balls_faced"),
        count("*").alias("total_matches")
    )

# Calculate batting averages
batting_stats = batting_stats.withColumn("batting_average", round(batting_stats["total_runs"] / batting_stats["total_matches"], 2))


# Calculate strike rates
batting_stats = batting_stats.withColumn("strike_rate", round((batting_stats["total_runs"] / batting_stats["total_balls_faced"]) * 100, 2))

# Categorize players as aggressive (>=150) or defensive (<150) based on strike rate
batting_stats = batting_stats.withColumn("player_category", when(batting_stats["strike_rate"] >= 150, "Aggressive").otherwise("Defensive"))


# Select only the desired columns for output
batting_stats = batting_stats.select("batsmen", "strike_rate", "player_category")

# Apply order by to sort the results by strike rate in descending order
batting_stats = batting_stats.orderBy(col("strike_rate").desc())

# Show the year-wise batting statistics and player categories
display(batting_stats)

# COMMAND ----------

# Extract the year from the date column
silver_df = silver_df.withColumn("Year", year(col("match_date")))

# Calculate the total count of boundaries (fours + sixes) for each player year-wise
boundary_counts = silver_df.groupBy("batsmen", "Year") \
    .agg(
        #sum("fours").alias("total_fours"),
        #sum("sixes").alias("total_sixes"),
        (sum("fours") + sum("sixes")).alias("total_boundaries")  # Calculate the total boundaries
    )

# Show the year-wise boundary counts
display(boundary_counts)

# COMMAND ----------

# Calculate the runs scored by each player against each team
runs_vs_teams = silver_df.groupBy("batsmen", "team_2") \
    .agg(
        sum("runs_scored").alias("Runs_Sored")
    )
runs_vs_teams = runs_vs_teams.withColumnRenamed("team_2", "Opponent_team")
# Show the runs scored by each player against each team
display(runs_vs_teams)

# COMMAND ----------


# Calculate the number of half-centuries (50-99) and centuries (100 or more) scored by each player
half_centuries_and_centuries = silver_df.groupBy("batsmen") \
    .agg(
        sum(when((col("runs_scored") >= 50) & (col("runs_scored") < 100), 1).otherwise(0)).alias("Half_centuries"),
        sum(when(col("runs_scored") >= 100, 1).otherwise(0)).alias("Centuries")
    )

# Show the number of half-centuries and centuries scored by each player
display(half_centuries_and_centuries)



# COMMAND ----------


