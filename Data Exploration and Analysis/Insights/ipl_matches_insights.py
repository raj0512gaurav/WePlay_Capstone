# Databricks notebook source
# Define the path to the Silver data in your data lake

silver_cleaned_data_path = "/pipelines/c8f60a52-2538-46ba-9a33-b67b68b89cca/tables/ipl_matches_silver"  
# Read the silver into a DataFrame
df = spark.read.format("delta").load(silver_cleaned_data_path)
display(df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Popular Ground Analysis
# MAGIC Description: To identify the most frequently used ground for matches

# COMMAND ----------


popular_grounds = df.groupBy("ground").count().orderBy("count", ascending=False)
display(popular_grounds)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Match_Outcome Analysis
# MAGIC Description: Analyze whether certain win modes are more common for specific teams.
# MAGIC

# COMMAND ----------

 
win_mode_analysis = df.groupBy("team_1", "win_mode").agg(count("*").alias("win_mode_count"))

# Show the results
display(win_mode_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Winning Team Analysis 
# MAGIC Description: No. of matches each team wins 
# MAGIC

# COMMAND ----------



# Calculate the number of matches won by each team
team1_wins = df.groupBy("team_1").agg(count(when(df["winner"] == df["team_1"], 1)).alias("wins_team1"))
team2_wins = df.groupBy("team_2").agg(count(when(df["winner"] == df["team_2"], 1)).alias("wins_team2"))

# Rename the "team_1" column to "team_name" in both DataFrames
team1_wins = team1_wins.withColumnRenamed("team_1", "team_name")
team2_wins = team2_wins.withColumnRenamed("team_2", "team_name")

# Union the results and calculate total wins
total_wins = team1_wins.join(team2_wins, team1_wins["team_name"] == team2_wins["team_name"], "full_outer") \
    .select(
        team1_wins["team_name"],
        (team1_wins["wins_team1"] + team2_wins["wins_team2"]).alias("total_wins")
    )

# Display the teams with total wins
display(total_wins.orderBy("total_wins", ascending=False))



# COMMAND ----------

# MAGIC %md
# MAGIC ##Match_date_Analysis 
# MAGIC Description: Analyze if there are any specific days of the week when matches are more frequent

# COMMAND ----------


# Extract month and day of the week from match_date
df = df.withColumn("month", date_format("match_date", "MMMM"))  # "MMMM" format gives the full month name
df = df.withColumn("day_of_week", dayofweek("match_date"))

# Count matches per month and per day of the week
matches_per_month = df.groupBy("month").count()
matches_per_day = df.groupBy("day_of_week").count()


# COMMAND ----------

display(matches_per_day)
display(matches_per_month)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Sucess rate 
# MAGIC Description:  Investigate whether winning the toss has a significant impact on the match result
# MAGIC

# COMMAND ----------


# Calculate the total number of matches and the number of matches won by the toss winner
total_matches = df.count()

# Calculate the number of matches won by each team as toss winner
toss_winner_matches_won = df.filter(df["toss_winner_code"] == df["winner"])

# Group the data by each team and count the number of matches won by the toss winner for each team
team_success_rate = toss_winner_matches_won.groupBy("toss_winner_code").count()

# Calculate the toss winner's success rate as a percentage for each team
team_success_rate = team_success_rate.withColumn("success_rate", (col("count") / total_matches) * 100)

# Display the success rate for each team
display(team_success_rate)

