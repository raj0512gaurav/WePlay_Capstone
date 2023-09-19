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

# Filter rows where a wicket was taken
wickets_df = df.filter(df["mode_of_dismissal"].isNotNull())

# Group the data by bowler and count the wickets