# Databricks notebook source
# Define the path to the bronze data in your data lake
silver_data_path = "/pipelines/c8f60a52-2538-46ba-9a33-b67b68b89cca/tables/ipl_deliveries_silver"  

# Read the bronze data into a DataFrame
silver_df = spark.read.format("delta").load(silver_data_path)

display(silver_df)

# COMMAND ----------


from pyspark.sql.functions import sum, count, col

# Calculate the total runs conceded by each bowler
runs_conceded_by_bowler = silver_df.groupBy("bowler") \
    .agg(
        sum("runs_total").alias("total_runs_conceded")
    )

# Calculate the bowling average for each bowler
bowler_average = runs_conceded_by_bowler.select("bowler", (col("total_runs_conceded") / 10).alias("Bowling_average"))

display(bowler_average)


# COMMAND ----------





# COMMAND ----------


