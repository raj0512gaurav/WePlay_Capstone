# Databricks notebook source
# Define the path to the bronze data in your data lake
silver_data_path = "/pipelines/957a6c11-57ab-4d4d-a6b3-6a8290b092d5/tables/ipl_batting_silver"  

# Read the bronze data into a DataFrame
silver_df = spark.read.format("delta").load(silver_data_path)
display(silver_df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ##Batting Average(year-wise)
# MAGIC Description :  Calculate batting averages for individual players to assess their consistency and 
# MAGIC overall performance year wise

# COMMAND ----------

# Extract the year from the date column
silver_df = silver_df.withColumn("Year", year(col("match_date")))

# Calculate total runs and total matches played by each player year-wise
batting_stats = silver_df.groupBy("batsmen", "Year") \
    .agg(sum("runs_scored").alias("total_runs"), count("*").alias("total_matches"))

# Calculate batting averages
batting_stats = batting_stats.withColumn("batting_average", round(batting_stats["total_runs"] / batting_stats["total_matches"], 2))

# Get distinct batsmen and years
distinct_batsmen = [row["batsmen"] for row in batting_stats.select("batsmen").distinct().collect()]
distinct_years = [str(row["Year"]) for row in batting_stats.select("Year").distinct().collect()]

# Set default values for the widgets
default_batsman = distinct_batsmen[0] if distinct_batsmen else ""
default_year = distinct_years[0] if distinct_years else ""

# Create dropdown widgets for selecting batsmen and year with default values
dbutils.widgets.dropdown("Batsman", default_batsman, distinct_batsmen)
dbutils.widgets.dropdown("Year", default_year, distinct_years)

# Get the selected values from the widgets
selected_batsman = dbutils.widgets.get("Batsman")
selected_year = dbutils.widgets.get("Year")

# Display batting stats based on the selected values
if selected_batsman and selected_year:
    if selected_year == 'all':
        filtered_stats = batting_stats.filter(col("batsmen") == selected_batsman)
    else:
        filtered_stats = batting_stats.filter((col("batsmen") == selected_batsman) & (col("Year") == int(selected_year)))
    display(filtered_stats)

# Show the year-wise batting averages
display(filtered_stats)
display(batting_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Batting Average(career-wise)
# MAGIC Description:  Calculate batting averages for individual players to assess their consistency and 
# MAGIC overall performance career wise
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *

# Extract the year from the date column
silver_df = silver_df.withColumn("Year", year(col("match_date")))

# Calculate total runs and total matches played by each player year-wise
batting_stats = silver_df.groupBy("batsmen", "Year") \
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


# Show the overall career batting averages
#display(overall_batting_avg)

# Get the selected values from the widgets
selected_batsman = dbutils.widgets.get("Batsman")


# Get distinct batsmen and years, and add "all" to the list of choices
distinct_batsmen = ["all"] + [str(row["batsmen"]) for row in overall_batting_avg.select("batsmen").distinct().collect()]




# Check if "all" is selected for batsmen and year
if selected_batsman == "all" and selected_year == "all":
    filtered_stats4 = overall_batting_avg
else:
    # Filter batting stats based on the selected values
    if selected_batsman != "all":
        overall_batting_avg = overall_batting_avg.filter(col("batsmen") == selected_batsman)

    # Select only the desired columns for output
filtered_stats4 = overall_batting_avg.select("batsmen","career_runs", "career_matches", "career_batting_average")

# Apply order by to sort the results by strike rate in descending order
filtered_stats4 = filtered_stats4.orderBy(col("career_runs").desc())

# Show the filtered DataFrame
display(filtered_stats4)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Batting strike rate
# MAGIC Description :  Analyze the strike rates of batsmen to identify aggressive(>=150) or defensive(<150) players

# COMMAND ----------

from pyspark.sql.functions import *
import pandas as pd

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

# Get the selected values from the widgets
selected_batsman = dbutils.widgets.get("Batsman")
selected_year = dbutils.widgets.get("Year")

# Get distinct batsmen and years, and add "all" to the list of choices
distinct_batsmen = ["all"] + [str(row["batsmen"]) for row in batting_stats.select("batsmen").distinct().collect()]
distinct_years = ["all"] + [str(row["Year"]) for row in batting_stats.select("Year").distinct().collect()]

# Create dropdown widgets for selecting batsmen and year with default values
dbutils.widgets.dropdown("Batsman", "all", distinct_batsmen)
dbutils.widgets.dropdown("Year", "all", distinct_years)

# Check if "all" is selected for batsmen and year
if selected_batsman == "all" and selected_year == "all":
    filtered_stats1 = batting_stats
else:
    # Filter batting stats based on the selected values
    if selected_batsman != "all":
        batting_stats = batting_stats.filter(col("batsmen") == selected_batsman)
    if selected_year != "all":
        batting_stats = batting_stats.filter(col("Year") == int(selected_year))

    # Select only the desired columns for output
    filtered_stats1 = batting_stats.select("batsmen", "Year", "strike_rate", "player_category")

# Apply order by to sort the results by strike rate in descending order
filtered_stats1 = filtered_stats1.orderBy(col("strike_rate").desc())

# Show the filtered DataFrame
display(filtered_stats1)



# COMMAND ----------

# MAGIC %md
# MAGIC ## Total boundaries 
# MAGIC Description:  To identify the hitting ability of a batsman
# MAGIC

# COMMAND ----------


# Calculate the total count of boundaries (fours + sixes) for each player year-wise
boundary_counts = silver_df.groupBy("batsmen", "Year") \
    .agg(
        #sum("fours").alias("total_fours"),
        #sum("sixes").alias("total_sixes"),
        (sum("fours") + sum("sixes")).alias("total_boundaries")  # Calculate the total boundaries
    )

# Get the selected values from the widgets
selected_batsman = dbutils.widgets.get("Batsman")
selected_year = dbutils.widgets.get("Year")

# Get distinct batsmen and years, and add "all" to the list of choices
distinct_batsmen = ["all"] + [str(row["batsmen"]) for row in batting_stats.select("batsmen").distinct().collect()]
distinct_years = ["all"] + [str(row["Year"]) for row in batting_stats.select("Year").distinct().collect()]

# Create dropdown widgets for selecting batsmen and year with default values
dbutils.widgets.dropdown("Batsman", "all", distinct_batsmen)
dbutils.widgets.dropdown("Year", "all", distinct_years)

# Check if "all" is selected for batsmen and year
if selected_batsman == "all" and selected_year == "all":
    filtered_stats2 = boundary_counts
else:
    # Filter batting stats based on the selected values
    if selected_batsman != "all":
        boundary_counts = boundary_counts.filter(col("batsmen") == selected_batsman)
    if selected_year != "all":
        boundary_counts = boundary_counts.filter(col("Year") == int(selected_year))

    # Select only the desired columns for output
    filtered_stats2 = boundary_counts.select("batsmen", "Year", "total_boundaries")

# Apply order by to sort the results by strike rate in descending order
filtered_stats2 = filtered_stats2.orderBy(col("total_boundaries").desc())

# Show the filtered DataFrame
display(filtered_stats2)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Centuries and half centuries
# MAGIC Description:  Identify players with the most centuries and half-centuries, indicating their ability to 
# MAGIC convert starts into big scores.

# COMMAND ----------


# Calculate the number of half-centuries (50-99) and centuries (100 or more) scored by each player
half_centuries_and_centuries = silver_df.groupBy("batsmen") \
    .agg(
        sum(when((col("runs_scored") >= 50) & (col("runs_scored") < 100), 1).otherwise(0)).alias("Half_centuries"),
        sum(when(col("runs_scored") >= 100, 1).otherwise(0)).alias("Centuries")
    )

# Show the number of half-centuries and centuries scored by each player
#display(half_centuries_and_centuries)

# Get the selected values from the widgets
selected_batsman = dbutils.widgets.get("Batsman")


# Get distinct batsmen and years, and add "all" to the list of choices
distinct_batsmen = ["all"] + [str(row["batsmen"]) for row in half_centuries_and_centuries.select("batsmen").distinct().collect()]




# Check if "all" is selected for batsmen and year
if selected_batsman == "all" and selected_year == "all":
    filtered_stats2 = half_centuries_and_centuries
else:
    # Filter batting stats based on the selected values
    if selected_batsman != "all":
        half_centuries_and_centuries = half_centuries_and_centuries.filter(col("batsmen") == selected_batsman)

    # Select only the desired columns for output
filtered_stats3 = half_centuries_and_centuries.select("batsmen", "Half_centuries", "Centuries")

# Apply order by to sort the results by strike rate in descending order
filtered_stats3 = filtered_stats3.orderBy(col("Half_centuries").desc())

# Show the filtered DataFrame
display(filtered_stats3)
