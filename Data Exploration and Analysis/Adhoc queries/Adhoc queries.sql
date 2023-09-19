-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Batsmen Stats

-- COMMAND ----------

SELECT
    batsmen AS player_name,
    COUNT(DISTINCT match_key) AS matches_played,
    SUM(runs_scored) AS total_runs_scored,
    SUM(CASE WHEN runs_scored >= 50 AND runs_scored < 100 THEN 1 ELSE 0 END) AS Half_centuries,
    SUM(CASE WHEN runs_scored >= 100 THEN 1 ELSE 0 END) AS Centuries,
    SUM(fours) AS Fours,
    SUM(sixes) AS Sixes,
    CASE WHEN SUM(CASE WHEN wicket_status NOT LIKE '%not out%' THEN 1 ELSE 0 END) > 0 THEN
    ROUND(CAST(SUM(runs_scored) AS DECIMAL) / SUM(CASE WHEN wicket_status NOT LIKE '%not out%' THEN 1 ELSE 0 END), 2)
    ELSE null
    END AS Batting_average,
    ROUND(
    CASE
        WHEN SUM(balls_faced) > 0 THEN (SUM(runs_scored) * 100.0) / SUM(balls_faced)
        ELSE NULL
    END, 2 ) AS strike_rate,
    SUM(CASE WHEN runs_scored < 10 AND wicket_status NOT LIKE '%not out%' THEN 1 ELSE 0 END) AS Times_dismissed_below_10
FROM
    ipl_batting_silver
GROUP BY
    batsmen
ORDER BY
    player_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Batsmen runs summary

-- COMMAND ----------

CREATE OR REPLACE FUNCTION GetRunsSummary(batsman_name STRING)
RETURNS TABLE (
    id INT,
    match_key STRING,
    match_date DATE,
    location_match STRING,
    R INT,
    cumulative_runs INT
)
    RETURN
    SELECT
        ROW_NUMBER() OVER (ORDER BY m.match_date) AS id,
        bs.match_key,
        m.match_date,
        m.ground,
        bs.runs_scored AS R,
        SUM(bs.runs_scored) OVER (ORDER BY m.match_date) AS cumulative_runs
    FROM
        ipl_batting_silver bs
    INNER JOIN
        ipl_matches_silver m
    ON
        bs.match_key = m.match_key
    WHERE
        bs.batsmen = batsman_name
    ORDER BY
        m.match_date;


SELECT * FROM GetRunsSummary('SR Tendulkar');


