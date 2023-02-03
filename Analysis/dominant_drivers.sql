-- Databricks notebook source
select driver_name,
       count(*) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
 FROM f1_presentation.calculated_race_results
 WHERE race_year BETWEEN 2011 AND 2020
GROUP BY driver_name
HAVING total_races > 50
Order BY avg_points DESC

-- COMMAND ----------

