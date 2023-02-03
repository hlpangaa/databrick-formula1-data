-- Databricks notebook source


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Step 1 - create database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Step 2 - create tables

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.circuits(
circuitId INT ,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT ,
url STRING)
using csv
OPTIONS (path "/mnt/formula1hlpangaa/raw/circuits.csv", header true)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.races(
raceId INT,
year INT,
round INT ,
circuitId INT,
name STRING,
date STRING,
time STRING)
using csv
OPTIONS (path "/mnt/formula1hlpangaa/raw/races.csv", header true)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.constructors(
constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING)
using json
OPTIONS (path "/mnt/formula1hlpangaa/raw/constructors.json")

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.drivers(
driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob DATE ,
nationality STRING,
url STRING)
using json
OPTIONS (path "/mnt/formula1hlpangaa/raw/drivers.json")

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.results(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
grid INT,
position INT,
positionText STRING,
points FLOAT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed STRING,
statusId INT)
using json
OPTIONS (path "/mnt/formula1hlpangaa/raw/results.json")

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.pitstops(
stop INT,
raceId INT,
driverId INT,
lap INT,
time TIMESTAMP,
duration STRING,
milliseconds INT)
using json
OPTIONS (path "/mnt/formula1hlpangaa/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
using csv
OPTIONS (path "/mnt/formula1hlpangaa/raw/lap_times")

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
qualifyId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING)
using json
OPTIONS (path "/mnt/formula1hlpangaa/raw/qualifying", multiLine true)

-- COMMAND ----------

