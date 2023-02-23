-- Databricks notebook source
DROP DATABASE f1_processed CASCADE; 

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/stdldehlpangaa/processed"

-- COMMAND ----------

DESC DATABASE f1_processed;

-- COMMAND ----------

