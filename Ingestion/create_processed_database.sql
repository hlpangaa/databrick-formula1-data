-- Databricks notebook source

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula1hlpangaa/processed"

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/formula1hlpangaa/presentation"

-- COMMAND ----------

DESC DATABASE f1_presentation;

-- COMMAND ----------

