-- Databricks notebook source
DROP DATABASE f1_processed CASCADE;
DROP DATABASE f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE f1_processed
LOCATION "/mnt/formula1hlapgnaa/processed";

CREATE DATABASE f1_f1_presentation
LOCATION "/mnt/formula1hlapgnaa/presentation";
