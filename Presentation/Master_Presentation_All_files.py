# Databricks notebook source
# DBTITLE 1,Run all presentation files
dbutils.notebook.run("Races_Results_Join",0)

# COMMAND ----------

dbutils.notebook.run("constructor_standings",0)
dbutils.notebook.run("driver_standings",0)