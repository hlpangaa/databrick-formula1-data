# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM v_race_results

# COMMAND ----------

# MAGIC %md
# MAGIC temp view is expected to be not found; glbal view can still be found

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC   FROM global_temp.gv_race_results;

# COMMAND ----------

