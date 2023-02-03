# Databricks notebook source
# MAGIC %md
# MAGIC ### Using the window function to calculate the rank of the driver

# COMMAND ----------

races_results_df = spark.read.parquet("/mnt/formula1hlpangaa/presentation/races_results")


# COMMAND ----------

display(races_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, when, count,col, rank, desc
from pyspark.sql import Window

rankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"))

driver_standing_df = races_results_df\
                                    .groupBy("race_year","driver_name")\
                                    .agg(sum("points").alias("total_points"),
                                        count(when(col("position")==1,True)).alias("wins"))\
                                    .withColumn("rank",rank().over(rankSpec))

# COMMAND ----------

display(driver_standing_df.filter("race_year = 2019 or race_year = 2020"))

# COMMAND ----------

##driver_standing_df.write.mode("overwrite").parquet("/mnt/formula1hlpangaa/presentation/driver_standings")
driver_standing_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1hlpangaa/presentation/driver_standings").filter("race_year = 2019 or race_year = 2020"))

# COMMAND ----------

dbutils.notebook.exit("Success")