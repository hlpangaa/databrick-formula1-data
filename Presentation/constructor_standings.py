# Databricks notebook source
# MAGIC %md
# MAGIC ### Using the window function to calculate the rank of the consturctor

# COMMAND ----------

races_results_df = spark.read.parquet("/mnt/formula1hlpangaa/presentation/races_results")


# COMMAND ----------

display(races_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, when, count,col, rank, desc
from pyspark.sql import Window

rankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"))

constructor_standing_df = races_results_df\
                                    .groupBy("race_year","team")\
                                    .agg(sum("points").alias("total_points"),
                                        count(when(col("position")==1,True)).alias("wins"))\
                                    .withColumn("rank",rank().over(rankSpec))

# COMMAND ----------

display(constructor_standing_df.filter("race_year = 2019 or race_year = 2020"))

# COMMAND ----------

##constructor_standing_df.write.mode("overwrite").parquet("/mnt/formula1hlpangaa/presentation/constructor_standing")
constructor_standing_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standing")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1hlpangaa/presentation/constructor_standing").filter("race_year = 2019 or race_year = 2020"))

# COMMAND ----------

dbutils.notebook.exit("Success")