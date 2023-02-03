# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Results.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the single line Json file using dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField,IntegerType,StringType,DateType,FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId",IntegerType(),True),
                                    StructField("raceId",IntegerType(),True),
                                    StructField("driverId",IntegerType(),True),
                                    StructField("constructorId",IntegerType(),True),
                                    StructField("number",IntegerType(),False),
                                    StructField("grid",IntegerType(),True),
                                    StructField("position",IntegerType(),False),
                                    StructField("positionText",StringType(),True),
                                    StructField("points",FloatType(),True),
                                    StructField("laps",IntegerType(),True),
                                    StructField("time",StringType(),False),
                                    StructField("milliseconds",IntegerType(),False),
                                    StructField("fastestLap",IntegerType(),False),
                                    StructField("rank",IntegerType(),False),
                                    StructField("fastestLapTime",StringType(),False),
                                    StructField("fastestLapSpeed",StringType(),False),
                                    StructField("statusId",IntegerType(),True)
                                    
                                   ])

# COMMAND ----------

results_df = spark.read \
.option("header",True) \
.schema(results_schema) \
.json("/mnt/formula1hlpangaa/raw/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - drop unwant columns

# COMMAND ----------

from pyspark.sql.functions import col 
results_selected_df = results_df.drop(col("statusId"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - rename columns and add ingestion_date column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
results_final_df = results_selected_df.withColumnRenamed("resultId","result_id") \
                                        .withColumnRenamed("raceId","race_id") \
                                        .withColumnRenamed("driverId","driver_id") \
                                        .withColumnRenamed("constructorId","constructor_id") \
                                        .withColumnRenamed("positionText","position_text") \
                                        .withColumnRenamed("positionOrder","position_order") \
                                        .withColumnRenamed("fastestLap","fastest_lap") \
                                        .withColumnRenamed("fastestLapSpeed","fastest_lap_speed") \
                                        .withColumnRenamed("fastestLapTime","fastest_lap_time") \
                                        .withColumn("Ingestion_date",current_timestamp())



# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write the output to parquet file

# COMMAND ----------

##results_final_df.write.mode("overwrite").partitionBy("race_id").parquet("/mnt/formula1hlpangaa/processed/results")
results_final_df.write.mode("overwrite").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

dbutils.notebook.exit("Success")