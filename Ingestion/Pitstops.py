# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Pitstops.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the multi-line Json file using dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField,IntegerType,StringType,DateType,TimestampType

# COMMAND ----------

pitstops_schema = StructType(fields=[StructField("stop",IntegerType(),True),
                                    StructField("raceId",IntegerType(),True),
                                    StructField("driverId",IntegerType(),True),
                                    StructField("lap",IntegerType(),True),
                                    StructField("time",TimestampType(),False),
                                    StructField("duration",StringType(),True),
                                    StructField("milliseconds",IntegerType(),False)
                                   ])

# COMMAND ----------

pitstops_df = spark.read \
.option("header",True) \
.option("multiline",True) \
.schema(pitstops_schema) \
.json("/mnt/formula1hlpangaa/raw/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - rename columns and add ingestion_date column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
pitstops_final_df = pitstops_df.withColumnRenamed("raceId","race_id") \
                                        .withColumnRenamed("driverId","driver_id") \
                                        .withColumn("Ingestion_date",current_timestamp())



# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write the output to parquet file

# COMMAND ----------

##pitstops_final_df.write.mode("overwrite").parquet("/mnt/formula1hlpangaa/processed/pitstops")
pitstops_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pitstops")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1hlpangaa/processed/pitstops"))

# COMMAND ----------

dbutils.notebook.exit("Success")