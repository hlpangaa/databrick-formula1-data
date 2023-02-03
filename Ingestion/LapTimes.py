# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest multiple lap_times_split*.csv files in the same folder

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read multiple csv files in the same folder using dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField,IntegerType,StringType,DateType,TimestampType

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId",IntegerType(),True),
                                    StructField("driverId",IntegerType(),True),
                                    StructField("lap",IntegerType(),True),
                                    StructField("position",IntegerType(),True),
                                    StructField("time",StringType(),False),
                                    StructField("milliseconds",IntegerType(),False)
                                   ])

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv("/mnt/formula1hlpangaa/raw/lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - rename columns and add ingestion_date column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
lap_times_final_df = lap_times_df.withColumnRenamed("raceId","race_id") \
                                        .withColumnRenamed("driverId","driver_id") \
                                        .withColumn("Ingestion_date",current_timestamp())



# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write the output to parquet file

# COMMAND ----------

##lap_times_final_df.write.mode("overwrite").parquet("/mnt/formula1hlpangaa/processed/lap_times")
lap_times_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1hlpangaa/processed/lap_times"))

# COMMAND ----------

dbutils.notebook.exit("Success")