# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Races.csv file

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1hlpangaa/raw/races.csv

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the csv using spark dataframe Reader method

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("year", IntegerType(), True),
                                     StructField("round", IntegerType(), True),
                                     StructField("circuitId", IntegerType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("date", StringType(), True),
                                     StructField("time", StringType(), True)])

# COMMAND ----------

races_df = spark.read \
                .option("header",True) \
                .schema(races_schema) \
                .csv('/mnt/formula1hlpangaa/raw/races.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Create the race_timestamp and ingestion date using withColumn

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, col, lit, to_timestamp, current_timestamp

# COMMAND ----------

races_timestamp_df = races_df.withColumn('race_timestamp', to_timestamp(concat(col('date'),lit(' '), col('time')),'yyyy-MM-dd HH:mm:ss')) \
                          .withColumn("ingestion_date", current_timestamp()) 

# COMMAND ----------

races_selected_df = races_timestamp_df.drop("date","time")

# COMMAND ----------

races_selected_df

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename the columns as required

# COMMAND ----------

races_final_df = races_selected_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("year", "race_year") \
.withColumnRenamed("circuitId", "circuit_id")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 4 - Write data to datalake as parquet using dataframe writer API

# COMMAND ----------

##races_final_df.write.mode("overwrite").partitionBy("race_year").parquet("/mnt/formula1hlpangaa/processed/races")
races_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1hlpangaa/processed/races"))

# COMMAND ----------

dbutils.notebook.exit("Success")