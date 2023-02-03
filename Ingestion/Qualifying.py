# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest multiple qualifying_split*.json files in the same folder

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the multiple Json files in the same folder using dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField,IntegerType,StringType,DateType,TimestampType

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId",IntegerType(),True),
                                    StructField("raceId",IntegerType(),True),
                                    StructField("driverId",IntegerType(),True),
                                    StructField("constructorId",IntegerType(),True),
                                    StructField("number",IntegerType(),False),
                                    StructField("position",IntegerType(),True),
                                    StructField("q1",StringType(),False),
                                    StructField("q2",StringType(),False),
                                    StructField("q3",StringType(),False)
                                   ])

# COMMAND ----------

qualifying_df = spark.read \
.option("multiline",True) \
.schema(qualifying_schema) \
.json("/mnt/formula1hlpangaa/raw/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - rename columns and add ingestion_date column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
qualifying_final_df = qualifying_df.withColumnRenamed("raceId","race_id") \
                                        .withColumnRenamed("driverId","driver_id") \
                                        .withColumnRenamed("qualifyId","qualify_id") \
                                        .withColumnRenamed("constructorId","constructor_id") \
                                        .withColumn("Ingestion_date",current_timestamp())



# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write the output to parquet file

# COMMAND ----------

##qualifying_final_df.write.mode("overwrite").parquet("/mnt/formula1hlpangaa/processed/qualifying")
qualifying_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1hlpangaa/processed/qualifying"))

# COMMAND ----------

dbutils.notebook.exit("Success")