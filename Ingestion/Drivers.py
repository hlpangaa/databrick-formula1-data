# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest drivers.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - use dataframe reader API to read a nested JSON file

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, StructType, StructField, DateType

# COMMAND ----------

names_schema = StructType(fields=[StructField("forename",StringType(),True),
                                 StructField("surname",StringType(),True)
])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId",IntegerType(),True),
                                    StructField("driverRef",StringType(),True),
                                    StructField("number",IntegerType(),True),
                                    StructField("code",StringType(),True),
                                    StructField("name",names_schema,True),
                                    StructField("dob",DateType(),True),
                                    StructField("nationality",StringType(),True),
                                    StructField("url",StringType(),True)
])

# COMMAND ----------

drivers_df = spark.read \
                  .option("header",True) \
                  .schema(drivers_schema) \
                   .json("/mnt/formula1hlpangaa/raw/drivers.json")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId","driver_id") \
                                     .withColumnRenamed("driverRef","driver_ref") \
                                     .withColumn("ingestion_date",current_timestamp()) \
                                     .withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname")))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Drop the unwanted columns

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write the output to praquet file

# COMMAND ----------

##drivers_final_df.write.mode("overwrite").parquet("/mnt/formula1hlpangaa/processed/drivers")
drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")