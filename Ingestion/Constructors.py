# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widget.text('p_file_date','2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 1 - use DDL format schema to use dataframe reader API

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read \
                       .option("header",True) \
                       .schema(constructors_schema) \
                       .json("/mnt/formula1hlpangaa/raw/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Remove the URL column

# COMMAND ----------

from pyspark.sql.functions import col
constructors_select_df = constructors_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md 
# MAGIC #####Step 3 - rename the column and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

constructors_final_df = constructors_select_df.withColumnRenamed("constructorId","constructor_id") \
                                                .withColumnRenamed("constructorRef","constructor_ref") \
                                                .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Wrtite the output to parquet file

# COMMAND ----------

##constructors_final_df.write.mode("overwrite").parquet("/mnt/formula1hlpangaa/processed/constructors")
constructors_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1hlpangaa/processed

# COMMAND ----------

dbutils.notebook.exit("Success")