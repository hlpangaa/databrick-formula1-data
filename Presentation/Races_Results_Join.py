# Databricks notebook source
# MAGIC %md
# MAGIC ### Present the races and results data using join function

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - read the parquet files from processed tables

# COMMAND ----------

races_df = spark.read.parquet("/mnt/formula1hlpangaa/processed/races")
circuits_df = spark.read.parquet("/mnt/formula1hlpangaa/processed/circuits")
drivers_df = spark.read.parquet("/mnt/formula1hlpangaa/processed/drivers")
constructors_df = spark.read.parquet("/mnt/formula1hlpangaa/processed/constructors")
results_df = spark.read.parquet("/mnt/formula1hlpangaa/processed/results")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - rename columns having ambiguous name 

# COMMAND ----------

from pyspark.sql.functions import col, to_date

races_final_df = races_df.withColumnRenamed("name","race_name")\
                         .withColumn("race_date",to_date(col("race_timestamp"),"yyyy-MM-dd"))

circuits_final_df = circuits_df.withColumnRenamed("location","circuit_location")

drivers_final_df = drivers_df.withColumnRenamed("name","driver_name") \
                             .withColumnRenamed("number","driver_number") \
                             .withColumnRenamed("nationality","driver_nationality") 

constructors_final_df = constructors_df.withColumnRenamed("name","team") 

results_final_df = results_df.withColumnRenamed("time","race_time") 



# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - join the data
# MAGIC 
# MAGIC 
# MAGIC *races result 
# MAGIC *races circuits
# MAGIC *results drivers
# MAGIC *results constructor

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

join_df = races_final_df.join(results_final_df, races_final_df['race_id']==results_final_df['race_id'])\
                        .join(circuits_final_df, races_final_df['circuit_id']==circuits_final_df['circuit_id'])\
                        .join(drivers_final_df, results_final_df['driver_id']==drivers_final_df['driver_id'])\
                        .join(constructors_final_df, results_final_df['constructor_id']==constructors_final_df['constructor_id'])\
                        .select(col('race_year'),\
                                col('race_name'),\
                                col('race_date'),\
                                col('circuit_location'),\
                                col('driver_name'),\
                                col('driver_number'),\
                                col('driver_nationality'),\
                                col('team'),\
                                col('grid'),\
                                col('fastest_lap'),\
                                col('race_time'),\
                                col('points'),\
                                col('position')
                               )

final_df = join_df.withColumn("created_date",current_timestamp())

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Step 4 - write the data into parquet file

# COMMAND ----------

##final_df.write.mode("overwrite").parquet("/mnt/formula1hlpangaa/presentation/races_results")
final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.races_results")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1hlpangaa/presentation/races_results"))

# COMMAND ----------

display(final_df.filter("race_year == 2020 and race_name =='Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

