# Databricks notebook source
from pyspark.sql.functions import current_timestamp

#
#  @notice Method for adding ingestion date
#  @param input_df: dataframe that need to add ingestion date
#

def add_ingestion_date(input_df):
  output_df = input_df.withColumn("ingestion_date", current_timestamp())
  return output_df

# COMMAND ----------

#
#  @notice Method for rearranging the partition column in the last column for incremental load
#  @param input_df: dataframe that need to perform this function
#  @param partition_column: the column that want to put in the last column
#

def re_arrange_partition_column(input_df, partition_column):
  column_list = []
  for column_name in input_df.schema.names:
    if column_name != partition_column:
      column_list.append(column_name)
  column_list.append(partition_column)
  output_df = input_df.select(column_list)
  return output_df

# COMMAND ----------

#
#  @notice Method for overwriting the partition of a table for incremental load
#  @param input_df: dataframe that need to perform this function
#  @param db_name: database that need to perform this function
#  @param table_name: table that need to perform this function
#  @param partition_column: the new partition column for the table 
#

def overwrite_partition(input_df, db_name, table_name, partition_column):
  output_df = re_arrange_partition_column(input_df, partition_column)
  spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
  if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
    output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
  else:
    output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

#
#  @notice Method for adding ingestion date
#  @param input_df: dataframe that need to add ingestion date
#

def df_column_to_list(input_df, column_name):
  df_row_list = input_df.select(column_name) \
                        .distinct() \
                        .collect()
  
  column_value_list = [row[column_name] for row in df_row_list]
  return column_value_list

# COMMAND ----------

#
#  @notice Method for adding ingestion date
#  @param input_df: dataframe that need to add ingestion date
#
#  Circuits, races, constructors and drivers contain data from all races. The size is small so can do full load.
#  Results, PitStops, LapTimes and Qualifying contains data only from that race. So they will do incremental load
#


def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
  spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")

  from delta.tables import DeltaTable
  if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
    deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
    deltaTable.alias("tgt").merge(
        input_df.alias("src"),
        merge_condition) \
      .whenMatchedUpdateAll()\
      .whenNotMatchedInsertAll()\
      .execute()
  else:
    input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")