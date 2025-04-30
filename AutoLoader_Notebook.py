# Databricks notebook source
# MAGIC %md
# MAGIC ## Incremental Data Loading Using AutoLoader

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA netflix_catalog.net_schema;

# COMMAND ----------

checkpoint_location = "abfss://silver@netflixstoragesam.dfs.core.windows.net/checkpoint"


# COMMAND ----------

    df = spark.readStream\
            .format("cloudFiles")\
            .option("cloudFiles.format", "csv")\
            .option("cloudFiles.schemaLocation", checkpoint_location)\
            .load("abfss://raw@netflixstoragesam.dfs.core.windows.net")

# COMMAND ----------

df.display()

# COMMAND ----------

df .writeStream\
        .option("checkpointLocation", checkpoint_location)\
        .trigger(processingTime= '10 seconds')\
        .start("abfss://bronze@netflixstoragesam.dfs.core.windows.net/netflix_titles")

# COMMAND ----------

