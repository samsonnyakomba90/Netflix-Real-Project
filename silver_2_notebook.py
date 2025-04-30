# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Data Transformation

# COMMAND ----------

df = spark.read.format("delta")\
                 .option("header", True)\
                     .option("inferSchema", True)\
                        .load("abfss://bronze@netflixstoragesam.dfs.core.windows.net/netflix_titles")
display(df)

# COMMAND ----------

df = df.fillna({"duration_minutes": 0, "duration_seasons": 1})
display(df)

# COMMAND ----------

df = df.withColumn("duration_minutes", col("duration_minutes").cast(IntegerType()))\
           .withColumn("duration_seasons", col("duration_seasons").cast(IntegerType()))\
                .withColumn("release_year", col("release_year").cast(TimestampNTZType()))\
                        .withColumn("show_id", col("show_id").cast(IntegerType()))

display(df)

# COMMAND ----------

df = df.withColumn("shorttitle",split(col("title"), ":")[0])
display(df)

# COMMAND ----------

df = df.withColumn("rating", split(col("rating"), "-")[0])
display(df)


# COMMAND ----------

df = df.withColumn("type_flag", when(col("type") == "TV Show", 1)\
                                     .when(col("type") == "Movie", 2)\
                                        .otherwise(0))
display(df)

# COMMAND ----------

from pyspark.sql import Window


# COMMAND ----------

df = df.withColumn("duration_ranking", dense_rank().over(Window.orderBy(col("duration_minutes").desc())))
display(df)


# COMMAND ----------

df.createOrReplaceTempView("temp_view")

# COMMAND ----------

df.createOrReplaceGlobalTempView("global_view")


# COMMAND ----------

df = spark.sql("SELECT * FROM global_temp.global_view")




display(df)


# COMMAND ----------

df_visual = df.groupBy("type").agg(count("*").alias("total_count"))
display(df_visual)

# COMMAND ----------

df.write.format("delta")\
            .mode("overwrite")\
                .option("path","abfss://silver@netflixstoragesam.dfs.core.windows.net/netflix_titles")\
                .save()


# COMMAND ----------

