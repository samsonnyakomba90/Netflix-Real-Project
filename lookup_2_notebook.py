# Databricks notebook source
dbutils.widgets.text("weekday", "7")


# COMMAND ----------

var = int(dbutils.widgets.get("weekday"))
print(var)

# COMMAND ----------

dbutils.jobs.taskValues.set("weekoutput", value=var)