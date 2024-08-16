# Databricks notebook source
a=4
b=4
c=a+b
print(c)

# COMMAND ----------

from pyspark.sql import SparkSession
spark=SparkSession.builder.getOrCreate()

# COMMAND ----------

df = spark \
    .read \
    .format('csv') \
    .option("header",True) \
    .option("inferSchema",True) \
    .load('/Volumes/databricks_catalog/s3_test/healthcare_1/csv files/csv/2024_05_08T04_08_53Z/allergies.csv') \
     
"""
    show the test
"""

display(df)

# COMMAND ----------

display(df)
