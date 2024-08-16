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
    CSV reader will return Data frame as an output
    We can use display() to display the data
    show() to display the data in tabular format
"""

display(df)


# COMMAND ----------

display(df)
"""
We can also use display() to display  the data in tabular format

"""

# COMMAND ----------

show.(df)
