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



# COMMAND ----------

df.show(20,truncate=False,vertical=False)

"""
     The show() function in Spark DataFrame is used to display the contents of the DataFrame in a tabular format.
     Spark will display a certain number of rows from the DataFrame, usually the first 20 rows by default
     Here are some common arguments you can pass 
     numRows: Specifies the number of rows to display. By default, it displays the first 20 rows.
     truncate: Specifies whether to truncate the displayed data if it's too wide. Truncation means cutting off some characters to fit the data within the display width. By default, it's set to True
     vertical: Specifies whether to display the output in a vertical format. By default, it's set to False, meaning the data is displayed horizontally.
     Usage:
     n: An alias for numRows.
     df.show(n=100, truncate=False, vertical=True)  # Display without truncating and in vertical format


"""

# COMMAND ----------

Options  = {
    "header": "true","inferSchema": "true"
}

df = spark \
    .read \
    .format('csv') \
    .options(**Options) \
    .load('/Volumes/databricks_catalog/s3_test/healthcare_1/csv files/csv/2024_05_08T04_08_53Z/allergies.csv') \
     
"""
    create dictionary for all options to pass to the options function
"""

display(df)
