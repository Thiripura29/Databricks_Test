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
    create dictionary for all options to pass to the option function
"""

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC printSchema() function is used to dispaly the schema of the dataframe.
# MAGIC A schema in spark displays the structure of the data . Which includes the data type of the coloumn
# MAGIC

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Print schema in json format

# COMMAND ----------

df.schema.json()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType,TimestampType,BooleanType,DoubleType
"""
No of jobs reduced - Important
To reduce the number of job, use schema for each file
Optimize the code by using schema
schema contains three main things structtype , structfield and stringtype
"""
schema = StructType([
    StructField("START", TimestampType(), True), 
    StructField("STOP", StringType(), True),
    StructField("PATIENT", StringType(), True),
    StructField("ENCOUNTER", StringType(), True),
    StructField("CODE", DoubleType(), True),
    StructField("SYSTEM", StringType(), True),
    StructField("DESCRIPTION", StringType(), True),
    StructField("TYPE", StringType(), True),
    StructField("CATEGORY", StringType(), True),
    StructField("REACTION1", IntegerType(), True),
    StructField("DESCRIPTION1", StringType(), True),
    StructField("SEVERITY1", StringType(), True),
    StructField("REACTION2", IntegerType(), True),
    StructField("DESCRIPTION2", StringType(), True),
    StructField("SEVERITY2", IntegerType(), True),
          
                     ])
df = spark \
    .read \
    .format('csv') \
    .option("header",True) \
    .schema(schema) \
    .load('/Volumes/databricks_catalog/s3_test/healthcare_1/csv files/csv/2024_05_08T04_08_53Z/allergies.csv') \

display(df)



# COMMAND ----------

# MAGIC %md
# MAGIC Display record in text format

# COMMAND ----------

display(spark.read.format('text').load('/Volumes/databricks_catalog/s3_test/healthcare_1/csv files/csv/2024_05_08T04_08_53Z/allergies.csv'))
