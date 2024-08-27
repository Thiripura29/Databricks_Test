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
    .load('/Volumes/databricks_catalog/heathcare/heathcare1/csv files/csv/2024_05_08T04_08_53Z/allergies.csv') \
     
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
    .load('/Volumes/databricks_catalog/heathcare/heathcare1/csv files/csv/2024_05_08T04_08_53Z/claims_transactions_illegal.csv') \
     
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
    .load('/Volumes/databricks_catalog/heathcare/heathcare1/csv files/csv/2024_05_08T04_08_53Z/allergies.csv') \

display(df)



# COMMAND ----------

# MAGIC %md
# MAGIC select () - select one or more columns from dataframe, Col()- create column object represent column name in dataframe
# MAGIC alias()-rename the column name in dataframe

# COMMAND ----------

from pyspark.sql.functions import col
df = spark \
    .read \
    .format('csv') \
    .option("header",True) \
    .option("inferSchema",True) \
    .load('/Volumes/databricks_catalog/heathcare/heathcare1/csv files/csv/2024_05_08T04_08_53Z/allergies.csv') \

good_record_df1=df.select(col("START"),col("STOP"),col("PATIENT"),col("ENCOUNTER"))
display(good_record_df1)
#-------------------------------------------
good_record_df2=df.select(col("START"),col("STOP"),col("PATIENT").alias("PATIENTNAME"),col("ENCOUNTER"))
display(good_record_df2)

#--------------------------------------------
good_record_df3=df.select("START","PATIENT")
display(good_record_df3)


# COMMAND ----------

# MAGIC %md
# MAGIC Display record in text format

# COMMAND ----------

display(spark.read.format('text').load('/Volumes/databricks_catalog/heathcare/heathcare1/csv files/csv/2024_05_08T04_08_53Z/allergies.csv'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC select () - select one or more columns from dataframe,
# MAGIC Col()- create column object represent column name in dataframe  
# MAGIC alias()-rename the column name in dataframe
# MAGIC

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
    StructField("SEVERITY2", BooleanType(),  True),
    StructField("_corrupt_record", StringType(), True),
          
                     ])
df_wrong_schema = spark \
    .read \
    .format('csv') \
    .option("header",True) \
    .schema(schema) \
    .load('/Volumes/databricks_catalog/heathcare/heathcare1/allergies (1).csv') \

good_records_df = df_wrong_schema.where("_corrupt_record is null")
bad_records_df = df_wrong_schema.where("_corrupt_record is not null")
display(bad_records_df)

# COMMAND ----------

# MAGIC %md
# MAGIC _corrupt_record = Column is used to find the good and bad record in dataframe . This column captures rows which does not parse correctly

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType,TimestampType,BooleanType,DoubleType
schema = StructType([StructField("emp_id", IntegerType(), True),StructField("name", StringType(), True),StructField("dept", StringType(), True),StructField("salary", DoubleType(), True),StructField("_corrupt_record", StringType(), True)])
df=spark.read.format('csv').option("header",True).schema(schema).load('/Volumes/databricks_catalog/heathcare/heathcare1/Employee.csv')
df_good_record=df.where("_corrupt_record is null")  
df_bad_record=df.where("_corrupt_record is not null")
display(df_good_record)


# COMMAND ----------

columnlist=df_good_record.columns
ignorecolumns=['_corrupt_record']
df_good_record=df_good_record.select([column for column in columnlist if column not in ignorecolumns])
display(df_good_record)

# COMMAND ----------

# MAGIC %md
# MAGIC withColumn() - This function is used to add, replace, or transform a column in a DataFrame. It allows you to create a new DataFrame with additional or modified columns based on existing ones.
# MAGIC
# MAGIC returns a new DataFrame with the specified modifications, leaving the original DataFrame unchanged.

# COMMAND ----------

from pyspark.sql.functions import col
salary_percentage=df_good_record.withColumn('salary_percentage',col('salary')/100)
display(salary_percentage)

# COMMAND ----------

# MAGIC %md
# MAGIC lit(123) creates a Column with a literal value of 123. This literal value can be any Python value (string, integer, float, boolean, etc.)
# MAGIC
# MAGIC withColumnRenamed() function is used to rename a column in a DataFrame. It allows you to create a new DataFrame with the specified column renamed.

# COMMAND ----------

from pyspark.sql.functions import lit
Location=df_good_record.withColumn('Newlocation',lit("Chennai"))
display(Location)
Location=Location.withColumnRenamed('Newlocation','OfficeLocation')
display(Location)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import lit


# COMMAND ----------

# MAGIC %md
# MAGIC rescued 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType,TimestampType,BooleanType,DoubleType
#spark.conf.set("spark.databricks.sql.rescuedDataColumn.filepath.enabled", "false")
schema = StructType([StructField("emp_id", IntegerType(), True),
                     StructField("name", StringType(), True),
                     StructField("dept", StringType(), True),
                     StructField("salary", DoubleType(), True)
                     ])
df=spark.read.format('csv')\
                   .option("header",True)\
                    .schema(schema)\
                    .option("rescuedDataColumn","_rescued_data")\
                    .load('/Volumes/databricks_catalog/heathcare/heathcare1/Employee.csv')
df_good_record=df.where("_rescued_data is null")  
df_bad_record=df.where("_rescued_data is not null")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Reads files under a provided location and returns the data in tabular form.
# MAGIC
# MAGIC Supports reading JSON, CSV, XML, TEXT, BINARYFILE, PARQUET, AVRO, and ORC file formats. Can detect the file format automatically and infer a unified schema across all files.
# MAGIC
# MAGIC Syntax
# MAGIC
# MAGIC read_files(path [, option_key => option_value ] [...])

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table Employee1 as 
# MAGIC (select * from read_files('/Volumes/databricks_catalog/heathcare/heathcare1/Employee.csv',
# MAGIC schema => 'emp_id int, Name string,DEPARTMENT string, Salary double',
# MAGIC format => 'CSV',
# MAGIC header =>true,
# MAGIC mode=>'PERMISSIVE',
# MAGIC rescuedDatacolumn =>'_rescued_column') );
# MAGIC
# MAGIC select * from Employee1

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists heathcare.heathcare1.Employee1;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table Emp_badrecord as (select * from Employee1 where _rescued_column is not null);
# MAGIC create or replace table Emp_goodrecord as (select * from Employee1 where _rescued_column is null)

# COMMAND ----------

# MAGIC %md
# MAGIC Imp : Spark.sql is gateway between pyspark SQl and SQL
# MAGIC

# COMMAND ----------

badrecord=spark.sql("select * from Emp_badrecord")
display(badrecord)
goodrecord=spark.sql("select * from Emp_goodrecord")
display(goodrecord)

# COMMAND ----------

# MAGIC %md
# MAGIC Create catelog and schema

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists lakehouse_dev
# MAGIC managed location 's3://hgs3-bucket/lakehouse/'

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists lakehouse_dev.AnalyticsDB

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the table without specifying the location
# MAGIC create external table if not exists lakehouse_dev.AnalyticsDB.claims_transactions (
# MAGIC ID string,
# MAGIC CLAIMID string,
# MAGIC CHARGEID integer,
# MAGIC PATIENTID string,
# MAGIC TYPE string,
# MAGIC AMOUNT double,
# MAGIC METHOD string,
# MAGIC FROMDATE timestamp,
# MAGIC TODATE timestamp,
# MAGIC PLACEOFSERVICE string,
# MAGIC PROCEDURECODE double,
# MAGIC MODIFIER1 string,
# MAGIC MODIFIER2 string,
# MAGIC DIAGNOSISREF1 integer,
# MAGIC DIAGNOSISREF2 integer,
# MAGIC DIAGNOSISREF3 integer,
# MAGIC DIAGNOSISREF4 integer,
# MAGIC UNITS integer,
# MAGIC DEPARTMENTID integer,
# MAGIC NOTES string,
# MAGIC UNITAMOUNT double,
# MAGIC TRANSFEROUTID integer,
# MAGIC TRANSFERTYPE string,
# MAGIC PAYMENTS double,
# MAGIC ADJUSTMENTS integer,
# MAGIC TRANSFERS double,
# MAGIC OUTSTANDING double,
# MAGIC APPOINTMENTID string,
# MAGIC LINENOTE string,
# MAGIC PATIENTINSURANCEID string,
# MAGIC FEESCHEDULEID integer,
# MAGIC PROVIDERID string,
# MAGIC SUPERVISINGPROVIDERID string,
# MAGIC _c33 string,
# MAGIC _c34 string
# MAGIC ) 
# MAGIC USING csv
# MAGIC location 's3://hgs3-bucket/test/claims_transactions_illegal.csv'
# MAGIC tblproperties (createdBy='Thiripura',createdDate='2024-08-27')

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from lakehouse_dev.analyticsdb.claims_transactions
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table Employee1

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended Employee1

# COMMAND ----------

extendedschema=spark.sql("describe table Employee1")
display(extendedschema)

# COMMAND ----------

extendedschema1=spark.sql("describe table extended Employee1")
display(extendedschema1)
