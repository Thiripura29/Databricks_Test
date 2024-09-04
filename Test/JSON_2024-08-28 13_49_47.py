# Databricks notebook source
# MAGIC %md
# MAGIC Read JSON file from s3 bucket

# COMMAND ----------

read_df=spark\
       .read\
       .format("json")\
       .option("multiline", "true")\
        .load("s3://s3-databrick-test/JSON/Allan198_Gottlieb798_20a1b578-95bd-6d4d-0945-55f228f50077.json")

display(read_df )


# COMMAND ----------

read_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

def identify_corrupted_records(df):
    non_corrupt_df=None
    corrupted_record_df=None
    #flag to indicate corrupted record found
    corrupted_record_found=False
  #flag to indicate all records are corrupted
    all_records_corrupted=False
#get the schema of the dataframe
    data_frame_schema=df.schema.fields
    print(data_frame_schema)
#check if dataframe has only one column
    if len(data_frame_schema)==1:
      print("Only one column in the dataframe is present ")
      corrupted_record_df=True
#Check if the dataframe has any corrupted record
    else:
        for field in data_frame_schema:
          if field.name== '_corrupt_record':
              print("Corrupted record found")
              corrupted_record_found=True #set the flag as true when corrupt record is found
    if  corrupted_record_found or all_records_corrupted:
        #cache the dataframe for data peformance
        df.cache()
        #Filter out the corrupt record in new dataframe
        corrupted_record_df=df.filter(col("_corrupt_record").isNotNull())
        #filter non corrupt record in new dataframe
        if not all_records_corrupted:     
            non_corrupt_df=df.filter(col("_corrupt_record").isNull())  
        
            return non_corrupt_df,corrupted_record_df
    
    else:
        return None,df

corrupted_record_df,non_corrupt_df=identify_corrupted_records(read_df)

# COMMAND ----------

if corrupted_record_df:
    print(corrupted_record_df.count())
else:
    print(non_corrupt_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Create a temporary view

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view temp_view
# MAGIC using JSON
# MAGIC options
# MAGIC (
# MAGIC   path 's3://s3-databrick-test/JSON/Allan198_Gottlieb798_20a1b578-95bd-6d4d-0945-55f228f50077.json',
# MAGIC   multiline 'true',
# MAGIC   inferschema 'true',
# MAGIC   header 'true'
# MAGIC );
# MAGIC
# MAGIC select * from temp_view;
# MAGIC

# COMMAND ----------

#Display the selected attributes from the dataframe
non_crpt_df=non_corrupt_df.select("attributes.age","attributes.AGE_MONTHS","attributes.Acute Myeloid Leukemia for PCOR Research Module","attributes.mend_encounter_reason")
display(non_crpt_df )

#Display the selected attributes from dataframe with renamed column
non_crpt_df1=non_corrupt_df.selectExpr("attributes.age as age","attributes.AGE_MONTHS as age_month","attributes.mend_encounter_reason as mend_encounter")
display(non_crpt_df1 )

#display all the attributes from dataframe
all_attributes1=non_corrupt_df.selectExpr("attributes.*").select("age","AGE_MONTHS","mend_encounter_reason")
display(all_attributes1)

#display all the symptoms from dataframe
all_attributes1=non_corrupt_df.selectExpr("attributes.*")
display(all_attributes1)

#select all columns from dataframe
all_column=non_corrupt_df.selectExpr("coverage","symptoms","record","attributes.*")
display(all_column)

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import col

required_columns=["age","age_months","mend_encounter_reason","first_name","last_name","marital_status","zip","address","county","city","state","C19_SCHEDULED_FIRST_SHOT","immunizations"]
#existing_columns=[col for col in non_corrupt_df.columns if col in required_columns]
data_frame2=all_column.selectExpr(required_columns)
display(data_frame2)


# COMMAND ----------

# MAGIC %md
# MAGIC CASE WHEN condition

# COMMAND ----------

from pyspark.sql.functions import expr
data_frame2=data_frame2.withColumn("age_Group",expr("CASE WHEN age < 18 THEN 'Minor'"+
                                                      "WHEN age BETWEEN 18 AND 64 THEN 'Adult'"+
                                                      "WHEN age > 64 THEN 'Senior'"+
                                                      "ELSE 'Unknown'"+
                                                      " END"))
display(data_frame2)

# COMMAND ----------

# MAGIC %md
# MAGIC DATE FUNCTION

# COMMAND ----------

from pyspark.sql.functions import expr,when,from_unixtime

#concert C19_SCHEDULED_FIRST_SHOT from milliseconds to seconds and convert to timestamp 
data_frame2=data_frame2.withColumn("C19_SCHEDULED_FIRST_SHOT_seconds",from_unixtime(col("C19_SCHEDULED_FIRST_SHOT")/1000))
data_frame2=data_frame2.withColumn("C19_SCHEDULED_FIRST_SHOT_timestamp",from_unixtime(data_frame2.C19_SCHEDULED_FIRST_SHOT_seconds).cast("timestamp"))

#Extract date, month ,year from C19_SCHEDULED_FIRST_SHOT_timestamp
data_frame2=data_frame2.withColumn("date",data_frame2.C19_SCHEDULED_FIRST_SHOT_seconds.cast("date"))
data_frame2=data_frame2.withColumn("day",expr("dayofmonth(C19_SCHEDULED_FIRST_SHOT_seconds)"))
data_frame2=data_frame2.withColumn("month",expr("month(C19_SCHEDULED_FIRST_SHOT_seconds)"))
data_frame2=data_frame2.withColumn("year",expr("year(C19_SCHEDULED_FIRST_SHOT_seconds)"))
data_frame2=data_frame2.withColumn("hour",expr("hour(C19_SCHEDULED_FIRST_SHOT_seconds)"))
data_frame2=data_frame2.withColumn("minute",expr("minute(C19_SCHEDULED_FIRST_SHOT_seconds)"))
data_frame2=data_frame2.withColumn("second",expr("second(C19_SCHEDULED_FIRST_SHOT_seconds)"))

#Extreact day of year,week of year,last day
data_frame2=data_frame2.withColumn("day_of_year",expr("dayofyear(C19_SCHEDULED_FIRST_SHOT_seconds)"))
data_frame2=data_frame2.withColumn("week_of_year",expr("weekofyear(C19_SCHEDULED_FIRST_SHOT_seconds)"))
data_frame2=data_frame2.withColumn("last_day",expr("last_day(C19_SCHEDULED_FIRST_SHOT_seconds)"))

#Calculate the next day of C19_SCHEDULED_FIRST_SHOT_timestamp and subtract 2 days from it

data_frame2=data_frame2.withColumn("next_day",expr("date_add(C19_SCHEDULED_FIRST_SHOT_seconds,8)"))
data_frame2=data_frame2.withColumn("Subtractdate",expr("date_sub(C19_SCHEDULED_FIRST_SHOT_seconds,2)"))
display(data_frame2)


# COMMAND ----------

# MAGIC %md
# MAGIC User defined function

# COMMAND ----------

from pyspark.sql.functions import expr,udf,col
from pyspark.sql.types import StringType, ArrayType
#Approach #1

#Create a function to create unique immunization from dictionary
def get_immunization(immunizations):
#Convert the immunization column to dictionary
    immunizations=immunizations.asDict()
    
#Create a empty set to store  unique immunizaton value
    immunization_list=set()
#iterate to the dictonary and add unique immunizarion value to the list
    for k,v in immunizations.items():
        # if (v) >0:
    
                  immunization_list.add(k)
#convert the set to the list and return 
    return list (immunization_list)

#create a UDF (user Defined Dunction) to apply the get_immunization function to the immunization column
immunization_df=udf(get_immunization,ArrayType(StringType()))

#create a new column immunization_array by applying the udf to the immunization column
transformed_df=data_frame2.withColumn("immunization_array",immunization_df(col("immunizations")))
display(transformed_df)

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import expr,udf,col
from pyspark.sql.types import StringType, ArrayType
#Approach #2

#Create a function to create unique immunization from dictionary
@udf(ArrayType(StringType()))
def get_immunization(immunizations):
#Convert the immunization column to dictionary
    immunizations=immunizations.asDict()
    
#Create a empty set to store  unique immunizaton value
    immunization_list=set()
#iterate to the dictonary and add unique immunizarion value to the list
    for k,v in immunizations.items():
        if len(v) >0:
    
                  immunization_list.add(k)
#convert the set to the list and return 
    return list (immunization_list)

#create a UDF (user Defined Dunction) to apply the get_immunization function to the immunization column
#immunization_df=udf(get_immunization,ArrayType(StringType()))

#create a new column immunization_array by applying the udf to the immunization column
transformed_df=data_frame2.withColumn("immunization_array",get_immunization(col("immunizations")))
display(transformed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC access python UDF funtion as spark sql

# COMMAND ----------

#Register the get_immunization with spark
spark.udf.register("get_immunization_sql", get_immunization)

data_frame2.createOrReplaceGlobalTempView(name='data_frame2')

df=spark.sql("select get_immunization_sql(immunizations) as immunization_Array,* from global_temp.data_frame2")
display(df)
                   

# COMMAND ----------

#SQL query to select all the columns from dataframe

sql_code="""
select array_size(immunization_Array) as immunization_size ,* from 

(select get_immunization_sql(immunizations) as immunization_Array ,* from global_temp.data_frame2)

"""
df=spark.sql(sql_code)
display(df)

# COMMAND ----------

#SQL query to select all the columns from dataframe
"""
array_size - Returns number of element in array
to_json - Returns the JSON representation of the array . Converts the column to JSON string{Struct type ,array type or map type. } Throw an exception if any other type is added
array_distict - Remove 
json_object_keys - Return all the keys from the JSON string and return array <String>
"""

sql_code="""
select array_size(immunization_Array) as immunization_size ,* from 

(select array_distinct(json_object_keys(to_json(immunizations))) as immunization_Array ,* from global_temp.data_frame2)

"""
df=spark.sql(sql_code)
display(df)

# COMMAND ----------

from pyspark.sql.functions import expr,udf,col
from pyspark.sql.types import StringType, ArrayType,IntegerType,MapType,StructType
#Approach #2

json_schema=ArrayType(ArrayType(IntegerType()))
#Create a function to create unique immunization from dictionary
@udf(json_schema)
def get_immunization_values(immunizations):
#Convert the immunization column to dictionary
    immunizations=immunizations.asDict()
    
#Create a empty set to store  unique immunizaton value
    immunization_value_list=[]
#iterate to the dictonary and add unique immunizarion value to the list
    for k,v in immunizations.items():
              
             immunization_value_list.append([int(item) for item in v])
#convert the set to the list and return 
    return list (immunization_value_list)

#create a UDF (user Defined Dunction) to apply the get_immunization function to the immunization column
#immunization_df=udf(get_immunization,ArrayType(StringType()))

#create a new column immunization_array by applying the udf to the immunization column
transformed_df=data_frame2.withColumn("immunization_array",get_immunization_values(col("immunizations")))
display(transformed_df)

spark.udf.register("get_immunization_udf",get_immunization_values)

# COMMAND ----------

from pyspark.sql.functions import expr,udf,col
from pyspark.sql.types import StringType, ArrayType,IntegerType,MapType,StructType
#Approach #2

udf_return_type=ArrayType(MapType(StringType(),ArrayType(IntegerType())))
#Create a function to create unique immunization from dictionary
@udf(udf_return_type)
def get_immunization_values(immunizations):
#Convert the immunization column to dictionary
    immunizations=immunizations.asDict()
    
#Create a empty set to store  unique immunizaton value
    immunization_value_list=[]
#iterate to the dictonary and add unique immunizarion value to the list
    for k,v in immunizations.items():
              
             immunization_value_list.append({k:[int(item) for item in v]})
#convert the set to the list and return 
    return list (immunization_value_list)

#create a UDF (user Defined Dunction) to apply the get_immunization function to the immunization column
#immunization_df=udf(get_immunization,ArrayType(StringType()))

#create a new column immunization_array by applying the udf to the immunization column
transformed_df=data_frame2.withColumn("immunization_array",get_immunization_values(col("immunizations")))
display(transformed_df)

spark.udf.register("get_immunization_json_udf",get_immunization_values)

# COMMAND ----------

from pyspark.sql import functions as F

df=data_frame2.withColumn(
    "immunization_values",
     F.expr("get_immunization_udf(immunizations)")
     ).withColumn(
     "Flatten_immunization_array",
     F.expr("flatten(immunization_values)")
     ).withColumn(
     "immunization_converted_map",
     F.expr("get_immunization_json_udf(immunizations)")
     ).withColumn(
         "immunization_values_flatten",F.expr("transform(Flatten_immunization_array, x -> from_unixtime(x))")
     ).withColumn(
         "immunization_transformed_values",
         F.expr("""
                transform(immunization_converted_map, 
                outeritem -> transform_values(
                    outeritem, (k,v) -> transform(v,x -> from_unixtime(x)))
                    )"""))
display(df)
