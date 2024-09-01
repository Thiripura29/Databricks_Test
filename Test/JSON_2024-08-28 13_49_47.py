# Databricks notebook source
# MAGIC %md
# MAGIC Read JSON file from s3 bucket

# COMMAND ----------

read_df=spark\
       .read\
       .format("json")\
       .option("multiline", "true")\
        .load("s3://hgs3-bucket/JSON/Allan198_Gottlieb798_20a1b578-95bd-6d4d-0945-55f228f50077.json")

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
# MAGIC   path 's3://hgs3-bucket/JSON/Allan198_Gottlieb798_20a1b578-95bd-6d4d-0945-55f228f50077.json',
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
all_attributes1=non_corrupt_df.selectExpr("symptoms.*")
display(all_attributes1)

#select all columns from dataframe
all_column=non_corrupt_df.selectExpr("coverage","symptoms","attributes","record")
display(all_column)

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import col

required_columns=["age","AGE_MONTHS","mend_encounter_reason","first_name","last_name","gender","race","ethnicity","zip","dob","ssn","state","county","city","address","phone","email","language","marital_status","employment_status","employment_status_code","employment_status_date","employment_status_code_date","employment_status_reason","employment_status_reason_code"]
existing_columns=[col for col in non_corrupt_df.columns if col in required_columns]
data_frame2=all_column.selectExpr(*existing_columns)
display(data_frame2)

