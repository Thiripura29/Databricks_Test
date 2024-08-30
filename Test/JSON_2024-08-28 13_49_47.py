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
