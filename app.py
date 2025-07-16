import boto3
import pandas as pd
import pymysql
from sqlalchemy import create_engine
import os
import botocore.exceptions

# ENV variables
s3_bucket = os.getenv("S3_BUCKET")
s3_key = os.getenv("S3_KEY")
rds_host = os.getenv("RDS_HOST")
rds_user = os.getenv("RDS_USER")
rds_pass = os.getenv("RDS_PASS")
rds_db = os.getenv("RDS_DB")
rds_table = os.getenv("RDS_TABLE")
glue_db = os.getenv("GLUE_DB")
glue_table = os.getenv("GLUE_TABLE")
glue_s3_loc = os.getenv("GLUE_S3_LOC")

# Step 1: Download CSV from S3
s3 = boto3.client('s3')
s3.download_file(s3_bucket, s3_key, 'data.csv')
# Step 2: Read CSV
df = pd.read_csv('data.csv')

print("Connecting to RDS instance")
# Step 3: Try inserting into RDS
try:
    engine = create_engine(f"mysql+pymysql://{rds_user}:{rds_pass}@{rds_host}/{rds_db}")
    df.to_sql(rds_table, engine, if_exists='replace', index=False)
    print("Data uploaded to RDS.")
except Exception as e:
    print("DS Upload Failed:", e)
    
    # Step 4: Fallback to Glue
    glue = boto3.client('glue', region_name=os.getenv('AWS_REGION'))
    try:
        glue.create_database(DatabaseInput={'Name': glue_db})
    except glue.exceptions.AlreadyExistsException:
        pass

    try:
        glue.create_table(
            DatabaseName=glue_db,
            TableInput={
                'Name': glue_table,
                'StorageDescriptor': {
                    'Columns': [{'Name': col, 'Type': 'string'} for col in df.columns],
                    'Location': glue_s3_loc,
                    'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                        'Parameters': {'field.delim': ','}
                    }
                },
                'TableType': 'EXTERNAL_TABLE'
            }
        )
        print(" Fallback to Glue succeeded.")
    except Exception as glue_error:
        print(" Glue fallback failed:", glue_error)
