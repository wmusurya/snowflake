import boto3
import pandas as pd
import json
import snowflake.connector
import pandas as pd
import numpy as np
import pathlib
import os
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import snowflake.connector
from snowflake.sqlalchemy import URL
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector.pandas_tools import pd_writer
import schedule
import time

# AWS S3 credentials
aws_access_key = 'AKIA43OTVN5XPSZNDXOW'
aws_secret_key = '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'
region_name = 'us-east-1'
bucket_name = 'snow1111'
file_name = 'Titanic.json'

# Establish AWS S3 connection
s3 = boto3.client(
    service_name='s3',
    region_name=region_name,
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key
)

# Read JSON data from S3
file = s3.get_object(Bucket=bucket_name, Key=file_name)
json_data = file['Body'].read().decode('utf-8')
df = pd.DataFrame([json.loads(json_data)])
print(df)


parm_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"connect.params")
param_file_path = pathlib.Path(parm_file)
parameters = {}
if param_file_path.exists():
    file_obj = open(parm_file)
    for line in file_obj:
        line = line.strip()
        if not line.startswith('#'):
            key_value = line.split('=')
            if len(key_value) == 2:
                parameters[key_value[0].strip()] = key_value[1].strip()


snowflake_user = parameters['snowflake_user']
snowflake_password = parameters['snowflake_password']
snowflake_account = parameters['snowflake_account']
snowflake_warehouse = parameters['snowflake_warehouse']
snowflake_database = parameters['snowflake_database']
snowflake_schema = parameters['snowflake_schema']
#snowflake_table = parameters['table_name']
snowflake_role = parameters['snowflake_role']

def s_loading():
    engine = create_engine(URL(
        account = snowflake_account,
        role = snowflake_role,
        user = snowflake_user,
        password=snowflake_password,
        database=snowflake_database,
        schema=snowflake_schema,
        warehouse=snowflake_warehouse,
        
        ))


    connection = engine.connect()
    
    
    with engine.begin() as con:
        df.to_sql("amazon",con=con,if_exists='replace',schema='PUBLIC',index=False,method=pd_writer)
    print('file loaded successfully')

        
s_loading()
