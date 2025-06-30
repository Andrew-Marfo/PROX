import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'RDS_HOST',
    'RDS_PORT',
    'RDS_DB_NAME',
    'RDS_USERNAME',
    'RDS_PASSWORD',
    'S3_OUTPUT_BUCKET',
    'TABLES_TO_EXTRACT'
])

# Database connection details
jdbc_url = f"jdbc:postgresql://{args['RDS_HOST']}:{args['RDS_PORT']}/{args['RDS_DB_NAME']}"
connection_properties = {
    "user": args['RDS_USERNAME'],
    "password": args['RDS_PASSWORD'],
    "driver": "org.postgresql.Driver"
}

# S3 output path
s3_output_bucket = args['S3_OUTPUT_BUCKET']

# Tables to extract (passed as comma-separated list)
tables = args['TABLES_TO_EXTRACT'].split(',')

# Process each table
for table in tables:
    print(f"Extracting table: {table}")
    
    # Read data from RDS
    df = glueContext.read.format("jdbc").option("url", jdbc_url) \
        .option("dbtable", table) \
        .option("user", connection_properties["user"]) \
        .option("password", connection_properties["password"]) \
        .load()
    
    # Write to S3 in Parquet format (you can change to CSV or other formats)
    output_path = f"s3://{s3_output_bucket}/{args['RDS_DB_NAME']}/{table}/"
    df.write.mode("overwrite").parquet(output_path)
    
    print(f"Successfully exported {table} to {output_path}")

job.commit()