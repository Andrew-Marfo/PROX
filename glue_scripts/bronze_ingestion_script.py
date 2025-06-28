import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import current_timestamp

# Setup
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Config
rds_connection_name = "proximity-rds-conn"   
output_path_base = "s3://proximity-bronze-bucket/"

tables = ["users", "service_providers", "bookings", "reviews", "dispute_requests"] 

for table in tables:
    print(f"Reading table: {table}")
    dynamic_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="mysql",
        connection_options={
            "connectionName": rds_connection_name,
            "database": "proximity_db",
            "dbtable": table
        }
    )
    
    df = dynamic_frame.toDF()
    
    df = df.withColumn("ingested_at", current_timestamp())
    
    output_path = f"{output_path_base}{table}/"
    print(f"Writing {table} to {output_path}")
    df.write.mode("overwrite").parquet(output_path)

job.commit()
