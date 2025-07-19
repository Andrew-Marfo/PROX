import sys
import logging
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)    

# Get parameters
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "REDSHIFT_USER",
    "REDSHIFT_PASSWORD",
    "REDSHIFT_DB",
    "REDSHIFT_HOST",
    "REDSHIFT_PORT",
    "REDSHIFT_SCHEMA",
    "S3_OUTPUT_BUCKET",
    "IAM_ROLE"
])

# Extract parameters
redshift_user = args["REDSHIFT_USER"]
redshift_password = args["REDSHIFT_PASSWORD"]
redshift_db = args["REDSHIFT_DB"]
redshift_host = args["REDSHIFT_HOST"]
redshift_port = args["REDSHIFT_PORT"]
redshift_schema = args["REDSHIFT_SCHEMA"]
s3_output_bucket = args["S3_OUTPUT_BUCKET"]
iam_role = args["IAM_ROLE"]

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Redshift connection options
redshift_tmp_dir = f"s3://{s3_output_bucket}/temp/"
redshift_conn_options = {
    "url": f"jdbc:redshift://{redshift_host}:{redshift_port}/{redshift_db}",
    "user": redshift_user,
    "password": redshift_password,
    "dbtable": "",  # Will be set per table
    "redshiftTmpDir": redshift_tmp_dir,
    "aws_iam_role": iam_role
}

tables = ["dim_user", "dim_date", "dim_service", "dim_location", "dim_dispute", "dim_review", "fact_booking"]

for table in tables:
    try: 
        s3_path = f"s3://{args['S3_OUTPUT_BUCKET']}/star_schema/{table}/"
        # Read from S3
        dyf = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [s3_path]},
            format="parquet"
        )
        # Write to Redshift
        redshift_conn_options["dbtable"] = f"{redshift_schema}.{table}"
        glueContext.write_dynamic_frame.from_jdbc_conf(
            frame=dyf,
            connection_type="redshift",
            connection_options=redshift_conn_options
        )
        logger.info(f"✅ Table {table} loaded into Redshift successfully.")
    except Exception as e:
        logger.error(f"❌ Failed to load table {table} into Redshift: {e}")

logger.info("✅ All tables loaded into Redshift successfully.")
