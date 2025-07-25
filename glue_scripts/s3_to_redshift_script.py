import sys
import logging
from awsglue.job import Job
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)    

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

redshift_user = args["REDSHIFT_USER"]
redshift_password = args["REDSHIFT_PASSWORD"]
redshift_db = args["REDSHIFT_DB"]
redshift_host = args["REDSHIFT_HOST"]
redshift_port = args["REDSHIFT_PORT"]
redshift_schema = args["REDSHIFT_SCHEMA"]
s3_output_bucket = args["S3_OUTPUT_BUCKET"]
iam_role = args["IAM_ROLE"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Redshift temp dir
redshift_tmp_dir = f"s3://{s3_output_bucket}/temp/"
jdbc_url = f"jdbc:redshift://{redshift_host}:{redshift_port}/{redshift_db}"

# Table DDLs
table_ddl_map = {
    "fact_booking": """CREATE TABLE IF NOT EXISTS {schema}.fact_booking (
        booking_id VARCHAR(255),
        seeker_id VARCHAR(255),
        provider_id VARCHAR(255),
        category_id VARCHAR(255),
        quote_id VARCHAR(255),
        location VARCHAR(255),
        seeker_name VARCHAR(255),
        amount_before_tax DOUBLE PRECISION,
        tax_amount DOUBLE PRECISION,
        total_amount DOUBLE PRECISION,
        booking_status VARCHAR(50),
        created_at TIMESTAMP,
        date_key VARCHAR(10),
        year VARCHAR(10),
        month VARCHAR(10)
    );""",
    "dim_date": """CREATE TABLE IF NOT EXISTS {schema}.dim_date (
        date DATE,
        date_key INTEGER,
        day INTEGER,
        month INTEGER,
        month_name VARCHAR(20),
        quarter INTEGER,
        year INTEGER,
        day_of_week INTEGER,
        day_name VARCHAR(20),
        is_weekend BOOLEAN
    );""",
    "dim_user": """CREATE TABLE IF NOT EXISTS {schema}.dim_user (
        user_id VARCHAR(255),
        role VARCHAR(50),
        phone_number VARCHAR(50),
        email VARCHAR(255),
        provider_id VARCHAR(255),
        pricing VARCHAR(100),
        business_name VARCHAR(255),
        verification_status VARCHAR(50),
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        name VARCHAR(255),
        user_status VARCHAR(50),
        created_at TIMESTAMP,
        full_name VARCHAR(255),
        year VARCHAR(10),
        month VARCHAR(10)
    );""",
    "dim_service": """CREATE TABLE IF NOT EXISTS {schema}.dim_service (
        service_id VARCHAR(255),
        provider_id VARCHAR(255),
        service_type VARCHAR(255),
        category_id VARCHAR(255),
        category_name VARCHAR(255),
        price DOUBLE PRECISION,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    );""",
    "dim_location": """CREATE TABLE IF NOT EXISTS {schema}.dim_location (
        location_name VARCHAR(255),
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION
    );""",
    "dim_dispute": """CREATE TABLE IF NOT EXISTS {schema}.dim_dispute (
        dispute_id VARCHAR(255),
        review_id VARCHAR(255),
        user_id VARCHAR(255),
        reason VARCHAR(65535),
        dispute_status VARCHAR(50),
        reported_at TIMESTAMP
    );""",
    "dim_review": """CREATE TABLE IF NOT EXISTS {schema}.dim_review (
        review_id VARCHAR(255),
        user_id VARCHAR(255),
        provider_id VARCHAR(255),
        ratings DOUBLE PRECISION,
        sentiment_label VARCHAR(50),
        sentiment_score DOUBLE PRECISION,
        is_suspicious BOOLEAN,
        comment VARCHAR(65535),
        created_at TIMESTAMP
    );"""
}

# Load data to Redshift
for table, ddl_template in table_ddl_map.items():
    try:
        s3_path = f"s3://{s3_output_bucket}/star_schema/{table}/"
        logger.info(f"🔄 Reading {table} from S3 path: {s3_path}")

        # Read data from S3
        dyf = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [s3_path]},
            format="parquet"
        )

        # Format the DDL statement with the schema
        create_stmt = ddl_template.format(schema=redshift_schema)

        # Write data to Redshift using from_options
        glueContext.write_dynamic_frame.from_options(
            frame=dyf,
            connection_type="redshift",
            connection_options={
                "url": jdbc_url,
                "user": redshift_user,
                "password": redshift_password,
                "dbtable": f"{redshift_schema}.{table}",
                "redshiftTmpDir": redshift_tmp_dir,
                "aws_iam_role": iam_role,
                "preactions": create_stmt
            },
            transformation_ctx=f"write_{table}"
        )
        logger.info(f"✅ Loaded {table} into Redshift successfully.")
    except Exception as e:
        logger.error(f"❌ Failed to load {table} into Redshift: {e}", exc_info=True)
        sys.exit(1)

job.commit()
logger.info("🎉 All tables created and loaded to Redshift successfully.")
