import sys
import logging
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'SOURCE_DATABASE',
    'S3_OUTPUT_BUCKET',
    'DB_TABLES'
])

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define schemas for all tables
schemas = {
    "bookings": StructType([
        StructField("id", StringType(), False),
        StructField("additional_information", StringType(), True),
        StructField("category_id", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("description", StringType(), True),
        StructField("end_date", DateType(), True),
        StructField("location", StringType(), True),
        StructField("preferred_date", DateType(), True),
        StructField("preferred_time", StringType(), True),  # TIME => StringType
        StructField("provider_id", StringType(), True),
        StructField("seeker_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("quote_id", StringType(), True),
    ]),
    "categories": StructType([
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("category_id", StringType(), False),
        StructField("category_name", StringType(), False),
        StructField("description", StringType(), True),
        StructField("status", StringType(), False),
    ]),
    "provider_reviews": StructType([
        StructField("review_id", StringType(), False),
        StructField("comment", StringType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("is_anonymous", BooleanType(), False),
        StructField("is_reported", BooleanType(), False),
        StructField("provider_id", StringType(), False),
        StructField("ratings", DoubleType(), False),
        StructField("user_id", StringType(), False),
    ]),
    "quote_item": StructType([
        StructField("quote_item_id", StringType(), False),
        StructField("description", StringType(), True),
        StructField("price", DoubleType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("quote_response_id", StringType(), True),
    ]),
    "quote_response": StructType([
        StructField("quote_response_id", StringType(), False),
        StructField("additional_notes", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("provider_id", StringType(), False),
        StructField("seeker_id", StringType(), False),
        StructField("seeker_name", StringType(), False),
        StructField("seeker_phone_number", StringType(), False),
        StructField("service_type", StringType(), False),
        StructField("status", StringType(), True),
        StructField("sub_total", DoubleType(), False),
        StructField("tax", DoubleType(), False),
        StructField("total", DoubleType(), False),
        StructField("updated_at", TimestampType(), True),
        StructField("valid_until", DateType(), False),
        StructField("quote_request_id", StringType(), True),
    ]),
        "reports": StructType([
        StructField("id", StringType(), False),
        StructField("reason", StringType(), False),
        StructField("reported_at", TimestampType(), True),
        StructField("review_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("user_id", StringType(), True),
    ]),
    "service_providers": StructType([
        StructField("latitude", DoubleType(), False),
        StructField("longitude", DoubleType(), False),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("provider_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("business_name", StringType(), False),
        StructField("description", StringType(), False),
        StructField("email", StringType(), True),
        StructField("name", StringType(), False),
        StructField("phone_number", StringType(), False),
        StructField("pricing", StringType(), True),
        StructField("verification_status", StringType(), True),
    ]),
     "users": StructType([
        StructField("created_at", TimestampType(), False),
        StructField("update_at", TimestampType(), False),
        StructField("user_id", StringType(), False),
        StructField("email", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("google_id", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("password", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("profile_picture", StringType(), True),
        StructField("role", StringType(), True),
        StructField("status", StringType(), True),
    ]),
    "feedback_sentiment": StructType([
        StructField("id", StringType(), False),
        StructField("is_suspicious", BooleanType(), False),
        StructField("sentiment_label", StringType(), False),
        StructField("sentiment_score", DoubleType(), False),
    ]),
}

# Tables to extract (passed as comma-separated list)
tables = args['DB_TABLES'].split(',')

# Loop through and process each table
for table_name in tables:
    try:
        logger.info(f"Processing table: {table_name}")

        # Load from catalog
        dyf = glueContext.create_dynamic_frame.from_catalog(
            database=args['SOURCE_DATABASE'],
            table_name=table_name
        )

        # Convert to DataFrame for validation
        df = dyf.toDF()
        schema = schemas[table_name]

        # Enforce schema
        validated_df = spark.createDataFrame(df.rdd, schema=schema)

        # Validate data types
        for field in schema.fields:
            if field.name not in validated_df.columns:
                raise AnalysisException(f"Missing field: {field.name} in table {table_name}")
            if not isinstance(validated_df.schema[field.name].dataType, type(field.dataType)):
                raise AnalysisException(f"Data type mismatch for field: {field.name} in table {table_name}")
        logger.info(f"✅ Table {table_name} schema validated successfully.")

        #Deduplicate data
        validated_df = validated_df.dropDuplicates()

        # Write to curated S3
        target_path = f"s3://{args['S3_OUTPUT_BUCKET']}/{table_name}/"
        validated_df.write.mode("overwrite").parquet(target_path)

        logger.info(f"✅ Successfully processed {table_name} to {target_path}")

    except AnalysisException as ae:
        logger.error(f"❌ AnalysisException while processing {table_name}: {str(ae)}", exc_info=True)
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Unexpected error while processing {table_name}: {str(e)}", exc_info=True)
        sys.exit(1)

# Finalize job
job.commit()
logger.info("✅ Glue job completed successfully.")
