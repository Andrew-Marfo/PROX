import sys
import logging
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'SOURCE_DATABASE',
    'TARGET_S3_BASE_PATH'
])

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define schemas for all tables
schemas = {
    "users": StructType([
        StructField("user_id", IntegerType(), False),
        StructField("email", StringType(), False),
        StructField("password", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("role", StringType(), True),
        StructField("profile_picture", StringType(), True),
        StructField("phone_number", StringType(), False),
        StructField("status", StringType(), True),
        StructField("googleId", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ]),
    "service_providers": StructType([
        StructField("provider_id", IntegerType(), False),
        StructField("user_id", IntegerType(), True),
        StructField("business_name", StringType(), True),
        StructField("email", StringType(), False),
        StructField("phone_number", StringType(), False),
        StructField("description", StringType(), True),
        StructField("location", StringType(), True),
        StructField("verificationStatus", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ]),
    "categories": StructType([
        StructField("category_id", IntegerType(), False),
        StructField("category_name", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ]),
    "services": StructType([
        StructField("service_id", IntegerType(), False),
        StructField("provider_id", IntegerType(), True),
        StructField("category_id", IntegerType(), True),
        StructField("service_name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("price", DecimalType(10, 2), True),
        StructField("availability", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ])
}

# List of tables to process
tables = schemas.keys()

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

        # Write to curated S3
        target_path = f"{args['TARGET_S3_BASE_PATH']}/{table_name}/"
        validated_df.write.mode("overwrite").parquet(target_path)

        logger.info(f"✅ Successfully processed {table_name} to {target_path}")

    except AnalysisException as ae:
        logger.error(f"❌ AnalysisException while processing {table_name}: {str(ae)}", exc_info=True)
    except Exception as e:
        logger.error(f"❌ Unexpected error while processing {table_name}: {str(e)}", exc_info=True)

# Finalize job
job.commit()
logger.info("✅ Glue job completed successfully.")
