import sys
import logging
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from pyspark.sql.functions import col, date_format, dayofmonth, month, quarter, year, explode, row_number, lit, dayofweek, concat
from pyspark.sql.window import Window
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
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

database_name = args['SOURCE_DATABASE']
output_bucket = args['S3_OUTPUT_BUCKET']

def reading_from_catalog(table_name):
    try:
        logger.info(f"Reading {table_name} from Glue Catalog...")
        data = glueContext.create_dynamic_frame.from_catalog(
            database=database_name,
            table_name=table_name
        ).toDF()
        logger.info(f"✅ Successfully read {table_name}")
        return data 
    except Exception as e:
        logger.error(f"❌ Error reading {table_name}: {e}")
        sys.exit(1)

# Load Silver DataFrames
bookings_df = reading_from_catalog("bookings")
users_df = reading_from_catalog("users")
service_providers_df = reading_from_catalog("service_providers")
categories_df = reading_from_catalog("categories")
provider_reviews_df = reading_from_catalog("provider_reviews")
reports_df = reading_from_catalog("reports")
quote_response_df = reading_from_catalog("quote_response")
quote_item_df = reading_from_catalog("quote_item")
feedback_sentiment_df = reading_from_catalog("feedback_sentiment")

# === FACT BOOKING ===
try:
    logger.info("Generating FactBooking...")
    fact_booking_df = bookings_df.alias("b") \
        .join(quote_response_df.alias("qr"), col("b.quote_id") == col("qr.quote_response_id"), "left") \
        .select(
            col("b.id").alias("booking_id"),
            col("b.seeker_id"),
            col("b.provider_id"),
            col("b.category_id"),
            col("b.quote_id"),
            col("b.location"),
            col("qr.seeker_name"),
            col("qr.sub_total").alias("amount_before_tax"),
            col("qr.tax").alias("tax_amount"),
            col("qr.total").alias("total_amount"),
            col("b.status").alias("booking_status"),
            col("b.created_at"),
            date_format(col("b.created_at"), "yyyyMMdd").alias("date_key")
        ) \
        .withColumn("year", year(col("b.created_at"))) \
        .withColumn("month", month(col("b.created_at"))) \
        .filter(col("booking_id").isNotNull())

    fact_booking_df.write.mode("overwrite").partitionBy("year", "month") \
        .parquet(f"s3://{output_bucket}/star_schema/fact_booking/")
    logger.info("✅ FactBooking saved.")
except AnalysisException as e:
    logger.error(f"❌ Failed FactBooking: {e}", exc_info=True)
    sys.exit(1)

# === DIM DATE ===
try:
    logger.info("Generating DimDate...")
    start_date = "2025-01-01"
    end_date = "2025-12-31"

    date_df = spark.sql(f"""
        SELECT sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day) as date_list
    """).select(explode("date_list").alias("date"))

    dim_date_df = date_df.withColumn("date_key", date_format("date", "yyyyMMdd").cast("int")) \
        .withColumn("day", dayofmonth("date")) \
        .withColumn("month", month("date")) \
        .withColumn("month_name", date_format("date", "MMMM")) \
        .withColumn("quarter", quarter("date")) \
        .withColumn("year", year("date")) \
        .withColumn("day_of_week", dayofweek("date")) \
        .withColumn("day_name", date_format("date", "EEEE")) \
        .withColumn("is_weekend", col("day_of_week").isin([7, 1]))


    dim_date_df.write.mode("overwrite").parquet(f"s3://{output_bucket}/star_schema/dim_date/")
    logger.info("✅ DimDate saved.")
except AnalysisException as e:
    logger.error(f"❌ Failed DimDate: {e}", exc_info=True)
    sys.exit(1)

# === DIM USER ===
try:
    logger.info("Generating DimUser...")
    dim_user_df = users_df.alias("u") \
        .join(service_providers_df.alias("sp"), col("u.user_id") == col("sp.user_id"), "left") \
        .select(
            col("u.user_id"),
            col("u.first_name"),
            col("u.last_name"),
            col("u.role"),
            col("u.phone_number"),
            col("u.email"),
            col("sp.provider_id"),
            col("sp.pricing"),
            col("sp.business_name"),
            col("sp.verification_status"),
            col("sp.latitude"),
            col("sp.longitude"),
            col("sp.name"),
            col("u.status").alias("user_status"),
            col("u.created_at")
        ) \
        .withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name"))) \
        .withColumn("year", year(col("created_at"))) \
        .withColumn("month", month(col("created_at"))) \
        .drop("first_name", "last_name")

    dim_user_df.write.mode("overwrite").partitionBy("year", "month") \
        .parquet(f"s3://{output_bucket}/star_schema/dim_user/")
    logger.info("✅ DimUser saved.")
except AnalysisException as e:
    logger.error(f"❌ Failed DimUser: {e}", exc_info=True)
    sys.exit(1)

# === DIM SERVICE ===
try:
    logger.info("Generating DimService...")
    dim_service_df = quote_response_df.alias("qr") \
        .join(categories_df.alias("c"), col("qr.service_type") == col("c.category_id"), "left") \
        .select(
            col("qr.quote_response_id").alias("service_id"),
            col("qr.provider_id"),
            col("qr.service_type"),
            col("c.category_id"),
            col("c.category_name"),
            col("qr.total").alias("price"),
            col("qr.created_at").alias("created_at"),
            col("qr.updated_at").alias("updated_at")
        )

    dim_service_df.write.mode("overwrite").parquet(f"s3://{output_bucket}/star_schema/dim_service/")
    logger.info("✅ DimService saved.")
except AnalysisException as e:
    logger.error(f"❌ Failed DimService: {e}", exc_info=True)
    sys.exit(1)

# === DIM LOCATION ===
try:
    logger.info("Generating DimLocation...")
    dim_location_df = service_providers_df \
        .select(
            col("name").alias("location_name"),
            col("latitude"),
            col("longitude")
        ) \
        .filter(col("name").isNotNull()) \
        .distinct()

    dim_location_df.write.mode("overwrite").parquet(f"s3://{output_bucket}/star_schema/dim_location/")
    logger.info("✅ DimLocation saved.")
except AnalysisException as e:
    logger.error(f"❌ Failed DimLocation: {e}", exc_info=True)
    sys.exit(1)

# === DIM DISPUTE ===
try:
    logger.info("Generating DimDispute...")
    dim_dispute_df = reports_df \
        .select(
            col("id").alias("dispute_id"),
            col("review_id"),
            col("user_id"),
            col("reason"),
            col("status").alias("dispute_status"),
            col("reported_at")
        ) \
        .filter(col("dispute_id").isNotNull())

    dim_dispute_df.write.mode("overwrite").parquet(f"s3://{output_bucket}/star_schema/dim_dispute/")
    logger.info("✅ DimDispute saved.")
except AnalysisException as e:
    logger.error(f"❌ Failed DimDispute: {e}", exc_info=True)
    sys.exit(1)

# === DIM REVIEW ===
try:
    logger.info("Generating DimReview...")
    dim_review_df = provider_reviews_df.alias("pr") \
        .join(feedback_sentiment_df.alias("fs"), col("pr.review_id") == col("fs.id"), "left") \
        .select(
            col("pr.review_id"),
            col("pr.user_id"),
            col("pr.provider_id"),
            col("pr.ratings"),
            col("fs.sentiment_label"),
            col("fs.sentiment_score"),
            col("fs.is_suspicious"),
            col("pr.comment"),
            col("pr.created_at")
        )

    dim_review_df.write.mode("overwrite").parquet(f"s3://{output_bucket}/star_schema/dim_review/")
    logger.info("✅ DimReview saved.")
except AnalysisException as e:
    logger.error(f"❌ Failed DimReview: {e}", exc_info=True)
    sys.exit(1)

# Finalize job
job.commit()
logger.info("✅ Glue job completed successfully.")
