import sys
import logging
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from pyspark.sql.types import *
from pyspark.sql.functions import col, date_format, dayofmonth, month, quarter, year, explode

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Job parameters (adjust as needed)
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
output_path = f"s3://{output_bucket}/star_schema/fact_booking/"

# FactBooking

# Read from Glue Catalog tables (as crawled from the silver bucket)
bookings_df = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name="bookings"
).toDF()

services_df = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name="services"
).toDF()

payments_df = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name="payment_records"
).toDF()

# Join: bookings -> services -> payments
fact_booking_df = bookings_df.alias("b") \
    .join(services_df.alias("s"), col("b.service_id") == col("s.service_id"), "left") \
    .join(payments_df.alias("p"), col("b.booking_id") == col("p.booking_id"), "left") \
    .select(
        col("b.booking_id"),
        col("b.user_id"),
        col("b.service_id"),
        col("s.provider_id"),
        col("b.quote_id"),
        col("b.scheduled_date"),
        col("b.status").alias("booking_status"),
        col("p.amount"),
        col("b.created_at"),
        date_format(col("b.created_at"), "yyyyMMdd").alias("date_key")
    )

# Filter invalid records
fact_booking_df = fact_booking_df.filter(col("booking_id").isNotNull())

# Write to star schema S3 bucket
fact_booking_df.write.mode("overwrite").parquet(output_path)
print("✅ FactBooking table generated and saved.")

# DimDate

# Generate a date range (e.g. from Jan 1, 2023 to Dec 31, 2025)
start_date = "2023-01-01"
end_date = "2025-12-31"

date_df = spark.sql(f"""
SELECT sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day) as date_list
""").select(explode("date_list").alias("date"))

dim_date_df = date_df \
    .withColumn("date_key", date_format("date", "yyyyMMdd").cast("int")) \
    .withColumn("day", dayofmonth("date")) \
    .withColumn("month", month("date")) \
    .withColumn("month_name", date_format("date", "MMMM")) \
    .withColumn("quarter", quarter("date")) \
    .withColumn("year", year("date")) \
    .withColumn("day_of_week", date_format("date", "u").cast("int")) \
    .withColumn("day_name", date_format("date", "EEEE")) \
    .withColumn("is_weekend", col("day_of_week").isin([6, 7]))

# Commit the job
job.commit()