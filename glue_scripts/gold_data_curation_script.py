import sys
import logging
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from pyspark.sql.types import *
from pyspark.sql.functions import col, date_format, dayofmonth, month, quarter, year, explode, row_number, lit
from pyspark.sql import Window

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

# Write FactsBooking to S3 in Parquet format
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

# Write DimDate to S3 in Parquet format
dim_date_output_path = f"s3://{output_bucket}/star_schema/dim_date/"
dim_date_df.write.mode("overwrite").parquet(dim_date_output_path)
print("✅ DimDate table generated and saved.")

# DimUser

# Load users and service_providers from Glue Catalog
users_df = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name="users"
).toDF()

providers_df = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name="service_providers"
).toDF()

# Join users with service_providers on user_id (left join since not all users are providers)
dim_user_df = users_df.alias("u") \
    .join(providers_df.alias("p"), col("u.user_id") == col("p.user_id"), "left") \
    .select(
        col("u.user_id"),
        col("u.first_name"),
        col("u.last_name"),
        col("u.role"),
        col("u.phone_number"),
        col("u.email"),
        col("p.provider_id"),
        col("p.business_name"),
        col("p.verificationStatus").alias("verification_status"),
        col("p.is_ai_generated"),
        col("u.created_at")
    ) \
    .withColumn("full_name", col("first_name") + " " + col("last_name")) \
    .drop("first_name", "last_name")

# Write DimUser to S3
dim_user_output_path = f"s3://{output_bucket}/star_schema/dim_user/"
dim_user_df.write.mode("overwrite").parquet(dim_user_output_path)
print("✅ DimUser table generated and saved.")

# DimService

# Load services and categories
services_df = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name="services"
).toDF()

categories_df = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name="categories"
).toDF()

# Join to enrich service with category name
dim_service_df = services_df.alias("s") \
    .join(categories_df.alias("c"), col("s.category_id") == col("c.category_id"), "left") \
    .select(
        col("s.service_id"),
        col("s.provider_id"),
        col("s.category_id"),
        col("c.category_name"),
        col("s.service_name"),
        col("s.description"),
        col("s.price"),
        col("s.created_at"),
        col("s.updated_at")
    )

# Write DimService to S3
dim_service_output_path = f"s3://{output_bucket}/star_schema/dim_service/"
dim_service_df.write.mode("overwrite").parquet(dim_service_output_path)
print("✅ DimService table generated and saved.")

# DimLocation

service_providers_df = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name="service_providers"
).toDF()

# Get distinct locations
location_df = service_providers_df \
    .select("location") \
    .filter(col("location").isNotNull()) \
    .distinct() \
    .withColumn("location_id", row_number().over(Window.orderBy("location"))) \
    .select("location_id", "location")

# Write DimLocation to S3
dim_location_output_path = f"s3://{output_bucket}/star_schema/dim_location/"
location_df.write.mode("overwrite").parquet(dim_location_output_path)
print("✅ DimLocation table generated and saved.")

# DimDispute

disputes_df = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name="dispute_requests"
).toDF()

dim_dispute_df = disputes_df \
    .select(
        col("dispute_id"),
        col("booking_id"),
        col("user_id"),
        col("reason"),
        col("status").alias("dispute_status"),
        col("created_at"),
        col("updated_at")
    ) \
    .filter(col("dispute_id").isNotNull())

# Write DimDispute to S3 in Parquet format
dim_dispute_output_path = f"s3://{output_bucket}/star_schema/dim_dispute/"
dim_dispute_df.write.mode("overwrite").parquet(dim_dispute_output_path)
print("✅ DimDispute table generated and saved.")

# DimReview

# Read both provider_reviews and job_reviews
provider_reviews_df = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name="provider_reviews"
).toDF()

job_reviews_df = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name="job_reviews"
).toDF()

# Transform provider reviews
provider_reviews_transformed = provider_reviews_df \
    .select(
        col("review_id"),
        col("user_id"),
        col("provider_id"),
        col("rating"),
        col("comment"),
        col("created_at")
    ) \
    .withColumn("review_type", lit("provider")) \
    .withColumn("booking_id", lit(None).cast("int"))  # Not applicable

# Transform job reviews
job_reviews_transformed = job_reviews_df \
    .select(
        col("review_id"),
        col("booking_id"),
        col("comment"),
        col("created_at")
    ) \
    .withColumn("review_type", lit("job")) \
    .withColumn("user_id", lit(None).cast("int")) \
    .withColumn("provider_id", lit(None).cast("int")) \
    .withColumn("rating", lit(None).cast("int"))

# Union the two
dim_review_df = provider_reviews_transformed.unionByName(job_reviews_transformed)

# Write to S3
dim_review_output_path = f"s3://{output_bucket}/star_schema/dim_review/"
dim_review_df.write.mode("overwrite").parquet(dim_review_output_path)
print("✅ DimReview table generated and saved.")


# Commit the job
job.commit()

