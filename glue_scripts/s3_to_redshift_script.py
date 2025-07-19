import sys
import psycopg2
from awsglue.utils import getResolvedOptions

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

# Extract Redshift connection params
host = args["REDSHIFT_HOST"]
port = args["REDSHIFT_PORT"]
dbname = args["REDSHIFT_DB"]
user = args["REDSHIFT_USER"]
password = args["REDSHIFT_PASSWORD"]
schema = args["REDSHIFT_SCHEMA"]
bucket = args["S3_OUTPUT_BUCKET"]
iam_role = args["IAM_ROLE"]

# Connect to Redshift
conn = psycopg2.connect(
    dbname=dbname,
    user=user,
    password=password,
    host=host,
    port=port
)
cursor = conn.cursor()

# Tables and their DDLs
tables = {
    "dim_user": f"""
        CREATE TABLE IF NOT EXISTS {schema}.dim_user (
            user_id INT,
            email VARCHAR(256),
            phone_number VARCHAR(50),
            role VARCHAR(50),
            provider_id INT,
            business_name VARCHAR(256),
            verification_status VARCHAR(50),
            is_ai_generated BOOLEAN,
            full_name VARCHAR(256),
            created_at TIMESTAMP
        );
    """,

    "dim_date": f"""
        CREATE TABLE IF NOT EXISTS {schema}.dim_date (
            date_key INT,
            date DATE,
            day INT,
            month INT,
            month_name VARCHAR(20),
            quarter INT,
            year INT,
            day_of_week INT,
            day_name VARCHAR(20),
            is_weekend BOOLEAN
        );
    """,

    "dim_service": f"""
        CREATE TABLE IF NOT EXISTS {schema}.dim_service (
            service_id INT,
            provider_id INT,
            category_id INT,
            category_name VARCHAR(255),
            service_name VARCHAR(255),
            description TEXT,
            price FLOAT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );
    """,

    "dim_location": f"""
        CREATE TABLE IF NOT EXISTS {schema}.dim_location (
            location_id INT,
            location VARCHAR(255)
        );
    """,

    "dim_dispute": f"""
        CREATE TABLE IF NOT EXISTS {schema}.dim_dispute (
            dispute_id INT,
            booking_id INT,
            user_id INT,
            reason TEXT,
            dispute_status VARCHAR(50),
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );
    """,

    "dim_review": f"""
        CREATE TABLE IF NOT EXISTS {schema}.dim_review (
            review_id INT,
            user_id INT,
            provider_id INT,
            booking_id INT,
            rating INT,
            comment TEXT,
            created_at TIMESTAMP,
            review_type VARCHAR(20)
        );
    """,

    "fact_booking": f"""
        CREATE TABLE IF NOT EXISTS {schema}.fact_booking (
            booking_id INT,
            user_id INT,
            service_id INT,
            provider_id INT,
            quote_id INT,
            scheduled_date DATE,
            booking_status VARCHAR(50),
            amount FLOAT,
            created_at TIMESTAMP,
            date_key VARCHAR(20),
            year INT,
            month INT
        )
        DISTKEY(booking_id)
        SORTKEY(date_key);
    """
}

# Run CREATE TABLEs
for table_name, ddl in tables.items():
    print(f"📌 Creating table if not exists: {table_name}")
    cursor.execute(ddl)
    conn.commit()

# COPY commands
for table_name in tables.keys():
    s3_path = f"s3://{bucket}/star_schema/{table_name}/"
    copy_sql = f"""
        COPY {schema}.{table_name}
        FROM '{s3_path}'
        IAM_ROLE '{iam_role}'
        FORMAT AS PARQUET;
    """
    print(f"🚀 Loading data into: {table_name}")
    cursor.execute(copy_sql)
    conn.commit()

# Clean up
cursor.close()
conn.close()
print("✅ All tables created and loaded successfully.")
