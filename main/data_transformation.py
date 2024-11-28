import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, regexp_replace, when, floor, datediff, current_date, explode

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


# Load configuration
with open("./config/config.json", "r") as config_file:
    config = json.load(config_file)

# Create SparkSession with optimized configuration
spark = SparkSession.builder \
    .appName(config['app_name']) \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()


def read_json_file(input_path: str):
    """Reads a JSON file into a DataFrame."""
    try:
        logger.info(f"Reading JSON file from {input_path}...")
        df = spark.read.json(input_path).select(col("id").alias("person_id"),
                               col("firstname").alias("first_name"),
                               col("lastname").alias("last_name"),
                               col("phone"),
                               col("email"),
                               col("birthday"),
                               col("address.*"))
        return df
    except Exception as e:
        logger.error(f"Error reading JSON file from {input_path}: {e}")
        raise


def mask_user_info(df):
    """Masks user-identifiable information."""
    try:
        logger.info(f"Masking user-identifiable information...{df.count()}")
        return df.withColumn("first_name", lit("****")) \
                 .withColumn("last_name", lit("****")) \
                 .withColumn("phone", lit("**********")) \
                 .withColumn("latitude", lit("****")) \
                 .withColumn("longitude", lit("****")) \
                 .withColumn("street", lit("****")) \
                 .withColumn("streetName", lit("****")) \
                 .withColumn("zipcode", lit("****"))

    except Exception as e:
        logger.error(f"Error masking user information: {e}")
        raise


def calculate_age_and_classify(df):
    """Generalizes the birthdate to a 10-year age group."""
    try:
        logger.info("Calculating age and classifying into 10-year age buckets...")
        # Calculate age as the difference between the current date and birthday in years
        df_with_age = df.withColumn(
            "age",
            floor(datediff(current_date(), col("birthday")) / 365.25)  # Approximate age in years
        )

        # Classify age into 10-year buckets
        df_with_age_bucket = df_with_age.withColumn(
            "age_group",
            when(col("age").between(0, 9), "[0-10]")
                .when(col("age").between(10, 19), "[10-20]")
                .when(col("age").between(20, 29), "[20-30]")
                .when(col("age").between(30, 39), "[30-40]")
                .when(col("age").between(40, 49), "[40-50]")
                .when(col("age").between(50, 59), "[50-60]")
                .when(col("age").between(60, 69), "[60-70]")
                .when(col("age").between(70, 79), "[70-80]")
                .otherwise("80+")
        )

        logger.info("Age calculation and classification completed.")
        return df_with_age_bucket
    except Exception as e:
        logger.error(f"Error generalizing birthdate: {e}")
        raise


def mask_email_domain(df):
    """Retains only the domain part of the email address."""
    try:
        logger.info("Masking email by removing the first part and keeping the domain...")
        return df.withColumn("email", regexp_replace(col("email"), "^[^@]+@", "****@"))
    except Exception as e:
        logger.error(f"Error masking email domain: {e}")
        raise


def write_parquet_file(df, output_path: str):
    """Writes the DataFrame to a Parquet file."""
    try:
        logger.info(f"Writing DataFrame to Parquet file at {output_path}...")
        df.select(col("person_id"),
                  col("first_name"),
                  col("last_name"),
                  col("phone"),
                  col("email"),
                  col("age_group"),
                  col("buildingNumber").alias("building_number"),
                  col("city"),
                  col("country"),
                  col("country_code"),
                  col("latitude"),
                  col("longitude"),
                  col("street"),
                  col("streetName").alias("street_name"),
                  col("zipcode")) \
            .write.mode("overwrite").parquet(output_path)
        logger.info("Data successfully written to Parquet file.")
    except Exception as e:
        logger.error(f"Error writing to Parquet file at {output_path}: {e}")
        raise


def main():
    input_path = f"{config['output_raw_path']}/*.json"
    output_path = config['output_cleansed_path']

    try:
        # Read input JSON file
        df = read_json_file(input_path)

        # Apply transformations
        df_masked = mask_user_info(df)
        df_with_age_classification = calculate_age_and_classify(df_masked)
        df_with_email_masked = mask_email_domain(df_with_age_classification)

        # Write to Parquet
        write_parquet_file(df_with_email_masked, output_path)

    except Exception as e:
        logger.error(f"An error occurred during the ETL process: {e}")


if __name__ == "__main__":
    main()
