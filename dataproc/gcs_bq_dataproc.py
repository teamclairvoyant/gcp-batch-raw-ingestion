from pyspark.sql import SparkSession
import logging
# Create a SparkSession
spark = SparkSession.builder \
    .appName("Read JSON from GCS and Write to BigQuery") \
    .getOrCreate()

# Set GCS JSON file path
gcs_bucket = "bronze-poc-group"
json_file_path = "mongodb/landing/sample_mflix/users-20230515-00000-of-00001.json"
gcs_file_path = f"gs://{gcs_bucket}/{json_file_path}"

logging.info("File:{}".format(gcs_file_path))

# Set BigQuery table details
project_id = "playground-375318"
dataset_name = "test"
table_name = "mongodb_users"

# Read JSON data from GCS
json_data = spark.read.json(gcs_file_path)
logging.info("Data:{}".format(json_data))
json_data.show()



# Stop the SparkSession
spark.stop()
