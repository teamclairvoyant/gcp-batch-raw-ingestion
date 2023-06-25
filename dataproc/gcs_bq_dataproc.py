from pyspark.sql import SparkSession
import logging
import argparse
from utils.file_util import read_json_file

def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument('--config_file', required=True)
    known_args, pipeline_args = parser.parse_known_args(argv)
    config = read_json_file(known_args.config_file)
    project_ing = config["project_ing"]

    spark = SparkSession.builder \
        .appName("gcs-batch-raw-ingestion") \
        .getOrCreate()

    for table_dict in config.get("tables", []):
        bucket_name = table_dict["bucket_name"]
        gcs_folder = table_dict["landing_folder"]
        file_pattern = table_dict["file_pattern"]
        json_path = "gs://{}/{}/{}".format(bucket_name, gcs_folder, file_pattern)
        logging.info("File:{}".format(json_path))

        if file_pattern.lower().__contains__("json"):
            data = spark.read.json(json_path)
            logging.info("Data Count:{}".format(data.count()))
            data.show()

        nq_project_id = project_ing
        bq_dataset_name = table_dict["bq_dataset"]
        bq_table_name = table_dict["bq_table_name"]

    spark.stop()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()