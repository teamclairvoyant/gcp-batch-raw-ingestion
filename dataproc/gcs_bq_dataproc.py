from pyspark.sql import SparkSession, Window
import logging
import json
from google.cloud import storage
from pyspark.sql.functions import sum

def run(argv=None, save_main_session=True):
    spark = SparkSession.builder \
        .appName("gcs-batch-raw-ingestion") \
        .getOrCreate()

    client = storage.Client()
    bucket_name = "bronze-poc-group"
    file_name = "gcp-batch-raw-ingestion/dataproc/config/gcs_to_bq_config.json"
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    json_string = blob.download_as_text()
    config = json.loads(json_string)
    project_ing = config.get("project_ing", "playground-375318")

    for table_dict in config.get("tables", []):
        name = table_dict["name"]
        if name == "transactions_per_user":
            src_bq_dataset = table_dict["src_bq_dataset"]
            transactions_tbl = table_dict["transactions_tbl"]
            customers_tbl = table_dict["customers_tbl"]
            transactions_tbl_nm = f"{project_ing}.{src_bq_dataset}.{transactions_tbl}"
            customers_tbl_nm = f"{project_ing}.{src_bq_dataset}.{customers_tbl}"

            transactions_tbl_df = spark.read.format("bigquery").option("table", transactions_tbl_nm).load()
            customers_tbl_df = spark.read.format("bigquery").option("table", customers_tbl_nm).load()

            cols = ["username", "transaction_code"]
            windowPartition = Window.partitionBy(cols).orderBy("username")
            df = transactions_tbl_df.join(customers_tbl_df, transactions_tbl_df["account_id"]== customers_tbl_df["accounts"], "left" )\
                .select(
                customers_tbl_df["username"],
                transactions_tbl_df["transaction_code"],
                sum(transactions_tbl_df["transaction_count"]).over(windowPartition).alias("transaction_count"),
                sum(transactions_tbl_df["total"]).over(windowPartition).alias("total_amount")
            ).orderBy("username").distinct()

            df.show(truncate=False)
            trg_bq_dataset = table_dict["trg_bq_dataset"]
            trg_bq_table_name = table_dict["trg_bq_table_name"]
            write_into_bigquery(df, project_ing, trg_bq_dataset, trg_bq_table_name)


        elif name == "transaction_per_user_per_account":
            src_bq_dataset = table_dict["src_bq_dataset"]
            transactions_tbl = table_dict["transactions_tbl"]
            customers_tbl = table_dict["customers_tbl"]
            transactions_tbl_nm = f"{project_ing}.{src_bq_dataset}.{transactions_tbl}"
            customers_tbl_nm = f"{project_ing}.{src_bq_dataset}.{customers_tbl}"

            transactions_tbl_df = spark.read.format("bigquery").option("table", transactions_tbl_nm).load()
            customers_tbl_df = spark.read.format("bigquery").option("table", customers_tbl_nm).load()

            cols = ["username","account_id","transaction_code"]
            windowPartition = Window.partitionBy(cols).orderBy("username")
            df = transactions_tbl_df.join(customers_tbl_df, transactions_tbl_df["account_id"]== customers_tbl_df["accounts"], "left" )\
                .select(
                customers_tbl_df["username"],
                transactions_tbl_df["transaction_code"],
                transactions_tbl_df["account_id"],
                sum(transactions_tbl_df["transaction_count"]).over(windowPartition).alias("transaction_count"),
                sum(transactions_tbl_df["total"]).over(windowPartition).alias("total_amount")
            ).orderBy("username").distinct()

            df.show(truncate=False)

            trg_bq_dataset = table_dict["trg_bq_dataset"]
            trg_bq_table_name = table_dict["trg_bq_table_name"]
            write_into_bigquery(df,project_ing,trg_bq_dataset,trg_bq_table_name )

    spark.stop()

def write_into_bigquery(df, project_ing, trg_bq_dataset, trg_bq_table_name ):
    df.write.format('bigquery') \
        .option('temporaryGcsBucket', 'bronze-poc-group') \
        .option('project', project_ing) \
        .option('dataset', trg_bq_dataset) \
        .option('table', trg_bq_table_name) \
        .mode('append') \
        .save()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()