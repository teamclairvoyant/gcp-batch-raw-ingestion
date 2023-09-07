"""A word-counting workflow."""
import argparse
import logging
import json
from pymongo.mongo_client import MongoClient as mongoclient
import apache_beam as beam
from mysql import connector as mysqlconnector
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from pathlib import Path
from datetime import datetime
from utils.gcs_util import get_secret

def get_project_root() -> Path:
    return Path(__file__).parent

def read_json_file(path: str) -> dict:
    root = get_project_root()
    #path_to_file = '{}/{}'.format(root, path)
    path_to_file = '{}'.format( path)
    logging.info(f'Reading file >> {path_to_file}')
    with open(path_to_file) as json_file:
        data = json.load(json_file)
    return data

class ReadDataFromSourceDoFn(beam.DoFn):
    def __init__(self, config, db_name, table_name, header, file_type, explode_cols, explode_cols_type):
        self.config = config
        self.db_name = db_name
        self.table_name = table_name
        self.header = header
        self.file_type = file_type
        self.explode_cols = explode_cols
        self.explode_cols_type = explode_cols_type

    def process(self, element):
        source = self.config['source']
        decrypt_path = self.config['secret_key']
        project = self.config['project']
        password = get_secret(project, decrypt_path)
        logging.info(f"Successfully fetched password from secret manager")

        if source.upper() == "MONGODB":
            uri = self.config['uri']
            uri = uri.replace("<mongo_user_name>", self.config['user_name'])
            uri = uri.replace("<mongo_user_password>", password)
            client = mongoclient(uri)
            try:
                database_name = self.db_name
                db = client[database_name]
                collection_name = self.table_name
                col = db[collection_name]
                x = col.find()
                header_list = self.header.split(',')

                # json
                if self.file_type.upper() == "JSON":
                    for data in x:
                        json_result = {}
                        for key in header_list:
                            if data.get(key):
                                json_result[key] = str(data.get(key))
                            else:
                                json_result[key] = ""

                        if self.explode_cols != "":
                            element_list = data.get(self.explode_cols)
                            tmp_json_result = json_result.copy()
                            for indx_col in element_list:
                                tmp_json_result[self.explode_cols] = str(indx_col)
                                if self.explode_cols_type == "json":
                                    for key, value in indx_col.items():
                                        tmp_json_result[key]= str(value)
                                    tmp_json_result.pop(self.explode_cols)
                                yield  tmp_json_result
                        else:
                            yield json_result

                # csv
                if self.file_type.upper() == "CSV":
                    for data in x:
                        csv_result = []
                        for key in header_list:
                            if data.get(key):
                                csv_result.append(str(data[key]))
                            else:
                                csv_result.append("")
                        yield ",".join(ele for ele in csv_result)

            except Exception as e:
                yield e

        elif source.upper() == "MYSQL":
            conn = None
            result = []
            try:
                conn = mysqlconnector.connect(
                    user=self.config['user_name'], password=password, host=self.config['uri'], database=self.db_name)
                cursor = conn.cursor()
                sql = "SELECT " + self.header + " from " + self.table_name
                cursor.execute(sql)
                rows = cursor.fetchall()
                if self.file_type.upper() == "JSON":
                    column_names = [desc[0] for desc in cursor.description]
                    for row in rows:
                        yield dict(zip(column_names, row))

                elif self.file_type.upper() == "CSV":
                    for row in rows:
                        yield ",".join(ele for ele in row)
            except Exception as err:
                yield err
            finally:
                if conn:
                    conn.close()
        else:
            logging.info(f"Unknown source")


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument('--config_file', required=True)
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    config = read_json_file(known_args.config_file)
    output_path = config['output_path']
    now = datetime.now()
    today = now.strftime("%Y%m%d")

    with beam.Pipeline(options=pipeline_options) as p:
        for table_dict in config.get("tables", []):
            db_name = table_dict["databse"]
            table_name = table_dict["table"]
            header = table_dict['header']
            file_type = table_dict['file_type']
            explode_cols = table_dict['explode_cols']
            explode_cols_type = table_dict['explode_cols_type']
            if file_type.upper() == "CSV":
                file_header = header
            else:
                file_header = None
            output_path_file = output_path + db_name + "/" + table_name + "-" + today
            data = (
                    p
                    | f"CreateDummyPipeline {db_name}.{table_name} {file_type}" >> beam.Create(['Dummy'])
                    | f"ReadDataFromDB {db_name}.{table_name} {file_type}" >> beam.ParDo(ReadDataFromSourceDoFn(
                                            config=config,
                                            db_name=db_name,
                                            table_name=table_name,
                                            header=header,
                                            file_type=file_type,
                                            explode_cols=explode_cols,
                                            explode_cols_type=explode_cols_type))
                    | f"WriteToGcs {db_name}.{table_name} {file_type}" >> WriteToText(
                                            file_path_prefix=output_path_file,
                                            file_name_suffix="." + file_type,
                                            header=file_header,
                                            max_records_per_shard=10000)
            )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
