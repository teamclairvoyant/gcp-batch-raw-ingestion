"""A word-counting workflow."""
import argparse
import logging
import ast
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import WriteToBigQuery
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from utils.file_util import read_json_file, process_csv
from utils.gcs_util import ReadFileContentFn, ListFilesInGcsBucketFn, MoveFilesToArchiveFn
from transform.transform_conform import ApplyTransformConformRules

class ProcessFileFn(beam.DoFn):
    def __init__(self, table_schema):
        self.table_schema = table_schema

    def process(self, element):
        file_name = element[1]
        if file_name.endswith(".json"):
            list_str = element[0].decode()
            list_l = list_str.split("\n")
            result = []
            for ele in list_l:
                if ele.strip():
                    row = ast.literal_eval(ele)
                    # tbl_row = {}
                    # schema_l = self.table_schema.split(",")
                    # for schema_ele in schema_l:
                    #     key = schema_ele.split(":")[0]
                    #     if row.get(key) != None:
                    #         tbl_row[key] = str(row[key])
                    #     else:
                    #         tbl_row[key] = ""
                    # result.append(tbl_row)
                    result.append(row)
            return result

        elif file_name.endswith(".csv"):
            sep = ","
            op_data = process_csv(sep, bytes(element[0].decode(), 'utf-8'))
            result = []
            for row in op_data:
                # logging.info(f'CSV Row:{row}')
                # tbl_row = {}
                # schema_l = self.table_schema.split(",")
                # for schema_ele in schema_l:
                #     key = schema_ele.split(":")[0]
                #     if row.get(key) != None:
                #         tbl_row[key] = str(row[key])
                #     else:
                #         tbl_row[key] = ""
                # result.append(tbl_row)
                result.append(row)
            return result


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument('--config_file', required=True)
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    config = read_json_file(known_args.config_file)
    custom_gcs_temp_location = config["custom_gcs_temp_location"]
    project_ing = config["project_ing"]

    with beam.Pipeline(options=pipeline_options) as p:
        for table_dict in config.get("tables", []):
            bucket_name = table_dict["bucket_name"]
            landing_folder = table_dict["landing_folder"]
            file_pattern = table_dict["file_pattern"]
            bq_dataset = table_dict["bq_dataset"]
            bq_table_name = table_dict["bq_table_name"]
            label = table_dict["label"]
            entity_name = label.upper()
            archive_bucket_name = table_dict["archive_bucket_name"]
            archive_folder = table_dict["archive_folder"]
            transformation_rules_uri = config["transformation_rules_uri"]
            transformation_rules = read_json_file(transformation_rules_uri)
            table_schema = (table_dict["bq_table_schema"])

            file_list = (
                    p
                    | f"CreateDummyPipeline {label}" >> beam.Create(['Dummy'])
                    | f"GetProcessFileList {label}" >> beam.ParDo(ListFilesInGcsBucketFn(
                                        file_pattern=file_pattern,
                                        project_ing=project_ing,
                                        bucket_name=bucket_name,
                                        landing_folder=landing_folder))
            )

            data = (
                    file_list
                    | f"ReadFileContentSideInput {label}" >> beam.ParDo(ReadFileContentFn(project=project_ing, bucket_name=bucket_name))
                    | f"ProcessFile {label}" >> beam.ParDo(ProcessFileFn(table_schema=table_schema))
                    | f"ApplyTransformationRules {label}" >> ApplyTransformConformRules(transformation_rules=transformation_rules, entity_name=entity_name)
            )

            write_date = (
                    data
                    | f"WriteToBigQuery {label}" >> WriteToBigQuery(
                                        table=f"{project_ing}:{bq_dataset}.{bq_table_name}",
                                        method="DEFAULT",
                                        custom_gcs_temp_location=custom_gcs_temp_location,
                                        write_disposition=BigQueryDisposition.WRITE_APPEND,
                                        create_disposition=BigQueryDisposition.CREATE_NEVER)
            )

            archive_files = (
                    file_list
                    | f"MoveFilesToArchive {label}" >> beam.ParDo(MoveFilesToArchiveFn(
                                        project=project_ing,
                                        bucket_name=bucket_name,
                                        archive_bucket_name=archive_bucket_name,
                                        archive_folder=archive_folder),
                                        data=beam.pvalue.AsList(data))
            )



class PrintJson(beam.DoFn):
    def process(self, element):
        logging.info("Result:{}".format(element))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
