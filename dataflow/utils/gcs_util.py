import google.cloud.storage as storage
import apache_beam as beam
from google.cloud import secretmanager
import logging
import re

def move_file_from_one_folder_to_another(source_blob_name, source_bucket_name, destination_bucket_name,
                                         destination_folder, project_ing):
    blob_name_split_list = source_blob_name.split('/')
    destination_blob_name = destination_folder + "/" + blob_name_split_list[len(blob_name_split_list) - 1]
    storage_client = storage.Client(project=project_ing)
    source_bucket = storage_client.get_bucket(source_bucket_name)
    source_blob = source_bucket.blob(source_blob_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)
    blob_copy = source_bucket.copy_blob(source_blob, destination_bucket, destination_blob_name)
    source_bucket.delete_blob(source_blob.name)
    logging.info(
        "{} moved to {} in GCS bucket {}".format(source_bucket_name, destination_blob_name, destination_bucket))


class ReadFileContentFn(beam.DoFn):
    def __init__(self, project, bucket_name):
        self.project = project
        self.bucket_name = bucket_name

    def setup(self):
        self.storage_client = storage.Client(project=self.project)

    def process(self, file_name):
        bucket_name = self.bucket_name
        logging.info(f"Reading {str(bucket_name)}/{file_name}")
        bucket = self.storage_client.get_bucket(str(bucket_name))
        blob = bucket.get_blob(file_name)
        yield blob.download_as_string(), file_name


class MoveFilesToArchiveFn(beam.DoFn):
    def __init__(self, project, bucket_name, archive_bucket_name, archive_folder):
        self.project = project
        self.bucket_name = bucket_name
        self.archive_bucket_name = archive_bucket_name
        self.archive_folder = archive_folder

    def process(self, file_name, *args, **kwargs):
        move_file_from_one_folder_to_another(source_blob_name=file_name,
                                             source_bucket_name=self.bucket_name,
                                             destination_bucket_name=self.archive_bucket_name,
                                             destination_folder=self.archive_folder,
                                             project_ing=self.project)


def list_blobs(bucket, folder):
    blobs = bucket.list_blobs(prefix=folder)
    file_paths = []
    for blob in blobs:
        file_paths.append(f"{blob.name}")
    return file_paths


class ListFilesInGcsBucketFn(beam.DoFn):
    def __init__(self, file_pattern, project_ing, bucket_name, landing_folder):
        self.file_pattern = file_pattern
        self.project_ing = project_ing
        self.bucket_name = bucket_name
        self.landing_folder = landing_folder

    def process(self, element, *args, **kwargs):
        storage_client = storage.Client(project=self.project_ing)
        bucket = storage_client.get_bucket(self.bucket_name)
        folder = self.landing_folder
        logging.info('Bucket: {} Folder: {}'.format(self.bucket_name, folder))
        list_entity_files = list_blobs(bucket=bucket, folder=folder)
        logging.info("Files available in gcs bucket: {}".format(str(list_entity_files)))
        for file in list_entity_files:
            pattern_p = re.compile(self.file_pattern)
            if pattern_p.match(file):
                logging.info("file:{}".format(file))
                yield file


def get_secret(project: str, secret_name: str, version: str = "latest") -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project}/secrets/{secret_name}/versions/{version}"
    response = client.access_secret_version(request={"name": name})
    payload = response.payload.data.decode("utf-8")
    return payload
