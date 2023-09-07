gcp-batch-raw-ingestion

Packages required:

python -m  pip install apache-beam[gcp]==2.46.0 
python -m  pip install google-cloud-storage==2.8.0
python3 -m pip install pymongo==3.9.0
python3 -m pip install pymongo[srv]
python3 -m pip install pandas==1.5.3
python3 -m pip install mysql-connector-python==8.0.33
python3 -m pip install pycryptodome==3.17
python -m pip install google-cloud-secret-manager==2.16.1


setup:
python setup.py install

mongodb_to_gcs template build: This template is to read the data from mongodb and write it in GCS bucket. Data is in form of json(one line one json.) or csv
python -m source_to_gcs --runner DataflowRunner  --project playground-375318  --staging_location gs://bronze-poc-group/gcp-batch-raw-ingestion/dataflow/staging  --temp_location gs://bronze-poc-group/gcp-batch-raw-ingestion/dataflow/temp --template_location gs://bronze-poc-group/gcp-batch-raw-ingestion/dataflow/templates/mongodb_to_gcs --region US --config_file config\mongodb_to_gcs_config.json  --setup_file ./setup.py --save_main_session

mysql_to_gcs template build: This template is to read the data from mysql and write it in GCS bucket. Data is in form of json(one line one json.) or csv
python -m source_to_gcs --runner DataflowRunner  --project playground-375318  --staging_location gs://bronze-poc-group/gcp-batch-raw-ingestion/dataflow/staging  --temp_location gs://bronze-poc-group/gcp-batch-raw-ingestion/dataflow/temp --template_location gs://bronze-poc-group/gcp-batch-raw-ingestion/dataflow/templates/mysql_to_gcs --region US --config_file config\mysql_to_gcs_config.json --setup_file ./setup.py --save_main_session

gcs_to_bq_json template build: This template is to read the data from gcs bucket json data(json- one line one json or csv) and write it in existing bigquery table 
python -m gcs_to_bq --runner DataflowRunner  --project playground-375318  --staging_location gs://bronze-poc-group/gcp-batch-raw-ingestion/dataflow/staging  --temp_location gs://bronze-poc-group/gcp-batch-raw-ingestion/dataflow/temp --template_location gs://bronze-poc-group/gcp-batch-raw-ingestion/dataflow/templates/gcs_to_bq --region US --config_file config\gcs_to_bq_config.json --setup_file ./setup.py --save_main_session



gcs_to_bq_config.json
label= <sequence_no>_<bq_table_name>
eg. bq_table_name is mongodb_users then label= 1_mongodb_users
Every label in this file should have entry in transformation_rules.json


transformation_rules.json
Keys in this config file are equal to label.upper() from gcs_to_bq_config.json



Execute code in gcloud shell:
gcloud dataflow jobs run mysql_to_gcs --gcs-location gs://bronze-poc-group/gcp-batch-raw-ingestion/dataflow/templates/mysql_to_gcs --region us-central1 --num-workers 2 --staging-location gs://bronze-poc-group/gcp-batch-raw-ingestion/dataflow/temp/ --additional-experiments use_portable_job_submission



Run dataproc job
# gs://bronze-poc-group/gcp-batch-raw-ingestion/dataproc/gcs_bq_dataproc.py
# gs://bronze-poc-group/gcp-batch-raw-ingestion/dataproc/jars/spark-3.1-bigquery-0.31.1.jar