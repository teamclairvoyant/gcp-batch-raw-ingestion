import setuptools

setuptools.setup(
    name='gcp_batch_raw_ingestion_dataproc',
    version='1.0',
    description='GCP POC Dataproc Ingestion',
    install_requires=['pyspark==3.4.1'],
    packages=setuptools.find_packages()
)