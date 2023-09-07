import setuptools


with open('requirements.txt') as f:
    required = f.read().splitlines()

setuptools.setup(
    name='Dataflow-Ingestion',
    version='1.0',
    description='GCP POC Dataflow Ingestion',
    # install_requires=["apache-beam[gcp]==2.46.0","google-cloud-storage==2.8.0","pymongo==3.9.0","pymongo[srv]",
    #                   'mysql-connector-python==8.0.23','pycryptodome==3.17','google-cloud-secret-manager==2.16.1'],
    install_requires=required,
    packages=setuptools.find_packages()
)