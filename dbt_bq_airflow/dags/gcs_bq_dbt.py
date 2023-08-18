# retail.py

from airflow.decorators import dag, task
from datetime import datetime
from airflow import Dataset


from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator

dbt_dataset = Dataset("dbt_load")

@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['retail'],
)
def retail():

    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_csv_to_gcs',
        src='include/dataset/*.csv',
        dst='raw/',
        bucket='test-datalake-222222',
        gcp_conn_id='gcp',
        mime_type='text/csv',
    )

    create_retail_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_retail_dataset',
        dataset_id='retail',
        gcp_conn_id='gcp',
    )

    gcs_to_bq_transactions = GoogleCloudStorageToBigQueryOperator(
    task_id                             = "gcs_to_bq_transactions",
    bucket                              = 'test-datalake-222222',
    source_objects                      = ['raw/account_transactions.csv'],
    destination_project_dataset_table   ='retail.account_transactions',
    write_disposition='WRITE_TRUNCATE',
    create_disposition          = 'CREATE_IF_NEEDED',
    gcp_conn_id='gcp'
    )

    gcs_to_bq_closed = GoogleCloudStorageToBigQueryOperator(
    task_id                             = "gcs_to_bq_closed",
    bucket                              = 'test-datalake-222222',
    source_objects                      = ['raw/account_closed.csv'],
    destination_project_dataset_table   ='retail.account_closed',
    write_disposition='WRITE_TRUNCATE',
    create_disposition          = 'CREATE_IF_NEEDED',
    gcp_conn_id='gcp'
    )

    gcs_to_bq_created = GoogleCloudStorageToBigQueryOperator(
    task_id                             = "gcs_to_bq_created",
    bucket                              = 'test-datalake-222222',
    source_objects                      = ['raw/account_created.csv'],
    destination_project_dataset_table   ='retail.account_created',
    write_disposition='WRITE_TRUNCATE',
    create_disposition          = 'CREATE_IF_NEEDED',
    gcp_conn_id='gcp'
    )

    gcs_to_bq_reopened = GoogleCloudStorageToBigQueryOperator(
    task_id                             = "gcs_to_bq_reopened",
    bucket                              = 'test-datalake-222222',
    source_objects                      = ['raw/account_reopened.csv'],
    destination_project_dataset_table   ='retail.account_reopened',
    write_disposition='WRITE_TRUNCATE',
    create_disposition          = 'CREATE_IF_NEEDED',
    gcp_conn_id='gcp',
    outlets = [dbt_dataset]
    )

    chain(
        upload_csv_to_gcs,
        create_retail_dataset,
        gcs_to_bq_transactions,
        gcs_to_bq_closed,
        gcs_to_bq_created,
        gcs_to_bq_reopened

    )

retail()