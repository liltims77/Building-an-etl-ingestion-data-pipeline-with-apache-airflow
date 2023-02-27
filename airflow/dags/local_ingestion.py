import os
import logging

from airflow.models import DAG 
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.contrib.operators.ssh_operator import SSHOperator

from airflow.contrib.operators.ssh_operator import SSHOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq 

dataset_file = "yellow_tripdata_2021-01.csv"
dataset_url = f"https://s3.amazonaws.com/nyc-tlc/csv_backup/trip+data/{dataset_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = dataset_file.replace('.csv', '.parquet')
S3_DATASET = os.environ.get("S3_DATASET", 'trips_data_all')


def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


def upload_to_aws(bucket, object_name, local_file):
    """
    Ref: https://https://s3.console.aws.amazon.com/s3/upload/aws-dee-project?region=us-east-1
    :param bucket: AWS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """


default_args = {
    "owner": "airflow",
    "provide_context": True,
}

with DAG(
    dag_id="local_ingestion_dag",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2021, 1, 1),
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['aws-dee-project'],
) as dag:


    download_dataset_task = BashOperator( 
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {https://s3.amazonaws.com/nyc-tlc/csv_backup/trip+data} > {path_to_local_home}/{dataset_file}"
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{dataset_file}",
        },
    )

    local_to_aws_task = PythonOperator(
        task_id="local_to_aws_task",
        python_callable=upload_to_aws,
        op_kwargs={
            "object_name": f"raw/{parquet_file}",
            "local_file": f"{path_to_local_home}/{parquet_file}",
        },
    )
   
    s3_task = SSHOperator(
        task_id="s3_task",
        ssh_conn_id='ssh_new',
        command='aws s3 cp s3://<bucket-name>/bashtest/scripts/ssh_connection.sh . && cat ssh_connection.sh | bash - ',
   )
   


    download_dataset_task >> format_to_parquet_task >> local_to_aws_task >> s3_task
