from datetime import timedelta
from datetime import datetime

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG, Dataset

# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago


dbt_dataset = Dataset("dbt_load")

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1, 0, 0, 0),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "scheduled_dbt_daily",
    default_args=default_args,
    catchup=False,
    schedule=[dbt_dataset],
)

# dbt_check = BashOperator(
#     task_id="dbt_check",
#     bash_command="cd /usr/local/airflow/include/dbt; source /usr/local/airflow/dbt_venv/bin/activate; dbt debug --profiles-dir /usr/local/airflow/include/dbt/",
#     dag=dag,
# )

dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command="cd /usr/local/airflow/include/dbt; source /usr/local/airflow/dbt_venv/bin/activate; dbt run --profiles-dir /usr/local/airflow/include/dbt/",
    dag=dag,
)

dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command="cd /usr/local/airflow/include/dbt/; source /usr/local/airflow/dbt_venv/bin/activate; dbt test --profiles-dir /usr/local/airflow/include/dbt/",
    dag=dag,
)

with dag:
    dbt_run >> dbt_test