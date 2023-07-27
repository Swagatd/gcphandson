from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# Define the Python function that will be executed in Task B.
def print_hello():
    print("Hello, World!")

# Define default_args, which will be used to define default parameters for the DAG.
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG object with the required parameters.
with DAG(
    'simple_dag',
    default_args=default_args,
    description='A simple DAG for Cloud Composer',
    schedule_interval=timedelta(days=1),
) as dag:

    # Define the first task, Task A. In this case, it's a DummyOperator representing a placeholder task.
    task_a = DummyOperator(task_id='task_a')

    # Define the second task, Task B. It's a PythonOperator that will execute the 'print_hello' function.
    task_b = PythonOperator(
        task_id='task_b',
        python_callable=print_hello,
    )

    # Set the dependencies between tasks.
    task_a >> task_b
