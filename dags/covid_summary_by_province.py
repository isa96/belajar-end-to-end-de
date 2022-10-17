from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

# project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
project_dir = "/mnt/c/users/izzan/downloads/programs/python-for-data-engineering-master"
# print(project_dir)

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email': ['izzansilmiaziz@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'covid_summary_by_province',
    default_args=default_args,
    description='Covid summary by province',
    schedule_interval='0 3 * * *',
    start_date=days_ago(2),
    tags=['covid'],
) as dag:
    start = DummyOperator(task_id='Start')

    task1 = BashOperator(
        task_id='covid_data_validation',
        bash_command=f'python {project_dir}/validations/covid_data_validations.py',
        dag=dag
    )

    task2 = BashOperator(
        task_id='summary_by_province',
        bash_command=f'python {project_dir}/pipelines/summary_by_province.py',
        dag=dag
    )

    task3 = BashOperator(
        task_id='summary_data_validation',
        bash_command=f'python {project_dir}/validations/summary_data_validations.py',
        dag=dag
    )
    end = DummyOperator(task_id='End')

    start >> task1 >> task2 >> task3 >> end
