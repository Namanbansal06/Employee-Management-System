from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='cooldown_dag',
    default_args=default_args,
    schedule_interval='*/7 * * * *',  # Runs at every 7th mint
    catchup=False,
    tags=['local', 'monthly'],
) as dag:

    run_local_script = BashOperator(
        task_id='run_local_python_script',
        bash_command="""source ~/venv/bin/activate && python /home/ubuntu/capstone/bootcamp-project/kafka/final_code2.py""",
    )
