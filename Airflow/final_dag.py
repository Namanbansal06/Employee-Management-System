from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime

def schedule_branch(**kwargs):
    date = kwargs['execution_date']
    branches = []

    if date.day == 15:
        branches.append('run_monthly_jobs')
        if date.month == 5:
            branches.append('run_yearly_jobs')
        else:
            branches.append('skip_yearly_jobs')
    else:
        branches.append('skip_monthly_jobs')
        branches.append('skip_yearly_jobs')

    return branches


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

script_args = {
    '--db_user': Variable.get('db_user'),
    '--db_pass': Variable.get('db_pass'),
    '--db_host': Variable.get('db_host'),
    '--pg_url': Variable.get('pg_url'),
    '--curr_ts': Variable.get('curr_ts'),
    '--start_date': Variable.get('start_date'),
    '--end_of_year': Variable.get('end_of_year'),
    '--start_of_year': Variable.get('start_of_year'),
    '--today': Variable.get('today'),
    '--CURRENT_YEAR': Variable.get('CURRENT_YEAR'),
}
with DAG(
    dag_id='glue_job_dags',
    default_args=default_args,
    schedule_interval='0 7 * * *',
    catchup=False,
    tags=['aws', 'glue'],
) as dag:

    daily_check_done = DummyOperator(task_id='daily_check_done')

    # ---------------- DAILY JOBS ----------------
    run_emp_data = GlueJobOperator(
        task_id='run_gp4_emp_data_transformation',
        job_name='gp4_emp_data_transformation',
        region_name='us-east-1',
        script_args=script_args,
    )

    run_emp_time = GlueJobOperator(
        task_id='run_gp4_emp_time_data_transformation',
        job_name='gp4_emp_time_data_transformation',
        region_name='us-east-1',
        script_args=script_args,
    )

    run_emp_leave = GlueJobOperator(
        task_id='run_gp4_emp_leave_data',
        job_name='gp4_emp_leave_data',
        region_name='us-east-1',
        script_args=script_args,
    )

    run_upcoming_leave = GlueJobOperator(
        task_id='run_gp4_emp_upcoming_leave_check',
        job_name='gp4_emp_upcoming_leave_check',
        region_name='us-east-1',
        trigger_rule=TriggerRule.ALL_SUCCESS,
        script_args=script_args,
    )

    run_daily_report = GlueJobOperator(
        task_id='run_gp4_daily_active_employee_report',
        job_name='gp4_daily_active_employee_report',
        region_name='us-east-1',
        trigger_rule=TriggerRule.ALL_SUCCESS,
        script_args=script_args,
    )
    # Set daily dependencies
    [run_emp_data, run_emp_time, run_emp_leave] >> run_upcoming_leave >> run_daily_report >> daily_check_done

    # ---------------- SCHEDULE CHECK ----------------
    schedule_check = BranchPythonOperator(
        task_id='schedule_check',
        python_callable=schedule_branch,
        provide_context=True,
    )
    daily_check_done >> schedule_check

    # ---------------- MONTHLY JOB ----------------
    run_monthly_jobs = DummyOperator(task_id='run_monthly_jobs')
    skip_monthly_jobs = DummyOperator(task_id='skip_monthly_jobs')

    run_monthly = GlueJobOperator(
        task_id='run_gp4_emp_max_availed_leave_check',
        job_name='gp4_emp_max_availed_leave_check',
        region_name='us-east-1',
        script_args=script_args,
    )

    # ---------------- YEARLY JOBS ----------------
    run_yearly_jobs = DummyOperator(task_id='run_yearly_jobs')
    skip_yearly_jobs = DummyOperator(task_id='skip_yearly_jobs')

    run_yearly_1 = GlueJobOperator(
        task_id='run_gp4_emp_leave_calender',
        job_name='gp4_emp_leave_calender',
        region_name='us-east-1',
        script_args=script_args,
    )

    run_yearly_2 = GlueJobOperator(
        task_id='run_gp4_emp_leave_quota',
        job_name='gp4_emp_leave_quota',
        region_name='us-east-1',
        script_args=script_args,
    )

    # Dummy to join yearly jobs
    yearly_done = DummyOperator(
        task_id='yearly_done',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    # Set branching paths
    schedule_check >> [run_monthly_jobs, skip_monthly_jobs, run_yearly_jobs, skip_yearly_jobs]

    # Set yearly job dependencies
    run_yearly_jobs >> [run_yearly_1, run_yearly_2] >> yearly_done

    # Monthly job should always follow yearly (if yearly runs)
    run_monthly_jobs >> run_monthly
    run_monthly.set_upstream(yearly_done)

