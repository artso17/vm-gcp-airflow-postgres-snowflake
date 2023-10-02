# Import necessary modules from Airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

PYDIR = '/home/aditya/airflow/dags/pyjobs'

default_args = {
    'owner': 'your_name',
    'start_date': days_ago(1),
    'depends_on_past': False,
    # 'retries': 1,
}


with DAG(
    'aditya_dag',
    default_args=default_args,
    description='A project 5 Digital Skola ETL Process using postgres and snowflake',
    schedule_interval='1 * * * *', 
    catchup=False,  
    ) :

    ti_1 = BashOperator(
        task_id='Extract_and_Load',
        bash_command=f'python3 {PYDIR}/aditya_etl1.py',  
    
    )

    ti_2 = BashOperator(
        task_id = 'Create_Data_Mart',
        bash_command = f'python3 {PYDIR}/aditya_etl2.py', 
        
    )
    
    ti_1 >> ti_2


