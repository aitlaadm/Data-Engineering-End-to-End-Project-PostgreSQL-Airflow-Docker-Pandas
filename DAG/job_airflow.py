import os
import sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator


parent_folder=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
parent_folder+="/scripts/"

sys.path.append(parent_folder)
print(sys.path)
# print(parent_folder)
from write_csv_to_postgres import write_csv_to_postgres_main
from write_df_to_postgres import write_df_to_postgres_main

start_date=datetime(2024,2,19,15,30)

default_args={
    'owner': 'Mohamed AIT LAADIK',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('csv_extract_airflow_docker', default_args=default_args, schedule='@daily', catchup=False) as dag:
    
    write_csv_to_postgres=PythonOperator(
        task_id='write_csv_to_postgres',
        python_callable=write_csv_to_postgres_main,
        retries=1,
        retry_delay=timedelta(seconds=15)
    )
    
    write_df_to_postgres=PythonOperator(
        task_id='write_df_to_postgres',
        python_callable=write_df_to_postgres_main,
        retries=1,
        retry_delay=timedelta(seconds=15)
    )
    
    write_csv_to_postgres >> write_df_to_postgres