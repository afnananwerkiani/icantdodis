from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

def transform_excel():
    path = '/home/afnanubuntu/rdbms_sample.xlsx'
    df = pd.read_excel(path, sheet_name=None)
    for sheet_name, data in df.items():
        print(f"Sheet: {sheet_name}")
        print(data.head())

default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
}

with DAG(
    dag_id='excel_transform_dag',
    default_args=default_args,
    schedule=None,
    tags=['excel', 'transformation']
) as dag:
    task = PythonOperator(
        task_id='transform_excel_task',
        python_callable=transform_excel
    )
