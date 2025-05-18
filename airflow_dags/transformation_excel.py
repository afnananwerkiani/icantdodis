from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

def transform_excel():  # The function definition
    path = '/home/afnanubuntu/Downloads/rdbms_sample.xlsx'  # Indented with 4 spaces or 1 tab
    df = pd.read_excel(path, sheet_name=None)  # Indented with 4 spaces or 1 tab
    for sheet_name, data in df.items():  # Indented with 4 spaces or 1 tab
        print(f"Sheet: {sheet_name}")  # Indented with 4 spaces or 1 tab
        print(data.head())  # Indented with 4 spaces or 1 tab

default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
}

with DAG(
    dag_id='excel_transform_dag',
    default_args=default_args,
    schedule=None,  # updated to use 'schedule'
    tags=['excel', 'transformation']
) as dag:
    task = PythonOperator(
        task_id='transform_excel_task',
        python_callable=transform_excel
    )

