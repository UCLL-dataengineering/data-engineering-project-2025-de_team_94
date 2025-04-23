from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from nashville_housing_pipeline.processor import process_data
from nashville_housing_pipeline.reader_writer import Reader, Writer
from nashville_housing_pipeline.validation import validate_dataset
import os

DATA_FOLDER =  '../../files'  
OUTPUT_FOLDER = '../../output'
OUTPUT_FILENAME = 'processed_data'
FILE_INDEX = 1                       

def read_data(**context):
    reader = Reader(DATA_FOLDER)
    df = reader.getDfByIndex(FILE_INDEX)
    context['ti'].xcom_push(key='raw_data', value=df.to_json())


def validate_data(**context):
    import pandas as pd
    df = pd.read_json(context['ti'].xcom_pull(key='raw_data'))
    errors = validate_dataset(df)
    if errors:
        raise ValueError(f"Validation failed with errors: {errors}")
    context['ti'].xcom_push(key='validated_data', value=df.to_json())


def process_and_write(**context):
    import pandas as pd
    df = pd.read_json(context['ti'].xcom_pull(key='validated_data'))
    df_processed = process_data(df)
    Writer(df_processed, OUTPUT_FILENAME, OUTPUT_FOLDER)


default_args = {
    'start_date': datetime(2025, 5, 4),  
    'retries': 0
}

dag = DAG(
    'real_estate_pipeline',
    default_args=default_args,
    schedule_interval=None, 
    catchup=False
)

with dag:
    task_read = PythonOperator(
        task_id='read_data',
        python_callable=read_data,
        provide_context=True
    )

    task_validate = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        provide_context=True
    )

    task_process_write = PythonOperator(
        task_id='process_and_write',
        python_callable=process_and_write,
        provide_context=True
    )

    task_read >> task_validate >> task_process_write
