import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.dates import days_ago
from datetime import timedelta

from videoGames_pipeline.reader_writer import Reader, Writer
from videoGames_pipeline.processor import process_data
from videoGames_pipeline.validation import validate_dataset

DATA_FOLDER = 'dags/files/realtime'
OUTPUT_FOLDER = 'dags/output'
OUTPUT_FILENAME = 'realtime_processed_data'
PROCESSED_FILES_TRACKER = 'dags/tmp/processed_files.txt'
TMP_RAW_PATH = '/tmp/raw_data.csv'
TMP_VALIDATED_PATH = '/tmp/validated_data.csv'

def check_new_file():
    reader = Reader(DATA_FOLDER, PROCESSED_FILES_TRACKER)
    return bool(reader.list_unprocessed_files())

def read_data(**context):
    reader = Reader(DATA_FOLDER, PROCESSED_FILES_TRACKER)
    df, filename = reader.read_first_unprocessed()

    df.to_csv(TMP_RAW_PATH, index=False)
    context['ti'].xcom_push(key='filename', value=filename)
    print(f"Read file: {filename}")

def validate_data(**context):
    import pandas as pd

    df = pd.read_csv(TMP_RAW_PATH)
    errors = validate_dataset(df)

    if errors:
        error_log_path = 'dags/tmp/validation_errors.log'
        os.makedirs(os.path.dirname(error_log_path), exist_ok=True)
        with open(error_log_path, 'w') as f:
            for index, error_list in errors.items():
                for error in error_list:
                    f.write(f"Row {index}: {error}\n")
        raise ValueError(f"Validation failed. Errors written to {error_log_path}")

    df.to_csv(TMP_VALIDATED_PATH, index=False)
    print("Validation successful.")

def process_data_task(**context):
    import pandas as pd

    df = pd.read_csv(TMP_VALIDATED_PATH)
    df_processed = process_data(df)

    processed_path = '/tmp/processed_data.csv'
    df_processed.to_csv(processed_path, index=False)
    context['ti'].xcom_push(key='processed_data_path', value=processed_path)

    print("Processing successful.")

def write_data_task(**context):
    import pandas as pd

    processed_path = context['ti'].xcom_pull(key='processed_data_path')
    filename = context['ti'].xcom_pull(key='filename')

    df = pd.read_csv(processed_path)
    Writer(df, filename, OUTPUT_FOLDER)

    # Update processed tracker
    os.makedirs(os.path.dirname(PROCESSED_FILES_TRACKER), exist_ok=True)
    with open(PROCESSED_FILES_TRACKER, 'a') as f:
        f.write(filename + '\n')

    print(f"Data written for file: {filename}")

default_args = {
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='demo_video_games_pipeline',
    default_args=default_args,
    schedule_interval='* * * * *',
    catchup=False,
    description='VideoGames Data Pipeline',
) as dag:

    wait_for_file = PythonSensor(
        task_id='wait_for_new_file',
        python_callable=check_new_file,
        poke_interval=10,
        timeout=30,
        mode='poke',
    )

    task_read = PythonOperator(
        task_id='read_data',
        python_callable=read_data,
    )

    task_validate = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
    )

    task_process = PythonOperator(
        task_id='process_data',
        python_callable=process_data_task,
    )

    task_write = PythonOperator(
        task_id='write_data',
        python_callable=write_data_task,
    )

    wait_for_file >> task_read >> task_validate >> task_process >> task_write
