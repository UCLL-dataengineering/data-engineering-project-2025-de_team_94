import os
from airflow import DAG
from videoGames_pipeline.processor import process_data
from videoGames_pipeline.reader_writer import Reader, Writer
from videoGames_pipeline.validation import validate_dataset
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.sensors.filesystem import FileSensor

DATA_FOLDER = 'dags/files/realtime'
OUTPUT_FOLDER = 'dags/output'
OUTPUT_FILENAME = 'realtime_processed_data'

def read_data(**context):
    import pandas as pd
    for file in os.listdir(DATA_FOLDER):
        if file.endswith('.csv'):
            file_path = os.path.join(DATA_FOLDER, file)
            print(f"Reading file from: {file_path}")
            df = pd.read_csv(file_path)

            tmp_path = '/tmp/raw_data.csv'
            df.to_csv(tmp_path, index=False)
            context['ti'].xcom_push(key='raw_data_path', value=tmp_path)
            context['ti'].xcom_push(key='csv_filename', value=file)
            return
    raise FileNotFoundError("No CSV file found in directory")

def validate_data(**context):
    import pandas as pd
    path = context['ti'].xcom_pull(key='raw_data_path')
    print(f"Reading from: {path}")
    df = pd.read_csv(path)
    print("Data loaded for validation")

    error_log_path = 'dags/tmp/validation_errors.log'

    errors = validate_dataset(df)
    if errors:
        with open(error_log_path, 'w') as f:
            f.write("Validation failed with errors:\n")
            for index, error_list in errors.items():
                for error in error_list:
                    f.write(f"Row {index}: {error}\n") 

        raise ValueError(f"Validation failed with errors: {errors}")

    validated_path = '/tmp/validated_data.csv'
    df.to_csv(validated_path, index=False)
    context['ti'].xcom_push(key='validated_data_path', value=validated_path)
    print("Validation successful")

def process_data_task(**context):
    import pandas as pd
    path = context['ti'].xcom_pull(key='validated_data_path')
    print(f"Reading validated data from: {path}")
    df = pd.read_csv(path)
    df_processed = process_data(df)
    context['ti'].xcom_push(key='processed_data', value=df_processed)
    print("Data processed successfully")

def write_data_task(**context):
    df_processed = context['ti'].xcom_pull(key='processed_data')
    Writer(df_processed, OUTPUT_FILENAME, OUTPUT_FOLDER)
    print("Data written successfully")

default_args = {
    'start_date': datetime(2023, 1, 1),  
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'demo_video_games_pipeline',
    description='VideoGames Data Pipeline',
    schedule_interval='* * * * *',  
    default_args=default_args,
    catchup=False
)

with dag:
    wait_for_csv = FileSensor(
        task_id='wait_for_csv',
        filepath='*.csv',
        fs_conn_id='fs_default',
        poke_interval=10,  
        timeout=600,  
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

    wait_for_csv >> task_read >> task_validate >> task_process >> task_write
