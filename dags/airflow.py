# Import necessary libraries and modules
from airflow import DAG
# from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.lab import load_data, data_preprocessing, build_save_model, load_model_predict

# NOTE:
# In Airflow 3.x, enabling XCom pickling should be done via environment variable:
# export AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
# The old airflow.configuration API is deprecated.

# Define default arguments for your DAG
default_args = {
    'owner': 'vn',
    'start_date': datetime(2025, 1, 15),
    'retries': 0,  # Number of retries in case of task failure
    'retry_delay': timedelta(minutes=5),  # Delay before retries
}

# Create a DAG instance named 'Airflow_Lab1' with the defined default arguments
with DAG(
    'Airflow_Lab1',
    default_args=default_args,
    description='Dag example for Lab 1 of Airflow series',
    catchup=False,
) as dag:

    load_data_task = PythonOperator(
        task_id='load_data_task',
        python_callable=load_data,
    )

    def preprocess_callable(ti):
        data_b64 = ti.xcom_pull(task_ids='load_data_task')
        return data_preprocessing(data_b64)

    data_preprocessing_task = PythonOperator(
        task_id='data_preprocessing_task',
        python_callable=preprocess_callable,
    )

    def build_model_callable(ti):
        data_b64 = ti.xcom_pull(task_ids='data_preprocessing_task')
        return build_save_model(data_b64, "model.sav")

    build_save_model_task = PythonOperator(
        task_id='build_save_model_task',
        python_callable=build_model_callable,
    )

    def load_model_callable(ti):
        return load_model_predict("model.sav")

    load_model_task = PythonOperator(
        task_id='load_model_task',
        python_callable=load_model_callable,
    )

    # Set dependencies
    load_data_task >> data_preprocessing_task >> build_save_model_task >> load_model_task

# If this script is run directly, allow command-line interaction with the DAG
if __name__ == "__main__":
    dag.test()
