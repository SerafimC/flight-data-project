from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

def run_simple_spark_script():
    # Define the path to your script
    script_path = '/rawobjects/simple_spark.py' 
    
    # Execute the script
    result = subprocess.run(['python', script_path], capture_output=True, text=True)
    
    # Log the output and errors
    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)
    
    # Raise an exception if the script failed
    if result.returncode != 0:
        raise Exception("Script execution failed")

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 11),
    'retries': 0,
}

# Instantiate the DAG
with DAG(
    'spark_test_dag',
    default_args=default_args,
    description='A simple DAG to run the FlightRadar API script',
    schedule_interval='@daily',  # Adjust the schedule as needed
    catchup=False,
) as dag:
    # Define the task that runs the Python script
    run_script = PythonOperator(
        task_id='run_simple_spark_script',
        python_callable=run_simple_spark_script,
    )

# Set the task in the DAG
run_script
