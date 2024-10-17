from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

def run_flight_radar_script():
    # Define the path to your script
    script_path = './../RawObjects/load_zones.py' 
    
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
    'start_date': datetime(2024, 10, 15),
    'retries': 1,
}

# Instantiate the DAG
with DAG(
    'flight_radar_dag',
    default_args=default_args,
    description='A simple DAG to run the FlightRadar API script',
    schedule_interval='@daily',  # Adjust the schedule as needed
    catchup=False,
) as dag:

    # Define the task that runs the Python script
    run_script = PythonOperator(
        task_id='run_flight_radar_script',
        python_callable=run_flight_radar_script,
    )

    # Set the task in the DAG
    run_script
