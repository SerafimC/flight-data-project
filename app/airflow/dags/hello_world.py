from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Função que será executada pela tarefa
def hello_world():
    print("Hello, World!")

# Definindo os argumentos padrão
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 16),
    'retries': 1,
}

# Criando a DAG
with DAG(
    'hello_world_dag',
    default_args=default_args,
    description='Uma simples DAG de Hello World',
    schedule_interval='@daily',  # Executa diariamente
) as dag:

    # Definindo a tarefa
    hello_task = PythonOperator(
        task_id='hello_task',
        python_callable=hello_world,
    )

# O fluxo de trabalho é simples, com uma única tarefa
hello_task
