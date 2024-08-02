from airflow import DAG
from airflow.operators.python import PythonOperator
from meowfact_etl import run_facts_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'schedule_interval': '@daily'
}

dag = DAG(
    'meowfact_dag',
    default_args = default_args,
    description="Meow fact dag"
)

run_etl = PythonOperator(
    task_id="complete_meowfact_etl",
    python_callable=run_facts_etl,
    dag = dag
)

run_etl