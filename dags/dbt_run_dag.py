from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum


with DAG(
    dag_id="dbt_run_dag",
    start_date=pendulum.today('UTC').add(days=-1),
    schedule_interval=None,
    catchup=False
    ) as dag:
    
    dbt_run = BashOperator(task_id="dbt_run", 
                           bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/.dbt")