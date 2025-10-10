from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

with DAG(
    dag_id="dbt_run",
    description="DBT DAG to create fact tables from BigQuery",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 1 * * *",
    catchup=False,
    tags=["etl", "bigquery", "dbt", "marts"]
) as dag:
    
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/dbt && dbt run --profiles-dir /opt/dbt"
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/dbt && dbt test --profiles-dir /opt/dbt"
    )

    start >> dbt_run >> dbt_test >> end