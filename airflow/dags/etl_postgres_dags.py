from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from db_utils import insert_users, insert_payment_methods, insert_shipping_methods, insert_products, insert_transactions
from generate_data import generate_users, generate_payment_methods, generate_shipping_methods, generate_products, generate_transactions
from utils.notif_callbacks import dag_success_notif_message, failed_notif_message

default_args = {
    "owner": "rifqy",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3)
}

with DAG(
    dag_id="init_dim_tables",
    description="ETL DAG to initialize dimensional tables in PostgreSQL",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["etl", "postgres", "dimensional_tables"],
    on_failure_callback=failed_notif_message
) as dag_init:
    
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    load_users = PythonOperator(
        task_id="load_users",
        python_callable=lambda: insert_users(generate_users(50)),
    )

    load_payment_methods = PythonOperator(
        task_id="load_payment_methods",
        python_callable=lambda: insert_payment_methods(generate_payment_methods()),
    )

    load_shipping_methods = PythonOperator(
        task_id="load_shipping_methods",
        python_callable=lambda: insert_shipping_methods(generate_shipping_methods()),
    )

    load_products = PythonOperator(
        task_id="load_products",
        python_callable=lambda: insert_products(generate_products()),
    )

    dim_init_dag_success = PythonOperator(
        task_id="dim_init_dag_success",
        python_callable=dag_success_notif_message,
    )

    start >> [load_users, load_payment_methods, load_shipping_methods, load_products] >> dim_init_dag_success >> end

with DAG(
    dag_id="load_transactions",
    description="ETL DAG to load transactions table in PostgreSQL",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["etl", "postgres", "transactions"],
    on_failure_callback=failed_notif_message
) as dag_transactions:
    
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    def load_transactions_dag():
        users = generate_users(50)
        payment_methods = generate_payment_methods()
        shipping_methods = generate_shipping_methods()
        products = generate_products()
        transactions = generate_transactions(
            n=200,
            users=users,
            products=products,
            payment_methods=payment_methods,
            shipping_methods=shipping_methods
        )
        return insert_transactions(transactions)
    
    load_transactions = PythonOperator(
        task_id="load_transactions",
        python_callable=load_transactions_dag,
    )

    fct_dag_success = PythonOperator(
        task_id="fct_dag_success",
        python_callable=dag_success_notif_message,
    )

    start >> load_transactions >> fct_dag_success >> end