import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.operators.dummy import DummyOperator
from utils.monitor_utils import monitor_start, monitor_finish, monitor_validate, monitor_failed

GCS_BUCKET = "rifqy_computerstore_capstone3"
BQ_PROJECT = "jcdeah-006"
BQ_DS = "rifqy_computerstore_capstone3"

DIMENSIONAL_TABLES = [
    ("users", "dim_users"),
    ("products", "dim_products"),
    ("payment_methods", "dim_payment_methods"),
    ("shipping_methods", "dim_shipping_methods"),
]

DIM_SCHEMA_MAP = {
    "users": [
        {"name": "user_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "full_name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "email", "type": "STRING", "mode": "REQUIRED"},
        {"name": "address", "type": "STRING", "mode": "NULLABLE"},
        {"name": "phone_number", "type": "STRING", "mode": "NULLABLE"},
        {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"}
    ],

    "products": [
        {"name": "product_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "product_name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "brand", "type": "STRING", "mode": "NULLABLE"},
        {"name": "category", "type": "STRING", "mode": "NULLABLE"},
        {"name": "currency", "type": "STRING", "mode": "NULLABLE"},
        {"name": "price", "type": "NUMERIC", "mode": "REQUIRED"},
        {"name": "cost", "type": "NUMERIC", "mode": "REQUIRED"},
        {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"}
    ],

    "payment_methods": [
        {"name": "payment_method_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "method_name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "provider", "type": "STRING", "mode": "REQUIRED"},
        {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"}
    ],

    "shipping_methods": [
        {"name": "shipping_method_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "carrier_name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "shipping_type", "type": "STRING", "mode": "REQUIRED"},
        {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"}
    ]
}

default_args = {
    "owner": "rifqy",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3)
}

with DAG(
    dag_id="postgres_to_bigquery_init_dim_tables",
    description="ETL DAG to initialize dimensional tables insertion from PostgreSQL to BigQuery",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["etl", "bigquery", "dimensional_tables"],
    on_failure_callback=monitor_failed
) as dag:
    
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    for source_table, destination_table in DIMENSIONAL_TABLES:
        monitor_start_dim = PythonOperator(
            task_id=f"monitor_start_{source_table}",
            python_callable=monitor_start,
            op_kwargs={
                "source_table": source_table,
                "destination_table": destination_table,
                "is_incremental": False
            },
            provide_context=True
        )

        postgres_to_gcs_dim = PostgresToGCSOperator(
            task_id=f"extract_{source_table}_to_gcs",
            postgres_conn_id="postgres_default",
            sql=f"SELECT * FROM {source_table}",
            bucket=GCS_BUCKET,
            filename=f"initial_load/{source_table}/{destination_table}.json",
            export_format="json",
            gcp_conn_id="google_cloud_default",
        )

        wait_for_gcs_dim_file = GCSObjectExistenceSensor(
            task_id=f"wait_for_{source_table}_gcs_file",
            bucket=GCS_BUCKET,
            object=f"initial_load/{source_table}/{destination_table}.json",
            google_cloud_conn_id="google_cloud_default",
            poke_interval=30,
            timeout=600,
            mode="poke"
        )

        gcs_to_bigquery_dim = GCSToBigQueryOperator(
            task_id=f"load_{source_table}_to_bigquery",
            bucket=GCS_BUCKET,
            source_objects=[f"initial_load/{source_table}/{destination_table}.json"],
            destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DS}.{destination_table}",
            source_format="NEWLINE_DELIMITED_JSON",
            schema_fields=DIM_SCHEMA_MAP[source_table],
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
            time_partitioning={"type": "DAY", "field": "created_at"},
            gcp_conn_id="google_cloud_default",
        )

        monitor_finish_dim = PythonOperator(
            task_id=f"monitor_finish_{source_table}",
            python_callable=monitor_finish,
            op_kwargs={
                "source_table": source_table,
                "destination_table": destination_table,
                "bq_project": BQ_PROJECT,
                "bq_dataset": BQ_DS,
                "is_incremental": False
            },
            provide_context=True
        )

        task_sleep_dim = PythonOperator(
            task_id=f'sleep_{source_table}',
            python_callable=lambda: time.sleep(3),
        )

        monitor_validate_dim = PythonOperator(
            task_id=f"monitor_validate_{source_table}",
            python_callable=monitor_validate,
            op_kwargs={
                "source_table": source_table,
                "destination_table": destination_table
            },
            provide_context=True
        )

        gcs_cleanup_dim = GCSDeleteObjectsOperator(
            task_id=f"gcs_{source_table}_cleanup",
            bucket_name=GCS_BUCKET,
            objects=[f"initial_load/{source_table}/{destination_table}.json"],
            trigger_rule="all_done",
        )

        start >> monitor_start_dim >> postgres_to_gcs_dim >> wait_for_gcs_dim_file >> gcs_to_bigquery_dim >> monitor_finish_dim >> task_sleep_dim >> monitor_validate_dim >> gcs_cleanup_dim >> end

SCHEMA_MAP = [
    {"name": "transaction_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "user_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "product_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "payment_method_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "shipping_method_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "quantity", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "total_amount", "type": "NUMERIC", "mode": "REQUIRED"},
    {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"}
]

with DAG(
    dag_id="postgres_to_bigquery_incremental_transactions",
    description="Incremental ETL from Postgres to BigQuery (transactions table)",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 1 * * *",
    catchup=False,
    default_args=default_args,
    tags=["etl", "bigquery", "transactions", "incremental"],
    on_failure_callback=monitor_failed
) as dag:
    
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    monitor_start_fct = PythonOperator(
        task_id="monitor_start_transactions",
        python_callable=monitor_start,
        op_kwargs={
            "source_table": source_table,
            "destination_table": destination_table,
            "is_incremental": True
        },
        provide_context=True
        )

    postgres_to_gcs_fct = PostgresToGCSOperator(
        task_id="load_transactions_to_gcs",
        postgres_conn_id="postgres_default",
        sql="""
            SELECT * FROM transactions
            WHERE DATE(created_at) = DATE('{{ ds }}'::timestamp - interval '1 day');
        """,
        bucket=GCS_BUCKET,
        filename="incremental_load/transactions/fct_transactions_{{ ds_nodash }}.json",
        export_format="json",
        gcp_conn_id="google_cloud_default",
        )

    wait_for_gcs_fct_file = GCSObjectExistenceSensor(
            task_id="wait_for_transactions_gcs_file",
            bucket=GCS_BUCKET,
            object="incremental_load/transactions/fct_transactions_{{ ds_nodash }}.json",
            google_cloud_conn_id="google_cloud_default",
            poke_interval=30,
            timeout=600,
            mode="poke"
        )

    gcs_to_bigquery_fct = GCSToBigQueryOperator(
        task_id="load_transactions_to_bigquery",
        bucket=GCS_BUCKET,
        source_objects=["incremental_load/transactions/fct_transactions_{{ ds_nodash }}.json"],
        destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DS}.fct_transactions",
        source_format="NEWLINE_DELIMITED_JSON",
        schema_fields=SCHEMA_MAP,
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        time_partitioning={"type": "DAY", "field": "created_at"},
        gcp_conn_id="google_cloud_default",
    )

    monitor_finish_fct = PythonOperator(
        task_id="monitor_finish_transactions",
        python_callable=monitor_finish,
        op_kwargs={
            "source_table": source_table,
             "destination_table": destination_table,
            "bq_project": BQ_PROJECT,
            "bq_dataset": BQ_DS,
            "is_incremental": True
         },
        provide_context=True
        )

    task_sleep_fct = PythonOperator(
            task_id='sleep_transactions',
            python_callable=lambda: time.sleep(3),
        )

    monitor_validate_fct = PythonOperator(
        task_id="monitor_validate_transactions",
        python_callable=monitor_validate,
        op_kwargs={
            "source_table": source_table,
            "destination_table": destination_table
        },
        provide_context=True
        )

    gcs_cleanup_fct = GCSDeleteObjectsOperator(
        task_id="gcs_transactions_cleanup",
        bucket_name=GCS_BUCKET,
        objects=["incremental_load/transactions/fct_transactions_{{ ds_nodash }}.json"],
        trigger_rule="all_done",
    )

    start >> monitor_start_fct >> postgres_to_gcs_fct >> wait_for_gcs_fct_file >> gcs_to_bigquery_fct >> monitor_finish_fct >> task_sleep_fct >> monitor_validate_fct >> gcs_cleanup_fct >> end