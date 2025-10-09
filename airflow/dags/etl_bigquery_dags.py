from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

GCS_BUCKET = "rifqy_computerstore_capstone3"
BQ_PROJECT = "jcdeah-006"
BQ_DS = "rifqy_computerstore_capstone3"

DIMENSIONAL_TABLES = ["users", "payment_methods", "shipping_methods", "products"]
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

with DAG(
    dag_id="postgres_to_bigquery_init_dim_tables",
    description="ETL DAG to initialize dimensional tables insertion from PostgreSQL to BigQuery",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["etl", "bigquery", "dimensional_tables"]
) as dag:
    
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    for table in DIMENSIONAL_TABLES:
        postgres_to_gcs_dim = PostgresToGCSOperator(
            task_id=f"extract_{table}_to_gcs",
            postgres_conn_id="postgres_default",
            sql=f"SELECT * FROM {table}",
            bucket=GCS_BUCKET,
            filename=f"initial_load/{table}/dim_{table}.json",
            export_format="json",
            gcp_conn_id="google_cloud_default",
        )

        gcs_to_bigquery_dim = GCSToBigQueryOperator(
            task_id=f"load_{table}_to_bigquery",
            bucket=GCS_BUCKET,
            source_objects=[f"initial_load/{table}/dim_{table}.json"],
            destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DS}.dim_{table}",
            source_format="NEWLINE_DELIMITED_JSON",
            schema_fields=DIM_SCHEMA_MAP[table],
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
            time_partitioning={"type": "DAY", "field": "created_at"},
            gcp_conn_id="google_cloud_default",
        )

        gcs_cleanup_dim = GCSDeleteObjectsOperator(
            task_id=f"gcs_{table}_cleanup",
            bucket_name=GCS_BUCKET,
            objects=[f"initial_load/{table}/dim_{table}.json"],
            trigger_rule="all_done",
        )

        start >> postgres_to_gcs_dim >> gcs_to_bigquery_dim >> gcs_cleanup_dim >> end

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
    tags=["etl", "bigquery", "transactions", "incremental"]
) as dag:
    
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    postgres_to_gcs_fct = PostgresToGCSOperator(
        task_id="load_transactions_to_gcs",
        postgres_conn_id="postgres_default",
        sql=f"""
            SELECT * FROM transactions
            WHERE DATE(created_at) = DATE("{{ ds }}"::timestamp - interval "1 day");
        """,
        bucket=GCS_BUCKET,
        filename="incremental_load/transactions/fct_transactions_{{ ds_nodash }}.json",
        export_format="json",
        gcp_conn_id="google_cloud_default",
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

    gcs_cleanup_fct = GCSDeleteObjectsOperator(
        task_id="gcs_transactions_cleanup",
        bucket_name=GCS_BUCKET,
        objects=["incremental_load/transactions/fct_transactions_{{ ds_nodash }}.json"],
        trigger_rule="all_done",
    )

    start >> postgres_to_gcs_fct >> gcs_to_bigquery_fct >> gcs_cleanup_fct >> end