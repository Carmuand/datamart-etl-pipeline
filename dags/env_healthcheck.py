"""DAG de verificación del entorno.

Comprueba que las Airflow Connections y Variables se crearon solas al iniciar
y que Airflow llega al repositorio analítico. No se programa: se dispara a mano.
"""
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

EXPECTED_VARIABLES = ["sales_csv_path", "batch_size", "min_valid_price", "target_timezone"]
EXPECTED_TABLES = [
    "dim_date", "dim_country", "dim_customer", "dim_product",
    "fact_sales", "fact_returns", "fact_daily_product_net", "rejected_records",
]


def check_variables():
    for name in EXPECTED_VARIABLES:
        print(f"Variable {name} = {Variable.get(name)}")


def check_warehouse():
    hook = PostgresHook(postgres_conn_id="warehouse_db")
    rows = hook.get_records(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
    )
    found = {r[0] for r in rows}
    missing = [t for t in EXPECTED_TABLES if t not in found]
    if missing:
        raise ValueError(f"Faltan tablas en el warehouse: {missing}")
    print(f"OK: {len(EXPECTED_TABLES)} tablas presentes en el warehouse")


with DAG(
    dag_id="env_healthcheck",
    description="Verifica Connections, Variables y tablas del warehouse",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["setup"],
) as dag:
    t_vars = PythonOperator(task_id="check_airflow_variables", python_callable=check_variables)
    t_dwh = PythonOperator(task_id="check_warehouse_connection_and_tables", python_callable=check_warehouse)
    t_vars >> t_dwh
