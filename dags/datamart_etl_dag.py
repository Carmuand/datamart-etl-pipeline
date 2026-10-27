import sys
import os

# Permitir que Airflow localice de forma nativa la carpeta include/ sin errores de sintaxis
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../include'))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from data_processor import run_etl_pipeline

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

def calculate_daily_net():
    """Calcula y actualiza de forma agregada la tabla fact_daily_product_net"""
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    hook = PostgresHook(postgres_conn_id='warehouse_db')
    hook.run("""
        INSERT INTO fact_daily_product_net (date_id, product_code, net_quantity, net_revenue)
        SELECT 
            coalesce(s.date_id, r.date_id) as date_id,
            coalesce(s.product_code, r.product_code) as product_code,
            SUM(coalesce(s.quantity, 0)) - SUM(coalesce(r.quantity, 0)) as net_quantity,
            SUM(coalesce(s.revenue_bruto, 0)) - SUM(coalesce(r.revenue_bruto, 0)) as net_revenue
        FROM fact_sales s
        FULL OUTER JOIN fact_returns r ON s.date_id = r.date_id AND s.product_code = r.product_code
        GROUP BY 1, 2
        ON CONFLICT (date_id, product_code) 
        DO UPDATE SET 
            net_quantity = EXCLUDED.net_quantity, 
            net_revenue = EXCLUDED.net_revenue;
    """)

with DAG(
    'datamart_etl_orchestrator',
    default_args=default_args,
    description='Pipeline ETL de producción modular para DataMart S.A.S.',
    schedule_interval='@daily',
    catchup=False,
    tags=['datamart', 'production', 'modular']
) as dag:

    # Tarea 1: Transformar y cargar el conjunto de datos histórico
    task_history = PythonOperator(
        task_id='transformar_cargar_historico',
        python_callable=run_etl_pipeline,
        op_kwargs={
            'file_path': '/opt/airflow/data/raw/online_retail_II.csv', # RUTA CORREGIDA
            'source_name': 'online_retail_II.csv'
        }
    )

    # Tarea 2: Transformar y cargar el conjunto de datos diario operacional de Riwi
    task_daily = PythonOperator(
        task_id='transformar_cargar_diario',
        python_callable=run_etl_pipeline,
        op_kwargs={
            'file_path': '/opt/airflow/data/raw/sales_transactions.csv', # RUTA CORREGIDA
            'source_name': 'sales_transactions.csv'
        }
    )

    # Tarea 3: Generar agregaciones analíticas de alta velocidad
    task_aggregate = PythonOperator(
        task_id='calcular_netas_diarias',
        python_callable=calculate_daily_net
    )

    # Flujo secuencial del pipeline de ingeniería de datos
    task_history >> task_daily >> task_aggregate
