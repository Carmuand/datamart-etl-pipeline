import pandas as pd
import numpy as np
import os
from datetime import datetime

def run_etl_pipeline(file_path, source_name):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    # 1. Obtener conexión directa al Data Warehouse
    hook = PostgresHook(postgres_conn_id='warehouse_db')
    engine = hook.get_sqlalchemy_engine()
    
    # 2. Asegurar la localización del archivo en el contenedor
    if not os.path.isabs(file_path):
        base_dir = '/opt/airflow'
        file_path = os.path.join(base_dir, file_path)
        
    print(f"Leyendo archivo real desde la ruta: {file_path}")
    
    # 3. Lectura veloz de todo el dataset (se remueve nrows=5000 para cargar el volumen real de datos)
    df = pd.read_csv(file_path, encoding='unicode_escape', engine='python', on_bad_lines='skip')
    
    if df.empty:
        print(f"El archivo {source_name} no tiene registros procesables o está vacío.")
        return

    # 4. Homogeneizar nombres de columnas a minúsculas y mapear
    df.columns = [c.replace(' ', '').lower() for c in df.columns]
    rename_map = {
        'transactionno': 'invoice_no', 'invoiceno': 'invoice_no', 'invoice': 'invoice_no',
        'productno': 'product_code', 'stockcode': 'product_code',
        'productname': 'description', 'description': 'description',
        'quantity': 'quantity', 
        'date': 'invoice_date', 'invoicedate': 'invoice_date',
        'price': 'unit_price', 'unitprice': 'unit_price',
        'customerno': 'customer_id', 'customerid': 'customer_id',
        'country': 'country'
    }
    df.rename(columns=rename_map, inplace=True)
    
    # Estandarizar valores nulos o vacíos críticos
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0).astype(int)
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce').fillna(0.0).astype(float)
    df['invoice_no'] = df['invoice_no'].astype(str).str.strip()

    # 5. INGESTA MASIVA EN DIMENSIONES IGNORANDO DUPLICADOS EXISTENTES
    
    # Ingestar Países (Evitando conflictos)
    unique_countries = df['country'].dropna().unique()
    for country in unique_countries:
        country_clean = str(country).strip().replace("'", "")
        if country_clean:
            hook.run("INSERT INTO dim_country (country_name) VALUES (%s) ON CONFLICT (country_name) DO NOTHING;", parameters=(country_clean,))
    
    # Ingestar Clientes (Limpieza de decimales float .0 y asignación de cliente anónimo)
    cust_numeric = pd.to_numeric(df['customer_id'], errors='coerce')
    df['customer_id'] = np.where(
        cust_numeric.isna(),
        'GENERICO_DATA_MART',
        cust_numeric.astype('Int64').astype(str)
    )
    df['customer_type'] = np.where(df['customer_id'] == 'GENERICO_DATA_MART', 'ANÓNIMO', 'IDENTIFICADO')
    
    unique_customers = df[['customer_id', 'customer_type']].drop_duplicates(subset=['customer_id'])
    for _, row_cust in unique_customers.iterrows():
        hook.run("INSERT INTO dim_customer (customer_id, customer_type) VALUES (%s, %s) ON CONFLICT (customer_id) DO NOTHING;", 
                 parameters=(row_cust['customer_id'], row_cust['customer_type']))
    
    # Ingestar Productos
    df['description'] = df['description'].fillna('SIN DESCRIPCION').astype(str).str.upper().str.strip()
    df['product_code'] = df['product_code'].astype(str).str.upper().str.strip()
    df['category'] = np.where(df['description'].str.contains('WOODEN|CANDLE|STORAGE|HOLDER', na=False), 'Hogar / Decoración', 'Otros')
    unique_products = df[['product_code', 'description', 'category']].rename(columns={'description': 'description_canonical'}).drop_duplicates(subset=['product_code'])
    for _, row_prod in unique_products.iterrows():
        hook.run("INSERT INTO dim_product (product_code, description_canonical, category) VALUES (%s, %s, %s) ON CONFLICT (product_code) DO NOTHING;", 
                 parameters=(row_prod['product_code'], row_prod['description_canonical'], row_prod['category']))
    
    # Ingestar Tiempo
    df['invoice_date'] = pd.to_datetime(df['invoice_date'], errors='coerce')
    df = df.dropna(subset=['invoice_date'])
    df['date_id'] = df['invoice_date'].dt.date
    unique_dates = pd.DataFrame({'date_id': df['date_id'].unique()})
    for _, row_date in unique_dates.iterrows():
        dt_val = pd.to_datetime(row_date['date_id'])
        hook.run("INSERT INTO dim_date (date_id, year, month, month_name, quarter) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (date_id) DO NOTHING;", 
                 parameters=(row_date['date_id'], dt_val.year, dt_val.month, dt_val.strftime('%B'), (dt_val.month - 1) // 3 + 1))
    
    # 6. PROCESAMIENTO E INGESTA MASIVA DE HECHOS
    geo_map = pd.read_sql("SELECT country_id, country_name FROM dim_country", con=engine)
    df = df.merge(geo_map, left_on='country', right_on='country_name', how='left')
    df['country_id'] = df['country_id'].fillna(1).astype(int)
    
    tasa_cambio = 5000.0 if source_name == 'online_retail_II.csv' else 1.0
    df['revenue_bruto'] = (df['quantity'] * df['unit_price']).abs() * tasa_cambio
    df['source_file'] = source_name
    
    # Segmentar e Ingestar masivamente Ventas estándar a fact_sales
    sales_df = df[(df['quantity'] > 0) & (~df['invoice_no'].str.upper().str.startswith('C', na=False))]
    if not sales_df.empty:
        sales_to_insert = sales_df[['invoice_no', 'product_code', 'customer_id', 'country_id', 'date_id', 'quantity', 'unit_price', 'revenue_bruto', 'source_file']]
        sales_to_insert.to_sql('fact_sales', con=engine, if_exists='append', index=False, method='multi')
        
    # Segmentar e Ingestar masivamente Devoluciones a fact_returns
    returns_df = df[(df['quantity'] <= 0) | (df['invoice_no'].str.upper().str.startswith('C', na=False))]
    if not returns_df.empty:
        returns_df['quantity'] = returns_df['quantity'].abs()
        returns_to_insert = returns_df[['invoice_no', 'product_code', 'customer_id', 'country_id', 'date_id', 'quantity', 'unit_price', 'revenue_bruto', 'source_file']]
        returns_to_insert.to_sql('fact_returns', con=engine, if_exists='append', index=False, method='multi')
        
    print(f"ETL completado de forma transparente y real para la fuente: {source_name}")