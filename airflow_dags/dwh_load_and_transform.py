from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os

DATA_DIR = "/home/afnanubuntu/Downloads/dwh project work/Cleaned DS"
POSTGRES_CONN_ID = "postgres_dwh"

# --------- Transformation Functions ---------
def transform_customer(df):
    df['Email'] = df['Email'].fillna('unknown@example.com')
    df['Phone'] = df['Phone'].astype(str).str.replace(r'\D', '', regex=True)
    df['IsActive'] = df['IsActive'].astype(bool)
    return df

def transform_item(df):
    df['Price'] = pd.to_numeric(df['Price'], errors='coerce').fillna(0)
    df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce').fillna(0).astype(int)
    return df

def transform_loyalty_points(df):
    df['Points'] = pd.to_numeric(df['Points'], errors='coerce').fillna(0).astype(int)
    df['Tier'] = df['Tier'].fillna('Unknown')
    return df

def transform_orders(df):
    df['OrderDate'] = pd.to_datetime(df['OrderDate'], dayfirst=True, errors='coerce')
    # Fill invalid dates with a default value (or choose to drop if preferred)
    df['OrderDate'] = df['OrderDate'].fillna(pd.Timestamp('1900-01-01'))
    df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce').fillna(1).astype(int)
    return df

def transform_ordered_items(df):
    df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce').fillna(0).astype(int)
    return df

def transform_payment(df):
    df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')
    df = df.dropna(subset=['Amount'])
    df['IsDefault'] = df['IsDefault'].astype(bool)
    return df

def transform_product_reviews(df):
    df['Rating'] = pd.to_numeric(df['Rating'], errors='coerce').fillna(0).astype(int)
    df['Comment'] = df['Comment'].fillna('')
    return df

def transform_returnment(df):
    df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce').fillna(0).astype(int)
    return df

def transform_returnment_reason(df):
    df['Description'] = df['Description'].fillna('Unknown')
    return df

def transform_shipment(df):
    df['ShippingFee'] = pd.to_numeric(df['ShippingFee'], errors='coerce').fillna(0)
    df['DeliveryAddress'] = df['DeliveryAddress'].fillna('Unknown')
    return df

def transform_shopping_cart(df):
    df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce').fillna(0).astype(int)
    return df

# --------- Load Table Function with Transformation ---------
def load_table(table_name, csv_file):
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    csv_path = os.path.join(DATA_DIR, csv_file)
    df = pd.read_csv(csv_path)

    # Apply transformations depending on table
    if table_name == 'customer':
        df = transform_customer(df)
    elif table_name == 'item':
        df = transform_item(df)
    elif table_name == 'loyalty_points':
        df = transform_loyalty_points(df)
    elif table_name == 'orders':
        df = transform_orders(df)
    elif table_name == 'ordered_items':
        df = transform_ordered_items(df)
    elif table_name == 'payment':
        df = transform_payment(df)
    elif table_name == 'product_reviews':
        df = transform_product_reviews(df)
    elif table_name == 'returnment':
        df = transform_returnment(df)
    elif table_name == 'returnment_reason':
        df = transform_returnment_reason(df)
    elif table_name == 'shipment':
        df = transform_shipment(df)
    elif table_name == 'shopping_cart':
        df = transform_shopping_cart(df)

    df = df.where(pd.notnull(df), None)  # Replace NaN with None for DB

    cursor.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY;")
    conn.commit()

    for _, row in df.iterrows():
        placeholders = ','.join(['%s'] * len(row))
        insert_sql = f"INSERT INTO {table_name} ({','.join(df.columns)}) VALUES ({placeholders});"
        cursor.execute(insert_sql, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()

# --------- Quality Check Function (now applies transformation before count) ---------
def quality_check(table_name, csv_file):
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
    table_count = cursor.fetchone()[0]

    csv_path = os.path.join(DATA_DIR, csv_file)
    df = pd.read_csv(csv_path)

    # Apply the same transformation before counting rows to match load_table behavior
    if table_name == 'customer':
        df = transform_customer(df)
    elif table_name == 'item':
        df = transform_item(df)
    elif table_name == 'loyalty_points':
        df = transform_loyalty_points(df)
    elif table_name == 'orders':
        df = transform_orders(df)
    elif table_name == 'ordered_items':
        df = transform_ordered_items(df)
    elif table_name == 'payment':
        df = transform_payment(df)
    elif table_name == 'product_reviews':
        df = transform_product_reviews(df)
    elif table_name == 'returnment':
        df = transform_returnment(df)
    elif table_name == 'returnment_reason':
        df = transform_returnment_reason(df)
    elif table_name == 'shipment':
        df = transform_shipment(df)
    elif table_name == 'shopping_cart':
        df = transform_shopping_cart(df)

    csv_count = len(df)

    cursor.close()
    conn.close()

    if table_count != csv_count:
        raise ValueError(f"Row count mismatch for {table_name}: Table has {table_count}, CSV (transformed) has {csv_count}")
    else:
        print(f"Quality check passed for {table_name}: {table_count} rows")

# --------- DAG Definition ---------
default_args = {
    "start_date": datetime(2024, 1, 1),
    "catchup": False,
}

with DAG("dwh_load_and_transform", schedule_interval=None, default_args=default_args, tags=["dwh"]) as dag:

    load_customer = PythonOperator(
        task_id="load_customer",
        python_callable=load_table,
        op_kwargs={"table_name": "customer", "csv_file": "Customer_cleaned.csv"},
    )
    load_item = PythonOperator(
        task_id="load_item",
        python_callable=load_table,
        op_kwargs={"table_name": "item", "csv_file": "Item_cleaned.csv"},
    )
    load_loyalty = PythonOperator(
        task_id="load_loyalty_points",
        python_callable=load_table,
        op_kwargs={"table_name": "loyalty_points", "csv_file": "LoyaltyPoints_cleaned.csv"},
    )
    load_orders = PythonOperator(
        task_id="load_orders",
        python_callable=load_table,
        op_kwargs={"table_name": "orders", "csv_file": "Order_cleaned.csv"},
    )
    load_ordered_items = PythonOperator(
        task_id="load_ordered_items",
        python_callable=load_table,
        op_kwargs={"table_name": "ordered_items", "csv_file": "OrderedItems_cleaned.csv"},
    )
    load_payment = PythonOperator(
        task_id="load_payment",
        python_callable=load_table,
        op_kwargs={"table_name": "payment", "csv_file": "Payment_cleaned.csv"},
    )
    load_product_reviews = PythonOperator(
        task_id="load_product_reviews",
        python_callable=load_table,
        op_kwargs={"table_name": "product_reviews", "csv_file": "ProductReviews_cleaned.csv"},
    )
    load_returnment = PythonOperator(
        task_id="load_returnment",
        python_callable=load_table,
        op_kwargs={"table_name": "returnment", "csv_file": "Returnment_cleaned.csv"},
    )
    load_returnment_reason = PythonOperator(
        task_id="load_returnment_reason",
        python_callable=load_table,
        op_kwargs={"table_name": "returnment_reason", "csv_file": "ReturnmentReason_cleaned.csv"},
    )
    load_shipment = PythonOperator(
        task_id="load_shipment",
        python_callable=load_table,
        op_kwargs={"table_name": "shipment", "csv_file": "Shipment_cleaned.csv"},
    )
    load_shopping_cart = PythonOperator(
        task_id="load_shopping_cart",
        python_callable=load_table,
        op_kwargs={"table_name": "shopping_cart", "csv_file": "ShoppingCart_cleaned.csv"},
    )

    qc_customer = PythonOperator(
        task_id="quality_check_customer",
        python_callable=quality_check,
        op_kwargs={"table_name": "customer", "csv_file": "Customer_cleaned.csv"},
    )
    qc_item = PythonOperator(
        task_id="quality_check_item",
        python_callable=quality_check,
        op_kwargs={"table_name": "item", "csv_file": "Item_cleaned.csv"},
    )
    qc_loyalty = PythonOperator(
        task_id="quality_check_loyalty_points",
        python_callable=quality_check,
        op_kwargs={"table_name": "loyalty_points", "csv_file": "LoyaltyPoints_cleaned.csv"},
    )
    qc_orders = PythonOperator(
        task_id="quality_check_orders",
        python_callable=quality_check,
        op_kwargs={"table_name": "orders", "csv_file": "Order_cleaned.csv"},
    )
    qc_ordered_items = PythonOperator(
        task_id="quality_check_ordered_items",
        python_callable=quality_check,
        op_kwargs={"table_name": "ordered_items", "csv_file": "OrderedItems_cleaned.csv"},
    )
    qc_payment = PythonOperator(
        task_id="quality_check_payment",
        python_callable=quality_check,
        op_kwargs={"table_name": "payment", "csv_file": "Payment_cleaned.csv"},
    )
    qc_product_reviews = PythonOperator(
        task_id="quality_check_product_reviews",
        python_callable=quality_check,
        op_kwargs={"table_name": "product_reviews", "csv_file": "ProductReviews_cleaned.csv"},
    )
    qc_returnment = PythonOperator(
        task_id="quality_check_returnment",
        python_callable=quality_check,
        op_kwargs={"table_name": "returnment", "csv_file": "Returnment_cleaned.csv"},
    )
    qc_returnment_reason = PythonOperator(
        task_id="quality_check_returnment_reason",
        python_callable=quality_check,
        op_kwargs={"table_name": "returnment_reason", "csv_file": "ReturnmentReason_cleaned.csv"},
    )
    qc_shipment = PythonOperator(
        task_id="quality_check_shipment",
        python_callable=quality_check,
        op_kwargs={"table_name": "shipment", "csv_file": "Shipment_cleaned.csv"},
    )
    qc_shopping_cart = PythonOperator(
        task_id="quality_check_shopping_cart",
        python_callable=quality_check,
        op_kwargs={"table_name": "shopping_cart", "csv_file": "ShoppingCart_cleaned.csv"},
    )

    load_tasks = [
        load_customer, load_item, load_loyalty, load_orders, load_ordered_items,
        load_payment, load_product_reviews, load_returnment, load_returnment_reason,
        load_shipment, load_shopping_cart
    ]

    qc_tasks = [
        qc_customer, qc_item, qc_loyalty, qc_orders, qc_ordered_items,
        qc_payment, qc_product_reviews, qc_returnment, qc_returnment_reason,
        qc_shipment, qc_shopping_cart
    ]

    for load_task, qc_task in zip(load_tasks, qc_tasks):
        load_task >> qc_task

