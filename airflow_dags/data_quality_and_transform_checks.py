from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

# 1. Count of rows in each table (Data completeness check)
def count_rows_each_table():
    pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    query = """
        SELECT 'customer' AS table_name, COUNT(*) AS row_count FROM customer
        UNION ALL
        SELECT 'item', COUNT(*) FROM item
        UNION ALL
        SELECT 'loyalty_points', COUNT(*) FROM loyalty_points
        UNION ALL
        SELECT 'orders', COUNT(*) FROM orders
        UNION ALL
        SELECT 'ordered_items', COUNT(*) FROM ordered_items
        UNION ALL
        SELECT 'payment', COUNT(*) FROM payment
        UNION ALL
        SELECT 'product_reviews', COUNT(*) FROM product_reviews
        UNION ALL
        SELECT 'returnment', COUNT(*) FROM returnment
        UNION ALL
        SELECT 'returnment_reason', COUNT(*) FROM returnment_reason
        UNION ALL
        SELECT 'shipment', COUNT(*) FROM shipment
        UNION ALL
        SELECT 'shopping_cart', COUNT(*) FROM shopping_cart;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    print("Row counts per table:")
    for row in results:
        print(f"{row[0]}: {row[1]}")
    cursor.close()
    conn.close()

# 2. Check for nulls in key columns (Data quality check)
def check_nulls_key_columns():
    pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    query = """
        SELECT
          'customer' AS table_name,
          COUNT(*) FILTER (WHERE customerid IS NULL) AS missing_customerid,
          COUNT(*) FILTER (WHERE email IS NULL) AS missing_email
        FROM customer
        UNION ALL
        SELECT
          'orders',
          COUNT(*) FILTER (WHERE ordernum IS NULL),
          COUNT(*) FILTER (WHERE orderdate IS NULL)
        FROM orders;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    print("Null counts in key columns:")
    for row in results:
        print(f"Table: {row[0]}, missing_customerid: {row[1]}, missing_email/orderdate: {row[2]}")
    cursor.close()
    conn.close()

# 4. Customers by Loyalty Tier (Transformation check)
def customers_by_loyalty_tier():
    pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    query = """
        SELECT
          tier,
          COUNT(DISTINCT customerid) AS customer_count
        FROM loyalty_points
        GROUP BY tier
        ORDER BY customer_count DESC;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    print("Customers by loyalty tier:")
    for row in results:
        print(f"{row[0]}: {row[1]}")
    cursor.close()
    conn.close()

# 5. Returns by Reason (Transformation check)
def returns_by_reason():
    pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    query = """
        SELECT
          rr.description AS reason,
          COUNT(r.returnid) AS return_count
        FROM returnment r
        JOIN returnment_reason rr ON r.reasonid = rr.reasonid
        GROUP BY reason
        ORDER BY return_count DESC;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    print("Returns by reason:")
    for row in results:
        print(f"{row[0]}: {row[1]}")
    cursor.close()
    conn.close()

default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
}

with DAG(
    'data_quality_and_transform_checks',
    schedule_interval='@daily',
    default_args=default_args,
    tags=['dwh']
) as dag:

    task_1_count_rows = PythonOperator(
        task_id='count_rows_each_table',
        python_callable=count_rows_each_table
    )

    task_2_check_nulls = PythonOperator(
        task_id='check_nulls_key_columns',
        python_callable=check_nulls_key_columns
    )

    task_4_customers_loyalty = PythonOperator(
        task_id='customers_by_loyalty_tier',
        python_callable=customers_by_loyalty_tier
    )

    task_5_returns_reason = PythonOperator(
        task_id='returns_by_reason',
        python_callable=returns_by_reason
    )

    # Set task dependencies (without task 3)
    task_1_count_rows >> task_2_check_nulls >> task_4_customers_loyalty >> task_5_returns_reason

