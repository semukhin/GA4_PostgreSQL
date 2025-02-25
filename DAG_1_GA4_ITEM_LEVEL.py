###########################################
# DAG #1: GA4_ITEM_LEVEL
###########################################

import os
import psycopg2
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# -----------------------
# Настройки PostgreSQL
# -----------------------
POSTGRES_CONFIG = {
    'host': '46.101.116.151',
    'database': 'google_db',
    'user': 'postgres',
    'password': 'atlantiX_2025_Atlantix'
}

# -----------------------
# Настройки GA4
# -----------------------
PROPERTY_ID = "properties/448093085"
GA_CREDENTIALS_PATH = "/home/GA4/keys/platform-atlanti-1723565627079-375e73b55d00.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GA_CREDENTIALS_PATH

START_DATE = "2024-09-01"
END_DATE = datetime.today().strftime("%Y-%m-%d")

# -----------------------
# Измерения и метрики
# -----------------------
ITEM_DIMENSIONS = [
    "date",         # YYYYMMDD
    "itemId",
    "itemName",
    "itemBrand",
    "itemCategory",
    "itemVariant",
    "currencyCode"
    # Можно добавить ещё 1-2 измерения, не превышая 9
]

ITEM_METRICS = [
    "itemsAddedToCart",
    "itemsPurchased",
    "itemsViewed",
    "itemRevenue",
    "itemRefundAmount",
    "itemDiscountAmount",
    #"itemViewEvents"
    # Всего 7 метрик, < 10
]

def create_item_table():
    """
    Создаём таблицу staging.ga4_item_data для хранения item-level данных.
    """
    query = """
    CREATE SCHEMA IF NOT EXISTS staging;

    CREATE TABLE IF NOT EXISTS staging.ga4_item_data (
        id SERIAL PRIMARY KEY,
        event_date DATE,
        item_id TEXT,
        item_name TEXT,
        item_brand TEXT,
        item_category TEXT,
        item_variant TEXT,
        currency_code TEXT,
        items_added_to_cart NUMERIC,
        items_purchased NUMERIC,
        items_viewed NUMERIC,
        item_revenue NUMERIC,
        item_refund_amount NUMERIC,
        item_discount_amount NUMERIC,
        item_view_events NUMERIC,
        imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        cur.close()
        conn.close()
        print("Таблица staging.ga4_item_data успешно создана или уже существует.")
    except Exception as e:
        print("Ошибка при создании таблицы item_data:", e)

def import_item_data(**kwargs):
    """
    Запрос item-level данных из GA4 и сохранение в таблицу staging.ga4_item_data.
    """
    client = BetaAnalyticsDataClient()

    request = RunReportRequest(
        property=PROPERTY_ID,
        dimensions=[{"name": d} for d in ITEM_DIMENSIONS],
        metrics=[{"name": m} for m in ITEM_METRICS],
        date_ranges=[{"start_date": START_DATE, "end_date": END_DATE}],
        limit=100000
    )

    # Выполняем запрос
    try:
        response = client.run_report(request)
    except Exception as e:
        print("Ошибка при запросе к GA4 (item-level):", e)
        return

    # Подключаемся к PostgreSQL
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor()
    except Exception as e:
        print("Ошибка при подключении к PostgreSQL (item-level):", e)
        return

    inserted_rows = 0
    for row in response.rows:
        dim_vals = [d.value for d in row.dimension_values]
        met_vals = [m.value for m in row.metric_values]

        # Парсим дату
        date_str = dim_vals[0]  # date (YYYYMMDD)
        try:
            event_date = datetime.strptime(date_str, '%Y%m%d').date()
        except:
            event_date = None

        # Пример: 
        # dim_vals = [date, itemId, itemName, itemBrand, itemCategory, itemVariant, currencyCode]
        # met_vals = [itemsAddedToCart, itemsPurchased, itemsViewed, itemRevenue, itemRefundAmount, itemDiscountAmount, itemViewEvents]
        sql = """
        INSERT INTO staging.ga4_item_data (
            event_date,
            item_id,
            item_name,
            item_brand,
            item_category,
            item_variant,
            currency_code,
            items_added_to_cart,
            items_purchased,
            items_viewed,
            item_revenue,
            item_refund_amount,
            item_discount_amount,
            item_view_events
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        data_tuple = (
            event_date,
            dim_vals[1],  # itemId
            dim_vals[2],  # itemName
            dim_vals[3],  # itemBrand
            dim_vals[4],  # itemCategory
            dim_vals[5],  # itemVariant
            dim_vals[6],  # currencyCode
            met_vals[0],  # itemsAddedToCart
            met_vals[1],  # itemsPurchased
            met_vals[2],  # itemsViewed
            met_vals[3],  # itemRevenue
            met_vals[4],  # itemRefundAmount
            met_vals[5],  # itemDiscountAmount
            met_vals[6],  # itemViewEvents
        )

        try:
            cur.execute(sql, data_tuple)
            inserted_rows += 1
        except Exception as e:
            print("Ошибка вставки строки (item-level):", e)

    conn.commit()
    cur.close()
    conn.close()
    print(f"Импорт item-level завершен. Вставлено {inserted_rows} строк(и).")

default_args_item = {
    'owner': 'semukhin',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="GA4_ITEM_LEVEL",
    default_args=default_args_item,
    description="DAG для импорта item-level данных из GA4",
    schedule_interval="@daily",
    catchup=False
) as dag_item:

    create_item_table_task = PythonOperator(
        task_id='create_item_table_task',
        python_callable=create_item_table
    )

    import_item_data_task = PythonOperator(
        task_id='import_item_data_task',
        python_callable=import_item_data
    )

    create_item_table_task >> import_item_data_task
