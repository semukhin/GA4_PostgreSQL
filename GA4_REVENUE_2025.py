from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import psycopg2
import psycopg2.extras
from google.analytics.data_v1beta import BetaAnalyticsDataClient, RunReportRequest
from google.analytics.data_v1beta.types import DateRange, Metric

# Настройки подключения к PostgreSQL
POSTGRES_CONFIG = {
    'host': '46.101.116.151',
    'database': 'google_db',
    'user': 'postgres',
    'password': 'atlantiX_2025_Atlantix'
}

# Google Analytics настройки
PROPERTY_ID = "properties/448093085"
GA_CREDENTIALS_PATH = "/home/GA4/keys/platform-atlanti-1723565627079-375e73b55d00.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GA_CREDENTIALS_PATH

START_DATE = "2024-09-01"
END_DATE = datetime.today().strftime("%Y-%m-%d")

def create_staging_table():
    """Создание таблицы в схеме staging для Revenue метрик."""
    query = """
    CREATE SCHEMA IF NOT EXISTS staging;

    CREATE TABLE IF NOT EXISTS staging.ga4_revenue_metrics (
        report_date DATE PRIMARY KEY,
        average_purchase_revenue FLOAT,
        arppu FLOAT,
        average_purchase_revenue_per_user FLOAT,
        arpu FLOAT,
        total_revenue FLOAT
    );
    """

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()

def fetch_ga4_revenue_metrics_for_date(date):
    """
    Получение Revenue метрик из GA4 за указанный день.
    Метрики:
      - averagePurchaseRevenue
      - averagePurchaseRevenuePerPayingUser
      - averagePurchaseRevenuePerUser
      - averageRevenuePerUser
      - totalRevenue
    """
    client = BetaAnalyticsDataClient()
    metrics_list = [
        "averagePurchaseRevenue",
        "averagePurchaseRevenuePerPayingUser",
        "averagePurchaseRevenuePerUser",
        "averageRevenuePerUser",
        "totalRevenue"
    ]
    request = RunReportRequest(
        property=PROPERTY_ID,
        date_ranges=[DateRange(start_date=date, end_date=date)],
        metrics=[Metric(name=metric) for metric in metrics_list]
    )
    response = client.run_report(request)
    return response.rows

def fetch_and_load_revenue_data():
    """Основная функция для получения Revenue метрик за весь период и загрузки данных в PostgreSQL."""
    results = []
    current_date = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_date = datetime.strptime(END_DATE, "%Y-%m-%d")

    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        rows = fetch_ga4_revenue_metrics_for_date(date_str)
        if rows:
            # Ожидается одна строка на дату с 5 метриками
            row = rows[0]
            results.append((
                date_str,
                float(row.metric_values[0].value or 0.0),  # averagePurchaseRevenue
                float(row.metric_values[1].value or 0.0),  # averagePurchaseRevenuePerPayingUser (ARPPU)
                float(row.metric_values[2].value or 0.0),  # averagePurchaseRevenuePerUser
                float(row.metric_values[3].value or 0.0),  # averageRevenuePerUser (ARPU)
                float(row.metric_values[4].value or 0.0)   # totalRevenue
            ))
        current_date += timedelta(days=1)

    if not results:
        print("Нет данных для загрузки.")
        return

    insert_query = """
    INSERT INTO staging.ga4_revenue_metrics (
        report_date, average_purchase_revenue, arppu, average_purchase_revenue_per_user, arpu, total_revenue
    ) VALUES %s 
    ON CONFLICT (report_date) DO UPDATE SET
        average_purchase_revenue = EXCLUDED.average_purchase_revenue,
        arppu = EXCLUDED.arppu,
        average_purchase_revenue_per_user = EXCLUDED.average_purchase_revenue_per_user,
        arpu = EXCLUDED.arpu,
        total_revenue = EXCLUDED.total_revenue;
    """

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, insert_query, results)
    conn.commit()
    cur.close()
    conn.close()

# Создаем DAG
with DAG(
    dag_id="GA4_REVENUE_2025",
    default_args={
        'owner': 'semukhin',
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    },
    description="Импорт Revenue метрик из GA4 в PostgreSQL за период с 01.09.2024 по текущую дату",
    schedule='@daily',
    start_date=datetime(2024, 9, 1),
    catchup=False,
) as dag:

    create_table = PythonOperator(
        task_id="create_staging_table",
        python_callable=create_staging_table
    )

    fetch_and_load_data = PythonOperator(
        task_id="fetch_and_load_revenue_data",
        python_callable=fetch_and_load_revenue_data
    )

    create_table >> fetch_and_load_data
