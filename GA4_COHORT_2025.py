from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import psycopg2
import psycopg2.extras
from google.analytics.data_v1beta import BetaAnalyticsDataClient, RunReportRequest
from google.analytics.data_v1beta.types import DateRange, Metric, Dimension

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

# Период выборки данных
START_DATE = "2024-09-01"
END_DATE = datetime.today().strftime("%Y-%m-%d")

def create_staging_table():
    """
    Создание схемы staging и таблицы для COHORT метрик.
    Таблица включает:
      - report_date: дата отчёта
      - cohort: имя когорты
      - cohort_nth_day: смещение в днях относительно первой сессии когорты
      - cohort_nth_week: смещение в неделях относительно первой сессии когорты
      - cohort_nth_month: смещение в месяцах относительно первой сессии когорты
      - cohort_active_users: активные пользователи когорты за данный период
      - cohort_total_users: общее число пользователей когорты
    Композитный primary key составлен из report_date, cohort, cohort_nth_day, cohort_nth_week и cohort_nth_month.
    """
    query = """
    CREATE SCHEMA IF NOT EXISTS staging;

    CREATE TABLE IF NOT EXISTS staging.ga4_cohort_metrics (
        report_date DATE,
        cohort TEXT,
        cohort_nth_day TEXT,
        cohort_nth_week TEXT,
        cohort_nth_month TEXT,
        cohort_active_users INT,
        cohort_total_users INT,
        PRIMARY KEY (report_date, cohort, cohort_nth_day, cohort_nth_week, cohort_nth_month)
    );
    """
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()

def fetch_ga4_cohort_metrics_for_date(date_str):
    """
    Получение COHORT метрик из GA4 за указанный день.
    Запрос включает измерения:
      - cohort
      - cohortNthDay
      - cohortNthWeek
      - cohortNthMonth
    и метрики:
      - cohortActiveUsers
      - cohortTotalUsers
    """
    client = BetaAnalyticsDataClient()
    request = RunReportRequest(
        property=PROPERTY_ID,
        date_ranges=[DateRange(start_date=date_str, end_date=date_str)],
        dimensions=[
            Dimension(name="cohort"),
            Dimension(name="cohortNthDay"),
            Dimension(name="cohortNthWeek"),
            Dimension(name="cohortNthMonth")
        ],
        metrics=[
            Metric(name="cohortActiveUsers"),
            Metric(name="cohortTotalUsers")
        ]
    )
    response = client.run_report(request)
    rows = []
    for row in response.rows:
        rows.append((
            date_str,
            row.dimension_values[0].value,  # cohort
            row.dimension_values[1].value,  # cohortNthDay
            row.dimension_values[2].value,  # cohortNthWeek
            row.dimension_values[3].value,  # cohortNthMonth
            int(row.metric_values[0].value or 0),  # cohortActiveUsers
            int(row.metric_values[1].value or 0)   # cohortTotalUsers
        ))
    return rows

def load_cohort_data_to_postgres():
    """
    Основная функция получения COHORT метрик за период и загрузка данных в PostgreSQL.
    """
    results = []
    current_date = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_date_obj = datetime.strptime(END_DATE, "%Y-%m-%d")

    while current_date <= end_date_obj:
        date_str = current_date.strftime("%Y-%m-%d")
        rows = fetch_ga4_cohort_metrics_for_date(date_str)
        if rows:
            results.extend(rows)
        else:
            print(f"Нет данных для даты {date_str}")
        current_date += timedelta(days=1)

    if not results:
        print("Нет данных для загрузки.")
        return

    insert_query = """
    INSERT INTO staging.ga4_cohort_metrics (
        report_date, cohort, cohort_nth_day, cohort_nth_week, cohort_nth_month, 
        cohort_active_users, cohort_total_users
    ) VALUES %s 
    ON CONFLICT (report_date, cohort, cohort_nth_day, cohort_nth_week, cohort_nth_month) DO UPDATE SET
        cohort_active_users = EXCLUDED.cohort_active_users,
        cohort_total_users = EXCLUDED.cohort_total_users;
    """

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, insert_query, results)
    conn.commit()
    cur.close()
    conn.close()

# Создаем DAG
with DAG(
    dag_id="GA4_COHORT_2025",
    default_args={
        'owner': 'semukhin',
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    },
    description="Импорт COHORT метрик из GA4 в PostgreSQL за период с 01.09.2024 по текущую дату",
    schedule_interval='@daily',
    start_date=datetime(2024, 9, 1),
    catchup=False,
) as dag:

    create_table = PythonOperator(
        task_id="create_staging_table",
        python_callable=create_staging_table
    )

    fetch_and_load_data = PythonOperator(
        task_id="fetch_and_load_cohort_metrics",
        python_callable=load_cohort_data_to_postgres
    )

    create_table >> fetch_and_load_data
