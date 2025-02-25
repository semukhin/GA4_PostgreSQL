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

START_DATE = "2024-09-01"
END_DATE = datetime.today().strftime("%Y-%m-%d")

def create_staging_table():
    """Создание таблицы в схеме staging для данных о событиях."""
    query = """
    CREATE SCHEMA IF NOT EXISTS staging;

    CREATE TABLE IF NOT EXISTS staging.ga4_events_metrics (
        report_date DATE,
        event_name TEXT,
        event_count INT,
        event_count_per_user FLOAT,
        event_value FLOAT,
        is_key_event BOOLEAN,
        platform TEXT,
        device_category TEXT,
        country TEXT,
        session_source TEXT,
        session_medium TEXT,
        PRIMARY KEY (report_date, event_name, device_category, country, session_source, session_medium)
    );

    CREATE TABLE IF NOT EXISTS staging.ga4_key_events_metrics (
        report_date DATE,
        key_event_name TEXT,
        event_count INT,
        event_count_per_user FLOAT,
        event_value FLOAT,
        user_key_event_rate FLOAT,
        session_key_event_rate FLOAT,
        device_category TEXT,
        country TEXT,
        session_source TEXT,
        session_medium TEXT,
        PRIMARY KEY (report_date, key_event_name, device_category, country)
    );
    """

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()

def fetch_ga4_event_metrics(date, dimensions_list, metrics_list):
    """Вспомогательная функция для запроса метрик о событиях из GA4 за конкретный день."""
    client = BetaAnalyticsDataClient()
    request = RunReportRequest(
        property=PROPERTY_ID,
        date_ranges=[DateRange(start_date=date, end_date=date)],
        metrics=[Metric(name=metric) for metric in metrics_list],
        dimensions=[Dimension(name=dimension) for dimension in dimensions_list]
    )
    response = client.run_report(request)
    return response.rows

def fetch_all_events_data():
    """Функция для получения данных о всех событиях."""
    dimensions = [
        "eventName", 
        "isKeyEvent", 
        "platform", 
        "deviceCategory", 
        "country",
        "sessionSource", 
        "sessionMedium"
    ]
    
    metrics = [
        "eventCount", 
        "eventCountPerUser", 
        "eventValue"
    ]

    results = []
    current_date = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_date = datetime.strptime(END_DATE, "%Y-%m-%d")

    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        print(f"Fetching all events data for {date_str}")

        rows = fetch_ga4_event_metrics(date_str, dimensions, metrics)

        for row in rows:
            event_name = row.dimension_values[0].value
            is_key_event = row.dimension_values[1].value.lower() == 'true'
            platform = row.dimension_values[2].value
            device_category = row.dimension_values[3].value
            country = row.dimension_values[4].value
            session_source = row.dimension_values[5].value
            session_medium = row.dimension_values[6].value
            
            results.append((
                date_str,
                event_name,
                int(row.metric_values[0].value or 0),      # eventCount
                float(row.metric_values[1].value or 0.0),  # eventCountPerUser
                float(row.metric_values[2].value or 0.0),  # eventValue
                is_key_event,
                platform,
                device_category,
                country,
                session_source,
                session_medium
            ))

        current_date += timedelta(days=1)

    return results

def fetch_key_events_data():
    """Функция для получения подробных данных о ключевых событиях."""
    dimensions = [
        "deviceCategory", 
        "country", 
        "sessionSource", 
        "sessionMedium"
    ]
    
    # Список ключевых событий
    key_events = [
        "main_form_submit",
        "platform_login",
        "platform_signup",
        "click_upgrade_pro",
        "click_upgrade_flexible",
        "purchase"
    ]
    
    results = []
    current_date = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_date = datetime.strptime(END_DATE, "%Y-%m-%d")

    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        
        for key_event in key_events:
            print(f"Fetching data for key event {key_event} on {date_str}")
            
            metrics = [
                f"keyEvents:{key_event}",
                "eventCountPerUser",
                "eventValue",
                f"userKeyEventRate:{key_event}",
                f"sessionKeyEventRate:{key_event}"
            ]

            rows = fetch_ga4_event_metrics(date_str, dimensions, metrics)

            for row in rows:
                device_category = row.dimension_values[0].value
                country = row.dimension_values[1].value
                session_source = row.dimension_values[2].value
                session_medium = row.dimension_values[3].value
                
                results.append((
                    date_str,
                    key_event,
                    int(row.metric_values[0].value or 0),      # keyEvents:event_name
                    float(row.metric_values[1].value or 0.0),  # eventCountPerUser
                    float(row.metric_values[2].value or 0.0),  # eventValue
                    float(row.metric_values[3].value or 0.0),  # userKeyEventRate:event_name
                    float(row.metric_values[4].value or 0.0),  # sessionKeyEventRate:event_name
                    device_category,
                    country,
                    session_source,
                    session_medium
                ))

        current_date += timedelta(days=1)

    return results

def load_all_events_data():
    """Загрузка данных о всех событиях в PostgreSQL."""
    data = fetch_all_events_data()
    if not data:
        print("Нет данных о событиях для загрузки.")
        return

    # Удаляем дубликаты данных по первичному ключу перед вставкой
    # Создаем словарь, где ключ - это первичный ключ, а значение - строка данных
    unique_data = {}
    for row in data:
        # Создаем ключ из полей, составляющих первичный ключ
        key = (row[0], row[1], row[7], row[8], row[9], row[10])  # (date, event_name, device_category, country, session_source, session_medium)
        # Если у нас есть несколько строк с одинаковым ключом, берем последнюю
        unique_data[key] = row
    
    # Преобразуем словарь обратно в список
    deduplicated_data = list(unique_data.values())
    print(f"Удалено {len(data) - len(deduplicated_data)} дубликатов из {len(data)} строк")

    insert_query = """
    INSERT INTO staging.ga4_events_metrics (
        report_date, event_name, event_count, event_count_per_user, event_value,
        is_key_event, platform, device_category, country, session_source, session_medium
    ) VALUES %s 
    ON CONFLICT (report_date, event_name, device_category, country, session_source, session_medium) DO UPDATE SET
        event_count = EXCLUDED.event_count,
        event_count_per_user = EXCLUDED.event_count_per_user,
        event_value = EXCLUDED.event_value,
        is_key_event = EXCLUDED.is_key_event,
        platform = EXCLUDED.platform;
    """

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()

    # Используем deduplicated_data вместо data
    psycopg2.extras.execute_values(cur, insert_query, deduplicated_data)
    conn.commit()
    cur.close()
    conn.close()

def load_key_events_data():
    """Загрузка данных о ключевых событиях в PostgreSQL."""
    data = fetch_key_events_data()
    if not data:
        print("Нет данных о ключевых событиях для загрузки.")
        return

    # Удаляем дубликаты данных по первичному ключу перед вставкой
    unique_data = {}
    for row in data:
        # Создаем ключ из полей, составляющих первичный ключ
        key = (row[0], row[1], row[7], row[8])  # (date, key_event_name, device_category, country)
        # Если у нас есть несколько строк с одинаковым ключом, берем последнюю
        unique_data[key] = row
    
    # Преобразуем словарь обратно в список
    deduplicated_data = list(unique_data.values())
    print(f"Удалено {len(data) - len(deduplicated_data)} дубликатов из {len(data)} строк ключевых событий")

    insert_query = """
    INSERT INTO staging.ga4_key_events_metrics (
        report_date, key_event_name, event_count, event_count_per_user, event_value,
        user_key_event_rate, session_key_event_rate, device_category, country,
        session_source, session_medium
    ) VALUES %s 
    ON CONFLICT (report_date, key_event_name, device_category, country) DO UPDATE SET
        event_count = EXCLUDED.event_count,
        event_count_per_user = EXCLUDED.event_count_per_user,
        event_value = EXCLUDED.event_value,
        user_key_event_rate = EXCLUDED.user_key_event_rate,
        session_key_event_rate = EXCLUDED.session_key_event_rate,
        session_source = EXCLUDED.session_source,
        session_medium = EXCLUDED.session_medium;
    """

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()

    # Используем deduplicated_data вместо data
    psycopg2.extras.execute_values(cur, insert_query, deduplicated_data)
    conn.commit()
    cur.close()
    conn.close()

# Создаем DAG
with DAG(
    dag_id="GA4_EVENTS_2025",
    default_args={
        'owner': 'semukhin',
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    },
    description="Импорт данных о событиях из GA4 в PostgreSQL за весь период с 01.09.2024 по текущую дату",
    schedule='@daily',
    start_date=datetime(2025, 2, 16),
    catchup=False,
) as dag:

    create_table = PythonOperator(
        task_id="create_staging_table",
        python_callable=create_staging_table
    )

    fetch_and_load_all_events = PythonOperator(
        task_id="fetch_and_load_all_events",
        python_callable=load_all_events_data
    )

    fetch_and_load_key_events = PythonOperator(
        task_id="fetch_and_load_key_events",
        python_callable=load_key_events_data
    )

    create_table >> fetch_and_load_all_events >> fetch_and_load_key_events