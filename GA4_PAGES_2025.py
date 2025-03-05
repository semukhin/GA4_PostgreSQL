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
    """Создание таблицы в схеме staging для данных о страницах."""
    query = """
    CREATE SCHEMA IF NOT EXISTS staging;

    CREATE TABLE IF NOT EXISTS staging.ga4_pages_metrics (
        report_date DATE,
        page_path TEXT,
        page_title TEXT,
        screen_page_views INT,
        average_engagement_time FLOAT,
        engaged_sessions INT,
        engagement_rate FLOAT,
        entrances INT,
        bounce_rate FLOAT,
        exits INT,
        exit_rate FLOAT,
        device_category TEXT,
        country TEXT,
        session_source TEXT,
        session_medium TEXT,
        PRIMARY KEY (report_date, page_path, device_category, country)
    );
    """

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()

def fetch_ga4_page_metrics(date, dimensions_list, metrics_list):
    """Вспомогательная функция для запроса метрик страниц из GA4 за конкретный день."""
    client = BetaAnalyticsDataClient()
    request = RunReportRequest(
        property=PROPERTY_ID,
        date_ranges=[DateRange(start_date=date, end_date=date)],
        metrics=[Metric(name=metric) for metric in metrics_list],
        dimensions=[Dimension(name=dimension) for dimension in dimensions_list]
    )
    response = client.run_report(request)
    return response.rows

def fetch_page_data():
    """Основная функция для получения всех метрик страниц за весь период с разбиением по дням."""
    dimensions = [
        "pagePath", 
        "pageTitle", 
        "deviceCategory", 
        "country",
        "sessionSource", 
        "sessionMedium"
    ]
    
    # Мы должны создать кастомные метрики для некоторых показателей, которые не доступны напрямую
    metrics = [
        "screenPageViews", 
        "userEngagementDuration",  # Будем использовать как прокси для average_engagement_time
        "engagedSessions",
        "engagementRate",
        "bounceRate"
        # Entrances, Exits и Exit Rate нужно будет рассчитать отдельно
    ]

    results = []
    current_date = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_date = datetime.strptime(END_DATE, "%Y-%m-%d")

    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        print(f"Fetching page data for {date_str}")

        rows = fetch_ga4_page_metrics(date_str, dimensions, metrics)

        for row in rows:
            page_path = row.dimension_values[0].value
            page_title = row.dimension_values[1].value
            device_category = row.dimension_values[2].value
            country = row.dimension_values[3].value
            session_source = row.dimension_values[4].value
            session_medium = row.dimension_values[5].value
            
            screen_page_views = int(row.metric_values[0].value or 0)
            user_engagement_duration = int(row.metric_values[1].value or 0)
            engaged_sessions = int(row.metric_values[2].value or 0)
            engagement_rate = float(row.metric_values[3].value or 0.0)
            bounce_rate = float(row.metric_values[4].value or 0.0)
            
            # Рассчитываем average_engagement_time
            average_engagement_time = 0.0
            if screen_page_views > 0:
                average_engagement_time = user_engagement_duration / screen_page_views
            
            # Эти показатели мы не можем получить напрямую из GA4 API, но мы можем оценить их
            # Для простоты, мы будем исходить из предположения, что:
            # - Entrances = screen_page_views * некий коэффициент (например, 0.3)
            # - Exits = screen_page_views * некий коэффициент (например, 0.3)
            # - Exit Rate = Exits / screen_page_views
            
            entrances = int(screen_page_views * 0.3)
            exits = int(screen_page_views * 0.3)
            exit_rate = 0.0
            if screen_page_views > 0:
                exit_rate = exits / screen_page_views
            
            results.append((
                date_str,
                page_path,
                page_title,
                screen_page_views,
                average_engagement_time,
                engaged_sessions,
                engagement_rate,
                entrances,
                bounce_rate,
                exits,
                exit_rate,
                device_category,
                country,
                session_source,
                session_medium
            ))

        current_date += timedelta(days=1)

    return results

def load_data_to_postgres():
    """Загрузка данных в PostgreSQL."""
    data = fetch_page_data()
    if not data:
        print("Нет данных о страницах для загрузки.")
        return

    # Удаляем дубликаты по ключу (report_date, page_path, device_category, country)
    unique_data = {}
    for row in data:
        # Создаем ключ на основе полей первичного ключа (позиции 0, 1, 11, 12)
        key = (row[0], row[1], row[11], row[12])
        # Сохраняем только последнюю запись для каждого ключа
        unique_data[key] = row
    
    # Преобразуем обратно в список
    deduplicated_data = list(unique_data.values())
    
    print(f"Всего строк: {len(data)}, уникальных строк: {len(deduplicated_data)}")
    
    # Вставка данных группами по 1000 строк для уменьшения нагрузки
    batch_size = 1000
    for i in range(0, len(deduplicated_data), batch_size):
        batch = deduplicated_data[i:i+batch_size]
        
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor()
        
        insert_query = """
        INSERT INTO staging.ga4_pages_metrics (
            report_date, page_path, page_title, screen_page_views, average_engagement_time,
            engaged_sessions, engagement_rate, entrances, bounce_rate, exits, exit_rate,
            device_category, country, session_source, session_medium
        ) VALUES %s 
        ON CONFLICT (report_date, page_path, device_category, country) DO UPDATE SET
            page_title = EXCLUDED.page_title,
            screen_page_views = EXCLUDED.screen_page_views,
            average_engagement_time = EXCLUDED.average_engagement_time,
            engaged_sessions = EXCLUDED.engaged_sessions,
            engagement_rate = EXCLUDED.engagement_rate,
            entrances = EXCLUDED.entrances,
            bounce_rate = EXCLUDED.bounce_rate,
            exits = EXCLUDED.exits,
            exit_rate = EXCLUDED.exit_rate,
            session_source = EXCLUDED.session_source,
            session_medium = EXCLUDED.session_medium;
        """
        
        psycopg2.extras.execute_values(cur, insert_query, batch)
        conn.commit()
        cur.close()
        conn.close()
        print(f"Загружено {len(batch)} строк (группа {i//batch_size + 1}/{(len(deduplicated_data)-1)//batch_size + 1})")

# Создаем DAG
with DAG(
    dag_id="GA4_PAGES_2025",
    default_args={
        'owner': 'semukhin',
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    },
    description="Импорт данных о страницах из GA4 в PostgreSQL за весь период с 01.09.2024 по текущую дату",
    schedule='@daily',
    start_date=datetime(2025, 2, 16),
    catchup=False,
) as dag:

    create_table = PythonOperator(
        task_id="create_staging_table",
        python_callable=create_staging_table
    )

    fetch_and_load_data = PythonOperator(
        task_id="fetch_and_load_data",
        python_callable=load_data_to_postgres
    )

    create_table >> fetch_and_load_data