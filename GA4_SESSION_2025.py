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
    """Создание таблицы в схеме staging для сессионных метрик."""
    query = """
    CREATE SCHEMA IF NOT EXISTS staging;

    CREATE TABLE IF NOT EXISTS staging.ga4_session_metrics (
        report_date DATE,
        sessions INT,
        sessions_per_user FLOAT,
        bounce_rate FLOAT,
        engaged_sessions INT,
        engagement_rate FLOAT,
        avg_session_duration FLOAT,
        session_key_event_rate FLOAT,
        events_per_session FLOAT,
        screen_page_views INT,
        screen_page_views_per_session FLOAT,
        scrolled_users INT,
        session_source TEXT,
        session_medium TEXT,
        session_campaign_name TEXT,
        landing_page TEXT,
        device_category TEXT,
        country TEXT,
        browser TEXT,
        PRIMARY KEY (report_date, device_category, country, session_source, session_medium)
    );
    """

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()

def fetch_ga4_session_metrics(date, dimensions_list, metrics_list):
    """Вспомогательная функция для запроса сессионных метрик из GA4 за конкретный день."""
    client = BetaAnalyticsDataClient()
    request = RunReportRequest(
        property=PROPERTY_ID,
        date_ranges=[DateRange(start_date=date, end_date=date)],
        metrics=[Metric(name=metric) for metric in metrics_list],
        dimensions=[Dimension(name=dimension) for dimension in dimensions_list]
    )
    response = client.run_report(request)
    return response.rows

def fetch_session_data():
    """Основная функция для получения всех сессионных метрик за весь период с разбиением по дням."""
    dimensions = [
        "deviceCategory", 
        "country", 
        "browser",
        "sessionSource", 
        "sessionMedium", 
        "sessionCampaignName",
        "landingPage"
    ]
    
    # Разделяем метрики на две группы, чтобы не превышать лимит в 10 метрик на запрос
    metrics_group1 = [
        "sessions", 
        "sessionsPerUser", 
        "bounceRate", 
        "engagedSessions", 
        "engagementRate",
        "averageSessionDuration", 
        "sessionKeyEventRate", 
        "eventsPerSession",
        "screenPageViews", 
        "screenPageViewsPerSession"
    ]
    
    metrics_group2 = [
        "scrolledUsers"
    ]

    results = []
    data_dict = {}  # Словарь для дедупликации данных
    current_date = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_date = datetime.strptime(END_DATE, "%Y-%m-%d")

    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        print(f"Fetching session data for {date_str}")

        # Запрос первой группы метрик
        rows_group1 = fetch_ga4_session_metrics(date_str, dimensions, metrics_group1)
        
        # Обработка первой группы метрик и сохранение в словарь
        for row in rows_group1:
            device_category = row.dimension_values[0].value
            country = row.dimension_values[1].value
            browser = row.dimension_values[2].value
            session_source = row.dimension_values[3].value
            session_medium = row.dimension_values[4].value
            session_campaign_name = row.dimension_values[5].value
            landing_page = row.dimension_values[6].value
            
            # Создаем ключ на основе полей первичного ключа
            key = (date_str, device_category, country, session_source, session_medium)
            
            # Сохраняем данные в словарь
            data_dict[key] = {
                'report_date': date_str,
                'sessions': int(row.metric_values[0].value or 0),
                'sessions_per_user': float(row.metric_values[1].value or 0.0),
                'bounce_rate': float(row.metric_values[2].value or 0.0),
                'engaged_sessions': int(row.metric_values[3].value or 0),
                'engagement_rate': float(row.metric_values[4].value or 0.0),
                'avg_session_duration': float(row.metric_values[5].value or 0.0),
                'session_key_event_rate': float(row.metric_values[6].value or 0.0),
                'events_per_session': float(row.metric_values[7].value or 0.0),
                'screen_page_views': int(row.metric_values[8].value or 0),
                'screen_page_views_per_session': float(row.metric_values[9].value or 0.0),
                'scrolled_users': 0,  # Инициализируем нулём, обновим из второго запроса
                'session_source': session_source,
                'session_medium': session_medium,
                'session_campaign_name': session_campaign_name,
                'landing_page': landing_page,
                'device_category': device_category,
                'country': country,
                'browser': browser
            }
        
        # Запрос второй группы метрик
        rows_group2 = fetch_ga4_session_metrics(date_str, dimensions, metrics_group2)
        
        # Обновляем значения для scrolledUsers из второго запроса
        for row in rows_group2:
            device_category = row.dimension_values[0].value
            country = row.dimension_values[1].value
            browser = row.dimension_values[2].value
            session_source = row.dimension_values[3].value
            session_medium = row.dimension_values[4].value
            session_campaign_name = row.dimension_values[5].value
            landing_page = row.dimension_values[6].value
            
            # Создаем ключ для поиска в словаре
            key = (date_str, device_category, country, session_source, session_medium)
            
            # Если ключ уже существует, обновляем значение scrolledUsers
            if key in data_dict:
                data_dict[key]['scrolled_users'] = int(row.metric_values[0].value or 0)
            # Если ключа нет (что маловероятно, но всё же), создаем новую запись
            else:
                data_dict[key] = {
                    'report_date': date_str,
                    'sessions': 0,
                    'sessions_per_user': 0.0,
                    'bounce_rate': 0.0,
                    'engaged_sessions': 0,
                    'engagement_rate': 0.0,
                    'avg_session_duration': 0.0,
                    'session_key_event_rate': 0.0,
                    'events_per_session': 0.0,
                    'screen_page_views': 0,
                    'screen_page_views_per_session': 0.0,
                    'scrolled_users': int(row.metric_values[0].value or 0),
                    'session_source': session_source,
                    'session_medium': session_medium,
                    'session_campaign_name': session_campaign_name,
                    'landing_page': landing_page,
                    'device_category': device_category,
                    'country': country,
                    'browser': browser
                }

        current_date += timedelta(days=1)
    
    # Преобразуем словарь в список кортежей для вставки в базу данных
    for data in data_dict.values():
        results.append((
            data['report_date'],
            data['sessions'],
            data['sessions_per_user'],
            data['bounce_rate'],
            data['engaged_sessions'],
            data['engagement_rate'],
            data['avg_session_duration'],
            data['session_key_event_rate'],
            data['events_per_session'],
            data['screen_page_views'],
            data['screen_page_views_per_session'],
            data['scrolled_users'],
            data['session_source'],
            data['session_medium'],
            data['session_campaign_name'],
            data['landing_page'],
            data['device_category'],
            data['country'],
            data['browser']
        ))

    return results

def load_data_to_postgres():
    """Загрузка данных в PostgreSQL."""
    data = fetch_session_data()
    if not data:
        print("Нет данных для загрузки.")
        return

    insert_query = """
    INSERT INTO staging.ga4_session_metrics (
        report_date, sessions, sessions_per_user, bounce_rate, engaged_sessions, engagement_rate,
        avg_session_duration, session_key_event_rate, events_per_session, screen_page_views,
        screen_page_views_per_session, scrolled_users, session_source, session_medium,
        session_campaign_name, landing_page, device_category, country, browser
    ) VALUES %s 
    ON CONFLICT (report_date, device_category, country, session_source, session_medium) DO UPDATE SET
        sessions = EXCLUDED.sessions,
        sessions_per_user = EXCLUDED.sessions_per_user,
        bounce_rate = EXCLUDED.bounce_rate,
        engaged_sessions = EXCLUDED.engaged_sessions,
        engagement_rate = EXCLUDED.engagement_rate,
        avg_session_duration = EXCLUDED.avg_session_duration,
        session_key_event_rate = EXCLUDED.session_key_event_rate,
        events_per_session = EXCLUDED.events_per_session,
        screen_page_views = EXCLUDED.screen_page_views,
        screen_page_views_per_session = EXCLUDED.screen_page_views_per_session,
        scrolled_users = EXCLUDED.scrolled_users,
        session_campaign_name = EXCLUDED.session_campaign_name,
        landing_page = EXCLUDED.landing_page,
        browser = EXCLUDED.browser;
    """

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()

    psycopg2.extras.execute_values(cur, insert_query, data)
    conn.commit()
    cur.close()
    conn.close()

# Создаем DAG
with DAG(
    dag_id="GA4_SESSION_2025",
    default_args={
        'owner': 'semukhin',
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    },
    description="Импорт сессионных данных из GA4 в PostgreSQL за весь период с 01.09.2024 по текущую дату",
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