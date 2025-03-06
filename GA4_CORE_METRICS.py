from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import psycopg2
import psycopg2.extras
import logging
import traceback
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Metric, Dimension

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('GA4_CORE_METRICS')

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

# Период данных
START_DATE = "2024-09-01"
END_DATE = datetime.today().strftime("%Y-%m-%d")

# Путь к файлу с исключениями пользователей
USER_EXCLUSIONS_PATH = "/home/GA4/user_exclusions.csv"

def load_user_exclusions():
    """Загрузить список email-адресов пользователей, которых нужно исключить из отчетов."""
    exclusions = set()
    try:
        with open(USER_EXCLUSIONS_PATH, 'r') as f:
            # Пропустить заголовок, если он есть
            if f.readline().strip().startswith('"user_id"'):
                pass
            # Сбросить указатель на начало файла
            f.seek(0)
            for line in f:
                email = line.strip().strip('"')
                if '@' in email:  # Простая проверка, что это похоже на email
                    exclusions.add(email)
        logger.info(f"Загружено {len(exclusions)} email-адресов для исключения")
    except Exception as e:
        logger.error(f"Ошибка при загрузке исключений пользователей: {e}")
    return exclusions

def create_tables():
    """Создание необходимых таблиц в схеме analytics."""
    queries = [
        """
        CREATE SCHEMA IF NOT EXISTS analytics;
        """,
        """
        CREATE TABLE IF NOT EXISTS analytics.ga4_user_metrics (
            report_date DATE,
            user_type TEXT,  -- new, returning, (not set)
            active_users INT,
            new_users INT,
            total_users INT,
            user_engagement_duration INT,
            user_key_event_rate FLOAT,
            active_1day_users INT,
            active_7day_users INT,
            active_28day_users INT,
            dau_per_wau FLOAT,
            dau_per_mau FLOAT,
            wau_per_mau FLOAT,
            PRIMARY KEY (report_date, user_type)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS analytics.ga4_session_metrics (
            report_date DATE,
            user_type TEXT,  -- new, returning, (not set)
            sessions INT,
            sessions_per_user FLOAT,
            bounce_rate FLOAT,
            engaged_sessions INT,
            engagement_rate FLOAT,
            avg_session_duration FLOAT,
            session_key_event_rate FLOAT,
            PRIMARY KEY (report_date, user_type)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS analytics.ga4_pageview_metrics (
            report_date DATE,
            user_type TEXT,  -- new, returning, (not set)
            screen_page_views INT,
            screen_page_views_per_session FLOAT,
            screen_page_views_per_user FLOAT,
            PRIMARY KEY (report_date, user_type)
        );
        """
    ]

    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        for query in queries:
            cursor.execute(query)
        conn.commit()
        logger.info("Таблицы успешно созданы или уже существуют")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Ошибка при создании таблиц: {e}")
        logger.error(traceback.format_exc())
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def test_ga4_connection():
    """Проверка соединения с API GA4."""
    try:
        client = BetaAnalyticsDataClient()
        request = RunReportRequest(
            property=PROPERTY_ID,
            date_ranges=[DateRange(start_date=START_DATE, end_date=END_DATE)],
            metrics=[Metric(name="activeUsers")]
        )
        response = client.run_report(request)
        
        if not response.rows:
            logger.error("API GA4 вернул пустой результат. Нет данных для указанного периода.")
            return False
        
        logger.info(f"Соединение с GA4 успешно установлено. Найдено {response.rows[0].metric_values[0].value} активных пользователей.")
        return True
    except Exception as e:
        logger.error(f"Ошибка при подключении к GA4: {e}")
        logger.error(traceback.format_exc())
        return False

def fetch_user_metrics():
    """Получение метрик пользователей из GA4."""
    logger.info(f"Получение метрик пользователей за период {START_DATE} - {END_DATE}")
    
    # Измерения
    dimensions = [
        "newVsReturning"  # Новые vs вернувшиеся пользователи
    ]
    
    # Метрики пользователей
    metrics = [
        "activeUsers",
        "newUsers",
        "totalUsers",
        "userEngagementDuration",
        "userKeyEventRate",
        "active1DayUsers",
        "active7DayUsers",
        "active28DayUsers",
        "dauPerWau",
        "dauPerMau",
        "wauPerMau"
    ]
    
    try:
        client = BetaAnalyticsDataClient()
        
        # Запрос данных по дням
        results = []
        current_date = datetime.strptime(START_DATE, "%Y-%m-%d")
        end_date = datetime.strptime(END_DATE, "%Y-%m-%d")
        
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            
            request = RunReportRequest(
                property=PROPERTY_ID,
                date_ranges=[DateRange(start_date=date_str, end_date=date_str)],
                dimensions=[Dimension(name=d) for d in dimensions],
                metrics=[Metric(name=m) for m in metrics]
            )
            
            response = client.run_report(request)
            
            # Обработка результатов
            for row in response.rows:
                user_type = row.dimension_values[0].value
                
                # Метрики
                active_users = int(row.metric_values[0].value or 0)
                new_users = int(row.metric_values[1].value or 0)
                total_users = int(row.metric_values[2].value or 0)
                user_engagement_duration = int(row.metric_values[3].value or 0)
                user_key_event_rate = float(row.metric_values[4].value or 0.0)
                active_1day_users = int(row.metric_values[5].value or 0)
                active_7day_users = int(row.metric_values[6].value or 0)
                active_28day_users = int(row.metric_values[7].value or 0)
                dau_per_wau = float(row.metric_values[8].value or 0.0)
                dau_per_mau = float(row.metric_values[9].value or 0.0)
                wau_per_mau = float(row.metric_values[10].value or 0.0)
                
                results.append((
                    date_str,
                    user_type,
                    active_users,
                    new_users,
                    total_users,
                    user_engagement_duration,
                    user_key_event_rate,
                    active_1day_users,
                    active_7day_users,
                    active_28day_users,
                    dau_per_wau,
                    dau_per_mau,
                    wau_per_mau
                ))
            
            current_date += timedelta(days=1)
            
        logger.info(f"Получено {len(results)} записей метрик пользователей")
        return results
    except Exception as e:
        logger.error(f"Ошибка при получении метрик пользователей: {e}")
        logger.error(traceback.format_exc())
        return []

def fetch_session_metrics():
    """Получение метрик сессий из GA4."""
    logger.info(f"Получение метрик сессий за период {START_DATE} - {END_DATE}")
    
    # Измерения
    dimensions = [
        "newVsReturning"  # Новые vs вернувшиеся пользователи
    ]
    
    # Метрики сессий
    metrics = [
        "sessions",
        "sessionsPerUser",
        "bounceRate",
        "engagedSessions",
        "engagementRate",
        "averageSessionDuration",
        "sessionKeyEventRate"
    ]
    
    try:
        client = BetaAnalyticsDataClient()
        
        # Запрос данных по дням
        results = []
        current_date = datetime.strptime(START_DATE, "%Y-%m-%d")
        end_date = datetime.strptime(END_DATE, "%Y-%m-%d")
        
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            
            request = RunReportRequest(
                property=PROPERTY_ID,
                date_ranges=[DateRange(start_date=date_str, end_date=date_str)],
                dimensions=[Dimension(name=d) for d in dimensions],
                metrics=[Metric(name=m) for m in metrics]
            )
            
            response = client.run_report(request)
            
            # Обработка результатов
            for row in response.rows:
                user_type = row.dimension_values[0].value
                
                # Метрики
                sessions = int(row.metric_values[0].value or 0)
                sessions_per_user = float(row.metric_values[1].value or 0.0)
                bounce_rate = float(row.metric_values[2].value or 0.0)
                engaged_sessions = int(row.metric_values[3].value or 0)
                engagement_rate = float(row.metric_values[4].value or 0.0)
                avg_session_duration = float(row.metric_values[5].value or 0.0)
                session_key_event_rate = float(row.metric_values[6].value or 0.0)
                
                results.append((
                    date_str,
                    user_type,
                    sessions,
                    sessions_per_user,
                    bounce_rate,
                    engaged_sessions,
                    engagement_rate,
                    avg_session_duration,
                    session_key_event_rate
                ))
            
            current_date += timedelta(days=1)
            
        logger.info(f"Получено {len(results)} записей метрик сессий")
        return results
    except Exception as e:
        logger.error(f"Ошибка при получении метрик сессий: {e}")
        logger.error(traceback.format_exc())
        return []

def fetch_pageview_metrics():
    """Получение метрик просмотров страниц из GA4."""
    logger.info(f"Получение метрик просмотров страниц за период {START_DATE} - {END_DATE}")
    
    # Измерения
    dimensions = [
        "newVsReturning"  # Новые vs вернувшиеся пользователи
    ]
    
    # Метрики просмотров страниц
    metrics = [
        "screenPageViews",
        "screenPageViewsPerSession",
        "screenPageViewsPerUser"
    ]
    
    try:
        client = BetaAnalyticsDataClient()
        
        # Запрос данных по дням
        results = []
        current_date = datetime.strptime(START_DATE, "%Y-%m-%d")
        end_date = datetime.strptime(END_DATE, "%Y-%m-%d")
        
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            
            request = RunReportRequest(
                property=PROPERTY_ID,
                date_ranges=[DateRange(start_date=date_str, end_date=date_str)],
                dimensions=[Dimension(name=d) for d in dimensions],
                metrics=[Metric(name=m) for m in metrics]
            )
            
            response = client.run_report(request)
            
            # Обработка результатов
            for row in response.rows:
                user_type = row.dimension_values[0].value
                
                # Метрики
                screen_page_views = int(row.metric_values[0].value or 0)
                screen_page_views_per_session = float(row.metric_values[1].value or 0.0)
                screen_page_views_per_user = float(row.metric_values[2].value or 0.0)
                
                results.append((
                    date_str,
                    user_type,
                    screen_page_views,
                    screen_page_views_per_session,
                    screen_page_views_per_user
                ))
            
            current_date += timedelta(days=1)
            
        logger.info(f"Получено {len(results)} записей метрик просмотров страниц")
        return results
    except Exception as e:
        logger.error(f"Ошибка при получении метрик просмотров страниц: {e}")
        logger.error(traceback.format_exc())
        return []

def load_user_metrics_to_db():
    """Загрузка метрик пользователей в базу данных."""
    metrics = fetch_user_metrics()
    if not metrics:
        logger.warning("Нет данных для загрузки в таблицу user_metrics")
        return
    
    # Загрузка данных в БД
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        
        # Вставка данных с обработкой дубликатов
        query = """
        INSERT INTO analytics.ga4_user_metrics (
            report_date, 
            user_type, 
            active_users, 
            new_users, 
            total_users, 
            user_engagement_duration, 
            user_key_event_rate, 
            active_1day_users, 
            active_7day_users, 
            active_28day_users, 
            dau_per_wau, 
            dau_per_mau, 
            wau_per_mau
        ) VALUES %s
        ON CONFLICT (report_date, user_type) DO UPDATE SET
            active_users = EXCLUDED.active_users,
            new_users = EXCLUDED.new_users,
            total_users = EXCLUDED.total_users,
            user_engagement_duration = EXCLUDED.user_engagement_duration,
            user_key_event_rate = EXCLUDED.user_key_event_rate,
            active_1day_users = EXCLUDED.active_1day_users,
            active_7day_users = EXCLUDED.active_7day_users,
            active_28day_users = EXCLUDED.active_28day_users,
            dau_per_wau = EXCLUDED.dau_per_wau,
            dau_per_mau = EXCLUDED.dau_per_mau,
            wau_per_mau = EXCLUDED.wau_per_mau;
        """
        
        psycopg2.extras.execute_values(cursor, query, metrics)
        conn.commit()
        logger.info(f"Загружено {len(metrics)} записей метрик пользователей")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Ошибка при загрузке метрик пользователей: {e}")
        logger.error(traceback.format_exc())
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def load_session_metrics_to_db():
    """Загрузка метрик сессий в базу данных."""
    metrics = fetch_session_metrics()
    if not metrics:
        logger.warning("Нет данных для загрузки в таблицу session_metrics")
        return
    
    # Загрузка данных в БД
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        
        # Вставка данных с обработкой дубликатов
        query = """
        INSERT INTO analytics.ga4_session_metrics (
            report_date, 
            user_type, 
            sessions, 
            sessions_per_user, 
            bounce_rate, 
            engaged_sessions, 
            engagement_rate, 
            avg_session_duration, 
            session_key_event_rate
        ) VALUES %s
        ON CONFLICT (report_date, user_type) DO UPDATE SET
            sessions = EXCLUDED.sessions,
            sessions_per_user = EXCLUDED.sessions_per_user,
            bounce_rate = EXCLUDED.bounce_rate,
            engaged_sessions = EXCLUDED.engaged_sessions,
            engagement_rate = EXCLUDED.engagement_rate,
            avg_session_duration = EXCLUDED.avg_session_duration,
            session_key_event_rate = EXCLUDED.session_key_event_rate;
        """
        
        psycopg2.extras.execute_values(cursor, query, metrics)
        conn.commit()
        logger.info(f"Загружено {len(metrics)} записей метрик сессий")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Ошибка при загрузке метрик сессий: {e}")
        logger.error(traceback.format_exc())
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def load_pageview_metrics_to_db():
    """Загрузка метрик просмотров страниц в базу данных."""
    metrics = fetch_pageview_metrics()
    if not metrics:
        logger.warning("Нет данных для загрузки в таблицу pageview_metrics")
        return
    
    # Загрузка данных в БД
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        
        # Вставка данных с обработкой дубликатов
        query = """
        INSERT INTO analytics.ga4_pageview_metrics (
            report_date, 
            user_type, 
            screen_page_views, 
            screen_page_views_per_session, 
            screen_page_views_per_user
        ) VALUES %s
        ON CONFLICT (report_date, user_type) DO UPDATE SET
            screen_page_views = EXCLUDED.screen_page_views,
            screen_page_views_per_session = EXCLUDED.screen_page_views_per_session,
            screen_page_views_per_user = EXCLUDED.screen_page_views_per_user;
        """
        
        psycopg2.extras.execute_values(cursor, query, metrics)
        conn.commit()
        logger.info(f"Загружено {len(metrics)} записей метрик просмотров страниц")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Ошибка при загрузке метрик просмотров страниц: {e}")
        logger.error(traceback.format_exc())
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Определение DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'GA4_CORE_METRICS',
    default_args=default_args,
    description='Импорт основных метрик из GA4 (пользователи, сессии, просмотры страниц)',
    schedule_interval='0 1 * * *',  # Каждый день в 01:00
    start_date=datetime(2025, 3, 5),
    catchup=False,
    tags=['ga4', 'metrics'],
) as dag:
    
    # Задачи
    test_connection = PythonOperator(
        task_id='test_ga4_connection',
        python_callable=test_ga4_connection,
    )
    
    create_db_tables = PythonOperator(
        task_id='create_db_tables',
        python_callable=create_tables,
    )
    
    load_user_metrics = PythonOperator(
        task_id='load_user_metrics',
        python_callable=load_user_metrics_to_db,
    )
    
    load_session_metrics = PythonOperator(
        task_id='load_session_metrics',
        python_callable=load_session_metrics_to_db,
    )
    
    load_pageview_metrics = PythonOperator(
        task_id='load_pageview_metrics',
        python_callable=load_pageview_metrics_to_db,
    )
    
    # Определение порядка выполнения задач
    test_connection >> create_db_tables
    create_db_tables >> [load_user_metrics, load_session_metrics, load_pageview_metrics]