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
logger = logging.getLogger('GA4_PLATFORM_DEVICE')

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

def create_tables():
    """Создание необходимых таблиц в схеме staging."""
    queries = [
        """
        CREATE SCHEMA IF NOT EXISTS staging;
        """,
        """
        CREATE TABLE IF NOT EXISTS staging.ga4_device_metrics (
            report_date DATE,
            device_category TEXT,
            browser TEXT,
            operating_system TEXT,
            users INT,
            sessions INT,
            bounce_rate FLOAT,
            engagement_rate FLOAT,
            avg_session_duration FLOAT,
            screen_page_views INT,
            screen_page_views_per_session FLOAT,
            PRIMARY KEY (report_date, device_category, browser, operating_system)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS staging.ga4_platform_metrics (
            report_date DATE,
            platform TEXT,
            app_version TEXT,
            device_category TEXT,
            users INT,
            sessions INT,
            bounce_rate FLOAT,
            engagement_rate FLOAT,
            avg_session_duration FLOAT,
            screen_page_views INT,
            screen_page_views_per_session FLOAT,
            PRIMARY KEY (report_date, platform, app_version, device_category)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS staging.ga4_screen_resolution_metrics (
            report_date DATE,
            screen_resolution TEXT,
            device_category TEXT,
            users INT,
            sessions INT,
            bounce_rate FLOAT,
            engagement_rate FLOAT,
            avg_session_duration FLOAT,
            screen_page_views INT,
            screen_page_views_per_session FLOAT,
            PRIMARY KEY (report_date, screen_resolution, device_category)
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
            raise ValueError("API GA4 вернул пустой результат")
        
        logger.info(f"Соединение с GA4 успешно установлено.")
        return True
    except Exception as e:
        logger.error(f"Ошибка при подключении к GA4: {e}")
        logger.error(traceback.format_exc())
        raise  # Перебрасываем исключение, чтобы задача завершилась с ошибкой

def fetch_device_metrics():
    """Получение метрик по устройствам из GA4."""
    logger.info(f"Получение метрик по устройствам за период {START_DATE} - {END_DATE}")
    
    # Измерения
    dimensions = [
        "deviceCategory",
        "browser",
        "operatingSystem"
    ]
    
    # Метрики
    metrics = [
        "activeUsers",
        "sessions",
        "bounceRate",
        "engagementRate",
        "averageSessionDuration",
        "screenPageViews",
        "screenPageViewsPerSession"
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
                metrics=[Metric(name=m) for m in metrics],
                limit=10000  # Увеличиваем лимит для получения большего количества комбинаций
            )
            
            response = client.run_report(request)
            
            # Обработка результатов
            for row in response.rows:
                device_category = row.dimension_values[0].value
                browser = row.dimension_values[1].value
                operating_system = row.dimension_values[2].value
                
                # Метрики
                users = int(row.metric_values[0].value or 0)
                sessions = int(row.metric_values[1].value or 0)
                bounce_rate = float(row.metric_values[2].value or 0.0)
                engagement_rate = float(row.metric_values[3].value or 0.0)
                avg_session_duration = float(row.metric_values[4].value or 0.0)
                screen_page_views = int(row.metric_values[5].value or 0)
                screen_page_views_per_session = float(row.metric_values[6].value or 0.0)
                
                results.append((
                    date_str,
                    device_category,
                    browser,
                    operating_system,
                    users,
                    sessions,
                    bounce_rate,
                    engagement_rate,
                    avg_session_duration,
                    screen_page_views,
                    screen_page_views_per_session
                ))
            
            current_date += timedelta(days=1)
            
        logger.info(f"Получено {len(results)} записей метрик по устройствам")
        return results
    except Exception as e:
        logger.error(f"Ошибка при получении метрик по устройствам: {e}")
        logger.error(traceback.format_exc())
        return []

def fetch_platform_metrics():
    """Получение метрик по платформам из GA4."""
    logger.info(f"Получение метрик по платформам за период {START_DATE} - {END_DATE}")
    
    # Измерения
    dimensions = [
        "platform",
        "appVersion",
        "deviceCategory"
    ]
    
    # Метрики
    metrics = [
        "activeUsers",
        "sessions",
        "bounceRate",
        "engagementRate",
        "averageSessionDuration",
        "screenPageViews",
        "screenPageViewsPerSession"
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
                platform = row.dimension_values[0].value
                app_version = row.dimension_values[1].value
                device_category = row.dimension_values[2].value
                
                # Метрики
                users = int(row.metric_values[0].value or 0)
                sessions = int(row.metric_values[1].value or 0)
                bounce_rate = float(row.metric_values[2].value or 0.0)
                engagement_rate = float(row.metric_values[3].value or 0.0)
                avg_session_duration = float(row.metric_values[4].value or 0.0)
                screen_page_views = int(row.metric_values[5].value or 0)
                screen_page_views_per_session = float(row.metric_values[6].value or 0.0)
                
                results.append((
                    date_str,
                    platform,
                    app_version,
                    device_category,
                    users,
                    sessions,
                    bounce_rate,
                    engagement_rate,
                    avg_session_duration,
                    screen_page_views,
                    screen_page_views_per_session
                ))
            
            current_date += timedelta(days=1)
            
        logger.info(f"Получено {len(results)} записей метрик по платформам")
        return results
    except Exception as e:
        logger.error(f"Ошибка при получении метрик по платформам: {e}")
        logger.error(traceback.format_exc())
        return []

def fetch_screen_resolution_metrics():
    """Получение метрик по разрешениям экрана из GA4."""
    logger.info(f"Получение метрик по разрешениям экрана за период {START_DATE} - {END_DATE}")
    
    # Измерения
    dimensions = [
        "screenResolution",
        "deviceCategory"
    ]
    
    # Метрики
    metrics = [
        "activeUsers",
        "sessions",
        "bounceRate",
        "engagementRate",
        "averageSessionDuration",
        "screenPageViews",
        "screenPageViewsPerSession"
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
                screen_resolution = row.dimension_values[0].value
                device_category = row.dimension_values[1].value
                
                # Метрики
                users = int(row.metric_values[0].value or 0)
                sessions = int(row.metric_values[1].value or 0)
                bounce_rate = float(row.metric_values[2].value or 0.0)
                engagement_rate = float(row.metric_values[3].value or 0.0)
                avg_session_duration = float(row.metric_values[4].value or 0.0)
                screen_page_views = int(row.metric_values[5].value or 0)
                screen_page_views_per_session = float(row.metric_values[6].value or 0.0)
                
                results.append((
                    date_str,
                    screen_resolution,
                    device_category,
                    users,
                    sessions,
                    bounce_rate,
                    engagement_rate,
                    avg_session_duration,
                    screen_page_views,
                    screen_page_views_per_session
                ))
            
            current_date += timedelta(days=1)
            
        logger.info(f"Получено {len(results)} записей метрик по разрешениям экрана")
        return results
    except Exception as e:
        logger.error(f"Ошибка при получении метрик по разрешениям экрана: {e}")
        logger.error(traceback.format_exc())
        return []

def load_device_metrics_to_db():
    """Загрузка метрик по устройствам в базу данных."""
    metrics = fetch_device_metrics()
    if not metrics:
        logger.warning("Нет данных для загрузки в таблицу device_metrics")
        return
    
    # Загрузка данных в БД
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        
        # Вставка данных с обработкой дубликатов
        query = """
        INSERT INTO staging.ga4_device_metrics (
            report_date, 
            device_category, 
            browser, 
            operating_system, 
            users, 
            sessions, 
            bounce_rate,
            engagement_rate,
            avg_session_duration,
            screen_page_views,
            screen_page_views_per_session
        ) VALUES %s
        ON CONFLICT (report_date, device_category, browser, operating_system) DO UPDATE SET
            users = EXCLUDED.users,
            sessions = EXCLUDED.sessions,
            bounce_rate = EXCLUDED.bounce_rate,
            engagement_rate = EXCLUDED.engagement_rate,
            avg_session_duration = EXCLUDED.avg_session_duration,
            screen_page_views = EXCLUDED.screen_page_views,
            screen_page_views_per_session = EXCLUDED.screen_page_views_per_session;
        """
        
        # Разбиваем на пакеты по 1000 записей
        batch_size = 1000
        for i in range(0, len(metrics), batch_size):
            batch = metrics[i:i+batch_size]
            psycopg2.extras.execute_values(cursor, query, batch)
            conn.commit()
            logger.info(f"Загружена партия {i//batch_size + 1} из {(len(metrics)-1)//batch_size + 1}, размер: {len(batch)}")
            
        logger.info(f"Загружено {len(metrics)} записей метрик по устройствам")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Ошибка при загрузке метрик по устройствам: {e}")
        logger.error(traceback.format_exc())
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def load_platform_metrics_to_db():
    """Загрузка метрик по платформам в базу данных."""
    metrics = fetch_platform_metrics()
    if not metrics:
        logger.warning("Нет данных для загрузки в таблицу platform_metrics")
        return
    
    # Загрузка данных в БД
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        
        # Вставка данных с обработкой дубликатов
        query = """
        INSERT INTO staging.ga4_platform_metrics (
            report_date, 
            platform, 
            app_version, 
            device_category, 
            users, 
            sessions, 
            bounce_rate,
            engagement_rate,
            avg_session_duration,
            screen_page_views,
            screen_page_views_per_session
        ) VALUES %s
        ON CONFLICT (report_date, platform, app_version, device_category) DO UPDATE SET
            users = EXCLUDED.users,
            sessions = EXCLUDED.sessions,
            bounce_rate = EXCLUDED.bounce_rate,
            engagement_rate = EXCLUDED.engagement_rate,
            avg_session_duration = EXCLUDED.avg_session_duration,
            screen_page_views = EXCLUDED.screen_page_views,
            screen_page_views_per_session = EXCLUDED.screen_page_views_per_session;
        """
        
        psycopg2.extras.execute_values(cursor, query, metrics)
        conn.commit()
        logger.info(f"Загружено {len(metrics)} записей метрик по платформам")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Ошибка при загрузке метрик по платформам: {e}")
        logger.error(traceback.format_exc())
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def load_screen_resolution_metrics_to_db():
    """Загрузка метрик по разрешениям экрана в базу данных."""
    metrics = fetch_screen_resolution_metrics()
    if not metrics:
        logger.warning("Нет данных для загрузки в таблицу screen_resolution_metrics")
        return
    
    # Загрузка данных в БД
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        
        # Вставка данных с обработкой дубликатов
        query = """
        INSERT INTO staging.ga4_screen_resolution_metrics (
            report_date, 
            screen_resolution, 
            device_category, 
            users, 
            sessions, 
            bounce_rate,
            engagement_rate,
            avg_session_duration,
            screen_page_views,
            screen_page_views_per_session
        ) VALUES %s
        ON CONFLICT (report_date, screen_resolution, device_category) DO UPDATE SET
            users = EXCLUDED.users,
            sessions = EXCLUDED.sessions,
            bounce_rate = EXCLUDED.bounce_rate,
            engagement_rate = EXCLUDED.engagement_rate,
            avg_session_duration = EXCLUDED.avg_session_duration,
            screen_page_views = EXCLUDED.screen_page_views,
            screen_page_views_per_session = EXCLUDED.screen_page_views_per_session;
        """
        
        psycopg2.extras.execute_values(cursor, query, metrics)
        conn.commit()
        logger.info(f"Загружено {len(metrics)} записей метрик по разрешениям экрана")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Ошибка при загрузке метрик по разрешениям экрана: {e}")
        logger.error(traceback.format_exc())
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Определение DAG
default_args = {
    'owner': 'semukhin',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'GA4_PLATFORM_DEVICE',
    default_args=default_args,
    description='Импорт метрик платформ и устройств из GA4',
    schedule_interval='0 4 * * *',  # Каждый день в 04:00
    start_date=datetime(2025, 3, 5),
    catchup=False,
    tags=['ga4', 'platform', 'device'],
) as dag:
    
    # Задачи
    test_connection = PythonOperator(
        task_id='test_ga4_connection',
        python_callable=test_ga4_connection,
        trigger_rule='all_success',
    )
    
    create_db_tables = PythonOperator(
        task_id='create_db_tables',
        python_callable=create_tables,
        trigger_rule='all_success',
    )
    
    load_device_metrics = PythonOperator(
        task_id='load_device_metrics',
        python_callable=load_device_metrics_to_db,
        trigger_rule='all_success',
    )
    
    load_platform_metrics = PythonOperator(
        task_id='load_platform_metrics',
        python_callable=load_platform_metrics_to_db,
        trigger_rule='all_success',
    )
    
    load_screen_resolution_metrics = PythonOperator(
        task_id='load_screen_resolution_metrics',
        python_callable=load_screen_resolution_metrics_to_db,
        trigger_rule='all_success',
    )
    
    # Определение порядка выполнения задач
    test_connection >> create_db_tables
    create_db_tables >> [load_device_metrics, load_platform_metrics, load_screen_resolution_metrics]