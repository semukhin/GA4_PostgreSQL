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
logger = logging.getLogger('GA4_ACQUISITION')

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
    """Создание необходимых таблиц в схеме analytics."""
    queries = [
        """
        CREATE SCHEMA IF NOT EXISTS analytics;
        """,
        """
        CREATE TABLE IF NOT EXISTS analytics.ga4_channel_metrics (
            report_date DATE,
            default_channel_group TEXT,
            user_type TEXT,  -- new, returning, (not set)
            sessions INT,
            users INT,
            new_users INT,
            session_conversion_rate FLOAT,
            engaged_sessions INT,
            engagement_rate FLOAT,
            session_duration FLOAT,
            bounce_rate FLOAT,
            PRIMARY KEY (report_date, default_channel_group, user_type)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS analytics.ga4_source_medium_metrics (
            report_date DATE,
            source TEXT,
            medium TEXT,
            user_type TEXT,  -- new, returning, (not set)
            sessions INT,
            users INT,
            new_users INT,
            session_conversion_rate FLOAT,
            engaged_sessions INT,
            engagement_rate FLOAT,
            session_duration FLOAT,
            bounce_rate FLOAT,
            PRIMARY KEY (report_date, source, medium, user_type)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS analytics.ga4_campaign_metrics (
            report_date DATE,
            campaign_name TEXT,
            source TEXT,
            medium TEXT,
            sessions INT,
            users INT,
            new_users INT,
            session_conversion_rate FLOAT,
            engaged_sessions INT,
            engagement_rate FLOAT,
            session_duration FLOAT,
            bounce_rate FLOAT,
            PRIMARY KEY (report_date, campaign_name, source, medium)
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
            metrics=[Metric(name="sessions")],
            dimensions=[Dimension(name="sessionDefaultChannelGroup")]
        )
        response = client.run_report(request)
        
        if not response.rows:
            logger.error("API GA4 вернул пустой результат. Нет данных для указанного периода.")
            return False
        
        logger.info(f"Соединение с GA4 успешно установлено.")
        return True
    except Exception as e:
        logger.error(f"Ошибка при подключении к GA4: {e}")
        logger.error(traceback.format_exc())
        return False

def fetch_channel_metrics():
    """Получение метрик по каналам из GA4."""
    logger.info(f"Получение метрик по каналам за период {START_DATE} - {END_DATE}")
    
    # Измерения
    dimensions = [
        "sessionDefaultChannelGroup",
        "newVsReturning"
    ]
    
    # Метрики каналов
    metrics = [
        "sessions",
        "activeUsers",
        "newUsers",
        "conversions",
        "engagedSessions",
        "engagementRate",
        "averageSessionDuration",
        "bounceRate"
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
                channel_group = row.dimension_values[0].value
                user_type = row.dimension_values[1].value
                
                # Метрики
                sessions = int(row.metric_values[0].value or 0)
                users = int(row.metric_values[1].value or 0)
                new_users = int(row.metric_values[2].value or 0)
                conversions = float(row.metric_values[3].value or 0.0)
                engaged_sessions = int(row.metric_values[4].value or 0)
                engagement_rate = float(row.metric_values[5].value or 0.0)
                session_duration = float(row.metric_values[6].value or 0.0)
                bounce_rate = float(row.metric_values[7].value or 0.0)
                
                # Расчёт конверсии сессий
                session_conversion_rate = 0.0
                if sessions > 0:
                    session_conversion_rate = conversions / sessions
                
                results.append((
                    date_str,
                    channel_group,
                    user_type,
                    sessions,
                    users,
                    new_users,
                    session_conversion_rate,
                    engaged_sessions,
                    engagement_rate,
                    session_duration,
                    bounce_rate
                ))
            
            current_date += timedelta(days=1)
            
        logger.info(f"Получено {len(results)} записей метрик по каналам")
        return results
    except Exception as e:
        logger.error(f"Ошибка при получении метрик по каналам: {e}")
        logger.error(traceback.format_exc())
        return []

def fetch_source_medium_metrics():
    """Получение метрик по источникам/каналам из GA4."""
    logger.info(f"Получение метрик по источникам/каналам за период {START_DATE} - {END_DATE}")
    
    # Измерения
    dimensions = [
        "sessionSource",
        "sessionMedium",
        "newVsReturning"
    ]
    
    # Метрики источников/каналов
    metrics = [
        "sessions",
        "activeUsers",
        "newUsers",
        "conversions",
        "engagedSessions",
        "engagementRate",
        "averageSessionDuration",
        "bounceRate"
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
                limit=10000  # Увеличиваем лимит, чтобы получить больше комбинаций
            )
            
            response = client.run_report(request)
            
            # Обработка результатов
            for row in response.rows:
                source = row.dimension_values[0].value
                medium = row.dimension_values[1].value
                user_type = row.dimension_values[2].value
                
                # Метрики
                sessions = int(row.metric_values[0].value or 0)
                users = int(row.metric_values[1].value or 0)
                new_users = int(row.metric_values[2].value or 0)
                conversions = float(row.metric_values[3].value or 0.0)
                engaged_sessions = int(row.metric_values[4].value or 0)
                engagement_rate = float(row.metric_values[5].value or 0.0)
                session_duration = float(row.metric_values[6].value or 0.0)
                bounce_rate = float(row.metric_values[7].value or 0.0)
                
                # Расчёт конверсии сессий
                session_conversion_rate = 0.0
                if sessions > 0:
                    session_conversion_rate = conversions / sessions
                
                results.append((
                    date_str,
                    source,
                    medium,
                    user_type,
                    sessions,
                    users,
                    new_users,
                    session_conversion_rate,
                    engaged_sessions,
                    engagement_rate,
                    session_duration,
                    bounce_rate
                ))
            
            current_date += timedelta(days=1)
            
        logger.info(f"Получено {len(results)} записей метрик по источникам/каналам")
        return results
    except Exception as e:
        logger.error(f"Ошибка при получении метрик по источникам/каналам: {e}")
        logger.error(traceback.format_exc())
        return []

def fetch_campaign_metrics():
    """Получение метрик по кампаниям из GA4."""
    logger.info(f"Получение метрик по кампаниям за период {START_DATE} - {END_DATE}")
    
    # Измерения
    dimensions = [
        "sessionCampaignName",
        "sessionSource",
        "sessionMedium"
    ]
    
    # Метрики кампаний
    metrics = [
        "sessions",
        "activeUsers",
        "newUsers",
        "conversions",
        "engagedSessions",
        "engagementRate",
        "averageSessionDuration",
        "bounceRate"
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
                limit=10000  # Увеличиваем лимит, чтобы получить больше комбинаций
            )
            
            response = client.run_report(request)
            
            # Обработка результатов
            for row in response.rows:
                campaign_name = row.dimension_values[0].value
                source = row.dimension_values[1].value
                medium = row.dimension_values[2].value
                
                # Пропускаем записи, где имя кампании не указано
                if not campaign_name or campaign_name == "(not set)":
                    continue
                
                # Метрики
                sessions = int(row.metric_values[0].value or 0)
                users = int(row.metric_values[1].value or 0)
                new_users = int(row.metric_values[2].value or 0)
                conversions = float(row.metric_values[3].value or 0.0)
                engaged_sessions = int(row.metric_values[4].value or 0)
                engagement_rate = float(row.metric_values[5].value or 0.0)
                session_duration = float(row.metric_values[6].value or 0.0)
                bounce_rate = float(row.metric_values[7].value or 0.0)
                
                # Расчёт конверсии сессий
                session_conversion_rate = 0.0
                if sessions > 0:
                    session_conversion_rate = conversions / sessions
                
                results.append((
                    date_str,
                    campaign_name,
                    source,
                    medium,
                    sessions,
                    users,
                    new_users,
                    session_conversion_rate,
                    engaged_sessions,
                    engagement_rate,
                    session_duration,
                    bounce_rate
                ))
            
            current_date += timedelta(days=1)
            
        logger.info(f"Получено {len(results)} записей метрик по кампаниям")
        return results
    except Exception as e:
        logger.error(f"Ошибка при получении метрик по кампаниям: {e}")
        logger.error(traceback.format_exc())
        return []

def load_channel_metrics_to_db():
    """Загрузка метрик по каналам в базу данных."""
    metrics = fetch_channel_metrics()
    if not metrics:
        logger.warning("Нет данных для загрузки в таблицу channel_metrics")
        return
    
    # Загрузка данных в БД
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        
        # Вставка данных с обработкой дубликатов
        query = """
        INSERT INTO analytics.ga4_channel_metrics (
            report_date, 
            default_channel_group, 
            user_type, 
            sessions, 
            users, 
            new_users, 
            session_conversion_rate,
            engaged_sessions,
            engagement_rate,
            session_duration,
            bounce_rate
        ) VALUES %s
        ON CONFLICT (report_date, default_channel_group, user_type) DO UPDATE SET
            sessions = EXCLUDED.sessions,
            users = EXCLUDED.users,
            new_users = EXCLUDED.new_users,
            session_conversion_rate = EXCLUDED.session_conversion_rate,
            engaged_sessions = EXCLUDED.engaged_sessions,
            engagement_rate = EXCLUDED.engagement_rate,
            session_duration = EXCLUDED.session_duration,
            bounce_rate = EXCLUDED.bounce_rate;
        """
        
        psycopg2.extras.execute_values(cursor, query, metrics)
        conn.commit()
        logger.info(f"Загружено {len(metrics)} записей метрик по каналам")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Ошибка при загрузке метрик по каналам: {e}")
        logger.error(traceback.format_exc())
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def load_source_medium_metrics_to_db():
    """Загрузка метрик по источникам/каналам в базу данных."""
    metrics = fetch_source_medium_metrics()
    if not metrics:
        logger.warning("Нет данных для загрузки в таблицу source_medium_metrics")
        return
    
    # Загрузка данных в БД
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        
        # Вставка данных с обработкой дубликатов
        query = """
        INSERT INTO analytics.ga4_source_medium_metrics (
            report_date, 
            source, 
            medium, 
            user_type, 
            sessions, 
            users, 
            new_users, 
            session_conversion_rate,
            engaged_sessions,
            engagement_rate,
            session_duration,
            bounce_rate
        ) VALUES %s
        ON CONFLICT (report_date, source, medium, user_type) DO UPDATE SET
            sessions = EXCLUDED.sessions,
            users = EXCLUDED.users,
            new_users = EXCLUDED.new_users,
            session_conversion_rate = EXCLUDED.session_conversion_rate,
            engaged_sessions = EXCLUDED.engaged_sessions,
            engagement_rate = EXCLUDED.engagement_rate,
            session_duration = EXCLUDED.session_duration,
            bounce_rate = EXCLUDED.bounce_rate;
        """
        
        # Разбиваем на пакеты по 1000 записей
        batch_size = 1000
        for i in range(0, len(metrics), batch_size):
            batch = metrics[i:i+batch_size]
            psycopg2.extras.execute_values(cursor, query, batch)
            conn.commit()
            logger.info(f"Загружена партия {i//batch_size + 1} из {(len(metrics)-1)//batch_size + 1}, размер: {len(batch)}")
            
        logger.info(f"Загружено {len(metrics)} записей метрик по источникам/каналам")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Ошибка при загрузке метрик по источникам/каналам: {e}")
        logger.error(traceback.format_exc())
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def load_campaign_metrics_to_db():
    """Загрузка метрик по кампаниям в базу данных."""
    metrics = fetch_campaign_metrics()
    if not metrics:
        logger.warning("Нет данных для загрузки в таблицу campaign_metrics")
        return
    
    # Загрузка данных в БД
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        
        # Вставка данных с обработкой дубликатов
        query = """
        INSERT INTO analytics.ga4_campaign_metrics (
            report_date, 
            campaign_name, 
            source, 
            medium, 
            sessions, 
            users, 
            new_users, 
            session_conversion_rate,
            engaged_sessions,
            engagement_rate,
            session_duration,
            bounce_rate
        ) VALUES %s
        ON CONFLICT (report_date, campaign_name, source, medium) DO UPDATE SET
            sessions = EXCLUDED.sessions,
            users = EXCLUDED.users,
            new_users = EXCLUDED.new_users,
            session_conversion_rate = EXCLUDED.session_conversion_rate,
            engaged_sessions = EXCLUDED.engaged_sessions,
            engagement_rate = EXCLUDED.engagement_rate,
            session_duration = EXCLUDED.session_duration,
            bounce_rate = EXCLUDED.bounce_rate;
        """
        
        # Разбиваем на пакеты по 1000 записей
        batch_size = 1000
        for i in range(0, len(metrics), batch_size):
            batch = metrics[i:i+batch_size]
            psycopg2.extras.execute_values(cursor, query, batch)
            conn.commit()
            logger.info(f"Загружена партия {i//batch_size + 1} из {(len(metrics)-1)//batch_size + 1}, размер: {len(batch)}")
            
        logger.info(f"Загружено {len(metrics)} записей метрик по кампаниям")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Ошибка при загрузке метрик по кампаниям: {e}")
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
    'GA4_ACQUISITION',
    default_args=default_args,
    description='Импорт метрик источников трафика из GA4 (каналы, источники/каналы, кампании)',
    schedule_interval='0 3 * * *',  # Каждый день в 03:00
    start_date=datetime(2025, 3, 5),
    catchup=False,
    tags=['ga4', 'acquisition'],
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
    
    load_channel_metrics = PythonOperator(
        task_id='load_channel_metrics',
        python_callable=load_channel_metrics_to_db,
    )
    
    load_source_medium_metrics = PythonOperator(
        task_id='load_source_medium_metrics',
        python_callable=load_source_medium_metrics_to_db,
    )
    
    load_campaign_metrics = PythonOperator(
        task_id='load_campaign_metrics',
        python_callable=load_campaign_metrics_to_db,
    )
    
    # Определение порядка выполнения задач
    test_connection >> create_db_tables
    create_db_tables >> [load_channel_metrics, load_source_medium_metrics, load_campaign_metrics]