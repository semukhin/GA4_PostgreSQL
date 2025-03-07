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
logger = logging.getLogger('GA4_USER_BEHAVIOR')

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

# Ключевые события, для которых нужно получить данные
KEY_EVENTS = [
    "main_form_submit",
    "platform_login",
    "platform_signup",
    "click_upgrade_pro",
    "click_upgrade_flexible",
    "purchase"
]

def create_tables():
    """Создание необходимых таблиц в схеме staging."""
    queries = [
        """
        CREATE SCHEMA IF NOT EXISTS staging;
        """,
        """
        CREATE TABLE IF NOT EXISTS staging.ga4_event_metrics (
            report_date DATE,
            event_name TEXT,
            user_type TEXT,  -- new, returning, (not set)
            event_count INT,
            event_count_per_user FLOAT,
            event_value FLOAT,
            is_key_event BOOLEAN,
            PRIMARY KEY (report_date, event_name, user_type)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS staging.ga4_key_event_metrics (
            report_date DATE,
            key_event_name TEXT,
            user_type TEXT,  -- new, returning, (not set)
            event_count INT,
            event_count_per_user FLOAT,
            user_key_event_rate FLOAT,
            session_key_event_rate FLOAT,
            PRIMARY KEY (report_date, key_event_name, user_type)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS staging.ga4_page_path_metrics (
            report_date DATE,
            page_path TEXT,
            page_title TEXT,
            landing_page_plus_query_string TEXT,
            screen_page_views INT,
            average_engagement_time FLOAT,
            bounce_rate FLOAT,
            exit_rate FLOAT,
            exits INT,
            PRIMARY KEY (report_date, page_path)
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

def fetch_event_metrics():
    """Получение метрик событий из GA4."""
    logger.info(f"Получение метрик событий за период {START_DATE} - {END_DATE}")
    
    # Измерения
    dimensions = [
        "eventName",
        "isKeyEvent",
        "newVsReturning"
    ]
    
    # Метрики событий
    metrics = [
        "eventCount",
        "eventCountPerUser",
        "eventValue"
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
                event_name = row.dimension_values[0].value
                is_key_event = row.dimension_values[1].value.lower() == 'true'
                user_type = row.dimension_values[2].value
                
                # Метрики
                event_count = int(row.metric_values[0].value or 0)
                event_count_per_user = float(row.metric_values[1].value or 0.0)
                event_value = float(row.metric_values[2].value or 0.0)
                
                results.append((
                    date_str,
                    event_name,
                    user_type,
                    event_count,
                    event_count_per_user,
                    event_value,
                    is_key_event
                ))
            
            current_date += timedelta(days=1)
            
        if not results:
            logger.error("Не удалось получить данные метрик событий")
            raise ValueError("Пустой результат при получении метрик событий")
            
        logger.info(f"Получено {len(results)} записей метрик событий")
        return results
    except Exception as e:
        logger.error(f"Ошибка при получении метрик событий: {e}")
        logger.error(traceback.format_exc())
        raise  # Перебрасываем исключение, чтобы задача завершилась с ошибкой


def fetch_key_event_metrics():
    """Получение метрик ключевых событий из GA4."""
    logger.info(f"Получение метрик ключевых событий за период {START_DATE} - {END_DATE}")
    
    # Метрики для каждого ключевого события
    all_results = []
    
    try:
        client = BetaAnalyticsDataClient()
        
        # Для каждого ключевого события
        for key_event in KEY_EVENTS:
            logger.info(f"Получение метрик для ключевого события: {key_event}")
            
            # Измерения
            dimensions = [
                "newVsReturning"
            ]
            
            # Метрики для ключевого события
            metrics = [
                f"keyEvents:{key_event}",
                "eventCountPerUser",
                f"userKeyEventRate:{key_event}",
                f"sessionKeyEventRate:{key_event}"
            ]
            
            # Запрос данных по дням
            current_date = datetime.strptime(START_DATE, "%Y-%m-%d")
            end_date = datetime.strptime(END_DATE, "%Y-%m-%d")
            
            while current_date <= end_date:
                date_str = current_date.strftime("%Y-%m-%d")
                
                try:
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
                        event_count = int(row.metric_values[0].value or 0)
                        event_count_per_user = float(row.metric_values[1].value or 0.0)
                        user_key_event_rate = float(row.metric_values[2].value or 0.0)
                        session_key_event_rate = float(row.metric_values[3].value or 0.0)
                        
                        all_results.append((
                            date_str,
                            key_event,
                            user_type,
                            event_count,
                            event_count_per_user,
                            user_key_event_rate,
                            session_key_event_rate
                        ))
                    
                except Exception as inner_e:
                    logger.warning(f"Предупреждение при получении метрик для ключевого события {key_event} за {date_str}: {inner_e}")
                
                current_date += timedelta(days=1)
            
        logger.info(f"Получено {len(all_results)} записей метрик ключевых событий")
        return all_results
    except Exception as e:
        logger.error(f"Ошибка при получении метрик ключевых событий: {e}")
        logger.error(traceback.format_exc())
        return []
    

def fetch_page_path_metrics():
    """Получение метрик по путям страниц из GA4."""
    logger.info(f"Получение метрик по путям страниц за период {START_DATE} - {END_DATE}")
    
    # Измерения - с landingPagePlusQueryString
    dimensions = [
        "pagePath",
        "pageTitle",
        "landingPagePlusQueryString"
    ]
    
    # Метрики - только гарантированно работающие
    metrics = [
        "screenPageViews",
        "userEngagementDuration",  # Используется для расчета average_engagement_time
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
                limit=1000  # Ограничим количество строк
            )
            
            response = client.run_report(request)
            
            # Обработка результатов
            for row in response.rows:
                page_path = row.dimension_values[0].value
                page_title = row.dimension_values[1].value
                landing_page_plus_query_string = row.dimension_values[2].value
                
                # Метрики
                screen_page_views = int(row.metric_values[0].value or 0)
                user_engagement_duration = int(row.metric_values[1].value or 0)
                bounce_rate = float(row.metric_values[2].value or 0.0)
                
                # Расчет среднего времени вовлеченности
                average_engagement_time = 0.0
                if screen_page_views > 0:
                    average_engagement_time = user_engagement_duration / screen_page_views
                
                # Заглушки для полей, которые невозможно получить из API
                exit_rate = 0.0  # Заглушка для exit_rate
                exits = 0  # Заглушка для exits
                
                results.append((
                    date_str,
                    page_path,
                    page_title,
                    landing_page_plus_query_string,
                    screen_page_views,
                    average_engagement_time,
                    bounce_rate,
                    exit_rate,  # Заглушка
                    exits  # Заглушка
                ))
            
            current_date += timedelta(days=1)
            
        logger.info(f"Получено {len(results)} записей метрик по путям страниц")
        return results
    except Exception as e:
        logger.error(f"Ошибка при получении метрик по путям страниц: {e}")
        logger.error(traceback.format_exc())
        return []

def load_page_path_metrics_to_db():
    """Загрузка метрик по путям страниц в базу данных."""
    metrics = fetch_page_path_metrics()
    if not metrics:
        logger.warning("Нет данных для загрузки в таблицу page_path_metrics")
        return
    
    # Загрузка данных в БД
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        
        # Вставка данных с обработкой дубликатов - обновляем SQL с новыми полями
        query = """
            INSERT INTO staging.ga4_page_path_metrics (
            report_date, 
            page_path, 
            page_title,
            landing_page_plus_query_string,
            screen_page_views, 
            average_engagement_time, 
            bounce_rate,
            exit_rate,
            exits
        ) VALUES %s
        ON CONFLICT (report_date, page_path) DO UPDATE SET
            page_title = EXCLUDED.page_title,
            landing_page_plus_query_string = EXCLUDED.landing_page_plus_query_string,
            screen_page_views = EXCLUDED.screen_page_views,
            average_engagement_time = EXCLUDED.average_engagement_time,
            bounce_rate = EXCLUDED.bounce_rate,
            exit_rate = EXCLUDED.exit_rate,
            exits = EXCLUDED.exits;
        """
        
        psycopg2.extras.execute_values(cursor, query, metrics)
        conn.commit()
        logger.info(f"Загружено {len(metrics)} записей метрик по путям страниц")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Ошибка при загрузке метрик по путям страниц: {e}")
        logger.error(traceback.format_exc())
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    

def load_event_metrics_to_db():
    """Загрузка метрик событий в базу данных."""
    metrics = fetch_event_metrics()
    if not metrics:
        logger.warning("Нет данных для загрузки в таблицу event_metrics")
        return
    
    # Устранение дубликатов перед загрузкой
    unique_metrics = {}
    for metric in metrics:
        # Используем первые три поля (report_date, event_name, user_type) как ключ
        key = (metric[0], metric[1], metric[2])
        # Сохраняем только последнюю запись для каждого ключа
        unique_metrics[key] = metric
    
    # Преобразуем обратно в список
    deduplicated_metrics = list(unique_metrics.values())
    
    logger.info(f"После удаления дубликатов осталось {len(deduplicated_metrics)} из {len(metrics)} записей")
    
    # Загрузка данных в БД
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        
        # Вставка данных с обработкой дубликатов
        query = """
        INSERT INTO staging.ga4_event_metrics (
            report_date, 
            event_name, 
            user_type, 
            event_count, 
            event_count_per_user, 
            event_value,
            is_key_event
        ) VALUES %s
        ON CONFLICT (report_date, event_name, user_type) DO UPDATE SET
            event_count = EXCLUDED.event_count,
            event_count_per_user = EXCLUDED.event_count_per_user,
            event_value = EXCLUDED.event_value,
            is_key_event = EXCLUDED.is_key_event;
        """
        
        psycopg2.extras.execute_values(cursor, query, deduplicated_metrics)
        conn.commit()
        logger.info(f"Загружено {len(deduplicated_metrics)} записей метрик событий")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Ошибка при загрузке метрик событий: {e}")
        logger.error(traceback.format_exc())
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def load_key_event_metrics_to_db():
    """Загрузка метрик ключевых событий в базу данных."""
    metrics = fetch_key_event_metrics()
    if not metrics:
        logger.warning("Нет данных для загрузки в таблицу key_event_metrics")
        return
    
    # Устранение дубликатов перед загрузкой
    unique_metrics = {}
    for metric in metrics:
        # Используем первые три поля (report_date, key_event_name, user_type) как ключ
        key = (metric[0], metric[1], metric[2])
        # Сохраняем только последнюю запись для каждого ключа
        unique_metrics[key] = metric
    
    # Преобразуем обратно в список
    deduplicated_metrics = list(unique_metrics.values())
    
    logger.info(f"После удаления дубликатов осталось {len(deduplicated_metrics)} из {len(metrics)} записей")
    
    # Загрузка данных в БД
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        
        # Вставка данных с обработкой дубликатов
        query = """
        INSERT INTO staging.ga4_key_event_metrics (
            report_date, 
            key_event_name, 
            user_type, 
            event_count, 
            event_count_per_user, 
            user_key_event_rate,
            session_key_event_rate
        ) VALUES %s
        ON CONFLICT (report_date, key_event_name, user_type) DO UPDATE SET
            event_count = EXCLUDED.event_count,
            event_count_per_user = EXCLUDED.event_count_per_user,
            user_key_event_rate = EXCLUDED.user_key_event_rate,
            session_key_event_rate = EXCLUDED.session_key_event_rate;
        """
        
        psycopg2.extras.execute_values(cursor, query, deduplicated_metrics)
        conn.commit()
        logger.info(f"Загружено {len(deduplicated_metrics)} записей метрик ключевых событий")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Ошибка при загрузке метрик ключевых событий: {e}")
        logger.error(traceback.format_exc())
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def load_page_path_metrics_to_db():
    """Загрузка метрик по путям страниц в базу данных."""
    metrics = fetch_page_path_metrics()
    if not metrics:
        logger.warning("Нет данных для загрузки в таблицу page_path_metrics")
        return
    
    # Устранение дубликатов перед загрузкой
    unique_metrics = {}
    for metric in metrics:
        # Используем первые два поля (report_date, page_path) как ключ
        key = (metric[0], metric[1])
        # Сохраняем только последнюю запись для каждого ключа
        unique_metrics[key] = metric
    
    # Преобразуем обратно в список
    deduplicated_metrics = list(unique_metrics.values())
    
    logger.info(f"После удаления дубликатов осталось {len(deduplicated_metrics)} из {len(metrics)} записей")
    
    # Загрузка данных в БД
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        
        # Вставка данных с обработкой дубликатов
        query = """
            INSERT INTO staging.ga4_page_path_metrics (
            report_date, 
            page_path, 
            page_title,
            landing_page_plus_query_string,
            screen_page_views, 
            average_engagement_time, 
            bounce_rate,
            exit_rate,
            exits
        ) VALUES %s
        ON CONFLICT (report_date, page_path) DO UPDATE SET
            page_title = EXCLUDED.page_title,
            landing_page_plus_query_string = EXCLUDED.landing_page_plus_query_string,
            screen_page_views = EXCLUDED.screen_page_views,
            average_engagement_time = EXCLUDED.average_engagement_time,
            bounce_rate = EXCLUDED.bounce_rate,
            exit_rate = EXCLUDED.exit_rate,
            exits = EXCLUDED.exits;
        """
        
        psycopg2.extras.execute_values(cursor, query, deduplicated_metrics)
        conn.commit()
        logger.info(f"Загружено {len(deduplicated_metrics)} записей метрик по путям страниц")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Ошибка при загрузке метрик по путям страниц: {e}")
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
    'GA4_USER_BEHAVIOR',
    default_args=default_args,
    description='Импорт метрик поведения пользователей из GA4 (события, ключевые события, пути страниц)',
    schedule_interval='0 2 * * *',  # Каждый день в 02:00
    start_date=datetime(2025, 3, 5),
    catchup=False,
    tags=['ga4', 'behavior'],
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
    
    load_event_metrics = PythonOperator(
        task_id='load_event_metrics',
        python_callable=load_event_metrics_to_db,
        trigger_rule='all_success',
    )
    
    load_key_event_metrics = PythonOperator(
        task_id='load_key_event_metrics',
        python_callable=load_key_event_metrics_to_db,
        trigger_rule='all_success',
    )
    
    load_page_path_metrics = PythonOperator(
        task_id='load_page_path_metrics',
        python_callable=load_page_path_metrics_to_db,
        trigger_rule='all_success',
    )
    
    # Определение порядка выполнения задач
    test_connection >> create_db_tables
    create_db_tables >> [load_event_metrics, load_key_event_metrics, load_page_path_metrics]