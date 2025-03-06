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
logger = logging.getLogger('GA4_GEOGRAPHIC')

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
        CREATE TABLE IF NOT EXISTS analytics.ga4_geo_country_metrics (
            report_date DATE,
            country TEXT,
            country_id TEXT,
            continent TEXT,
            continent_id TEXT,
            users INT,
            new_users INT,
            sessions INT,
            bounce_rate FLOAT,
            engagement_rate FLOAT,
            avg_session_duration FLOAT,
            screen_page_views INT,
            screen_page_views_per_session FLOAT,
            PRIMARY KEY (report_date, country)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS analytics.ga4_geo_region_metrics (
            report_date DATE,
            region TEXT,
            country TEXT,
            country_id TEXT,
            users INT,
            new_users INT,
            sessions INT,
            bounce_rate FLOAT,
            engagement_rate FLOAT,
            avg_session_duration FLOAT,
            screen_page_views INT,
            screen_page_views_per_session FLOAT,
            PRIMARY KEY (report_date, region, country)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS analytics.ga4_geo_city_metrics (
            report_date DATE,
            city TEXT,
            city_id TEXT,
            region TEXT,
            country TEXT,
            country_id TEXT,
            users INT,
            new_users INT,
            sessions INT,
            bounce_rate FLOAT,
            engagement_rate FLOAT,
            avg_session_duration FLOAT,
            screen_page_views INT,
            screen_page_views_per_session FLOAT,
            PRIMARY KEY (report_date, city, country)
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
            metrics=[Metric(name="activeUsers")],
            dimensions=[Dimension(name="country")]
        )
        response = client.run_report(request)
        
        if not response.rows:
            logger.error("API GA4 вернул пустой результат. Нет данных для указанного периода.")
            return False
        
        logger.info(f"Соединение с GA4 успешно установлено. Получены данные по {len(response.rows)} странам.")
        return True
    except Exception as e:
        logger.error(f"Ошибка при подключении к GA4: {e}")
        logger.error(traceback.format_exc())
        return False

def fetch_country_metrics():
    """Получение метрик по странам из GA4."""
    logger.info(f"Получение метрик по странам за период {START_DATE} - {END_DATE}")
    
    # Измерения
    dimensions = [
        "country",
        "countryId",
        "continent",
        "continentId"
    ]
    
    # Метрики
    metrics = [
        "activeUsers",
        "newUsers",
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
            logger.info(f"Запрос данных по странам для даты {date_str}")
            
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
                    country = row.dimension_values[0].value
                    country_id = row.dimension_values[1].value
                    continent = row.dimension_values[2].value
                    continent_id = row.dimension_values[3].value
                    
                    # Если страна не указана, используем "(not set)"
                    if not country or country == "(not set)":
                        country = "(not set)"
                    
                    # Метрики
                    users = int(row.metric_values[0].value or 0)
                    new_users = int(row.metric_values[1].value or 0)
                    sessions = int(row.metric_values[2].value or 0)
                    bounce_rate = float(row.metric_values[3].value or 0.0)
                    engagement_rate = float(row.metric_values[4].value or 0.0)
                    avg_session_duration = float(row.metric_values[5].value or 0.0)
                    screen_page_views = int(row.metric_values[6].value or 0)
                    screen_page_views_per_session = float(row.metric_values[7].value or 0.0)
                    
                    results.append((
                        date_str,
                        country,
                        country_id,
                        continent,
                        continent_id,
                        users,
                        new_users,
                        sessions,
                        bounce_rate,
                        engagement_rate,
                        avg_session_duration,
                        screen_page_views,
                        screen_page_views_per_session
                    ))
            except Exception as e:
                logger.error(f"Ошибка при получении данных по странам для {date_str}: {e}")
                logger.error(traceback.format_exc())
            
            current_date += timedelta(days=1)
            
        logger.info(f"Получено {len(results)} записей метрик по странам")
        return results
    except Exception as e:
        logger.error(f"Ошибка при получении метрик по странам: {e}")
        logger.error(traceback.format_exc())
        return []

def fetch_region_metrics():
    """Получение метрик по регионам из GA4."""
    logger.info(f"Получение метрик по регионам за период {START_DATE} - {END_DATE}")
    
    # Измерения
    dimensions = [
        "region",
        "country",
        "countryId"
    ]
    
    # Метрики
    metrics = [
        "activeUsers",
        "newUsers",
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
            logger.info(f"Запрос данных по регионам для даты {date_str}")
            
            try:
                request = RunReportRequest(
                    property=PROPERTY_ID,
                    date_ranges=[DateRange(start_date=date_str, end_date=date_str)],
                    dimensions=[Dimension(name=d) for d in dimensions],
                    metrics=[Metric(name=m) for m in metrics],
                    limit=5000  # Ограничиваем количество возвращаемых строк
                )
                
                response = client.run_report(request)
                
                # Обработка результатов
                for row in response.rows:
                    region = row.dimension_values[0].value
                    country = row.dimension_values[1].value
                    country_id = row.dimension_values[2].value
                    
                    # Если регион или страна не указаны, используем "(not set)"
                    if not region or region == "(not set)":
                        region = "(not set)"
                    if not country or country == "(not set)":
                        country = "(not set)"
                    
                    # Метрики
                    users = int(row.metric_values[0].value or 0)
                    new_users = int(row.metric_values[1].value or 0)
                    sessions = int(row.metric_values[2].value or 0)
                    bounce_rate = float(row.metric_values[3].value or 0.0)
                    engagement_rate = float(row.metric_values[4].value or 0.0)
                    avg_session_duration = float(row.metric_values[5].value or 0.0)
                    screen_page_views = int(row.metric_values[6].value or 0)
                    screen_page_views_per_session = float(row.metric_values[7].value or 0.0)
                    
                    results.append((
                        date_str,
                        region,
                        country,
                        country_id,
                        users,
                        new_users,
                        sessions,
                        bounce_rate,
                        engagement_rate,
                        avg_session_duration,
                        screen_page_views,
                        screen_page_views_per_session
                    ))
            except Exception as e:
                logger.error(f"Ошибка при получении данных по регионам для {date_str}: {e}")
                logger.error(traceback.format_exc())
            
            current_date += timedelta(days=1)
            
        logger.info(f"Получено {len(results)} записей метрик по регионам")
        return results
    except Exception as e:
        logger.error(f"Ошибка при получении метрик по регионам: {e}")
        logger.error(traceback.format_exc())
        return []

def fetch_city_metrics():
    """Получение метрик по городам из GA4."""
    logger.info(f"Получение метрик по городам за период {START_DATE} - {END_DATE}")
    
    # Измерения
    dimensions = [
        "city",
        "cityId",
        "region",
        "country",
        "countryId"
    ]
    
    # Метрики
    metrics = [
        "activeUsers",
        "newUsers",
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
            logger.info(f"Запрос данных по городам для даты {date_str}")
            
            try:
                request = RunReportRequest(
                    property=PROPERTY_ID,
                    date_ranges=[DateRange(start_date=date_str, end_date=date_str)],
                    dimensions=[Dimension(name=d) for d in dimensions],
                    metrics=[Metric(name=m) for m in metrics],
                    limit=5000  # Ограничиваем количество возвращаемых строк (топ-5000 городов по пользователям)
                )
                
                response = client.run_report(request)
                
                # Обработка результатов
                for row in response.rows:
                    city = row.dimension_values[0].value
                    city_id = row.dimension_values[1].value
                    region = row.dimension_values[2].value
                    country = row.dimension_values[3].value
                    country_id = row.dimension_values[4].value
                    
                    # Если город, регион или страна не указаны, используем "(not set)"
                    if not city or city == "(not set)":
                        city = "(not set)"
                    if not region or region == "(not set)":
                        region = "(not set)"
                    if not country or country == "(not set)":
                        country = "(not set)"
                    
                    # Метрики
                    users = int(row.metric_values[0].value or 0)
                    new_users = int(row.metric_values[1].value or 0)
                    sessions = int(row.metric_values[2].value or 0)
                    bounce_rate = float(row.metric_values[3].value or 0.0)
                    engagement_rate = float(row.metric_values[4].value or 0.0)
                    avg_session_duration = float(row.metric_values[5].value or 0.0)
                    screen_page_views = int(row.metric_values[6].value or 0)
                    screen_page_views_per_session = float(row.metric_values[7].value or 0.0)
                    
                    results.append((
                        date_str,
                        city,
                        city_id,
                        region,
                        country,
                        country_id,
                        users,
                        new_users,
                        sessions,
                        bounce_rate,
                        engagement_rate,
                        avg_session_duration,
                        screen_page_views,
                        screen_page_views_per_session
                    ))
            except Exception as e:
                logger.error(f"Ошибка при получении данных по городам для {date_str}: {e}")
                logger.error(traceback.format_exc())
            
            current_date += timedelta(days=1)
            
        logger.info(f"Получено {len(results)} записей метрик по городам")
        return results
    except Exception as e:
        logger.error(f"Ошибка при получении метрик по городам: {e}")
        logger.error(traceback.format_exc())
        return []

def load_country_metrics_to_db():
    """Загрузка метрик по странам в базу данных."""
    metrics = fetch_country_metrics()
    if not metrics:
        logger.warning("Нет данных для загрузки в таблицу ga4_geo_country_metrics")
        return
    
    # Загрузка данных в БД
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        
        # Вставка данных с обработкой дубликатов
        query = """
        INSERT INTO analytics.ga4_geo_country_metrics (
            report_date, 
            country, 
            country_id, 
            continent, 
            continent_id, 
            users, 
            new_users,
            sessions,
            bounce_rate,
            engagement_rate,
            avg_session_duration,
            screen_page_views,
            screen_page_views_per_session
        ) VALUES %s
        ON CONFLICT (report_date, country) DO UPDATE SET
            country_id = EXCLUDED.country_id,
            continent = EXCLUDED.continent,
            continent_id = EXCLUDED.continent_id,
            users = EXCLUDED.users,
            new_users = EXCLUDED.new_users,
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
            
        logger.info(f"Загружено {len(metrics)} записей метрик по странам")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Ошибка при загрузке метрик по странам: {e}")
        logger.error(traceback.format_exc())
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def load_region_metrics_to_db():
    """Загрузка метрик по регионам в базу данных."""
    metrics = fetch_region_metrics()
    if not metrics:
        logger.warning("Нет данных для загрузки в таблицу ga4_geo_region_metrics")
        return
    
    # Загрузка данных в БД
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        
        # Вставка данных с обработкой дубликатов
        query = """
        INSERT INTO analytics.ga4_geo_region_metrics (
            report_date, 
            region, 
            country, 
            country_id, 
            users, 
            new_users,
            sessions,
            bounce_rate,
            engagement_rate,
            avg_session_duration,
            screen_page_views,
            screen_page_views_per_session
        ) VALUES %s
        ON CONFLICT (report_date, region, country) DO UPDATE SET
            country_id = EXCLUDED.country_id,
            users = EXCLUDED.users,
            new_users = EXCLUDED.new_users,
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
            
        logger.info(f"Загружено {len(metrics)} записей метрик по регионам")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Ошибка при загрузке метрик по регионам: {e}")
        logger.error(traceback.format_exc())
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def load_city_metrics_to_db():
    """Загрузка метрик по городам в базу данных."""
    metrics = fetch_city_metrics()
    if not metrics:
        logger.warning("Нет данных для загрузки в таблицу ga4_geo_city_metrics")
        return
    
    # Загрузка данных в БД
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        
        # Вставка данных с обработкой дубликатов
        query = """
        INSERT INTO analytics.ga4_geo_city_metrics (
            report_date, 
            city, 
            city_id, 
            region, 
            country, 
            country_id, 
            users, 
            new_users,
            sessions,
            bounce_rate,
            engagement_rate,
            avg_session_duration,
            screen_page_views,
            screen_page_views_per_session
        ) VALUES %s
        ON CONFLICT (report_date, city, country) DO UPDATE SET
            city_id = EXCLUDED.city_id,
            region = EXCLUDED.region,
            country_id = EXCLUDED.country_id,
            users = EXCLUDED.users,
            new_users = EXCLUDED.new_users,
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
            
        logger.info(f"Загружено {len(metrics)} записей метрик по городам")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Ошибка при загрузке метрик по городам: {e}")
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
    'GA4_GEOGRAPHIC',
    default_args=default_args,
    description='Импорт географических метрик из GA4 (страны, регионы, города)',
    schedule_interval='0 5 * * *',  # Каждый день в 05:00
    start_date=datetime(2025, 3, 5),
    catchup=False,
    tags=['ga4', 'geographic'],
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
    
    load_country_metrics = PythonOperator(
        task_id='load_country_metrics',
        python_callable=load_country_metrics_to_db,
    )
    
    load_region_metrics = PythonOperator(
        task_id='load_region_metrics',
        python_callable=load_region_metrics_to_db,
    )
    
    load_city_metrics = PythonOperator(
        task_id='load_city_metrics',
        python_callable=load_city_metrics_to_db,
    )
    
    # Определение порядка выполнения задач
    test_connection >> create_db_tables
    create_db_tables >> load_country_metrics >> load_region_metrics >> load_city_metrics