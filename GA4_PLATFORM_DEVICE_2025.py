from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import psycopg2
import psycopg2.extras
from google.analytics.data_v1beta import BetaAnalyticsDataClient, RunReportRequest
from google.analytics.data_v1beta.types import DateRange, Metric, Dimension
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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

# Определение важных измерений и метрик
DIMENSIONS = [
    "deviceCategory",
    "browser",
    "operatingSystem",
    "operatingSystemWithVersion",
    "platform",
    "language",
    "screenResolution",
    "mobileDeviceModel",
    "mobileDeviceBranding",
    "appVersion"
]

# Метрики разбиты на группы по 9 для соблюдения ограничений API GA4
METRICS_GROUP_1 = [
    "activeUsers",
    "newUsers",
    "totalUsers",
    "sessions",
    "sessionsPerUser",
    "engagementRate",
    "engagedSessions",
    "averageSessionDuration",
    "bounceRate"
]

METRICS_GROUP_2 = [
    "screenPageViews",
    "screenPageViewsPerSession",
    "screenPageViewsPerUser",
    "userEngagementDuration"
]

def create_staging_table():
    """Создание таблицы в схеме staging для метрик и измерений Platform Device."""
    logger.info("Создание таблицы staging.ga4_platform_device_metrics")
    
    # Создаем SQL запрос для создания таблицы
    query = """
    CREATE SCHEMA IF NOT EXISTS staging;

    CREATE TABLE IF NOT EXISTS staging.ga4_platform_device_metrics (
        report_date DATE,
        device_category TEXT,
        browser TEXT,
        operating_system TEXT,
        operating_system_version TEXT,
        platform TEXT,
        language TEXT,
        screen_resolution TEXT,
        mobile_device_model TEXT,
        mobile_device_brand TEXT,
        app_version TEXT,
        active_users INT,
        new_users INT,
        total_users INT,
        sessions INT,
        sessions_per_user FLOAT,
        engagement_rate FLOAT,
        engaged_sessions INT,
        average_session_duration FLOAT,
        bounce_rate FLOAT,
        screen_page_views INT,
        screen_page_views_per_session FLOAT,
        screen_page_views_per_user FLOAT,
        user_engagement_duration INT,
        record_id VARCHAR(64)
    );
    """

    # Выполняем SQL запрос
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()
    try:
        cur.execute(query)
        conn.commit()
        logger.info("Таблица staging.ga4_platform_device_metrics успешно создана/обновлена")
    except Exception as e:
        conn.rollback()
        logger.error(f"Ошибка при создании таблицы: {e}")
    finally:
        cur.close()
        conn.close()

def fetch_ga4_metrics_by_dimension(date_str, dimension_name):
    """
    Получение метрик из GA4 за указанный день по конкретному измерению.
    Разбиваем метрики на группы из-за ограничения API (максимум 9 метрик за запрос).
    """
    client = BetaAnalyticsDataClient()
    results = []
    logger.info(f"Получение данных для измерения {dimension_name} за {date_str}")
    
    try:
        # Получаем первую группу метрик
        request_group1 = RunReportRequest(
            property=PROPERTY_ID,
            date_ranges=[DateRange(start_date=date_str, end_date=date_str)],
            dimensions=[Dimension(name=dimension_name)],
            metrics=[Metric(name=metric) for metric in METRICS_GROUP_1]
        )
        response_group1 = client.run_report(request_group1)
        
        # Если нет данных в первой группе, возвращаем пустой список
        if not response_group1.rows:
            logger.warning(f"Нет данных для измерения {dimension_name} за {date_str} (группа 1)")
            return []
            
        # Получаем вторую группу метрик
        request_group2 = RunReportRequest(
            property=PROPERTY_ID,
            date_ranges=[DateRange(start_date=date_str, end_date=date_str)],
            dimensions=[Dimension(name=dimension_name)],
            metrics=[Metric(name=metric) for metric in METRICS_GROUP_2]
        )
        response_group2 = client.run_report(request_group2)
        
        # Если нет данных во второй группе, возвращаем пустой список
        if not response_group2.rows:
            logger.warning(f"Нет данных для измерения {dimension_name} за {date_str} (группа 2)")
            return []
            
        # Объединяем результаты из обеих групп
        dimension_value_map_group2 = {}
        for row in response_group2.rows:
            dimension_value = row.dimension_values[0].value
            dimension_value_map_group2[dimension_value] = row.metric_values
        
        for row1 in response_group1.rows:
            dimension_value = row1.dimension_values[0].value
            
            # Проверяем наличие этого значения измерения во второй группе результатов
            if dimension_value in dimension_value_map_group2:
                metric_values_group2 = dimension_value_map_group2[dimension_value]
                
                # Парсим значения метрик из первой группы
                active_users = int(row1.metric_values[0].value or 0)
                new_users = int(row1.metric_values[1].value or 0)
                total_users = int(row1.metric_values[2].value or 0)
                sessions = int(row1.metric_values[3].value or 0)
                sessions_per_user = float(row1.metric_values[4].value or 0.0)
                engagement_rate = float(row1.metric_values[5].value or 0.0)
                engaged_sessions = int(row1.metric_values[6].value or 0)
                average_session_duration = float(row1.metric_values[7].value or 0.0)
                bounce_rate = float(row1.metric_values[8].value or 0.0)
                
                # Парсим значения метрик из второй группы
                screen_page_views = int(metric_values_group2[0].value or 0)
                screen_page_views_per_session = float(metric_values_group2[1].value or 0.0)
                screen_page_views_per_user = float(metric_values_group2[2].value or 0.0)
                user_engagement_duration = int(metric_values_group2[3].value or 0)
                conversions = int(metric_values_group2[4].value or 0) if len(metric_values_group2) > 4 else 0
                conversion_rate = float(metric_values_group2[5].value or 0.0) if len(metric_values_group2) > 5 else 0.0
                
                # Записываем результат с указанием измерения и его значения
                results.append({
                    "report_date": date_str,
                    "dimension_name": dimension_name,
                    "dimension_value": dimension_value,
                    "active_users": active_users,
                    "new_users": new_users,
                    "total_users": total_users,
                    "sessions": sessions,
                    "sessions_per_user": sessions_per_user,
                    "engagement_rate": engagement_rate,
                    "engaged_sessions": engaged_sessions,
                    "average_session_duration": average_session_duration,
                    "bounce_rate": bounce_rate,
                    "screen_page_views": screen_page_views,
                    "screen_page_views_per_session": screen_page_views_per_session,
                    "screen_page_views_per_user": screen_page_views_per_user,
                    "user_engagement_duration": user_engagement_duration,
                    "conversions": conversions,
                    "conversion_rate": conversion_rate
                })
            
        logger.info(f"Получено {len(results)} результатов для измерения {dimension_name} за {date_str}")
        return results
        
    except Exception as e:
        logger.error(f"Ошибка при получении данных для измерения {dimension_name} за {date_str}: {e}")
        return []

def fetch_ga4_combined_dimensions(date_str):
    """
    Получение данных для комбинации нескольких измерений.
    Функция собирает данные по deviceCategory, operatingSystem, browser за один запрос.
    """
    client = BetaAnalyticsDataClient()
    results = []
    logger.info(f"Получение комбинированных данных по основным измерениям за {date_str}")
    
    try:
        # Определяем набор основных измерений для комбинированного отчета
        combined_dimensions = [
            "deviceCategory",
            "operatingSystem",
            "browser",
            "platform",
            "language",
            "screenResolution",
            "mobileDeviceBranding",
            "mobileDeviceModel",
            "appVersion"
        ]
        
        # Запрос первой группы метрик
        request_group1 = RunReportRequest(
            property=PROPERTY_ID,
            date_ranges=[DateRange(start_date=date_str, end_date=date_str)],
            dimensions=[Dimension(name=dim) for dim in combined_dimensions],
            metrics=[Metric(name=metric) for metric in METRICS_GROUP_1],
            limit=10000  # Увеличиваем лимит для получения большего количества комбинаций
        )
        response_group1 = client.run_report(request_group1)
        
        # Если нет данных, возвращаем пустой список
        if not response_group1.rows:
            logger.warning(f"Нет комбинированных данных за {date_str}")
            return []
        
        # Запрос второй группы метрик
        request_group2 = RunReportRequest(
            property=PROPERTY_ID,
            date_ranges=[DateRange(start_date=date_str, end_date=date_str)],
            dimensions=[Dimension(name=dim) for dim in combined_dimensions],
            metrics=[Metric(name=metric) for metric in METRICS_GROUP_2],
            limit=10000
        )
        response_group2 = client.run_report(request_group2)
        
        # Создаем карту для второй группы метрик
        metrics_group2_map = {}
        for row in response_group2.rows:
            # Создаем ключ из комбинации значений измерений
            key_parts = [value.value for value in row.dimension_values]
            key = "|".join(key_parts)
            metrics_group2_map[key] = row.metric_values
        
        # Обрабатываем данные из первой группы и дополняем их данными из второй группы
        for row1 in response_group1.rows:
            # Извлекаем значения измерений
            device_category = row1.dimension_values[0].value
            operating_system = row1.dimension_values[1].value
            browser = row1.dimension_values[2].value
            platform = row1.dimension_values[3].value
            language = row1.dimension_values[4].value
            screen_resolution = row1.dimension_values[5].value
            mobile_device_brand = row1.dimension_values[6].value
            mobile_device_model = row1.dimension_values[7].value
            app_version = row1.dimension_values[8].value
            
            # Создаем ключ для поиска соответствующих метрик во второй группе
            key_parts = [device_category, operating_system, browser, platform, language, screen_resolution, 
                         mobile_device_brand, mobile_device_model, app_version]
            key = "|".join(key_parts)
            
            # Проверяем, есть ли данные во второй группе для этой комбинации измерений
            if key in metrics_group2_map:
                metric_values_group2 = metrics_group2_map[key]
                
                # Парсим значения метрик из первой группы
                active_users = int(row1.metric_values[0].value or 0)
                new_users = int(row1.metric_values[1].value or 0)
                total_users = int(row1.metric_values[2].value or 0)
                sessions = int(row1.metric_values[3].value or 0)
                sessions_per_user = float(row1.metric_values[4].value or 0.0)
                engagement_rate = float(row1.metric_values[5].value or 0.0)
                engaged_sessions = int(row1.metric_values[6].value or 0)
                average_session_duration = float(row1.metric_values[7].value or 0.0)
                bounce_rate = float(row1.metric_values[8].value or 0.0)
                
                # Парсим значения метрик из второй группы
                screen_page_views = int(metric_values_group2[0].value or 0)
                screen_page_views_per_session = float(metric_values_group2[1].value or 0.0)
                screen_page_views_per_user = float(metric_values_group2[2].value or 0.0)
                user_engagement_duration = int(metric_values_group2[3].value or 0)
                
                # Записываем результат для этой комбинации измерений
                results.append((
                    date_str,
                    device_category,
                    browser,
                    operating_system,
                    "",  # operating_system_version - пустой, т.к. используем operatingSystem, а не operatingSystemWithVersion
                    platform,
                    language,
                    screen_resolution,
                    mobile_device_model,
                    mobile_device_brand,
                    app_version,
                    active_users,
                    new_users,
                    total_users,
                    sessions,
                    sessions_per_user,
                    engagement_rate,
                    engaged_sessions,
                    average_session_duration,
                    bounce_rate,
                    screen_page_views,
                    screen_page_views_per_session,
                    screen_page_views_per_user,
                    user_engagement_duration
                ))
        
        logger.info(f"Получено {len(results)} комбинированных результатов за {date_str}")
        return results
        
    except Exception as e:
        logger.error(f"Ошибка при получении комбинированных данных за {date_str}: {e}")
        return []

def fetch_mobile_device_details(date_str):
    """
    Отдельная функция для получения детальных данных по мобильным устройствам.
    Сочетает mobileDeviceModel, mobileDeviceBranding и deviceCategory для мобильных устройств.
    """
    client = BetaAnalyticsDataClient()
    results = []
    logger.info(f"Получение детальных данных по мобильным устройствам за {date_str}")
    
    try:
        # Запрос первой группы метрик
        request_group1 = RunReportRequest(
            property=PROPERTY_ID,
            date_ranges=[DateRange(start_date=date_str, end_date=date_str)],
            dimensions=[
                Dimension(name="deviceCategory"),
                Dimension(name="mobileDeviceBranding"),
                Dimension(name="mobileDeviceModel")
            ],
            metrics=[Metric(name=metric) for metric in METRICS_GROUP_1]
        )
        response_group1 = client.run_report(request_group1)
        
        # Если нет данных, возвращаем пустой список
        if not response_group1.rows:
            logger.warning(f"Нет данных по мобильным устройствам за {date_str}")
            return []
        
        # Запрос второй группы метрик
        request_group2 = RunReportRequest(
            property=PROPERTY_ID,
            date_ranges=[DateRange(start_date=date_str, end_date=date_str)],
            dimensions=[
                Dimension(name="deviceCategory"),
                Dimension(name="mobileDeviceBranding"),
                Dimension(name="mobileDeviceModel")
            ],
            metrics=[Metric(name=metric) for metric in METRICS_GROUP_2]
        )
        response_group2 = client.run_report(request_group2)
        
        # Создаем карту для второй группы метрик
        metrics_group2_map = {}
        for row in response_group2.rows:
            key = f"{row.dimension_values[0].value}|{row.dimension_values[1].value}|{row.dimension_values[2].value}"
            metrics_group2_map[key] = row.metric_values
        
        # Обрабатываем данные из первой группы и дополняем их данными из второй группы
        for row1 in response_group1.rows:
            device_category = row1.dimension_values[0].value
            mobile_device_brand = row1.dimension_values[1].value
            mobile_device_model = row1.dimension_values[2].value
            
            # Ключ для поиска во второй группе метрик
            key = f"{device_category}|{mobile_device_brand}|{mobile_device_model}"
            
            # Проверяем, есть ли данные во второй группе
            if key in metrics_group2_map:
                metric_values_group2 = metrics_group2_map[key]
                
                # Парсим значения метрик из первой группы
                active_users = int(row1.metric_values[0].value or 0)
                new_users = int(row1.metric_values[1].value or 0)
                total_users = int(row1.metric_values[2].value or 0)
                sessions = int(row1.metric_values[3].value or 0)
                sessions_per_user = float(row1.metric_values[4].value or 0.0)
                engagement_rate = float(row1.metric_values[5].value or 0.0)
                engaged_sessions = int(row1.metric_values[6].value or 0)
                average_session_duration = float(row1.metric_values[7].value or 0.0)
                bounce_rate = float(row1.metric_values[8].value or 0.0)
                
                # Парсим значения метрик из второй группы
                screen_page_views = int(metric_values_group2[0].value or 0)
                screen_page_views_per_session = float(metric_values_group2[1].value or 0.0)
                screen_page_views_per_user = float(metric_values_group2[2].value or 0.0)
                user_engagement_duration = int(metric_values_group2[3].value or 0)
                
                # Записываем результат для мобильных устройств
                results.append((
                    date_str,
                    device_category,
                    "",  # browser - пустой для этого отчета
                    "",  # operating_system - пустой для этого отчета
                    "",  # operating_system_version - пустой для этого отчета
                    "",  # platform - пустой для этого отчета
                    "",  # language - пустой для этого отчета
                    "",  # screen_resolution - пустой для этого отчета
                    mobile_device_model,
                    mobile_device_brand,
                    "",  # app_version - пустой для этого отчета
                    active_users,
                    new_users,
                    total_users,
                    sessions,
                    sessions_per_user,
                    engagement_rate,
                    engaged_sessions,
                    average_session_duration,
                    bounce_rate,
                    screen_page_views,
                    screen_page_views_per_session,
                    screen_page_views_per_user,
                    user_engagement_duration
                ))
        
        logger.info(f"Получено {len(results)} результатов по мобильным устройствам за {date_str}")
        return results
        
    except Exception as e:
        logger.error(f"Ошибка при получении данных по мобильным устройствам за {date_str}: {e}")
        return []

def fetch_app_version_data(date_str):
    """
    Отдельная функция для получения данных по appVersion.
    Позволяет анализировать метрики в разрезе версий приложения.
    """
    client = BetaAnalyticsDataClient()
    results = []
    logger.info(f"Получение данных по версиям приложения за {date_str}")
    
    try:
        # Запрос первой группы метрик
        request_group1 = RunReportRequest(
            property=PROPERTY_ID,
            date_ranges=[DateRange(start_date=date_str, end_date=date_str)],
            dimensions=[
                Dimension(name="platform"),
                Dimension(name="appVersion")
            ],
            metrics=[Metric(name=metric) for metric in METRICS_GROUP_1]
        )
        response_group1 = client.run_report(request_group1)
        
        # Если нет данных, возвращаем пустой список
        if not response_group1.rows:
            logger.warning(f"Нет данных по версиям приложения за {date_str}")
            return []
        
        # Запрос второй группы метрик
        request_group2 = RunReportRequest(
            property=PROPERTY_ID,
            date_ranges=[DateRange(start_date=date_str, end_date=date_str)],
            dimensions=[
                Dimension(name="platform"),
                Dimension(name="appVersion")
            ],
            metrics=[Metric(name=metric) for metric in METRICS_GROUP_2]
        )
        response_group2 = client.run_report(request_group2)
        
        # Создаем карту для второй группы метрик
        metrics_group2_map = {}
        for row in response_group2.rows:
            key = f"{row.dimension_values[0].value}|{row.dimension_values[1].value}"
            metrics_group2_map[key] = row.metric_values
        
        # Обрабатываем данные из первой группы и дополняем их данными из второй группы
        for row1 in response_group1.rows:
            platform = row1.dimension_values[0].value
            app_version = row1.dimension_values[1].value
            
            # Ключ для поиска во второй группе метрик
            key = f"{platform}|{app_version}"
            
            # Проверяем, есть ли данные во второй группе
            if key in metrics_group2_map:
                metric_values_group2 = metrics_group2_map[key]
                
                # Парсим значения метрик из первой группы
                active_users = int(row1.metric_values[0].value or 0)
                new_users = int(row1.metric_values[1].value or 0)
                total_users = int(row1.metric_values[2].value or 0)
                sessions = int(row1.metric_values[3].value or 0)
                sessions_per_user = float(row1.metric_values[4].value or 0.0)
                engagement_rate = float(row1.metric_values[5].value or 0.0)
                engaged_sessions = int(row1.metric_values[6].value or 0)
                average_session_duration = float(row1.metric_values[7].value or 0.0)
                bounce_rate = float(row1.metric_values[8].value or 0.0)
                
                # Парсим значения метрик из второй группы
                screen_page_views = int(metric_values_group2[0].value or 0)
                screen_page_views_per_session = float(metric_values_group2[1].value or 0.0)
                screen_page_views_per_user = float(metric_values_group2[2].value or 0.0)
                user_engagement_duration = int(metric_values_group2[3].value or 0)
                
                # Записываем результат по версиям приложения
                results.append((
                    date_str,
                    "",  # device_category - пустой для этого отчета
                    "",  # browser - пустой для этого отчета
                    "",  # operating_system - пустой для этого отчета
                    "",  # operating_system_version - пустой для этого отчета
                    platform,
                    "",  # language - пустой для этого отчета
                    "",  # screen_resolution - пустой для этого отчета
                    "",  # mobile_device_model - пустой для этого отчета
                    "",  # mobile_device_brand - пустой для этого отчета
                    app_version,
                    active_users,
                    new_users,
                    total_users,
                    sessions,
                    sessions_per_user,
                    engagement_rate,
                    engaged_sessions,
                    average_session_duration,
                    bounce_rate,
                    screen_page_views,
                    screen_page_views_per_session,
                    screen_page_views_per_user,
                    user_engagement_duration
                ))
        
        logger.info(f"Получено {len(results)} результатов по версиям приложения за {date_str}")
        return results
        
    except Exception as e:
        logger.error(f"Ошибка при получении данных по версиям приложения за {date_str}: {e}")
        return []

def fetch_operating_system_with_version(date_str):
    """
    Отдельная функция для получения данных по operatingSystemWithVersion.
    Этот отчет выполняется отдельно от основного комбинированного отчета.
    """
    client = BetaAnalyticsDataClient()
    results = []
    logger.info(f"Получение данных по операционной системе с версией за {date_str}")
    
    try:
        # Запрос первой группы метрик
        request_group1 = RunReportRequest(
            property=PROPERTY_ID,
            date_ranges=[DateRange(start_date=date_str, end_date=date_str)],
            dimensions=[
                Dimension(name="deviceCategory"),
                Dimension(name="operatingSystemWithVersion")
            ],
            metrics=[Metric(name=metric) for metric in METRICS_GROUP_1]
        )
        response_group1 = client.run_report(request_group1)
        
        # Если нет данных, возвращаем пустой список
        if not response_group1.rows:
            logger.warning(f"Нет данных по операционной системе с версией за {date_str}")
            return []
        
        # Запрос второй группы метрик
        request_group2 = RunReportRequest(
            property=PROPERTY_ID,
            date_ranges=[DateRange(start_date=date_str, end_date=date_str)],
            dimensions=[
                Dimension(name="deviceCategory"),
                Dimension(name="operatingSystemWithVersion")
            ],
            metrics=[Metric(name=metric) for metric in METRICS_GROUP_2]
        )
        response_group2 = client.run_report(request_group2)
        
        # Создаем карту для второй группы метрик
        metrics_group2_map = {}
        for row in response_group2.rows:
            key = f"{row.dimension_values[0].value}|{row.dimension_values[1].value}"
            metrics_group2_map[key] = row.metric_values
        
        # Обрабатываем данные из первой группы и дополняем их данными из второй группы
        for row1 in response_group1.rows:
            device_category = row1.dimension_values[0].value
            os_with_version = row1.dimension_values[1].value
            
            # Разделяем operatingSystemWithVersion на OS и версию
            parts = os_with_version.split(' ')
            if len(parts) > 1:
                operating_system = parts[0]
                os_version = ' '.join(parts[1:])
            else:
                operating_system = os_with_version
                os_version = ""
            
            # Ключ для поиска во второй группе метрик
            key = f"{device_category}|{os_with_version}"
            
            # Проверяем, есть ли данные во второй группе
            if key in metrics_group2_map:
                metric_values_group2 = metrics_group2_map[key]
                
                # Парсим значения метрик из первой группы
                active_users = int(row1.metric_values[0].value or 0)
                new_users = int(row1.metric_values[1].value or 0)
                total_users = int(row1.metric_values[2].value or 0)
                sessions = int(row1.metric_values[3].value or 0)
                sessions_per_user = float(row1.metric_values[4].value or 0.0)
                engagement_rate = float(row1.metric_values[5].value or 0.0)
                engaged_sessions = int(row1.metric_values[6].value or 0)
                average_session_duration = float(row1.metric_values[7].value or 0.0)
                bounce_rate = float(row1.metric_values[8].value or 0.0)
                
                # Парсим значения метрик из второй группы
                screen_page_views = int(metric_values_group2[0].value or 0)
                screen_page_views_per_session = float(metric_values_group2[1].value or 0.0)
                screen_page_views_per_user = float(metric_values_group2[2].value or 0.0)
                user_engagement_duration = int(metric_values_group2[3].value or 0)
                
                # Записываем результат с версией ОС
                results.append((
                    date_str,
                    device_category,
                    "",  # browser - пустой для этого отчета
                    operating_system,
                    os_version,
                    "",  # platform - пустой для этого отчета
                    "",  # language - пустой для этого отчета
                    "",  # screen_resolution - пустой для этого отчета
                    "",  # mobile_device_model - пустой для этого отчета
                    "",  # mobile_device_brand - пустой для этого отчета
                    "",  # app_version - пустой для этого отчета
                    active_users,
                    new_users,
                    total_users,
                    sessions,
                    sessions_per_user,
                    engagement_rate,
                    engaged_sessions,
                    average_session_duration,
                    bounce_rate,
                    screen_page_views,
                    screen_page_views_per_session,
                    screen_page_views_per_user,
                    user_engagement_duration
                ))
        
        logger.info(f"Получено {len(results)} результатов по операционной системе с версией за {date_str}")
        return results
        
    except Exception as e:
        logger.error(f"Ошибка при получении данных по операционной системе с версией за {date_str}: {e}")
        return []

def fetch_and_load_platform_device_data():
    """
    Основная функция получения и загрузки данных по Platform/Device 
    за период с START_DATE до END_DATE.
    """
    # Сначала создаем таблицу, чтобы гарантировать её существование
    create_staging_table()
    logger.info("Таблица создана/проверена перед загрузкой данных")
    
    current_date = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_date = datetime.strptime(END_DATE, "%Y-%m-%d")
    
    logger.info(f"Начало получения данных с {START_DATE} по {END_DATE}")
    
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        all_results = []
        
        # Получаем комбинированные данные по основным измерениям
        combined_results = fetch_ga4_combined_dimensions(date_str)
        all_results.extend(combined_results)
        
        # Получаем данные по operatingSystemWithVersion
        os_version_results = fetch_operating_system_with_version(date_str)
        all_results.extend(os_version_results)
        
        # Получаем детальные данные по мобильным устройствам
        mobile_device_results = fetch_mobile_device_details(date_str)
        all_results.extend(mobile_device_results)
        
        # Получаем данные по версиям приложения
        app_version_results = fetch_app_version_data(date_str)
        all_results.extend(app_version_results)
        
        # Загружаем полученные данные в PostgreSQL
        if all_results:
            load_data_to_postgres(all_results)
            logger.info(f"Успешно загружено {len(all_results)} записей за {date_str}")
        else:
            logger.warning(f"Нет данных для загрузки за {date_str}")
        
        current_date += timedelta(days=1)
    
    logger.info("Завершено получение и загрузка данных по Platform/Device")

def load_data_to_postgres(data):
    """Загрузка данных в PostgreSQL."""
    if not data:
        logger.warning("Нет данных для загрузки в PostgreSQL")
        return
    
    # Убедимся, что таблица существует
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor()
        cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'staging' AND table_name = 'ga4_platform_device_metrics');")
        table_exists = cur.fetchone()[0]
        
        if not table_exists:
            logger.warning("Таблица staging.ga4_platform_device_metrics не существует. Создаем...")
            create_staging_table()
            logger.info("Таблица staging.ga4_platform_device_metrics создана")
        
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Ошибка при проверке/создании таблицы: {e}")
        return
    
    # Запрос для вставки/обновления данных
    insert_query = """
    INSERT INTO staging.ga4_platform_device_metrics (
        report_date,
        device_category,
        browser,
        operating_system,
        operating_system_version,
        platform,
        language,
        screen_resolution,
        mobile_device_model,
        mobile_device_brand,
        app_version,
        active_users,
        new_users,
        total_users,
        sessions,
        sessions_per_user,
        engagement_rate,
        engaged_sessions,
        average_session_duration,
        bounce_rate,
        screen_page_views,
        screen_page_views_per_session,
        screen_page_views_per_user,
        user_engagement_duration
    ) VALUES %s
    """

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()
    
    try:
        # Обрабатываем данные партиями для оптимизации
        batch_size = 1000
        for i in range(0, len(data), batch_size):
            batch = data[i:i+batch_size]
            psycopg2.extras.execute_values(cur, insert_query, batch)
            conn.commit()
            logger.info(f"Загружена партия {i//batch_size + 1} ({len(batch)} записей)")
        
        logger.info(f"Всего загружено: {len(data)} записей в staging.ga4_platform_device_metrics")
    except Exception as e:
        conn.rollback()
        logger.error(f"Ошибка загрузки данных: {e}")
    finally:
        cur.close()
        conn.close()

# Создаем DAG
with DAG(
    dag_id="GA4_PLATFORM_DEVICE_2025",
    default_args={
        'owner': 'semukhin',
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    },
    description="Импорт метрик и измерений Platform/Device из GA4 в PostgreSQL",
    schedule_interval='@daily',
    start_date=datetime(2025, 2, 25),
    catchup=False,
) as dag:

    fetch_and_load_data = PythonOperator(
        task_id="fetch_and_load_platform_device_data",
        python_callable=fetch_and_load_platform_device_data
    )