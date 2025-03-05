from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import psycopg2
import psycopg2.extras
import logging
from google.analytics.data_v1beta import BetaAnalyticsDataClient, RunReportRequest
from google.analytics.data_v1beta.types import DateRange, Metric, Dimension
import traceback

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('GA4_USER_DAG')

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

# Настройка дат для сбора данных за весь период
START_DATE = "2024-09-01"
END_DATE = datetime.today().strftime("%Y-%m-%d")

def create_staging_table():
    """Создание таблицы в схеме staging."""
    logger.info("Начинаем создание/проверку таблицы в схеме staging")
    
    query = """
    CREATE SCHEMA IF NOT EXISTS staging;

    CREATE TABLE IF NOT EXISTS staging.ga4_user_metrics_new (
        report_date DATE,
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
        first_time_purchasers INT,
        first_time_purchaser_rate FLOAT,
        total_purchasers INT,
        first_time_purchasers_per_new_user FLOAT,
        purchaser_rate FLOAT,
        average_revenue_per_user FLOAT,
        average_purchase_revenue FLOAT,
        average_purchase_revenue_per_user FLOAT,
        device_category TEXT,
        platform TEXT,
        browser TEXT,
        operating_system TEXT,
        country TEXT,
        new_vs_returning TEXT,
        PRIMARY KEY (report_date, device_category, country, new_vs_returning)
    );
    """

    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        logger.info("Таблица успешно создана/проверена")
    except Exception as e:
        logger.error(f"Ошибка при создании таблицы: {e}")
        raise
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def test_ga4_connection(**context):
    """Проверка соединения с GA4 и наличия данных."""
    logger.info(f"Тестирование соединения с GA4 API за весь период {START_DATE} - {END_DATE}")
    
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
        
        total_active_users = response.rows[0].metric_values[0].value
        logger.info(f"Соединение успешно! Общее количество активных пользователей за весь период: {total_active_users}")
        
        # Сохраним результат в контексте для использования в других задачах
        context['ti'].xcom_push(key='total_active_users', value=total_active_users)
        return True
    
    except Exception as e:
        logger.error(f"Ошибка при тестировании соединения с GA4: {e}")
        logger.error(traceback.format_exc())
        return False

def fetch_ga4_metrics_batch(date, dimensions_list, metrics_list, batch_name=""):
    """Вспомогательная функция для запроса метрик из GA4 за конкретный день."""
    logger.info(f"Запрос данных из GA4 за {date} с {len(dimensions_list)} измерениями и {len(metrics_list)} метриками ({batch_name})")
    
    try:
        client = BetaAnalyticsDataClient()
        request = RunReportRequest(
            property=PROPERTY_ID,
            date_ranges=[DateRange(start_date=date, end_date=date)],
            metrics=[Metric(name=metric) for metric in metrics_list],
            dimensions=[Dimension(name=dimension) for dimension in dimensions_list]
        )
        response = client.run_report(request)
        
        logger.info(f"Получено {len(response.rows)} строк данных для {date} ({batch_name})")
        return response.rows
    
    except Exception as e:
        logger.error(f"Ошибка при запросе данных за {date} ({batch_name}): {e}")
        logger.error(traceback.format_exc())
        return []

def fetch_ga4_user_metrics():
    """Основная функция для получения всех метрик за весь период с разбиением по дням."""
    logger.info(f"Начинаем сбор данных за весь период с {START_DATE} по {END_DATE}")
    
    # Уменьшаем количество измерений для начала
    # Это позволит получить больше агрегированных данных и меньше нулевых значений
    dimensions = [
        "deviceCategory",
        "country",
        "newVsReturning"
    ]
    
    # Разделяем метрики на два запроса, так как GA4 имеет ограничение на 10 метрик за один запрос
    metrics_batch1 = [
        "activeUsers", "newUsers", "totalUsers", 
        "userEngagementDuration", "userKeyEventRate",
        "active1DayUsers", "active7DayUsers", "active28DayUsers", 
        "dauPerWau", "dauPerMau"
    ]
    
    metrics_batch2 = [
        "wauPerMau",
        "firstTimePurchasers", "firstTimePurchaserRate", "totalPurchasers", 
        "firstTimePurchasersPerNewUser", "purchaserRate", "averageRevenuePerUser",
        "averagePurchaseRevenue", "averagePurchaseRevenuePerUser"
    ]

    results = []
    current_date = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_date = datetime.strptime(END_DATE, "%Y-%m-%d")
    
    days_processed = 0
    total_days = (end_date - current_date).days + 1

    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        logger.info(f"Получение данных за {date_str} [{days_processed+1}/{total_days}]")

        # Получаем данные за текущий день из первого пакета метрик
        rows_batch1 = fetch_ga4_metrics_batch(date_str, dimensions, metrics_batch1, "пакет 1")
        
        # Получаем данные за текущий день из второго пакета метрик
        rows_batch2 = fetch_ga4_metrics_batch(date_str, dimensions, metrics_batch2, "пакет 2")
        
        # Проверка, есть ли данные хотя бы в одном из пакетов
        if not rows_batch1 and not rows_batch2:
            logger.warning(f"Нет данных для {date_str}, переходим к следующему дню")
            # Если данных нет, добавим хотя бы одну строку с нулями для сохранения структуры
            dummy_row = (
                date_str, 0, 0, 0, 0, 0.0, 0, 0, 0, 0.0, 0.0, 0.0, 0, 0.0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 
                "not available", "", "", "", "not available", "not available"
            )
            results.append(dummy_row)
        else:
            # Создаем словарь для объединения данных из двух пакетов по ключу измерений
            combined_data = {}
            
            # Обрабатываем первый пакет метрик
            if rows_batch1:
                for row in rows_batch1:
                    dimension_values = tuple(dim.value for dim in row.dimension_values)
                    metric_values = [metric.value for metric in row.metric_values]
                    combined_data[dimension_values] = (metric_values, [])  # (метрики_пакет1, пустой_список_для_пакет2)
            
            # Обрабатываем второй пакет метрик и объединяем с первым
            if rows_batch2:
                for row in rows_batch2:
                    dimension_values = tuple(dim.value for dim in row.dimension_values)
                    metric_values = [metric.value for metric in row.metric_values]
                    
                    if dimension_values in combined_data:
                        # Добавляем метрики из второго пакета к существующим данным
                        combined_data[dimension_values] = (combined_data[dimension_values][0], metric_values)
                    else:
                        # Если для этого измерения нет данных из первого пакета, создаем пустые значения
                        empty_batch1 = ["0"] * len(metrics_batch1)
                        combined_data[dimension_values] = (empty_batch1, metric_values)
            
            # Преобразуем объединенные данные в строки для БД
            for dimension_values, (batch1_metrics, batch2_metrics) in combined_data.items():
                # Проверяем и заполняем пустые значения для второго пакета
                if not batch2_metrics:
                    batch2_metrics = ["0"] * len(metrics_batch2)
                
                # Объединяем все метрики
                all_metrics = batch1_metrics + batch2_metrics
                
                # Конвертируем строковые значения в числовые
                numeric_metrics = []
                all_metrics_names = metrics_batch1 + metrics_batch2
                
                for i, value in enumerate(all_metrics):
                    try:
                        metric_name = all_metrics_names[i] if i < len(all_metrics_names) else f"unknown_{i}"
                        
                        # Определяем тип метрики (float или int) по её названию
                        if any(float_indicator in metric_name.lower() for float_indicator in ["rate", "per", "average"]):
                            numeric_metrics.append(float(value or 0.0))
                        else:
                            numeric_metrics.append(int(value or 0))
                    except (ValueError, TypeError):
                        logger.warning(f"Проблема с конвертацией значения '{value}' в число для метрики {metric_name}")
                        if any(float_indicator in metric_name.lower() for float_indicator in ["rate", "per", "average"]):
                            numeric_metrics.append(0.0)
                        else:
                            numeric_metrics.append(0)
                
                # Заполняем значения для других измерений, которые не запрашиваем
                platform = ""
                browser = ""
                operating_system = ""
                
                # Подготавливаем кортеж со всеми данными для вставки в БД
                result_row = (
                    date_str,
                    numeric_metrics[0],   # activeUsers
                    numeric_metrics[1],   # newUsers
                    numeric_metrics[2],   # totalUsers
                    numeric_metrics[3],   # userEngagementDuration
                    numeric_metrics[4],   # userKeyEventRate
                    numeric_metrics[5],   # active1DayUsers
                    numeric_metrics[6],   # active7DayUsers
                    numeric_metrics[7],   # active28DayUsers
                    numeric_metrics[8],   # dauPerWau
                    numeric_metrics[9],   # dauPerMau
                    numeric_metrics[10],  # wauPerMau
                    numeric_metrics[11],  # firstTimePurchasers
                    numeric_metrics[12],  # firstTimePurchaserRate
                    numeric_metrics[13],  # totalPurchasers
                    numeric_metrics[14],  # firstTimePurchasersPerNewUser
                    numeric_metrics[15],  # purchaserRate
                    numeric_metrics[16],  # averageRevenuePerUser
                    numeric_metrics[17],  # averagePurchaseRevenue
                    numeric_metrics[18],  # averagePurchaseRevenuePerUser
                    dimension_values[0],  # deviceCategory
                    platform,             # platform
                    browser,              # browser
                    operating_system,     # operatingSystem
                    dimension_values[1],  # country
                    dimension_values[2]   # newVsReturning
                )
                
                results.append(result_row)
        
        current_date += timedelta(days=1)
        days_processed += 1
    
    logger.info(f"Всего собрано {len(results)} строк данных за {days_processed} дней")
    return results

def check_for_valid_data(date):
    """Проверка наличия данных хотя бы для одной метрики."""
    logger.info(f"Проверка наличия данных за {date}")
    
    try:
        client = BetaAnalyticsDataClient()
        request = RunReportRequest(
            property=PROPERTY_ID,
            date_ranges=[DateRange(start_date=date, end_date=date)],
            metrics=[Metric(name="activeUsers")]
        )
        response = client.run_report(request)
        
        if response.rows:
            users_count = response.rows[0].metric_values[0].value
            logger.info(f"За {date} найдено {users_count} активных пользователей")
            return int(users_count) > 0
        
        logger.warning(f"За {date} нет данных об активных пользователях")
        return False
    
    except Exception as e:
        logger.error(f"Ошибка при проверке данных за {date}: {e}")
        return False

def load_data_to_postgres(**context):
    """Загрузка данных в PostgreSQL."""
    logger.info("Начинаем загрузку данных в PostgreSQL")
    
    # Получаем данные
    data = fetch_ga4_user_metrics()
    if not data:
        logger.warning("Нет данных для загрузки.")
        return
    
    logger.info(f"Получено {len(data)} строк данных для загрузки")

    # Удаление дубликатов из данных перед вставкой
    # Создаем словарь, где ключом является комбинация полей первичного ключа
    unique_data = {}
    for row in data:
        # Ключ: (report_date, device_category, country, new_vs_returning)
        key = (row[0], row[20], row[24], row[25])
        # Если есть дубликаты, берем только последнюю запись
        unique_data[key] = row

    # Преобразуем обратно в список
    deduplicated_data = list(unique_data.values())
    
    logger.info(f"После удаления дубликатов: {len(deduplicated_data)} строк")
    
    # Проверка наличия не-нулевых значений
    non_zero_rows = sum(1 for row in deduplicated_data if any(
        isinstance(val, (int, float)) and val > 0 
        for val in row[1:20]  # Проверяем только числовые метрики
    ))
    
    logger.info(f"Строк с ненулевыми значениями: {non_zero_rows} ({(non_zero_rows/len(deduplicated_data))*100:.2f}%)")
    
    if non_zero_rows == 0:
        logger.warning("ВСЕ строки содержат только нулевые значения! Проверьте настройки GA4.")
        
        # Проверим наличие данных для нескольких дней
        sample_dates = [
            END_DATE,  # Сегодня
            (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d"),  # Вчера
            (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")   # Неделю назад
        ]
        
        logger.info("Проверка наличия данных для выборочных дат...")
        for date in sample_dates:
            has_data = check_for_valid_data(date)
            logger.info(f"Дата {date}: {'есть данные' if has_data else 'нет данных'}")

    # Разбиваем данные на пакеты по 1000 записей, чтобы избежать переполнения
    batch_size = 1000
    for i in range(0, len(deduplicated_data), batch_size):
        batch = deduplicated_data[i:i+batch_size]
        
        insert_query = """
        INSERT INTO staging.ga4_user_metrics_new (
            report_date, active_users, new_users, total_users, user_engagement_duration, user_key_event_rate, 
            active_1day_users, active_7day_users, active_28day_users, dau_per_wau, dau_per_mau, wau_per_mau, 
            first_time_purchasers, first_time_purchaser_rate, total_purchasers, first_time_purchasers_per_new_user,
            purchaser_rate, average_revenue_per_user, average_purchase_revenue, average_purchase_revenue_per_user,
            device_category, platform, browser, operating_system, country, new_vs_returning
        ) VALUES %s 
        ON CONFLICT (report_date, device_category, country, new_vs_returning) DO UPDATE SET
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
            wau_per_mau = EXCLUDED.wau_per_mau,
            first_time_purchasers = EXCLUDED.first_time_purchasers,
            first_time_purchaser_rate = EXCLUDED.first_time_purchaser_rate,
            total_purchasers = EXCLUDED.total_purchasers,
            first_time_purchasers_per_new_user = EXCLUDED.first_time_purchasers_per_new_user,
            purchaser_rate = EXCLUDED.purchaser_rate,
            average_revenue_per_user = EXCLUDED.average_revenue_per_user,
            average_purchase_revenue = EXCLUDED.average_purchase_revenue,
            average_purchase_revenue_per_user = EXCLUDED.average_purchase_revenue_per_user,
            platform = EXCLUDED.platform,
            browser = EXCLUDED.browser,
            operating_system = EXCLUDED.operating_system;
        """

        try:
            conn = psycopg2.connect(**POSTGRES_CONFIG)
            cur = conn.cursor()

            psycopg2.extras.execute_values(cur, insert_query, batch)
            conn.commit()
            
            logger.info(f"Загружен пакет {i//batch_size + 1}, записей: {len(batch)}")
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных в PostgreSQL: {e}")
            logger.error(traceback.format_exc())
            raise
        finally:
            if 'cur' in locals():
                cur.close()
            if 'conn' in locals():
                conn.close()

# Создаем DAG
with DAG(
    dag_id="GA4_USER_2025_IMPROVED",
    default_args={
        'owner': 'semukhin',
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    },
    description="Улучшенный импорт данных USER из GA4 в PostgreSQL за весь период с 01.09.2024",
    schedule='@daily',
    start_date=datetime(2025, 2, 16),
    catchup=False,
) as dag:

    # Задача проверки соединения с GA4
    test_connection = PythonOperator(
        task_id="test_ga4_connection",
        python_callable=test_ga4_connection,
        provide_context=True,
    )

    # Задача создания таблицы
    create_table = PythonOperator(
        task_id="create_staging_table",
        python_callable=create_staging_table,
    )

    # Задача загрузки всех данных за весь период
    fetch_and_load_data = PythonOperator(
        task_id="fetch_and_load_data",
        python_callable=load_data_to_postgres,
        provide_context=True,
    )
    
    # Задаем последовательность выполнения
    test_connection >> create_table >> fetch_and_load_data