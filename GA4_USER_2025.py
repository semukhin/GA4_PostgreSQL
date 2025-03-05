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
    """Создание таблицы в схеме staging."""
    query = """
    CREATE SCHEMA IF NOT EXISTS staging;

    CREATE TABLE IF NOT EXISTS staging.ga4_user_metrics (
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

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()

def fetch_ga4_metrics(date, dimensions_list, metrics_list):
    """Вспомогательная функция для запроса метрик из GA4 за конкретный день."""
    client = BetaAnalyticsDataClient()
    request = RunReportRequest(
        property=PROPERTY_ID,
        date_ranges=[DateRange(start_date=date, end_date=date)],
        metrics=[Metric(name=metric) for metric in metrics_list],
        dimensions=[Dimension(name=dimension) for dimension in dimensions_list]
    )
    response = client.run_report(request)
    return response.rows

def fetch_ga4_user_metrics():
    """Основная функция для получения всех метрик за весь период с разбиением по дням."""
    dimensions = [
        "deviceCategory", 
        "platform", 
        "browser", 
        "operatingSystem", 
        "country", 
        "newVsReturning"
    ]
    
    metrics_part1 = [
        "activeUsers", "newUsers", "totalUsers", "userEngagementDuration", "userKeyEventRate",
        "active1DayUsers", "active7DayUsers", "active28DayUsers", "dauPerWau", "dauPerMau"
    ]

    metrics_part2 = [
        "wauPerMau", "firstTimePurchasers", "firstTimePurchaserRate", "totalPurchasers", 
        "firstTimePurchasersPerNewUser", "purchaserRate", "averageRevenuePerUser",
        "averagePurchaseRevenue", "averagePurchaseRevenuePerUser"
    ]

    results = []
    current_date = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_date = datetime.strptime(END_DATE, "%Y-%m-%d")

    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        print(f"Получение данных за {date_str}")

        # Выполняем два отдельных запроса для метрик
        rows_part1 = fetch_ga4_metrics(date_str, dimensions, metrics_part1)
        rows_part2 = fetch_ga4_metrics(date_str, dimensions, metrics_part2)

        # Создаем словарь для объединения данных из двух запросов
        combined_data = {}
        
        # Обрабатываем первую часть метрик
        for row in rows_part1:
            dimension_key = tuple(dim.value for dim in row.dimension_values)
            metric_values = [metric.value for metric in row.metric_values]
            combined_data[dimension_key] = metric_values
        
        # Обрабатываем вторую часть метрик и объединяем с первой
        for row in rows_part2:
            dimension_key = tuple(dim.value for dim in row.dimension_values)
            metric_values = [metric.value for metric in row.metric_values]
            
            if dimension_key in combined_data:
                combined_data[dimension_key].extend(metric_values)
            else:
                # Если ключ не найден, создаем пустые значения для первой части метрик
                combined_data[dimension_key] = ["0"] * len(metrics_part1) + metric_values
        
        # Формируем итоговый результат
        for dimension_key, metric_values in combined_data.items():
            # Если есть неполные данные (отсутствует вторая часть), добавляем нули
            if len(metric_values) < len(metrics_part1) + len(metrics_part2):
                metric_values.extend(["0"] * (len(metrics_part1) + len(metrics_part2) - len(metric_values)))
                
            results.append((
                date_str,
                int(metric_values[0] or 0),  # activeUsers
                int(metric_values[1] or 0),  # newUsers
                int(metric_values[2] or 0),  # totalUsers
                int(metric_values[3] or 0),  # userEngagementDuration
                float(metric_values[4] or 0.0),  # userKeyEventRate
                int(metric_values[5] or 0),  # active1DayUsers
                int(metric_values[6] or 0),  # active7DayUsers
                int(metric_values[7] or 0),  # active28DayUsers
                float(metric_values[8] or 0.0),  # dauPerWau
                float(metric_values[9] or 0.0),  # dauPerMau
                float(metric_values[10] or 0.0),  # wauPerMau
                int(metric_values[11] or 0),  # firstTimePurchasers
                float(metric_values[12] or 0.0),  # firstTimePurchaserRate
                int(metric_values[13] or 0),  # totalPurchasers
                float(metric_values[14] or 0.0),  # firstTimePurchasersPerNewUser
                float(metric_values[15] or 0.0),  # purchaserRate
                float(metric_values[16] or 0.0),  # averageRevenuePerUser
                float(metric_values[17] or 0.0),  # averagePurchaseRevenue
                float(metric_values[18] or 0.0),  # averagePurchaseRevenuePerUser
                dimension_key[0],  # device_category
                dimension_key[1],  # platform
                dimension_key[2],  # browser
                dimension_key[3],  # operating_system
                dimension_key[4],  # country
                dimension_key[5]   # new_vs_returning
            ))

        current_date += timedelta(days=1)

    return results

def load_data_to_postgres():
    """Загрузка данных в PostgreSQL."""
    data = fetch_ga4_user_metrics()
    if not data:
        print("Нет данных для загрузки.")
        return

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
    
    print(f"Всего строк: {len(data)}, после удаления дубликатов: {len(deduplicated_data)}")

    # Разбиваем данные на пакеты по 1000 записей, чтобы избежать переполнения
    batch_size = 1000
    for i in range(0, len(deduplicated_data), batch_size):
        batch = deduplicated_data[i:i+batch_size]
        
        insert_query = """
        INSERT INTO staging.ga4_user_metrics (
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

        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor()

        psycopg2.extras.execute_values(cur, insert_query, batch)
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"Загружен пакет {i//batch_size + 1}, записей: {len(batch)}")

# Создаем DAG
with DAG(
    dag_id="GA4_USER_2025",
    default_args={
        'owner': 'semukhin',
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    },
    description="Импорт данных USER из GA4 в PostgreSQL за весь период с 01.09.2024 по текущую дату",
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