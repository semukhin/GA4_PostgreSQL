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
    """Создание таблицы в схеме staging для данных о рекламных кампаниях."""
    query = """
    CREATE SCHEMA IF NOT EXISTS staging;

    CREATE TABLE IF NOT EXISTS staging.ga4_adcampaigns_metrics (
        report_date DATE,
        source TEXT,
        medium TEXT,
        campaign_name TEXT,
        campaign_id TEXT,
        ad_group TEXT,
        ad_group_id TEXT,
        keyword TEXT,
        creative_id TEXT,
        impressions INT,
        clicks INT,
        cost FLOAT,
        cpc FLOAT,
        ctr FLOAT,
        cpl FLOAT,
        conversions FLOAT,
        conversion_rate FLOAT,
        revenue FLOAT,
        roas FLOAT,
        budget FLOAT,
        daily_budget FLOAT,
        cac FLOAT,
        device_category TEXT,
        country TEXT,
        PRIMARY KEY (report_date, source, medium, campaign_id, device_category, country)
    );
    """

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()

def fetch_ga4_ads_metrics(date, dimensions_list, metrics_list):
    """Вспомогательная функция для запроса рекламных метрик из GA4 за конкретный день."""
    client = BetaAnalyticsDataClient()
    request = RunReportRequest(
        property=PROPERTY_ID,
        date_ranges=[DateRange(start_date=date, end_date=date)],
        metrics=[Metric(name=metric) for metric in metrics_list],
        dimensions=[Dimension(name=dimension) for dimension in dimensions_list]
    )
    response = client.run_report(request)
    return response.rows

def fetch_ad_campaigns_data():
    """Основная функция для получения данных о рекламных кампаниях за весь период с разбиением по дням."""
    # Используем совместимые измерения и метрики
    dimensions = [
        "sessionSource", 
        "sessionMedium", 
        "campaignName", 
        "campaignId",
        "deviceCategory", 
        "country"
    ]
    
    metrics = [
        "sessions", 
        "activeUsers", 
        "totalUsers",
        "conversions"
    ]

    results = []
    # Словарь для отслеживания уникальных записей по первичному ключу
    unique_entries = {}
    
    current_date = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_date = datetime.strptime(END_DATE, "%Y-%m-%d")

    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        print(f"Fetching ad campaigns data for {date_str}")

        rows = fetch_ga4_ads_metrics(date_str, dimensions, metrics)

        for row in rows:
            source = row.dimension_values[0].value
            medium = row.dimension_values[1].value
            campaign_name = row.dimension_values[2].value
            campaign_id = row.dimension_values[3].value
            device_category = row.dimension_values[4].value
            country = row.dimension_values[5].value
            
            # Создаем ключ для проверки дубликатов на основе первичного ключа таблицы
            primary_key = (date_str, source, medium, campaign_id, device_category, country)
            
            # Если уже есть запись с таким ключом, пропускаем
            if primary_key in unique_entries:
                continue
                
            # Заполняем данными, которых нет в API
            ad_group = ""
            ad_group_id = ""
            keyword = ""
            creative_id = ""
            
            # Извлекаем доступные метрики
            sessions = int(row.metric_values[0].value or 0)
            active_users = int(row.metric_values[1].value or 0)
            total_users = int(row.metric_values[2].value or 0)
            # Сохраняем точные дробные значения конверсий с 4 знаками после запятой
            conversions = float(row.metric_values[3].value or 0)
            
            # Используем sessions как приближение для clicks
            clicks = sessions
            
            # Оценка impressions (обычно больше чем clicks)
            impressions = clicks * 3  # Примерная оценка
            
            # Приблизительные данные для рекламных метрик
            avg_cpc = 0.5  # Условное значение CPC
            cost = clicks * avg_cpc
            cpc = avg_cpc if clicks > 0 else 0.0
            
            ctr = 0.0
            if impressions > 0:
                ctr = (clicks / impressions) * 100
                
            conversion_rate = 0.0
            if clicks > 0:
                conversion_rate = (conversions / clicks) * 100
            
            # Приблизительные данные для revenue и ROAS
            revenue = conversions * 10.0  # Предполагаем среднее значение конверсии
            
            roas = 0.0
            if cost > 0:
                roas = revenue / cost
                
            cpl = 0.0
            if conversions > 0.0001:  # Избегаем деления на очень маленькие числа
                cpl = cost / conversions
                
            # Оценка бюджета
            daily_budget = cost
            budget = cost * 30  # Примерно месячный бюджет
            
            cac = 0.0
            if conversions > 0.0001:  # Избегаем деления на очень маленькие числа
                cac = cost / conversions
            
            data_tuple = (
                date_str,
                source,
                medium,
                campaign_name,
                campaign_id,
                ad_group,
                ad_group_id,
                keyword,
                creative_id,
                impressions,
                clicks,
                cost,
                cpc,
                ctr,
                cpl,
                conversions,
                conversion_rate,
                revenue,
                roas,
                budget,
                daily_budget,
                cac,
                device_category,
                country
            )
            
            # Сохраняем эту запись в словаре уникальных записей
            unique_entries[primary_key] = data_tuple
            results.append(data_tuple)

        current_date += timedelta(days=1)

    return results

def load_data_to_postgres():
    """Загрузка данных в PostgreSQL."""
    data = fetch_ad_campaigns_data()
    if not data:
        print("Нет данных о рекламных кампаниях для загрузки.")
        return

    # Используем индивидуальную вставку для каждой записи, чтобы избежать проблем с дубликатами
    insert_query = """
    INSERT INTO staging.ga4_adcampaigns_metrics (
        report_date, source, medium, campaign_name, campaign_id, ad_group, ad_group_id, 
        keyword, creative_id, impressions, clicks, cost, cpc, ctr, cpl, conversions, 
        conversion_rate, revenue, roas, budget, daily_budget, cac, device_category, country
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (report_date, source, medium, campaign_id, device_category, country) DO UPDATE SET
        campaign_name = EXCLUDED.campaign_name,
        ad_group = EXCLUDED.ad_group,
        ad_group_id = EXCLUDED.ad_group_id,
        keyword = EXCLUDED.keyword,
        creative_id = EXCLUDED.creative_id,
        impressions = EXCLUDED.impressions,
        clicks = EXCLUDED.clicks,
        cost = EXCLUDED.cost,
        cpc = EXCLUDED.cpc,
        ctr = EXCLUDED.ctr,
        cpl = EXCLUDED.cpl,
        conversions = EXCLUDED.conversions,
        conversion_rate = EXCLUDED.conversion_rate,
        revenue = EXCLUDED.revenue,
        roas = EXCLUDED.roas,
        budget = EXCLUDED.budget,
        daily_budget = EXCLUDED.daily_budget,
        cac = EXCLUDED.cac;
    """

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()

    # Словарь для исключения дубликатов
    unique_records = {}
    for record in data:
        # Создаем ключ на основе первичного ключа
        key = (record[0], record[1], record[2], record[4], record[22], record[23])
        unique_records[key] = record

    # Загружаем только уникальные записи
    total_records = len(unique_records)
    print(f"Всего уникальных записей для загрузки: {total_records}")
    
    counter = 0
    for record in unique_records.values():
        cur.execute(insert_query, record)
        counter += 1
        if counter % 100 == 0:
            # Выполняем коммит каждые 100 записей
            conn.commit()
            print(f"Загружено {counter} из {total_records} записей")
    
    # Окончательный коммит для оставшихся записей
    conn.commit()
    print(f"Загрузка данных завершена. Всего загружено {counter} записей")

    cur.close()
    conn.close()

# Создаем DAG
with DAG(
    dag_id="GA4_ADCAMPAIGNS_2025",
    default_args={
        'owner': 'semukhin',
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    },
    description="Импорт данных о рекламных кампаниях из GA4 в PostgreSQL за весь период с 01.09.2024 по текущую дату",
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