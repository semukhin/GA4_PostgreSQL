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

# Полный список измерений (23 шт) разбиваем на три части
DIMENSIONS_PART1 = [
    "achievementId", "character", "comparison", "fileExtension", "fileName",
    "groupId", "level", "linkClasses", "linkDomain"
]

DIMENSIONS_PART2 = [
    "linkId", "linkText", "linkUrl", "method", "outbound", "percentScrolled",
    "searchTerm", "testDataFilterId", "testDataFilterName"
]

DIMENSIONS_PART3 = [
    "videoProvider", "videoTitle", "videoUrl", "virtualCurrencyName", "visible"
]

# Метрики разбиты на две группы, как и ранее
METRICS_GROUP1 = [
    "advertiserAdClicks", "advertiserAdCost", "advertiserAdCostPerClick",
    "advertiserAdCostPerKeyEvent", "advertiserAdImpressions", "cartToViewRate",
    "crashAffectedUsers", "crashFreeUsersRate", "organicGoogleSearchAveragePosition",
    "organicGoogleSearchClicks"
]

METRICS_GROUP2 = [
    "organicGoogleSearchClickThroughRate", "organicGoogleSearchImpressions",
    "publisherAdClicks", "publisherAdImpressions", "purchaserRate",
    "purchaseToViewRate", "returnOnAdSpend", "scrolledUsers", "totalAdRevenue"
]

def create_staging_table():
    """
    Создает схему и таблицу для хранения данных OTHER.
    Все измерения и метрики сохраняются в одной таблице.
    """
    query = """
    CREATE SCHEMA IF NOT EXISTS staging;

    CREATE TABLE IF NOT EXISTS staging.ga4_other_metrics (
        report_date DATE,
        -- Измерения
        achievement_id TEXT,
        character TEXT,
        comparison TEXT,
        file_extension TEXT,
        file_name TEXT,
        group_id TEXT,
        level TEXT,
        link_classes TEXT,
        link_domain TEXT,
        link_id TEXT,
        link_text TEXT,
        link_url TEXT,
        method TEXT,
        outbound TEXT,
        percent_scrolled TEXT,
        search_term TEXT,
        test_data_filter_id TEXT,
        test_data_filter_name TEXT,
        video_provider TEXT,
        video_title TEXT,
        video_url TEXT,
        virtual_currency_name TEXT,
        visible TEXT,
        -- Метрики (группа 1)
        advertiser_ad_clicks INTEGER,
        advertiser_ad_cost NUMERIC,
        advertiser_ad_cost_per_click NUMERIC,
        advertiser_ad_cost_per_key_event NUMERIC,
        advertiser_ad_impressions INTEGER,
        cart_to_view_rate NUMERIC,
        crash_affected_users INTEGER,
        crash_free_users_rate NUMERIC,
        organic_google_search_average_position NUMERIC,
        organic_google_search_clicks INTEGER,
        -- Метрики (группа 2)
        organic_google_search_click_through_rate NUMERIC,
        organic_google_search_impressions INTEGER,
        publisher_ad_clicks INTEGER,
        publisher_ad_impressions INTEGER,
        purchaser_rate NUMERIC,
        purchase_to_view_rate NUMERIC,
        return_on_ad_spend NUMERIC,
        scrolled_users INTEGER,
        total_ad_revenue NUMERIC,
        PRIMARY KEY (report_date, achievement_id, character, comparison)
    );
    """
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()

def run_report(dimensions, metrics, date):
    """
    Вспомогательная функция для выполнения запроса к GA4 с указанными измерениями и метриками.
    """
    client = BetaAnalyticsDataClient()
    request = RunReportRequest(
        property=PROPERTY_ID,
        date_ranges=[DateRange(start_date=date, end_date=date)],
        metrics=[Metric(name=m) for m in metrics],
        dimensions=[Dimension(name=d) for d in dimensions]
    )
    response = client.run_report(request)
    return response.rows

def fetch_ga4_other_metrics():
    """
    Для каждого дня делаются 5 запросов:
      • 3 запроса для получения всех 23 измерений, разделенных на три части.
      • 2 запроса для получения двух групп метрик.
    Затем данные объединяются по порядку.
    ВНИМАНИЕ: Объединение по порядку работает, если GA4 возвращает строки в одинаковом порядке для всех запросов.
    """
    results = []
    current_date = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_date_dt = datetime.strptime(END_DATE, "%Y-%m-%d")

    while current_date <= end_date_dt:
        date_str = current_date.strftime("%Y-%m-%d")
        # Запросы по измерениям (каждый запрос возвращает часть измерений)
        rows_dims1 = run_report(DIMENSIONS_PART1, [], date_str)
        rows_dims2 = run_report(DIMENSIONS_PART2, [], date_str)
        rows_dims3 = run_report(DIMENSIONS_PART3, [], date_str)
        # Запросы по метрикам
        rows_metrics1 = run_report([], METRICS_GROUP1, date_str)
        rows_metrics2 = run_report([], METRICS_GROUP2, date_str)

        # Проверяем, что количество строк совпадает во всех запросах
        n = len(rows_dims1)
        if not (n == len(rows_dims2) == len(rows_dims3) == len(rows_metrics1) == len(rows_metrics2)):
            raise ValueError(f"Нарушено соответствие строк для даты {date_str}")

        for i in range(n):
            dims1 = [d.value for d in rows_dims1[i].dimension_values]
            dims2 = [d.value for d in rows_dims2[i].dimension_values]
            dims3 = [d.value for d in rows_dims3[i].dimension_values]
            m1 = [mv.value for mv in rows_metrics1[i].metric_values]
            m2 = [mv.value for mv in rows_metrics2[i].metric_values]

            # Объединяем измерения в один список (порядок должен соответствовать таблице)
            dims = dims1 + dims2 + dims3

            combined_row = (
                date_str,
                dims[0],  dims[1],  dims[2],  dims[3],  dims[4],
                dims[5],  dims[6],  dims[7],  dims[8],
                dims[9],  dims[10], dims[11], dims[12], dims[13], dims[14],
                dims[15], dims[16], dims[17], dims[18], dims[19],
                dims[20], dims[21], dims[22],
                int(m1[0] or 0),                        # advertiserAdClicks
                float(m1[1] or 0.0),                      # advertiserAdCost
                float(m1[2] or 0.0),                      # advertiserAdCostPerClick
                float(m1[3] or 0.0),                      # advertiserAdCostPerKeyEvent
                int(m1[4] or 0),                          # advertiserAdImpressions
                float(m1[5] or 0.0),                      # cartToViewRate
                int(m1[6] or 0),                          # crashAffectedUsers
                float(m1[7] or 0.0),                      # crashFreeUsersRate
                float(m1[8] or 0.0),                      # organicGoogleSearchAveragePosition
                int(m1[9] or 0),                          # organicGoogleSearchClicks
                float(m2[0] or 0.0),                      # organicGoogleSearchClickThroughRate
                int(m2[1] or 0),                          # organicGoogleSearchImpressions
                int(m2[2] or 0),                          # publisherAdClicks
                int(m2[3] or 0),                          # publisherAdImpressions
                float(m2[4] or 0.0),                      # purchaserRate
                float(m2[5] or 0.0),                      # purchaseToViewRate
                float(m2[6] or 0.0),                      # returnOnAdSpend
                int(m2[7] or 0),                          # scrolledUsers
                float(m2[8] or 0.0)                       # totalAdRevenue
            )
            results.append(combined_row)

        current_date += timedelta(days=1)
    return results

def load_data_to_postgres():
    """
    Загружает объединённые данные OTHER в PostgreSQL.
    """
    data = fetch_ga4_other_metrics()
    if not data:
        print("Нет данных для загрузки.")
        return

    insert_query = """
    INSERT INTO staging.ga4_other_metrics (
        report_date, achievement_id, character, comparison, file_extension, file_name,
        group_id, level, link_classes, link_domain, link_id, link_text, link_url, method,
        outbound, percent_scrolled, search_term, test_data_filter_id, test_data_filter_name,
        video_provider, video_title, video_url, virtual_currency_name, visible,
        advertiser_ad_clicks, advertiser_ad_cost, advertiser_ad_cost_per_click,
        advertiser_ad_cost_per_key_event, advertiser_ad_impressions, cart_to_view_rate,
        crash_affected_users, crash_free_users_rate, organic_google_search_average_position,
        organic_google_search_clicks, organic_google_search_click_through_rate,
        organic_google_search_impressions, publisher_ad_clicks, publisher_ad_impressions,
        purchaser_rate, purchase_to_view_rate, return_on_ad_spend, scrolled_users,
        total_ad_revenue
    ) VALUES %s 
    ON CONFLICT (report_date, achievement_id, character, comparison) DO UPDATE SET
        achievement_id = EXCLUDED.achievement_id,
        character = EXCLUDED.character,
        file_extension = EXCLUDED.file_extension,
        file_name = EXCLUDED.file_name,
        group_id = EXCLUDED.group_id,
        level = EXCLUDED.level,
        link_classes = EXCLUDED.link_classes,
        link_domain = EXCLUDED.link_domain,
        link_id = EXCLUDED.link_id,
        link_text = EXCLUDED.link_text,
        link_url = EXCLUDED.link_url,
        method = EXCLUDED.method,
        outbound = EXCLUDED.outbound,
        percent_scrolled = EXCLUDED.percent_scrolled,
        search_term = EXCLUDED.search_term,
        test_data_filter_id = EXCLUDED.test_data_filter_id,
        test_data_filter_name = EXCLUDED.test_data_filter_name,
        video_provider = EXCLUDED.video_provider,
        video_title = EXCLUDED.video_title,
        video_url = EXCLUDED.video_url,
        virtual_currency_name = EXCLUDED.virtual_currency_name,
        visible = EXCLUDED.visible,
        advertiser_ad_clicks = EXCLUDED.advertiser_ad_clicks,
        advertiser_ad_cost = EXCLUDED.advertiser_ad_cost,
        advertiser_ad_cost_per_click = EXCLUDED.advertiser_ad_cost_per_click,
        advertiser_ad_cost_per_key_event = EXCLUDED.advertiser_ad_cost_per_key_event,
        advertiser_ad_impressions = EXCLUDED.advertiser_ad_impressions,
        cart_to_view_rate = EXCLUDED.cart_to_view_rate,
        crash_affected_users = EXCLUDED.crash_affected_users,
        crash_free_users_rate = EXCLUDED.crash_free_users_rate,
        organic_google_search_average_position = EXCLUDED.organic_google_search_average_position,
        organic_google_search_clicks = EXCLUDED.organic_google_search_clicks,
        organic_google_search_click_through_rate = EXCLUDED.organic_google_search_click_through_rate,
        organic_google_search_impressions = EXCLUDED.organic_google_search_impressions,
        publisher_ad_clicks = EXCLUDED.publisher_ad_clicks,
        publisher_ad_impressions = EXCLUDED.publisher_ad_impressions,
        purchaser_rate = EXCLUDED.purchaser_rate,
        purchase_to_view_rate = EXCLUDED.purchase_to_view_rate,
        return_on_ad_spend = EXCLUDED.return_on_ad_spend,
        scrolled_users = EXCLUDED.scrolled_users,
        total_ad_revenue = EXCLUDED.total_ad_revenue;
    """

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()
    unique_data = list({(row[0], row[1], row[2], row[3]): row for row in data}.values())
    psycopg2.extras.execute_values(cur, insert_query, unique_data)
    conn.commit()
    cur.close()
    conn.close()

# Создаем DAG
with DAG(
    dag_id="GA4_PUBLISHER_2025",
    default_args={
        'owner': 'semukhin',
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    },
    description="Импорт данных OTHER из GA4 в PostgreSQL за период с 01.09.2024 по текущую дату",
    schedule_interval=None,
    start_date=datetime(2024, 9, 1),
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
