from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import psycopg2
import psycopg2.extras
from google.analytics.data_v1beta import BetaAnalyticsDataClient, RunReportRequest
from google.analytics.data_v1beta.types import DateRange, Metric, Dimension, FilterExpression, Filter

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
    """
    Создает схему и таблицу для хранения данных OTHER.
    Поля измерений: achievement_id, character_name, file_extension, file_name,
    group_id, level, link_classes, link_domain, link_id, link_text, link_url,
    method, outbound, percent_scrolled, search_term, video_provider, video_title,
    video_url, virtual_currency_name, visible.
    Поля метрик разделены на две группы.
    Первичный ключ: (report_date, achievement_id, character_name)
    """
    query = """
    CREATE SCHEMA IF NOT EXISTS staging;

    CREATE TABLE IF NOT EXISTS staging.ga4_other_metrics (
        report_date DATE,
        achievement_id TEXT,
        character_name TEXT,
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
        video_provider TEXT,
        video_title TEXT,
        video_url TEXT,
        virtual_currency_name TEXT,
        visible TEXT,
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
        organic_google_search_click_through_rate NUMERIC,
        organic_google_search_impressions INTEGER,
        publisher_ad_clicks INTEGER,
        publisher_ad_impressions INTEGER,
        purchaser_rate NUMERIC,
        purchase_to_view_rate NUMERIC,
        return_on_ad_spend NUMERIC,
        scrolled_users INTEGER,
        total_ad_revenue NUMERIC,
        PRIMARY KEY (report_date, achievement_id, character_name)
    );
    """
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()

# Списки измерений и метрик
DIMENSIONS_PART1 = [
    "achievementId", "character_name", "fileExtension", "fileName",
    "groupId", "level", "linkClasses", "linkDomain"
]

DIMENSIONS_PART2 = [
    "linkId", "linkText", "linkUrl", "method", "outbound", "percentScrolled",
    "searchTerm"
]

DIMENSIONS_PART3 = [
    "videoProvider", "videoTitle", "videoUrl", "virtualCurrencyName", "visible"
]

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

# Количество ожидаемых значений в каждой группе
NUM_DIMS_PART1 = len(DIMENSIONS_PART1)
NUM_DIMS_PART2 = len(DIMENSIONS_PART2)
NUM_DIMS_PART3 = len(DIMENSIONS_PART3)
NUM_METRICS_GROUP1 = len(METRICS_GROUP1)
NUM_METRICS_GROUP2 = len(METRICS_GROUP2)

class FakeValue:
    def __init__(self, value):
        self.value = value

class FakeRow:
    def __init__(self, values, is_dimension=True):
        attr = 'dimension_values' if is_dimension else 'metric_values'
        setattr(self, attr, [FakeValue(v) for v in values])

def default_row(expected_count, is_dimension=True):
    """
    Возвращает одну "фейковую" строку с пустыми значениями (для измерений)
    или нулями (для метрик).
    """
    default_value = "" if is_dimension else 0
    return [FakeRow([default_value] * expected_count, is_dimension=is_dimension)]

def run_report(dimensions, metrics, date, filter_expr=None):
    """
    Выполняет запрос к GA4 за указанный день.
    """
    client = BetaAnalyticsDataClient()
    request = RunReportRequest(
        property=PROPERTY_ID,
        date_ranges=[DateRange(start_date=date, end_date=date)],
        dimensions=[Dimension(name=d) for d in dimensions],
        metrics=[Metric(name=m) for m in metrics],
        dimension_filter=filter_expr
    )
    response = client.run_report(request)
    return response.rows

def safe_run_report(dimensions, metrics, date, filter_expr=None, expected_count=0, is_dimension=True):
    """
    Обёртка, которая возвращает фейковые строки, если реальный запрос не дал результатов.
    """
    try:
        rows = run_report(dimensions, metrics, date, filter_expr)
        if not rows or len(rows) == 0:
            return default_row(expected_count, is_dimension)
        return rows
    except Exception:
        # Если случилась ошибка, возвращаем фейковые значения
        return default_row(expected_count, is_dimension)

def fetch_ga4_other_metrics():
    """
    Цикл по дням: по каждому дню запрашиваем 3 части измерений и 2 группы метрик.
    Если данных нет — подставляются фейковые строки.
    """
    results = []
    filter_expr = FilterExpression(
        filter=Filter(
            field_name="eventName",
            string_filter=Filter.StringFilter(value="OTHER", match_type=Filter.StringFilter.MatchType.EXACT)
        )
    )

    current_date = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_date_dt = datetime.strptime(END_DATE, "%Y-%m-%d")

    while current_date <= end_date_dt:
        date_str = current_date.strftime("%Y-%m-%d")

        # Измерения
        rows_dims1 = safe_run_report(DIMENSIONS_PART1, [], date_str,
                                     expected_count=NUM_DIMS_PART1, is_dimension=True)
        rows_dims2 = safe_run_report(DIMENSIONS_PART2, [], date_str,
                                     expected_count=NUM_DIMS_PART2, is_dimension=True)
        rows_dims3 = safe_run_report(DIMENSIONS_PART3, [], date_str,
                                     expected_count=NUM_DIMS_PART3, is_dimension=True)

        # Метрики (агрегированные по date, фильтр eventName=OTHER)
        rows_metrics1 = safe_run_report(["date"], METRICS_GROUP1, date_str,
                                        filter_expr, expected_count=NUM_METRICS_GROUP1, is_dimension=False)
        rows_metrics2 = safe_run_report(["date"], METRICS_GROUP2, date_str,
                                        filter_expr, expected_count=NUM_METRICS_GROUP2, is_dimension=False)

        # Извлекаем первую строку метрик
        m1 = [mv.value for mv in rows_metrics1[0].metric_values]
        m2 = [mv.value for mv in rows_metrics2[0].metric_values]

        # Берём минимальное количество строк среди трёх частей измерений
        n = min(len(rows_dims1), len(rows_dims2), len(rows_dims3))
        for i in range(n):
            dims1 = [d.value for d in rows_dims1[i].dimension_values]
            dims2 = [d.value for d in rows_dims2[i].dimension_values]
            dims3 = [d.value for d in rows_dims3[i].dimension_values]

            dims = dims1 + dims2 + dims3

            # Собираем кортеж для вставки
            combined_row = (
                date_str,
                dims[0],  # achievementId
                dims[1],  # character_name
                dims[2],  # fileExtension
                dims[3],  # fileName
                dims[4],  # groupId
                dims[5],  # level
                dims[6],  # linkClasses
                dims[7],  # linkDomain
                dims[8],  # linkId
                dims[9],  # linkText
                dims[10], # linkUrl
                dims[11], # method
                dims[12], # outbound
                dims[13], # percentScrolled
                dims[14], # searchTerm
                dims[15], # videoProvider
                dims[16], # videoTitle
                dims[17], # videoUrl
                dims[18], # virtualCurrencyName
                dims[19], # visible

                # Метрики (группа 1)
                int(m1[0] or 0),
                float(m1[1] or 0.0),
                float(m1[2] or 0.0),
                float(m1[3] or 0.0),
                int(m1[4] or 0),
                float(m1[5] or 0.0),
                int(m1[6] or 0),
                float(m1[7] or 0.0),
                float(m1[8] or 0.0),
                int(m1[9] or 0),

                # Метрики (группа 2)
                float(m2[0] or 0.0),
                int(m2[1] or 0),
                int(m2[2] or 0),
                int(m2[3] or 0),
                float(m2[4] or 0.0),
                float(m2[5] or 0.0),
                float(m2[6] or 0.0),
                int(m2[7] or 0),
                float(m2[8] or 0.0)
            )
            results.append(combined_row)

        current_date += timedelta(days=1)

    return results

def load_data_to_postgres():
    """
    Получаем данные из fetch_ga4_other_metrics и вставляем в PostgreSQL
    с ON CONFLICT (report_date, achievement_id, character_name).
    """
    data = fetch_ga4_other_metrics()
    if not data:
        print("Нет данных для загрузки.")
        return

    insert_query = """
    INSERT INTO staging.ga4_other_metrics (
        report_date, achievement_id, character_name, file_extension, file_name,
        group_id, level, link_classes, link_domain, link_id, link_text, link_url, method,
        outbound, percent_scrolled, search_term, video_provider, video_title, video_url,
        virtual_currency_name, visible,
        advertiser_ad_clicks, advertiser_ad_cost, advertiser_ad_cost_per_click,
        advertiser_ad_cost_per_key_event, advertiser_ad_impressions, cart_to_view_rate,
        crash_affected_users, crash_free_users_rate, organic_google_search_average_position,
        organic_google_search_clicks, organic_google_search_click_through_rate,
        organic_google_search_impressions, publisher_ad_clicks, publisher_ad_impressions,
        purchaser_rate, purchase_to_view_rate, return_on_ad_spend, scrolled_users,
        total_ad_revenue
    ) VALUES %s 
    ON CONFLICT (report_date, achievement_id, character_name) DO UPDATE SET
        character_name = EXCLUDED.character_name,
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

    # Убираем дубли по ключу (report_date, achievement_id, character_name)
    unique_data = list({(row[0], row[1], row[2]): row for row in data}.values())

    psycopg2.extras.execute_values(cur, insert_query, unique_data)
    conn.commit()
    cur.close()
    conn.close()

# Создаём DAG
with DAG(
    dag_id="GA4_OTHER_2025",
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
