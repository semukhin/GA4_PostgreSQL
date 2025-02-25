import os
import psycopg2
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# -----------------------------------------------------------------------------
# Настройки подключения к PostgreSQL
# -----------------------------------------------------------------------------
POSTGRES_CONFIG = {
    'host': '46.101.116.151',
    'database': 'google_db',
    'user': 'postgres',
    'password': 'atlantiX_2025_Atlantix'
}

# -----------------------------------------------------------------------------
# Google Analytics настройки
# -----------------------------------------------------------------------------
PROPERTY_ID = "properties/448093085"
GA_CREDENTIALS_PATH = "/home/GA4/keys/platform-atlanti-1723565627079-375e73b55d00.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GA_CREDENTIALS_PATH

START_DATE = "2024-09-01"
END_DATE = datetime.today().strftime("%Y-%m-%d")

# -----------------------------------------------------------------------------
# Полный список измерений (включая 'date' для реальной даты события)
# -----------------------------------------------------------------------------
ECOMMERCE_DIMENSIONS = [
    "date",                         
    "currencyCode",                 
    "itemAffiliation",
    "itemBrand",
    "itemCategory",
    "itemCategory2",
    "itemCategory3",
    "itemCategory4",
    "itemCategory5",
    "itemId",
    "itemListId",
    "itemListName",
    "itemListPosition",
    "itemLocationID",
    "itemName",
    "itemPromotionCreativeName",
    "itemPromotionCreativeSlot",
    "itemPromotionId",
    "itemPromotionName",
    "itemVariant",
    "orderCoupon",
    "shippingTier",
    "transactionId"
]

# -----------------------------------------------------------------------------
# Полный список метрик
# -----------------------------------------------------------------------------
ECOMMERCE_METRICS = [
    "addToCarts",
    "checkouts",
    "ecommercePurchases",
    "grossItemRevenue",
    "grossPurchaseRevenue",
    "itemDiscountAmount",
    "itemListClickEvents",
    "itemListClickThroughRate",
    "itemListViewEvents",
    "itemPromotionClickThroughRate",
    "itemRefundAmount",
    "itemRevenue",
    "itemsAddedToCart",
    "itemsCheckedOut",
    "itemsClickedInList",
    "itemsClickedInPromotion",
    "itemsPurchased",
    "itemsViewed",
    "itemsViewedInList",
    "itemsViewedInPromotion",
    "itemViewEvents",
    "promotionClicks",
    "promotionViews",
    "purchaseRevenue",
    "refundAmount",
    "shippingAmount",
    "taxAmount",
    "transactions",
    "transactionsPerPurchaser"
]

# -----------------------------------------------------------------------------
# Ограничения GA4: не более 9 измерений и не более 10 метрик за один запрос
# -----------------------------------------------------------------------------
MAX_DIMENSIONS = 9
MAX_METRICS = 10

# -----------------------------------------------------------------------------
# Функция для "нарезки" списка на куски
# -----------------------------------------------------------------------------
def chunkify(lst, n):
    """
    Разбивает список lst на куски размером не более n.
    Пример: chunkify([1,2,3,4,5], 2) -> [[1,2],[3,4],[5]]
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]

# -----------------------------------------------------------------------------
# Создание схемы staging и таблицы
# -----------------------------------------------------------------------------
def create_staging_table():
    """
    Создание схемы staging и таблицы для хранения данных GA4.
    """
    query = """
    CREATE SCHEMA IF NOT EXISTS staging;

    CREATE TABLE IF NOT EXISTS staging.ga4_ecommerce_data (
        id SERIAL PRIMARY KEY,
        event_date DATE,
        currency_code TEXT,
        item_affiliation TEXT,
        item_brand TEXT,
        item_category TEXT,
        item_category2 TEXT,
        item_category3 TEXT,
        item_category4 TEXT,
        item_category5 TEXT,
        item_id TEXT,
        item_list_id TEXT,
        item_list_name TEXT,
        item_list_position TEXT,
        item_location_id TEXT,
        item_name TEXT,
        item_promotion_creative_name TEXT,
        item_promotion_creative_slot TEXT,
        item_promotion_id TEXT,
        item_promotion_name TEXT,
        item_variant TEXT,
        order_coupon TEXT,
        shipping_tier TEXT,
        transaction_id TEXT,
        add_to_carts NUMERIC,
        checkouts NUMERIC,
        ecommerce_purchases NUMERIC,
        gross_item_revenue NUMERIC,
        gross_purchase_revenue NUMERIC,
        item_discount_amount NUMERIC,
        item_list_click_events NUMERIC,
        item_list_click_through_rate NUMERIC,
        item_list_view_events NUMERIC,
        item_promotion_click_through_rate NUMERIC,
        item_refund_amount NUMERIC,
        item_revenue NUMERIC,
        items_added_to_cart NUMERIC,
        items_checked_out NUMERIC,
        items_clicked_in_list NUMERIC,
        items_clicked_in_promotion NUMERIC,
        items_purchased NUMERIC,
        items_viewed NUMERIC,
        items_viewed_in_list NUMERIC,
        items_viewed_in_promotion NUMERIC,
        item_view_events NUMERIC,
        promotion_clicks NUMERIC,
        promotion_views NUMERIC,
        purchase_revenue NUMERIC,
        refund_amount NUMERIC,
        shipping_amount NUMERIC,
        tax_amount NUMERIC,
        transactions NUMERIC,
        transactions_per_purchaser NUMERIC,
        imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        conn = psycopg2.connect(
            host=POSTGRES_CONFIG['host'],
            database=POSTGRES_CONFIG['database'],
            user=POSTGRES_CONFIG['user'],
            password=POSTGRES_CONFIG['password']
        )
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        cur.close()
        conn.close()
        print("Схема staging и таблица успешно созданы или уже существуют.")
    except Exception as e:
        print("Ошибка при создании схемы или таблицы:", e)

# -----------------------------------------------------------------------------
# Функция для одного запроса к GA4
# -----------------------------------------------------------------------------
def run_ga4_request(client, dims, mets):
    """
    Запрашивает у GA4 данные по списку измерений dims и списку метрик mets.
    Возвращает response.rows или выбрасывает исключение при ошибке.
    """
    request = RunReportRequest(
        property=PROPERTY_ID,
        dimensions=[{"name": d} for d in dims],
        metrics=[{"name": m} for m in mets],
        date_ranges=[{"start_date": START_DATE, "end_date": END_DATE}],
        limit=100000
    )
    response = client.run_report(request)
    return response.rows

# -----------------------------------------------------------------------------
# Основная функция импорта (с разбиением и пропуском несовместимых запросов)
# -----------------------------------------------------------------------------
def import_ecommerce_data(**kwargs):
    """
    Импорт данных по ecommerce-измерениям и метрикам из GA4,
    обходя лимиты и пропуская несовместимые комбинации (400 Incompatible).
    """
    print("Начинаем импорт ECOMMERCE-данных из GA4 с разбиением по измерениям/метрикам...")

    client = BetaAnalyticsDataClient()

    dimension_chunks = list(chunkify(ECOMMERCE_DIMENSIONS, MAX_DIMENSIONS))
    metric_chunks = list(chunkify(ECOMMERCE_METRICS, MAX_METRICS))

    try:
        conn = psycopg2.connect(
            host=POSTGRES_CONFIG['host'],
            database=POSTGRES_CONFIG['database'],
            user=POSTGRES_CONFIG['user'],
            password=POSTGRES_CONFIG['password']
        )
        cur = conn.cursor()
    except Exception as e:
        print("Ошибка при подключении к PostgreSQL:", e)
        return

    total_inserted = 0

    for dims in dimension_chunks:
        for mets in metric_chunks:
            print(f"Запрос GA4 для измерений: {dims}, метрик: {mets}")
            try:
                rows = run_ga4_request(client, dims, mets)
            except Exception as e:
                # Если получили 400 (Incompatible), пропускаем
                print(f"Ошибка при запросе к GA4: {e}")
                continue

            inserted_rows = 0
            for row in rows:
                dimension_values = [dim_value.value for dim_value in row.dimension_values]
                metric_values = [met_value.value for met_value in row.metric_values]

                # Маппинг для измерений
                dim_dict = {}
                for i, d in enumerate(dims):
                    dim_dict[d] = dimension_values[i] if i < len(dimension_values) else None

                # Маппинг для метрик
                met_dict = {}
                for j, m in enumerate(mets):
                    met_dict[m] = metric_values[j] if j < len(metric_values) else None

                # Парсим дату из GA4 (если есть измерение 'date')
                date_str = dim_dict.get("date")
                if date_str:
                    try:
                        event_date = datetime.strptime(date_str, '%Y%m%d').date()
                    except ValueError:
                        event_date = None
                else:
                    event_date = None

                sql = """
                    INSERT INTO staging.ga4_ecommerce_data (
                        event_date,
                        currency_code,
                        item_affiliation,
                        item_brand,
                        item_category,
                        item_category2,
                        item_category3,
                        item_category4,
                        item_category5,
                        item_id,
                        item_list_id,
                        item_list_name,
                        item_list_position,
                        item_location_id,
                        item_name,
                        item_promotion_creative_name,
                        item_promotion_creative_slot,
                        item_promotion_id,
                        item_promotion_name,
                        item_variant,
                        order_coupon,
                        shipping_tier,
                        transaction_id,
                        add_to_carts,
                        checkouts,
                        ecommerce_purchases,
                        gross_item_revenue,
                        gross_purchase_revenue,
                        item_discount_amount,
                        item_list_click_events,
                        item_list_click_through_rate,
                        item_list_view_events,
                        item_promotion_click_through_rate,
                        item_refund_amount,
                        item_revenue,
                        items_added_to_cart,
                        items_checked_out,
                        items_clicked_in_list,
                        items_clicked_in_promotion,
                        items_purchased,
                        items_viewed,
                        items_viewed_in_list,
                        items_viewed_in_promotion,
                        item_view_events,
                        promotion_clicks,
                        promotion_views,
                        purchase_revenue,
                        refund_amount,
                        shipping_amount,
                        tax_amount,
                        transactions,
                        transactions_per_purchaser
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s
                    );
                """

                data_tuple = (
                    event_date,
                    dim_dict.get("currencyCode"),
                    dim_dict.get("itemAffiliation"),
                    dim_dict.get("itemBrand"),
                    dim_dict.get("itemCategory"),
                    dim_dict.get("itemCategory2"),
                    dim_dict.get("itemCategory3"),
                    dim_dict.get("itemCategory4"),
                    dim_dict.get("itemCategory5"),
                    dim_dict.get("itemId"),
                    dim_dict.get("itemListId"),
                    dim_dict.get("itemListName"),
                    dim_dict.get("itemListPosition"),
                    dim_dict.get("itemLocationID"),
                    dim_dict.get("itemName"),
                    dim_dict.get("itemPromotionCreativeName"),
                    dim_dict.get("itemPromotionCreativeSlot"),
                    dim_dict.get("itemPromotionId"),
                    dim_dict.get("itemPromotionName"),
                    dim_dict.get("itemVariant"),
                    dim_dict.get("orderCoupon"),
                    dim_dict.get("shippingTier"),
                    dim_dict.get("transactionId"),
                    met_dict.get("addToCarts"),
                    met_dict.get("checkouts"),
                    met_dict.get("ecommercePurchases"),
                    met_dict.get("grossItemRevenue"),
                    met_dict.get("grossPurchaseRevenue"),
                    met_dict.get("itemDiscountAmount"),
                    met_dict.get("itemListClickEvents"),
                    met_dict.get("itemListClickThroughRate"),
                    met_dict.get("itemListViewEvents"),
                    met_dict.get("itemPromotionClickThroughRate"),
                    met_dict.get("itemRefundAmount"),
                    met_dict.get("itemRevenue"),
                    met_dict.get("itemsAddedToCart"),
                    met_dict.get("itemsCheckedOut"),
                    met_dict.get("itemsClickedInList"),
                    met_dict.get("itemsClickedInPromotion"),
                    met_dict.get("itemsPurchased"),
                    met_dict.get("itemsViewed"),
                    met_dict.get("itemsViewedInList"),
                    met_dict.get("itemsViewedInPromotion"),
                    met_dict.get("itemViewEvents"),
                    met_dict.get("promotionClicks"),
                    met_dict.get("promotionViews"),
                    met_dict.get("purchaseRevenue"),
                    met_dict.get("refundAmount"),
                    met_dict.get("shippingAmount"),
                    met_dict.get("taxAmount"),
                    met_dict.get("transactions"),
                    met_dict.get("transactionsPerPurchaser")
                )

                try:
                    cur.execute(sql, data_tuple)
                    inserted_rows += 1
                except Exception as e:
                    print("Ошибка при вставке строки:", e)

            conn.commit()
            total_inserted += inserted_rows
            print(f"  → Вставлено {inserted_rows} строк для dims={dims}, mets={mets}.")

    cur.close()
    conn.close()
    print(f"Импорт завершен. Всего вставлено {total_inserted} строк(и) во все chunks.")

# -----------------------------------------------------------------------------
# Настройки по умолчанию для дага
# -----------------------------------------------------------------------------
default_args = {
    'owner': 'semukhin',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# -----------------------------------------------------------------------------
# Определяем DAG
# -----------------------------------------------------------------------------
with DAG(
    dag_id="GA4_ECOMMERCE_2025",
    default_args=default_args,
    description='DAG для импорта ecommerce-данных GA4 с разбиением по 9 измерений и 10 метрик, пропуском несовместимых',
    schedule_interval='@daily',
    catchup=False
) as dag:

    create_table_task = PythonOperator(
        task_id='create_staging_table_task',
        python_callable=create_staging_table
    )

    import_task = PythonOperator(
        task_id='import_ecommerce_data_task',
        python_callable=import_ecommerce_data
    )

    create_table_task >> import_task
