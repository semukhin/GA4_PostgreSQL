# Файл GA4_PPC_METRICS.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
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
logger = logging.getLogger('GA4_PPC_METRICS')

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

# Ключевые события, для которых собираем метрики
KEY_EVENTS = [
    "platform_signup",  # Регистрация
    "main_form_submit",  # Лид
    "click_upgrade_pro",  # Клик по обновлению до Pro
    "click_upgrade_flexible",  # Клик по обновлению до Flexible
    "purchase"  # Покупка
]

def create_ppc_metrics_table():
    """
    Создание таблицы для хранения агрегированных метрик PPC.
    Объединяет данные из таблиц рекламных кампаний, конверсий и бюджетов.
    """
    logger.info("Создание таблицы для агрегированных метрик PPC")
    
    query = """
    CREATE SCHEMA IF NOT EXISTS analytics;
    
    CREATE TABLE IF NOT EXISTS staging.ga4_ppc_aggregated_metrics (
        report_date DATE,
        campaign_name TEXT,
        source TEXT,
        medium TEXT,
        network_type TEXT,
        
        -- Базовые метрики
        impressions INT,
        clicks INT,
        cost NUMERIC(12,2),
        ctr NUMERIC(8,4),
        cpc NUMERIC(12,2),
        
        -- Конверсионные метрики
        registrations INT,
        leads INT,
        purchases INT,
        revenue NUMERIC(12,2),
        
        -- Расчетные метрики
        conversion_rate NUMERIC(8,4),
        cost_per_registration NUMERIC(12,2),
        cost_per_lead NUMERIC(12,2),
        cost_per_acquisition NUMERIC(12,2),
        roas NUMERIC(8,4),
        
        -- Бюджетные метрики
        daily_budget NUMERIC(12,2),
        monthly_budget NUMERIC(12,2),
        spend_percentage NUMERIC(8,4),
        estimated_monthly_spend NUMERIC(12,2),
        
        -- Метрики сравнения
        wow_change_percentage NUMERIC(8,4),
        
        PRIMARY KEY (report_date, campaign_name, network_type)
    );
    """
    
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        logger.info("Таблица staging.ga4_ppc_aggregated_metrics успешно создана или уже существует")
        return True
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Ошибка при создании таблицы: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def aggregate_ppc_metrics():
    """
    Агрегирует метрики PPC из различных таблиц в единую таблицу для аналитики.
    Объединяет данные о кликах, конверсиях, бюджетах и WoW метриках.
    """
    logger.info("Агрегация метрик PPC из различных источников данных")
    
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        
        # Удаляем существующие данные для избежания дубликатов
        cursor.execute("TRUNCATE TABLE staging.ga4_ppc_aggregated_metrics;")
        
        # Запрос для агрегации данных из разных таблиц
        aggregate_query = """
        INSERT INTO staging.ga4_ppc_aggregated_metrics (
            report_date, 
            campaign_name, 
            source, 
            medium, 
            network_type,
            impressions, 
            clicks, 
            cost, 
            ctr, 
            cpc,
            registrations, 
            leads, 
            purchases, 
            revenue,
            conversion_rate, 
            cost_per_registration, 
            cost_per_lead, 
            cost_per_acquisition, 
            roas,
            daily_budget, 
            monthly_budget, 
            spend_percentage, 
            estimated_monthly_spend,
            wow_change_percentage
        )
        SELECT 
            a.report_date,
            a.campaign_name,
            a.source,
            a.medium,
            a.network_type,
            a.impressions,
            a.clicks,
            a.cost,
            a.ctr,
            a.cpc,
            COALESCE(c.registrations, 0),
            COALESCE(c.leads, 0),
            COALESCE(c.paid_users, 0),
            COALESCE(e.revenue, 0),
            CASE WHEN a.clicks > 0 THEN 
                (COALESCE(c.registrations, 0) + COALESCE(c.leads, 0)) / a.clicks::FLOAT 
            ELSE 0 END,
            CASE WHEN COALESCE(c.registrations, 0) > 0 THEN 
                a.cost / c.registrations::FLOAT 
            ELSE 0 END,
            CASE WHEN COALESCE(c.leads, 0) > 0 THEN 
                a.cost / c.leads::FLOAT 
            ELSE 0 END,
            CASE WHEN COALESCE(c.paid_users, 0) > 0 THEN 
                a.cost / c.paid_users::FLOAT 
            ELSE 0 END,
            CASE WHEN a.cost > 0 THEN 
                COALESCE(e.revenue, 0) / a.cost 
            ELSE 0 END,
            COALESCE(b.daily_budget, 0),
            COALESCE(b.monthly_budget, 0),
            COALESCE(b.spend_percentage, 0),
            COALESCE(b.estimated_monthly_spend, 0),
            COALESCE(w.wow_change_percentage, 0)
        FROM 
            staging.ga4_google_ads_metrics a
        LEFT JOIN 
            staging.ga4_campaign_budget_metrics b ON a.report_date = b.report_date 
                AND a.campaign_name = b.campaign_name
        LEFT JOIN 
            staging.ga4_conversion_rates c ON a.report_date = c.report_date 
                AND a.campaign_name = c.campaign_name 
                AND a.network_type = c.network_type
        LEFT JOIN (
            SELECT 
                report_date, 
                campaign_name, 
                network_type, 
                SUM(revenue) as revenue
            FROM 
                staging.ga4_conversion_metrics
            WHERE 
                event_name = 'purchase'
            GROUP BY 
                report_date, campaign_name, network_type
        ) e ON a.report_date = e.report_date 
            AND a.campaign_name = e.campaign_name 
            AND a.network_type = e.network_type
        LEFT JOIN (
            SELECT 
                report_date, 
                'cost' as metric_name, 
                wow_change_percentage
            FROM 
                staging.ga4_wow_metrics
            WHERE 
                metric_name = 'cost'
        ) w ON a.report_date = w.report_date
        """
        
        cursor.execute(aggregate_query)
        rows_affected = cursor.rowcount
        conn.commit()
        
        logger.info(f"Успешно агрегировано {rows_affected} строк метрик PPC")
        return True
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Ошибка при агрегации метрик PPC: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def validate_key_events():
    """
    Проверяет наличие и доступность ключевых событий в Google Analytics 4.
    Используется для валидации перед выполнением основных операций агрегации.
    """
    logger.info("Проверка наличия ключевых событий в GA4")
    
    try:
        client = BetaAnalyticsDataClient()
        
        # Запрос для получения всех доступных событий
        request = RunReportRequest(
            property=PROPERTY_ID,
            date_ranges=[DateRange(start_date="30daysAgo", end_date="yesterday")],
            dimensions=[Dimension(name="eventName")],
            metrics=[Metric(name="eventCount")]
        )
        
        response = client.run_report(request)
        
        # Получаем список всех доступных событий
        available_events = [row.dimension_values[0].value for row in response.rows]
        
        # Проверяем наличие всех необходимых ключевых событий
        missing_events = []
        for event in KEY_EVENTS:
            if event not in available_events:
                missing_events.append(event)
        
        if missing_events:
            logger.warning(f"Следующие ключевые события отсутствуют в GA4: {', '.join(missing_events)}")
            return False
        
        logger.info("Все ключевые события доступны в GA4")
        return True
    except Exception as e:
        logger.error(f"Ошибка при проверке ключевых событий: {e}")
        logger.error(traceback.format_exc())
        return False

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
    'GA4_PPC_METRICS',
    default_args=default_args,
    description='Сбор и агрегация метрик PPC Google для отчетов',
    schedule_interval='0 6 * * *',  # Каждый день в 06:00, после основных дагов
    start_date=datetime(2025, 3, 5),
    catchup=False,
    tags=['ga4', 'ppc', 'metrics'],
) as dag:
    
    # Проверка наличия ключевых событий в GA4
    validate_events = PythonOperator(
        task_id='validate_key_events',
        python_callable=validate_key_events,
    )
    
    # Ожидание завершения других DAG
    wait_for_ads = ExternalTaskSensor(
        task_id='wait_for_ads_data',
        external_dag_id='GA4_ADVERTISING_METRICS',
        external_task_id='load_wow_metrics',
        allowed_states=['success'],
        timeout=3600,
        poke_interval=60,
        mode='reschedule'
    )
    
    wait_for_user_behavior = ExternalTaskSensor(
        task_id='wait_for_user_behavior_data',
        external_dag_id='GA4_USER_BEHAVIOR',
        external_task_id='load_key_event_metrics',
        allowed_states=['success'],
        timeout=3600,
        poke_interval=60,
        mode='reschedule',
        trigger_rule='all_success',
)
    
    # Создание таблицы для агрегированных метрик PPC
    create_table = PythonOperator(
        task_id='create_ppc_metrics_table',
        python_callable=create_ppc_metrics_table,
        trigger_rule='all_success',

    )
    
    # Агрегация всех метрик PPC в единую таблицу
    aggregate_metrics = PythonOperator(
        task_id='aggregate_ppc_metrics',
        python_callable=aggregate_ppc_metrics,
        trigger_rule='all_success', 

    )
    
    # Определение порядка выполнения задач
    validate_events >> [wait_for_ads, wait_for_user_behavior]
    [wait_for_ads, wait_for_user_behavior] >> create_table >> aggregate_metrics