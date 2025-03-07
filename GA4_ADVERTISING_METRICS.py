from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
import json
import requests
from datetime import datetime, timedelta
import os
import psycopg2
import psycopg2.extras
import logging
import traceback
import pandas as pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Metric, Dimension, Filter, FilterExpression

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('GA4_ADVERTISING_METRICS')

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

# Определение ключевых событий конверсии
CONVERSION_EVENTS = [
    "platform_signup",  # Регистрация пользователя
    "main_form_submit",  # Заполнение основной формы (лид)
    "click_upgrade_pro",  # Клик по обновлению до Pro
    "click_upgrade_flexible",  # Клик по обновлению до Flexible
    "purchase"  # Покупка
]


def create_tables():
    """Создание необходимых таблиц в схеме staging."""
    queries = [
        """
        CREATE SCHEMA IF NOT EXISTS staging;
        """,
        """
        CREATE TABLE IF NOT EXISTS staging.ga4_google_ads_metrics (
            report_date DATE,
            campaign_name TEXT,
            ad_group TEXT,
            source TEXT,
            medium TEXT,
            network_type TEXT,
            impressions INT,
            clicks INT,
            cost NUMERIC(12,2),
            ctr NUMERIC(8,4),
            cpc NUMERIC(12,2),
            conversions INT,
            cost_per_conversion NUMERIC(12,2),
            PRIMARY KEY (report_date, campaign_name, ad_group, network_type)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS staging.ga4_conversion_metrics (
            report_date DATE,
            campaign_name TEXT,
            ad_group TEXT,
            source TEXT,
            medium TEXT,
            network_type TEXT,
            event_name TEXT,
            event_count INT,
            conversion_rate NUMERIC(8,4),
            revenue NUMERIC(12,2),
            cost_per_event NUMERIC(12,2),
            roas NUMERIC(8,4),
            PRIMARY KEY (report_date, campaign_name, event_name, network_type)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS staging.ga4_campaign_budget_metrics (
            report_date DATE,
            campaign_name TEXT,
            source TEXT,
            medium TEXT,
            daily_budget NUMERIC(12,2),
            monthly_budget NUMERIC(12,2),
            spend NUMERIC(12,2),
            spend_percentage NUMERIC(8,4),
            avg_daily_spend NUMERIC(12,2),
            estimated_monthly_spend NUMERIC(12,2),
            PRIMARY KEY (report_date, campaign_name)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS staging.ga4_wow_metrics (
            report_date DATE,
            metric_name TEXT,
            metric_value NUMERIC(12,2),
            previous_week_value NUMERIC(12,2),
            wow_change_percentage NUMERIC(8,4),
            week_number INT,
            year INT,
            PRIMARY KEY (report_date, metric_name)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS staging.ga4_conversion_rates (
            report_date DATE,
            campaign_name TEXT,
            source TEXT,
            medium TEXT,
            network_type TEXT,
            visitors INT,              -- количество кликов
            registrations INT,         -- количество регистраций
            leads INT,                 -- количество лидов
            paid_users INT,            -- количество платных пользователей
            visitors_to_regs_rate FLOAT,   -- CR visitors to regs
            visitors_to_leads_rate FLOAT,  -- CR visitors to leads
            regs_to_paid_users_rate FLOAT, -- CR regs to paid users
            cost_per_registration FLOAT,   -- Cost per regs
            cost_per_lead FLOAT,           -- CPL
            cost_per_acquisition FLOAT,    -- CAC
            PRIMARY KEY (report_date, campaign_name, network_type)
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

def fetch_google_ads_metrics():
    """Получение метрик Google Ads из GA4."""
    logger.info(f"Получение метрик Google Ads за период {START_DATE} - {END_DATE}")
    
    # Фильтры для выделения данных только из Google Ads
    google_ads_filter = FilterExpression(
        filter=Filter(
            field_name="sessionSource",
            string_filter=Filter.StringFilter(value="google", match_type=Filter.StringFilter.MatchType.CONTAINS)
        )
    )

    # Для поисковой рекламы (Search)
    search_filter = FilterExpression(
        filter=Filter(
            field_name="sessionGoogleAdsAdNetworkType",
            string_filter=Filter.StringFilter(value="Search", match_type=Filter.StringFilter.MatchType.EXACT)
        )
    )

    # Для медийной рекламы (Display)
    display_filter = FilterExpression(
        filter=Filter(
            field_name="sessionGoogleAdsAdNetworkType",
            string_filter=Filter.StringFilter(value="Display", match_type=Filter.StringFilter.MatchType.EXACT)
        )
    )

    # Измерения
    dimensions = [
        "date",
        "sessionGoogleAdsCampaignName",
        "sessionGoogleAdsAdGroupName",
        "sessionSource",
        "sessionMedium",
        "sessionGoogleAdsAdNetworkType"
    ]
    
    # Исправленные метрики для Google Ads (используем валидные метрики GA4)
    metrics = [
        "sessions",              # Вместо advertiserAdImpressions
        "engagementRate",        # Вместо advertiserAdClicks
        "totalAdRevenue",        # 
        "screenPageViewsPerSession", 
        "conversions"             # Конверсии
    ]
    
    try:
        client = BetaAnalyticsDataClient()
        
        # Запрос данных для всех сетей (общие метрики)
        results = []
        
        # 1. Получаем общие данные по всем сетям
        general_request = RunReportRequest(
            property=PROPERTY_ID,
            date_ranges=[DateRange(start_date=START_DATE, end_date=END_DATE)],
            dimensions=[Dimension(name=d) for d in dimensions],
            metrics=[Metric(name=m) for m in metrics],
            dimension_filter=google_ads_filter,
            limit=50000  # Увеличиваем лимит для получения большего количества комбинаций
        )
        
        general_response = client.run_report(general_request)
        process_response(general_response, results, network_type_filter="all")
        
        # 2. Получаем данные по поисковой рекламе (Search)
        search_request = RunReportRequest(
            property=PROPERTY_ID,
            date_ranges=[DateRange(start_date=START_DATE, end_date=END_DATE)],
            dimensions=[Dimension(name=d) for d in dimensions],
            metrics=[Metric(name=m) for m in metrics],
            dimension_filter=search_filter,
            limit=50000
        )
        
        search_response = client.run_report(search_request)
        process_response(search_response, results, network_type_filter="search")
        
        # 3. Получаем данные по медийной рекламе (Display)
        display_request = RunReportRequest(
            property=PROPERTY_ID,
            date_ranges=[DateRange(start_date=START_DATE, end_date=END_DATE)],
            dimensions=[Dimension(name=d) for d in dimensions],
            metrics=[Metric(name=m) for m in metrics],
            dimension_filter=display_filter,
            limit=50000
        )
        
        display_response = client.run_report(display_request)
        process_response(display_response, results, network_type_filter="display")
            
        logger.info(f"Получено {len(results)} записей метрик Google Ads")
        
        if not results:
            raise ValueError("Не удалось получить данные метрик Google Ads")
        
        return results
    except Exception as e:
        logger.error(f"Ошибка при получении метрик Google Ads: {e}")
        logger.error(traceback.format_exc())
        raise  # Перебрасываем исключение вверх


def process_response(response, results, network_type_filter="all"):
    """Обработка ответа от API GA4 и добавление результатов в список."""
    for row in response.rows:
        date_str = row.dimension_values[0].value
        # Преобразуем из формата YYYYMMDD в YYYY-MM-DD
        date_formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        
        campaign_name = row.dimension_values[1].value or "(not set)"
        ad_group = row.dimension_values[2].value or "(not set)"
        source = row.dimension_values[3].value or "(not set)"
        medium = row.dimension_values[4].value or "(not set)"
        network_type = row.dimension_values[5].value or "(not set)"
        
        # Добавляем суффикс к campaign_name в зависимости от типа фильтра
        # для различения данных по разным сетям
        if network_type_filter != "all":
            campaign_name = f"{campaign_name} ({network_type_filter})"
        
        # Метрики
        impressions = int(float(row.metric_values[0].value or 0))
        clicks = int(float(row.metric_values[1].value or 0))
        cost = float(row.metric_values[2].value or 0.0)
        cpc = float(row.metric_values[3].value or 0.0)
        conversions = int(float(row.metric_values[4].value or 0))
        
        # Расчёт CTR (Click-Through Rate)
        ctr = 0.0
        if impressions > 0:
            ctr = clicks / impressions
        
        # Расчёт стоимости за конверсию
        cost_per_conversion = 0.0
        if conversions > 0:
            cost_per_conversion = cost / conversions
        
        results.append((
            date_formatted,
            campaign_name,
            ad_group,
            source,
            medium,
            network_type,
            impressions,
            clicks,
            cost,
            ctr,
            cpc,
            conversions,
            cost_per_conversion,
            network_type_filter  # Добавляем информацию о типе сети
        ))

def fetch_conversion_metrics():
    """Получение метрик конверсии по событиям из GA4."""
    logger.info(f"Получение метрик конверсии за период {START_DATE} - {END_DATE}")
    
    all_results = []
    
    # Для каждого события конверсии
    for event_name in CONVERSION_EVENTS:
        logger.info(f"Получение метрик для события конверсии: {event_name}")
        
        # Измерения
        dimensions = [
            "date",
            "sessionGoogleAdsCampaignName",
            "sessionGoogleAdsAdGroupName",
            "sessionSource",
            "sessionMedium",
            "sessionGoogleAdsAdNetworkType"
        ]
        
        # Метрики для конверсий
        metrics = [
            "eventCount",
            "conversions",
            "totalAdRevenue",
            "purchaseRevenue"  # Доход от покупок
        ]
        
        # Фильтр по имени события
        event_filter = FilterExpression(
            filter=Filter(
                field_name="eventName",
                string_filter=Filter.StringFilter(value=event_name, match_type=Filter.StringFilter.MatchType.EXACT)
            )
        )
        
        try:
            client = BetaAnalyticsDataClient()
            
            request = RunReportRequest(
                property=PROPERTY_ID,
                date_ranges=[DateRange(start_date=START_DATE, end_date=END_DATE)],
                dimensions=[Dimension(name=d) for d in dimensions],
                metrics=[Metric(name=m) for m in metrics],
                dimension_filter=event_filter,
                limit=50000
            )
            
            response = client.run_report(request)
            
            # Обработка результатов
            for row in response.rows:
                date_str = row.dimension_values[0].value
                # Преобразуем из формата YYYYMMDD в YYYY-MM-DD
                date_formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                
                campaign_name = row.dimension_values[1].value or "(not set)"
                ad_group = row.dimension_values[2].value or "(not set)"
                source = row.dimension_values[3].value or "(not set)"
                medium = row.dimension_values[4].value or "(not set)"
                network_type = row.dimension_values[5].value or "(not set)"
                
                # Метрики
                event_count = int(row.metric_values[0].value or 0)
                conversions = int(row.metric_values[1].value or 0)
                cost = float(row.metric_values[2].value or 0.0)
                revenue = float(row.metric_values[3].value or 0.0)
                
                # Расчёт конверсии
                conversion_rate = 0.0
                if event_count > 0:
                    conversion_rate = conversions / event_count
                
                # Расчёт стоимости за событие
                cost_per_event = 0.0
                if event_count > 0:
                    cost_per_event = cost / event_count
                
                # Расчёт ROAS (Return on Ad Spend)
                roas = 0.0
                if cost > 0:
                    roas = revenue / cost
                
                all_results.append((
                    date_formatted,
                    campaign_name,
                    ad_group,
                    source,
                    medium,
                    network_type,
                    event_name,
                    event_count,
                    conversion_rate,
                    revenue,
                    cost_per_event,
                    roas
                ))
                
        except Exception as e:
            logger.error(f"Ошибка при получении метрик для события {event_name}: {e}")
            logger.error(traceback.format_exc())
    
    logger.info(f"Получено {len(all_results)} записей метрик конверсии")
    return all_results

def fetch_campaign_budget_metrics():
    """Получение метрик бюджетов кампаний из GA4."""
    logger.info(f"Получение метрик бюджетов кампаний за период {START_DATE} - {END_DATE}")
    
    # Измерения
    dimensions = [
        "date",
        "sessionGoogleAdsCampaignName",
        "sessionSource",
        "sessionMedium"
    ]
    
    # Метрики для бюджетов
    metrics = [
        "totalAdRevenue"  # Затраты на рекламу
    ]
    
    try:
        client = BetaAnalyticsDataClient()
        
        request = RunReportRequest(
            property=PROPERTY_ID,
            date_ranges=[DateRange(start_date=START_DATE, end_date=END_DATE)],
            dimensions=[Dimension(name=d) for d in dimensions],
            metrics=[Metric(name=m) for m in metrics],
            limit=50000
        )
        
        response = client.run_report(request)
        
        # Преобразуем данные в DataFrame для более удобной агрегации
        df_rows = []
        for row in response.rows:
            date_str = row.dimension_values[0].value
            # Преобразуем из формата YYYYMMDD в YYYY-MM-DD
            date_formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            
            campaign_name = row.dimension_values[1].value or "(not set)"
            source = row.dimension_values[2].value or "(not set)"
            medium = row.dimension_values[3].value or "(not set)"
            cost = float(row.metric_values[0].value or 0.0)
            
            df_rows.append({
                'date': date_formatted,
                'campaign_name': campaign_name,
                'source': source,
                'medium': medium,
                'cost': cost
            })
        
        if not df_rows:
            logger.warning("Нет данных по затратам на рекламу")
            return []
        
        # Преобразуем в DataFrame
        df = pd.DataFrame(df_rows)
        df['date'] = pd.to_datetime(df['date'])
        
        # Добавляем информацию о месяце
        df['month'] = df['date'].dt.to_period('M')
        
        # Агрегируем данные по дням и кампаниям
        daily_agg = df.groupby(['date', 'campaign_name', 'source', 'medium'])['cost'].sum().reset_index()
        
        # Агрегируем данные по месяцам и кампаниям для расчета ожидаемых месячных затрат
        monthly_agg = df.groupby(['month', 'campaign_name'])['cost'].sum().reset_index()
        monthly_agg['days_in_month'] = monthly_agg['month'].dt.days_in_month
        monthly_agg['days_passed'] = monthly_agg.apply(
            lambda x: min((pd.Timestamp.now() - pd.Timestamp(x['month'].start_time)).days + 1, x['days_in_month']), 
            axis=1
        )
        monthly_agg['avg_daily_spend'] = monthly_agg['cost'] / monthly_agg['days_passed']
        monthly_agg['estimated_monthly_spend'] = monthly_agg['avg_daily_spend'] * monthly_agg['days_in_month']
        
        # Преобразуем месячные данные в словарь для быстрого доступа
        monthly_data = {}
        for _, row in monthly_agg.iterrows():
            campaign = row['campaign_name']
            month_key = row['month'].strftime('%Y-%m')
            monthly_data[(campaign, month_key)] = {
                'avg_daily_spend': row['avg_daily_spend'],
                'estimated_monthly_spend': row['estimated_monthly_spend']
            }
        
        # Формируем результаты
        results = []
        for _, row in daily_agg.iterrows():
            date_str = row['date'].strftime('%Y-%m-%d')
            campaign = row['campaign_name']
            source = row['source']
            medium = row['medium']
            spend = row['cost']
            
            # Получаем данные о месяце
            month_key = row['date'].strftime('%Y-%m')
            monthly_info = monthly_data.get((campaign, month_key), {
                'avg_daily_spend': 0.0,
                'estimated_monthly_spend': 0.0
            })
            
            # Предполагаемые бюджеты (в отсутствие реальных данных)
            # В реальности эти данные должны приходить из API рекламных систем
            daily_budget = monthly_info['avg_daily_spend'] * 1.2  # Предполагаем, что бюджет на 20% выше среднего расхода
            monthly_budget = monthly_info['estimated_monthly_spend'] * 1.2
            
            # Расчёт процента использования бюджета
            spend_percentage = 0.0
            if daily_budget > 0:
                spend_percentage = spend / daily_budget
            
            results.append((
                date_str,
                campaign,
                source,
                medium,
                daily_budget,
                monthly_budget,
                spend,
                spend_percentage,
                monthly_info['avg_daily_spend'],
                monthly_info['estimated_monthly_spend']
            ))
        
        logger.info(f"Получено {len(results)} записей метрик бюджетов кампаний")
        return results
    except Exception as e:
        logger.error(f"Ошибка при получении метрик бюджетов кампаний: {e}")
        logger.error(traceback.format_exc())
        return []

def calculate_wow_metrics():
    """Расчет метрик week-over-week для основных показателей."""
    logger.info("Расчет метрик week-over-week")
    
    # Получаем метрики из БД для WoW анализа
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        
        # Получаем данные по метрикам Google Ads
        cursor.execute("""
        SELECT 
            report_date, 
            SUM(impressions) as impressions, 
            SUM(clicks) as clicks, 
            SUM(cost) as cost,
            SUM(conversions) as conversions
        FROM 
            staging.ga4_google_ads_metrics
        GROUP BY 
            report_date
        ORDER BY 
            report_date
        """)
        
        # Преобразуем в DataFrame
        metrics_data = cursor.fetchall()
        if not metrics_data:
            logger.error("Нет данных для расчета метрик week-over-week")
            raise ValueError("Отсутствуют необходимые метрики для расчета WoW")
            
        df = pd.DataFrame(metrics_data, columns=['date', 'impressions', 'clicks', 'cost', 'conversions'])
        df['date'] = pd.to_datetime(df['date'])
        
        # Добавляем информацию о неделе
        df['year'] = df['date'].dt.isocalendar().year
        df['week'] = df['date'].dt.isocalendar().week
        
        # Агрегируем данные по неделям
        weekly_agg = df.groupby(['year', 'week'])[['impressions', 'clicks', 'cost', 'conversions']].sum().reset_index()
        
        # Расчет метрик WoW
        weekly_agg['impressions_prev'] = weekly_agg['impressions'].shift(1)
        weekly_agg['clicks_prev'] = weekly_agg['clicks'].shift(1)
        weekly_agg['cost_prev'] = weekly_agg['cost'].shift(1)
        weekly_agg['conversions_prev'] = weekly_agg['conversions'].shift(1)
        
        # Расчет процентного изменения
        weekly_agg['impressions_wow'] = (weekly_agg['impressions'] / weekly_agg['impressions_prev'] - 1) * 100
        weekly_agg['clicks_wow'] = (weekly_agg['clicks'] / weekly_agg['clicks_prev'] - 1) * 100
        weekly_agg['cost_wow'] = (weekly_agg['cost'] / weekly_agg['cost_prev'] - 1) * 100
        weekly_agg['conversions_wow'] = (weekly_agg['conversions'] / weekly_agg['conversions_prev'] - 1) * 100
        
        # Расчет дополнительных метрик
        weekly_agg['ctr'] = weekly_agg['clicks'] / weekly_agg['impressions'] * 100
        weekly_agg['ctr_prev'] = weekly_agg['clicks_prev'] / weekly_agg['impressions_prev'] * 100
        weekly_agg['ctr_wow'] = (weekly_agg['ctr'] / weekly_agg['ctr_prev'] - 1) * 100
        
        weekly_agg['cpc'] = weekly_agg['cost'] / weekly_agg['clicks']
        weekly_agg['cpc_prev'] = weekly_agg['cost_prev'] / weekly_agg['clicks_prev']
        weekly_agg['cpc_wow'] = (weekly_agg['cpc'] / weekly_agg['cpc_prev'] - 1) * 100
        
        weekly_agg['cpl'] = weekly_agg['cost'] / weekly_agg['conversions']
        weekly_agg['cpl_prev'] = weekly_agg['cost_prev'] / weekly_agg['conversions_prev']
        weekly_agg['cpl_wow'] = (weekly_agg['cpl'] / weekly_agg['cpl_prev'] - 1) * 100
        
        # Исправление: вместо apply используем правильный формат дат
        # Вместо этого неправильного кода:
        # weekly_agg['last_day_of_week'] = weekly_agg.apply(
        #    lambda x: f"{x['year']}-{x['week']:02d}-7", axis=1
        # )
        
        # Используем правильное решение:
        # Создаем функцию для получения последнего дня недели
        def get_last_day_of_week(year, week):
            # Конвертируем год и номер недели в дату (первый день недели)
            first_day = datetime.strptime(f'{year}-{week}-1', '%Y-%W-%w')
            # Получаем последний день недели (добавляем 6 дней)
            last_day = first_day + timedelta(days=6)
            return last_day.strftime('%Y-%m-%d')
        
        # Применяем функцию к каждой строке отдельно
        weekly_agg['last_day_of_week'] = weekly_agg.apply(
            lambda row: get_last_day_of_week(row['year'], row['week']), 
            axis=1
        )
        
        # Формируем результаты
        results = []
        for _, row in weekly_agg.iterrows():
            if pd.isna(row['impressions_prev']):
                continue  # Пропускаем первую неделю без предыдущих данных
            
            # Используем уже рассчитанную дату последнего дня недели
            last_day_str = row['last_day_of_week']
            week, year = row['week'], row['year']
            
            # Метрики Impressions
            results.append((
                last_day_str,
                'impressions',
                row['impressions'],
                row['impressions_prev'],
                row['impressions_wow'],
                week,
                year
            ))
            
            # Метрики Clicks
            results.append((
                last_day_str,
                'clicks',
                row['clicks'],
                row['clicks_prev'],
                row['clicks_wow'],
                week,
                year
            ))
            
            # Метрики Cost
            results.append((
                last_day_str,
                'cost',
                row['cost'],
                row['cost_prev'],
                row['cost_wow'],
                week,
                year
            ))
            
            # Метрики Conversions
            results.append((
                last_day_str,
                'conversions',
                row['conversions'],
                row['conversions_prev'],
                row['conversions_wow'],
                week,
                year
            ))
            
            # Метрики CTR
            results.append((
                last_day_str,
                'ctr',
                row['ctr'],
                row['ctr_prev'],
                row['ctr_wow'],
                week,
                year
            ))
            
            # Метрики CPC
            results.append((
                last_day_str,
                'cpc',
                row['cpc'],
                row['cpc_prev'],
                row['cpc_wow'],
                week,
                year
            ))
            
            # Метрики CPL
            results.append((
                last_day_str,
                'cpl',
                row['cpl'],
                row['cpl_prev'],
                row['cpl_wow'],
                week,
                year
            ))
        
        if not results:
            logger.error("Нет данных для week-over-week метрик после обработки")
            raise ValueError("Не удалось рассчитать метрики week-over-week")
            
        logger.info(f"Рассчитано {len(results)} метрик week-over-week")
        return results
    except Exception as e:
        logger.error(f"Ошибка при расчете метрик week-over-week: {e}")
        logger.error(traceback.format_exc())
        raise  # Перебрасываем исключение, чтобы задача завершилась с ошибкой
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def load_google_ads_metrics_to_db():
    """Загрузка метрик Google Ads в базу данных."""
    try:
        metrics = fetch_google_ads_metrics()
        if not metrics:
            logger.error("Получение метрик Google Ads вернуло пустой результат")
            raise ValueError("Нет данных для загрузки в таблицу google_ads_metrics")
        
        # Загрузка данных в БД
        conn = None
        cursor = None
        try:
            conn = psycopg2.connect(**POSTGRES_CONFIG)
            cursor = conn.cursor()
            
            # Вставка данных с обработкой дубликатов
            query = """
            INSERT INTO staging.ga4_google_ads_metrics (
                report_date, 
                campaign_name, 
                ad_group, 
                source, 
                medium, 
                network_type,
                impressions,
                clicks,
                cost,
                ctr,
                cpc,
                conversions,
                cost_per_conversion
            ) VALUES %s
            ON CONFLICT (report_date, campaign_name, ad_group, network_type) DO UPDATE SET
                source = EXCLUDED.source,
                medium = EXCLUDED.medium,
                impressions = EXCLUDED.impressions,
                clicks = EXCLUDED.clicks,
                cost = EXCLUDED.cost,
                ctr = EXCLUDED.ctr,
                cpc = EXCLUDED.cpc,
                conversions = EXCLUDED.conversions,
                cost_per_conversion = EXCLUDED.cost_per_conversion;
            """
            
            # Разбиваем на пакеты по 1000 записей
            batch_size = 1000
            rows_loaded = 0
            for i in range(0, len(metrics), batch_size):
                batch = metrics[i:i+batch_size]
                psycopg2.extras.execute_values(cursor, query, batch)
                conn.commit()
                rows_loaded += len(batch)
                logger.info(f"Загружена партия {i//batch_size + 1} из {(len(metrics)-1)//batch_size + 1}, размер: {len(batch)}")
                
            logger.info(f"Загружено {rows_loaded} записей метрик Google Ads")
            return True
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Ошибка при загрузке метрик Google Ads в БД: {e}")
            logger.error(traceback.format_exc())
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    except Exception as e:
        logger.error(f"Ошибка в процессе load_google_ads_metrics_to_db: {e}")
        logger.error(traceback.format_exc())
        raise

def load_conversion_metrics_to_db():
    """Загрузка метрик конверсии в базу данных."""
    try:
        metrics = fetch_conversion_metrics()
        if not metrics:
            logger.error("Получение метрик конверсии вернуло пустой результат")
            raise ValueError("Нет данных для загрузки в таблицу conversion_metrics")
        
        # Загрузка данных в БД
        conn = None
        cursor = None
        try:
            conn = psycopg2.connect(**POSTGRES_CONFIG)
            cursor = conn.cursor()
            
            # Вставка данных с обработкой дубликатов
            query = """
            INSERT INTO staging.ga4_conversion_metrics (
                report_date, 
                campaign_name, 
                ad_group, 
                source, 
                medium, 
                network_type,
                event_name,
                event_count,
                conversion_rate,
                revenue,
                cost_per_event,
                roas
            ) VALUES %s
            ON CONFLICT (report_date, campaign_name, event_name, network_type) DO UPDATE SET
                ad_group = EXCLUDED.ad_group,
                source = EXCLUDED.source,
                medium = EXCLUDED.medium,
                event_count = EXCLUDED.event_count,
                conversion_rate = EXCLUDED.conversion_rate,
                revenue = EXCLUDED.revenue,
                cost_per_event = EXCLUDED.cost_per_event,
                roas = EXCLUDED.roas;
            """
            
            # Разбиваем на пакеты по 1000 записей
            batch_size = 1000
            rows_loaded = 0
            for i in range(0, len(metrics), batch_size):
                batch = metrics[i:i+batch_size]
                psycopg2.extras.execute_values(cursor, query, batch)
                conn.commit()
                rows_loaded += len(batch)
                logger.info(f"Загружена партия {i//batch_size + 1} из {(len(metrics)-1)//batch_size + 1}, размер: {len(batch)}")
                
            logger.info(f"Загружено {rows_loaded} записей метрик конверсии")
            return True
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Ошибка при загрузке метрик конверсии в БД: {e}")
            logger.error(traceback.format_exc())
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    except Exception as e:
        logger.error(f"Ошибка в процессе load_conversion_metrics_to_db: {e}")
        logger.error(traceback.format_exc())
        raise

def load_campaign_budget_metrics_to_db():
    """Загрузка метрик бюджетов кампаний в базу данных."""
    try:
        metrics = fetch_campaign_budget_metrics()
        if not metrics:
            logger.error("Получение метрик бюджетов кампаний вернуло пустой результат")
            raise ValueError("Нет данных для загрузки в таблицу campaign_budget_metrics")
        
        # Загрузка данных в БД
        conn = None
        cursor = None
        try:
            conn = psycopg2.connect(**POSTGRES_CONFIG)
            cursor = conn.cursor()
            
            # Вставка данных с обработкой дубликатов
            query = """
            INSERT INTO staging.ga4_campaign_budget_metrics (
                report_date, 
                campaign_name, 
                source, 
                medium, 
                daily_budget,
                monthly_budget,
                spend,
                spend_percentage,
                avg_daily_spend,
                estimated_monthly_spend
            ) VALUES %s
            ON CONFLICT (report_date, campaign_name) DO UPDATE SET
                source = EXCLUDED.source,
                medium = EXCLUDED.medium,
                daily_budget = EXCLUDED.daily_budget,
                monthly_budget = EXCLUDED.monthly_budget,
                spend = EXCLUDED.spend,
                spend_percentage = EXCLUDED.spend_percentage,
                avg_daily_spend = EXCLUDED.avg_daily_spend,
                estimated_monthly_spend = EXCLUDED.estimated_monthly_spend;
            """
            
            psycopg2.extras.execute_values(cursor, query, metrics)
            conn.commit()
            logger.info(f"Загружено {len(metrics)} записей метрик бюджетов кампаний")
            return True
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Ошибка при загрузке метрик бюджетов кампаний в БД: {e}")
            logger.error(traceback.format_exc())
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    except Exception as e:
        logger.error(f"Ошибка в процессе load_campaign_budget_metrics_to_db: {e}")
        logger.error(traceback.format_exc())
        raise

def load_wow_metrics_to_db():
    """Загрузка метрик week-over-week в базу данных."""
    try:
        metrics = calculate_wow_metrics()
        if not metrics:
            logger.error("Расчет метрик week-over-week вернул пустой результат")
            raise ValueError("Нет данных для загрузки в таблицу wow_metrics")
        
        # Загрузка данных в БД
        conn = None
        cursor = None
        try:
            conn = psycopg2.connect(**POSTGRES_CONFIG)
            cursor = conn.cursor()
            
            # Вставка данных с обработкой дубликатов
            query = """
            INSERT INTO staging.ga4_wow_metrics (
                report_date, 
                metric_name, 
                metric_value, 
                previous_week_value, 
                wow_change_percentage,
                week_number,
                year
            ) VALUES %s
            ON CONFLICT (report_date, metric_name) DO UPDATE SET
                metric_value = EXCLUDED.metric_value,
                previous_week_value = EXCLUDED.previous_week_value,
                wow_change_percentage = EXCLUDED.wow_change_percentage,
                week_number = EXCLUDED.week_number,
                year = EXCLUDED.year;
            """
            
            psycopg2.extras.execute_values(cursor, query, metrics)
            conn.commit()
            logger.info(f"Загружено {len(metrics)} записей метрик week-over-week")
            return True
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Ошибка при загрузке метрик week-over-week в БД: {e}")
            logger.error(traceback.format_exc())
            raise  # Перебрасываем исключение, чтобы задача завершилась с ошибкой
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    except Exception as e:
        logger.error(f"Ошибка при загрузке метрик week-over-week: {e}")
        logger.error(traceback.format_exc())
        raise  # Перебрасываем исключение, чтобы задача завершилась с ошибкой


def calculate_conversion_metrics():
    """Расчет метрик конверсии на основе данных о кампаниях и ключевых событиях."""
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        
        # Проверяем существование таблицы перед запросом
        cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'staging' 
            AND table_name = 'ga4_key_event_metrics'
        )
        """)
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            logger.error("Таблица staging.ga4_key_event_metrics не существует. Невозможно рассчитать метрики конверсии.")
            raise ValueError("Таблица с ключевыми событиями не существует")
        
        # Запрос для сбора данных о кликах и расходах по кампаниям
        cursor.execute("""
        SELECT 
            report_date, 
            campaign_name, 
            source, 
            medium, 
            network_type,
            clicks as visitors,
            cost
        FROM 
            staging.ga4_google_ads_metrics
        WHERE 
            report_date BETWEEN %s AND %s
        """, (START_DATE, END_DATE))
        
        campaign_data = {}
        for row in cursor.fetchall():
            key = (row[0], row[1], row[4])  # date, campaign, network_type
            campaign_data[key] = {
                'date': row[0],
                'campaign': row[1],
                'source': row[2],
                'medium': row[3],
                'network_type': row[4],
                'visitors': row[5],
                'cost': row[6],
                'registrations': 0,
                'leads': 0,
                'paid_users': 0
            }
        
        # Запрос для сбора данных о регистрациях (platform_signup)
        cursor.execute("""
        SELECT 
            report_date, 
            campaign_name, 
            network_type,
            SUM(event_count) as registrations
        FROM 
            staging.ga4_key_event_metrics
        WHERE 
            key_event_name = 'platform_signup' AND
            report_date BETWEEN %s AND %s
        GROUP BY
            report_date, campaign_name, network_type
        """, (START_DATE, END_DATE))
        
        for row in cursor.fetchall():
            key = (row[0], row[1], row[2])  # date, campaign, network_type
            if key in campaign_data:
                campaign_data[key]['registrations'] = row[3]
        
        # Запрос для сбора данных о лидах (main_form_submit)
        cursor.execute("""
        SELECT 
            report_date, 
            campaign_name, 
            network_type,
            SUM(event_count) as leads
        FROM 
            staging.ga4_key_event_metrics
        WHERE 
            key_event_name = 'main_form_submit' AND
            report_date BETWEEN %s AND %s
        GROUP BY
            report_date, campaign_name, network_type
        """, (START_DATE, END_DATE))
        
        for row in cursor.fetchall():
            key = (row[0], row[1], row[2])  # date, campaign, network_type
            if key in campaign_data:
                campaign_data[key]['leads'] = row[3]
        
        # Запрос для сбора данных о платных пользователях (purchase)
        cursor.execute("""
        SELECT 
            report_date, 
            campaign_name, 
            network_type,
            SUM(event_count) as paid_users
        FROM 
            staging.ga4_key_event_metrics
        WHERE 
            key_event_name = 'purchase' AND
            report_date BETWEEN %s AND %s
        GROUP BY
            report_date, campaign_name, network_type
        """, (START_DATE, END_DATE))
        
        for row in cursor.fetchall():
            key = (row[0], row[1], row[2])  # date, campaign, network_type
            if key in campaign_data:
                campaign_data[key]['paid_users'] = row[3]
        
        # Расчет метрик конверсии и вставка в таблицу
        results = []
        for key, data in campaign_data.items():
            visitors = data['visitors'] or 1  # избегаем деления на ноль
            registrations = data['registrations'] or 0
            leads = data['leads'] or 0
            paid_users = data['paid_users'] or 0
            cost = data['cost'] or 0
            
            visitors_to_regs_rate = registrations / visitors if visitors > 0 else 0
            visitors_to_leads_rate = leads / visitors if visitors > 0 else 0
            regs_to_paid_users_rate = paid_users / registrations if registrations > 0 else 0
            
            cost_per_registration = cost / registrations if registrations > 0 else 0
            cost_per_lead = cost / leads if leads > 0 else 0
            cost_per_acquisition = cost / paid_users if paid_users > 0 else 0
            
            results.append((
                data['date'],
                data['campaign'],
                data['source'],
                data['medium'],
                data['network_type'],
                visitors,
                registrations,
                leads,
                paid_users,
                visitors_to_regs_rate,
                visitors_to_leads_rate,
                regs_to_paid_users_rate,
                cost_per_registration,
                cost_per_lead,
                cost_per_acquisition
            ))
        
        return results
    except Exception as e:
        logger.error(f"Ошибка при расчете метрик конверсии: {e}")
        logger.error(traceback.format_exc())
        return []
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def load_conversion_metrics_to_db():
    """Загрузка рассчитанных метрик конверсии в базу данных."""
    metrics = calculate_conversion_metrics()
    if not metrics:
        logger.warning("Нет данных для загрузки в таблицу ga4_conversion_rates")
        return
    
    # Загрузка данных в БД
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        
        # Вставка данных с обработкой дубликатов
        query = """
        INSERT INTO staging.ga4_conversion_rates (
            report_date, 
            campaign_name, 
            source, 
            medium, 
            network_type,
            visitors,
            registrations,
            leads,
            paid_users,
            visitors_to_regs_rate,
            visitors_to_leads_rate,
            regs_to_paid_users_rate,
            cost_per_registration,
            cost_per_lead,
            cost_per_acquisition
        ) VALUES %s
        ON CONFLICT (report_date, campaign_name, network_type) DO UPDATE SET
            source = EXCLUDED.source,
            medium = EXCLUDED.medium,
            visitors = EXCLUDED.visitors,
            registrations = EXCLUDED.registrations,
            leads = EXCLUDED.leads,
            paid_users = EXCLUDED.paid_users,
            visitors_to_regs_rate = EXCLUDED.visitors_to_regs_rate,
            visitors_to_leads_rate = EXCLUDED.visitors_to_leads_rate,
            regs_to_paid_users_rate = EXCLUDED.regs_to_paid_users_rate,
            cost_per_registration = EXCLUDED.cost_per_registration,
            cost_per_lead = EXCLUDED.cost_per_lead,
            cost_per_acquisition = EXCLUDED.cost_per_acquisition;
        """
        
        psycopg2.extras.execute_values(cursor, query, metrics)
        conn.commit()
        logger.info(f"Загружено {len(metrics)} записей метрик конверсии")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Ошибка при загрузке метрик конверсии: {e}")
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
    'GA4_ADVERTISING_METRICS',
    default_args=default_args,
    description='Импорт расширенных метрик рекламы и бюджетов из GA4',
    schedule_interval='30 5 * * *',  
    start_date=datetime(2025, 3, 5),
    catchup=False,
    tags=['ga4', 'advertising', 'google-ads'],
) as dag:
    
    # Задачи
    test_connection = PythonOperator(
        task_id='test_ga4_connection',
        python_callable=test_ga4_connection,
        # Добавляем этот параметр, чтобы Python функция вызывала исключения
        trigger_rule='all_success',
    )
    
    create_db_tables = PythonOperator(
        task_id='create_db_tables',
        python_callable=create_tables,
        # Добавляем проверку результата выполнения
        trigger_rule='all_success',
    )

    wait_for_user_behavior = ExternalTaskSensor(
        task_id='wait_for_user_behavior',
        external_dag_id='GA4_USER_BEHAVIOR',
        external_task_id='load_key_event_metrics',
        allowed_states=['success'],
        timeout=3600,
        poke_interval=60,
        mode='reschedule'
    )
    
    load_google_ads_metrics = PythonOperator(
        task_id='load_google_ads_metrics',
        python_callable=load_google_ads_metrics_to_db,
        trigger_rule='all_success',
    )
    
    load_conversion_metrics = PythonOperator(
        task_id='load_conversion_metrics',
        python_callable=load_conversion_metrics_to_db,
        trigger_rule='all_success',
    )
    
    load_campaign_budget_metrics = PythonOperator(
        task_id='load_campaign_budget_metrics',
        python_callable=load_campaign_budget_metrics_to_db,
        trigger_rule='all_success',
    )
    
    load_wow_metrics = PythonOperator(
        task_id='load_wow_metrics',
        python_callable=load_wow_metrics_to_db,
        trigger_rule='all_success',
    )
    
    # Определение порядка выполнения задач
    test_connection >> create_db_tables
    create_db_tables >> [load_google_ads_metrics, load_conversion_metrics, load_campaign_budget_metrics]
    [load_google_ads_metrics, load_conversion_metrics, load_campaign_budget_metrics] >> load_wow_metrics