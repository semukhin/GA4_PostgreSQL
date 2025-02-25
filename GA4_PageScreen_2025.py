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
    """Создание таблицы в схеме staging для PageScreen метрик с триггером для генерации хеш-идентификатора."""
    # Создаем подключение
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()
    
    # Создаем схему если она не существует
    cur.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    conn.commit()
    
    # Создаем функцию для генерации хеш-идентификатора с явным указанием схемы
    function_query = """
    CREATE OR REPLACE FUNCTION staging.generate_pagescreen_record_id()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.record_id := MD5(NEW.report_date::text || NEW.page_path || 
                           COALESCE(NEW.page_referrer, '') || 
                           COALESCE(NEW.landing_page, '') || 
                           NEW.host_name);
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    """
    cur.execute(function_query)
    conn.commit()
    
    # Проверяем существование таблицы
    check_table_query = """
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'staging' AND tablename = 'ga4_pagescreen_metrics') THEN
            -- Создаем таблицу с обычным столбцом record_id
            CREATE TABLE staging.ga4_pagescreen_metrics (
                report_date DATE,
                page_path TEXT,
                page_title TEXT,
                page_referrer TEXT,
                landing_page TEXT,
                host_name TEXT,
                screen_page_views INT,
                screen_page_views_per_session FLOAT,
                screen_page_views_per_user FLOAT,
                record_id VARCHAR(64),
                PRIMARY KEY (report_date, page_path, page_referrer, landing_page, host_name)
            );
            
            -- Создаем индекс по хеш-идентификатору для быстрого поиска
            CREATE INDEX idx_ga4_pagescreen_record_id ON staging.ga4_pagescreen_metrics(record_id);
            
            RAISE NOTICE 'Таблица staging.ga4_pagescreen_metrics создана';
        ELSE
            -- Проверяем наличие столбца record_id
            IF NOT EXISTS (
                SELECT FROM information_schema.columns 
                WHERE table_schema = 'staging' AND table_name = 'ga4_pagescreen_metrics' AND column_name = 'record_id'
            ) THEN
                -- Добавляем столбец record_id к существующей таблице
                ALTER TABLE staging.ga4_pagescreen_metrics ADD COLUMN record_id VARCHAR(64);
                
                -- Создаем индекс по хеш-идентификатору
                CREATE INDEX idx_ga4_pagescreen_record_id ON staging.ga4_pagescreen_metrics(record_id);
                
                RAISE NOTICE 'Добавлен хеш-идентификатор в существующую таблицу staging.ga4_pagescreen_metrics';
            ELSE
                RAISE NOTICE 'Таблица staging.ga4_pagescreen_metrics уже существует с хеш-идентификатором';
            END IF;
        END IF;
    END $$;
    """
    cur.execute(check_table_query)
    conn.commit()
    
    # Создаем триггер с правильным указанием схемы функции
    trigger_query = """
    DROP TRIGGER IF EXISTS ga4_pagescreen_record_id_trigger ON staging.ga4_pagescreen_metrics;
    CREATE TRIGGER ga4_pagescreen_record_id_trigger
    BEFORE INSERT OR UPDATE ON staging.ga4_pagescreen_metrics
    FOR EACH ROW EXECUTE FUNCTION staging.generate_pagescreen_record_id();
    """
    cur.execute(trigger_query)
    conn.commit()
    
    # Обновляем существующие записи, если есть
    update_query = """
    UPDATE staging.ga4_pagescreen_metrics
    SET record_id = MD5(report_date::text || page_path || 
                       COALESCE(page_referrer, '') || 
                       COALESCE(landing_page, '') || 
                       host_name)
    WHERE record_id IS NULL;
    """
    cur.execute(update_query)
    
    # Сохраняем изменения и закрываем соединение
    conn.commit()
    cur.close()
    conn.close()
    
    print("Таблица staging.ga4_pagescreen_metrics успешно создана/обновлена с хеш-идентификатором")

def fetch_ga4_data_for_date(date_str):
    """Получение данных PageScreen из GA4 за указанную дату."""
    client = BetaAnalyticsDataClient()
    
    # Определяем измерения
    dimensions = [
        "pagePath",
        "pageTitle", 
        "pageReferrer",
        "landingPage",
        "hostName"
    ]
    
    # Определяем метрики
    metrics = [
        "screenPageViews",
        "screenPageViewsPerSession",
        "screenPageViewsPerUser"
    ]
    
    try:
        request = RunReportRequest(
            property=PROPERTY_ID,
            date_ranges=[DateRange(start_date=date_str, end_date=date_str)],
            dimensions=[Dimension(name=dim) for dim in dimensions],
            metrics=[Metric(name=metric) for metric in metrics],
            limit=50000  # Устанавливаем высокий лимит для получения всех данных
        )
        
        response = client.run_report(request)
        results = []
        
        for row in response.rows:
            try:
                # Извлекаем значения измерений
                page_path = row.dimension_values[0].value or ""
                page_title = row.dimension_values[1].value or ""
                page_referrer = row.dimension_values[2].value or ""
                landing_page = row.dimension_values[3].value or ""
                host_name = row.dimension_values[4].value or ""
                
                # Извлекаем и конвертируем значения метрик
                screen_page_views = int(float(row.metric_values[0].value or 0))
                screen_page_views_per_session = float(row.metric_values[1].value or 0.0)
                screen_page_views_per_user = float(row.metric_values[2].value or 0.0)
                
                results.append((
                    date_str,
                    page_path,
                    page_title,
                    page_referrer,
                    landing_page,
                    host_name,
                    screen_page_views,
                    screen_page_views_per_session,
                    screen_page_views_per_user
                ))
            except (IndexError, ValueError) as e:
                print(f"Ошибка обработки данных строки для даты {date_str}: {e}")
                continue
        
        print(f"Успешно получено {len(results)} строк для даты {date_str}")
        return results
    
    except Exception as e:
        print(f"Ошибка получения данных для даты {date_str}: {e}")
        return []

def fetch_and_load_pagescreen_data():
    """Основная функция для получения и загрузки данных PageScreen."""
    all_data = []
    current_date = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_date = datetime.strptime(END_DATE, "%Y-%m-%d")
    
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        print(f"Получение данных для даты: {date_str}")
        data = fetch_ga4_data_for_date(date_str)
        all_data.extend(data)
        current_date += timedelta(days=1)
    
    if not all_data:
        print("Нет данных для загрузки.")
        return
    
    # Удаляем дубликаты по первичному ключу
    unique_data = {}
    for row in all_data:
        # Создаем ключ на основе составного первичного ключа
        key = (row[0], row[1], row[3], row[4], row[5])  # report_date, page_path, page_referrer, landing_page, host_name
        # Если у нас уже есть запись с таким ключом, перезаписываем ее
        unique_data[key] = row
    
    # Преобразуем обратно в список
    deduplicated_data = list(unique_data.values())
    print(f"Удалено {len(all_data) - len(deduplicated_data)} дубликатов. Осталось {len(deduplicated_data)} уникальных записей.")
    
    insert_query = """
    INSERT INTO staging.ga4_pagescreen_metrics (
        report_date,
        page_path,
        page_title,
        page_referrer,
        landing_page,
        host_name,
        screen_page_views,
        screen_page_views_per_session,
        screen_page_views_per_user
    ) VALUES %s
    ON CONFLICT (report_date, page_path, page_referrer, landing_page, host_name) DO UPDATE SET
        page_title = EXCLUDED.page_title,
        screen_page_views = EXCLUDED.screen_page_views,
        screen_page_views_per_session = EXCLUDED.screen_page_views_per_session,
        screen_page_views_per_user = EXCLUDED.screen_page_views_per_user;
    """

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()

    try:
        # Обрабатываем данные партиями для оптимизации
        batch_size = 1000
        for i in range(0, len(deduplicated_data), batch_size):
            batch = deduplicated_data[i:i+batch_size]
            psycopg2.extras.execute_values(cur, insert_query, batch)
            conn.commit()
            print(f"Загружена партия {i//batch_size + 1} ({len(batch)} записей)")
        
        print(f"Всего загружено: {len(deduplicated_data)} записей в staging.ga4_pagescreen_metrics")
    except Exception as e:
        conn.rollback()
        print(f"Ошибка загрузки данных: {e}")
    finally:
        cur.close()
        conn.close()

# Создаем DAG
with DAG(
    dag_id="GA4_PAGESCREEN_2025",
    default_args={
        'owner': 'semukhin',
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    },
    description="Импорт метрик и измерений PageScreen из GA4 в PostgreSQL за период с 01.09.2024 по текущую дату с хеш-идентификатором",
    schedule_interval='@daily',
    start_date=datetime(2025, 2, 16),
    catchup=False,
) as dag:

    create_table = PythonOperator(
        task_id="create_staging_table",
        python_callable=create_staging_table
    )

    fetch_and_load_data = PythonOperator(
        task_id="fetch_and_load_pagescreen_data",
        python_callable=fetch_and_load_pagescreen_data
    )

    create_table >> fetch_and_load_data