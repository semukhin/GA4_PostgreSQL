from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

# Создаем мастер-DAG, который будет запускать все остальные DAG в нужном порядке

with DAG(
    dag_id="GA4_MASTER_2025",
    default_args={
        'owner': 'semukhin',
        'depends_on_past': False,
        'retries': 0,
    },
    description="Мастер-DAG для запуска всех импортов данных GA4",
    schedule='0 2 * * *',  # Запуск каждый день в 2:00
    start_date=datetime(2025, 2, 16),
    catchup=False,
) as dag:

    start = DummyOperator(
        task_id="start"
    )
    
    end = DummyOperator(
        task_id="end"
    )

    # Сенсоры для всех DAG, которые будут запускаться
    user_sensor = ExternalTaskSensor(
        task_id='wait_for_user_data',
        external_dag_id='GA4_USER_2025',
        external_task_id='fetch_and_load_data',
        allowed_states=['success'],
        timeout=3600,
        poke_interval=60,
        mode='reschedule'
    )
    
    session_sensor = ExternalTaskSensor(
        task_id='wait_for_session_data',
        external_dag_id='GA4_SESSION_2025',
        external_task_id='fetch_and_load_data',
        allowed_states=['success'],
        timeout=3600,
        poke_interval=60,
        mode='reschedule'
    )
    
    events_sensor = ExternalTaskSensor(
        task_id='wait_for_events_data',
        external_dag_id='GA4_EVENTS_2025',
        external_task_id='fetch_and_load_key_events',
        allowed_states=['success'],
        timeout=3600,
        poke_interval=60,
        mode='reschedule'
    )
    
    pages_sensor = ExternalTaskSensor(
        task_id='wait_for_pages_data',
        external_dag_id='GA4_PAGES_2025',
        external_task_id='fetch_and_load_data',
        allowed_states=['success'],
        timeout=3600,
        poke_interval=60,
        mode='reschedule'
    )
    
    adcampaigns_sensor = ExternalTaskSensor(
        task_id='wait_for_adcampaigns_data',
        external_dag_id='GA4_ADCAMPAIGNS_2025',
        external_task_id='fetch_and_load_data',
        allowed_states=['success'],
        timeout=3600,
        poke_interval=60,
        mode='reschedule'
    )
    
    # Определяем порядок выполнения
    start >> [user_sensor, session_sensor, events_sensor, pages_sensor, adcampaigns_sensor] >> end