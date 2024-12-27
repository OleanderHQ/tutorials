from datetime import timedelta
from airflow.models.dag import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import DagRunState

default_args = {
    'owner': 'datascience',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['datascience@oleander.dev'],
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'etl_restaurants',
    schedule_interval='@once',
    catchup=False,
    default_args=default_args,
    description='Loads newly registered restaurants daily.'
)

wait = ExternalTaskSensor(
    task_id='wait_for_init',
    external_dag_id='init',
    allowed_states=[DagRunState.SUCCESS],
    timeout=90,
    mode='poke',
    dag=dag,
)

t1 = PostgresOperator(
    task_id='if_not_exists',
    postgres_conn_id='food_delivery_db',
    sql='''
    CREATE TABLE IF NOT EXISTS cities (
      id         SERIAL PRIMARY KEY,
      name       VARCHAR(64) NOT NULL,
      state      VARCHAR(64) NOT NULL,
      country    VARCHAR(64) NOT NULL
    );

    CREATE TABLE IF NOT EXISTS business_hours (
      id         SERIAL PRIMARY KEY,
      weekday    VARCHAR(64) NOT NULL,
      open_time  TIME NOT NULL,
      close_time TIME NOT NULL
    );

    CREATE TABLE IF NOT EXISTS restaurants (
      id                SERIAL PRIMARY KEY,
      created_at        TIMESTAMP NOT NULL,
      updated_at        TIMESTAMP NOT NULL,
      name              VARCHAR(64) NOT NULL,
      email             VARCHAR(64) UNIQUE NOT NULL,
      address           VARCHAR(64) NOT NULL,
      phone             VARCHAR(64) NOT NULL,
      city_id           INTEGER REFERENCES cities(id),
      business_hours_id INTEGER REFERENCES business_hours(id),
      description       TEXT
    );''',
    dag=dag
)

t2 = PostgresOperator(
    task_id='etl',
    postgres_conn_id='food_delivery_db',
    sql='''
    INSERT INTO cities (id, name, state, country)
      SELECT id, name, state, country
        FROM tmp_cities;

    INSERT INTO business_hours (id, weekday, open_time, close_time)
      SELECT id, weekday, open_time, close_time
        FROM tmp_business_hours;

    INSERT INTO restaurants (id, created_at, updated_at, name, email, address, phone, city_id, business_hours_id, description)
      SELECT id, created_at, updated_at, name, email, address, phone, city_id, business_hours_id, description
        FROM tmp_restaurants;
    ''',
    dag=dag
)

wait >> t1 >> t2
