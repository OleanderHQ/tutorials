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
    'etl_categories',
    schedule_interval='@once',
    catchup=False,
    default_args=default_args,
    description='Loads newly added menus categories daily.'
)

wait = ExternalTaskSensor(
    task_id='wait_for_etl_menus',
    external_dag_id='etl_menus',
    allowed_states=[DagRunState.SUCCESS],
    timeout=600,
    mode='poke',
    dag=dag,
)

t1 = PostgresOperator(
    task_id='if_not_exists',
    postgres_conn_id='food_delivery_db',
    sql='''
    CREATE TABLE IF NOT EXISTS categories (
      id          SERIAL PRIMARY KEY,
      name        VARCHAR(64) NOT NULL,
      menu_id     INTEGER REFERENCES menus(id),
      description TEXT,
      UNIQUE (name, menu_id)
    );''',
    dag=dag
)

t2 = PostgresOperator(
    task_id='insert',
    postgres_conn_id='food_delivery_db',
    sql='''
    INSERT INTO categories (id, name, menu_id, description)
      SELECT id, name, menu_id, description
        FROM tmp_categories;
    ''',
    dag=dag
)

wait >> t1 >> t2
