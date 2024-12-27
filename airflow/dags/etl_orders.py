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
    'etl_orders',
    schedule_interval='@hourly',
    catchup=False,
    default_args=default_args,
    description='Loads newly placed orders daily.'
)

wait = ExternalTaskSensor(
    task_id='wait_for_etl_menu_items',
    external_dag_id='etl_menu_items',
    allowed_states=[DagRunState.SUCCESS],
    timeout=600,
    mode='poke',
    dag=dag,
)

t1 = PostgresOperator(
    task_id='if_not_exists',
    postgres_conn_id='food_delivery_db',
    sql='''
    CREATE TABLE IF NOT EXISTS orders (
      id           SERIAL PRIMARY KEY,
      placed_on    TIMESTAMP NOT NULL,
      menu_item_id INTEGER REFERENCES menu_items(id),
      quantity     INTEGER NOT NULL,
      discount_id  INTEGER REFERENCES discounts(id),
      comment      TEXT
    );''',
    dag=dag
)

t2 = PostgresOperator(
    task_id='insert',
    postgres_conn_id='food_delivery_db',
    sql='''
    INSERT INTO orders (id, placed_on, menu_item_id, quantity, discount_id, comment)
      SELECT id, placed_on, menu_item_id, quantity, discount_id, comment
        FROM tmp_orders;
    ''',
    dag=dag
)

wait >> t1 >> t2
