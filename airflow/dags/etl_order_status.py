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
    'etl_order_status',
    schedule_interval='@once',
    catchup=False,
    default_args=default_args,
    description='Loads order statues updates daily.'
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
    );

    CREATE TABLE IF NOT EXISTS order_status (
      id              SERIAL PRIMARY KEY,
      transitioned_at TIMESTAMP NOT NULL,
      status          VARCHAR(64),
      order_id        INTEGER REFERENCES orders(id),
      customer_id     INTEGER REFERENCES customers(id),
      restaurant_id   INTEGER REFERENCES restaurants(id),
      driver_id       INTEGER REFERENCES drivers(id)
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

    INSERT INTO order_status (id, transitioned_at, status, order_id, customer_id, driver_id, restaurant_id)
      SELECT id, transitioned_at, status, order_id, customer_id, driver_id, restaurant_id
        FROM tmp_order_status;
    ''',
    dag=dag
)

wait >> t1 >> t2
