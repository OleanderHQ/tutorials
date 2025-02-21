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
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'orders_popular_day_of_week',
    schedule_interval='@hourly',
    catchup=False,
    default_args=default_args,
    description='Determines the popular day of week orders are placed.'
)

wait = ExternalTaskSensor(
    task_id='wait_for_delivery_times_7_days',
    external_dag_id='delivery_times_7_days',
    allowed_states=[DagRunState.SUCCESS],
    timeout=600,
    mode='poke',
    dag=dag,
)

t1 = PostgresOperator(
    task_id='if_not_exists',
    postgres_conn_id='food_delivery_db',
    sql='''
    CREATE TABLE IF NOT EXISTS popular_orders_day_of_week (
      order_day_of_week VARCHAR(64) NOT NULL,
      order_placed_on   TIMESTAMP NOT NULL,
      orders_placed     INTEGER NOT NULL
    );''',
    dag=dag
)

t2 = PostgresOperator(
    task_id='insert',
    postgres_conn_id='food_delivery_db',
    sql='''
    INSERT INTO popular_orders_day_of_week (order_day_of_week, order_placed_on, orders_placed)
      SELECT EXTRACT(ISODOW FROM order_placed_on) AS order_day_of_week,
             order_placed_on,
             COUNT(*) AS orders_placed
        FROM top_delivery_times
       GROUP BY order_placed_on;
    ''',
    dag=dag
)

wait >> t1 >> t2
