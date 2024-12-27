from datetime import timedelta
from airflow.models.dag import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago

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
    'email_discounts',
    schedule_interval='@hourly',
    catchup=False,
    default_args=default_args,
    description='Email discounts to customers that have experienced order delays daily.'
)

t1 = PostgresOperator(
    task_id='if_not_exists',
    postgres_conn_id='food_delivery_db',
    sql='''
    CREATE TABLE IF NOT EXISTS discounts (
      id            SERIAL PRIMARY KEY,
      name          VARCHAR(64) NOT NULL,
      type          VARCHAR(32) NOT NULL,
      value         NUMERIC NOT NULL,
      start_date    TIMESTAMP NOT NULL,
      end_date      TIMESTAMP NOT NULL,
      is_active     BOOLEAN DEFAULT TRUE,
      description   TEXT
    );''',
    dag=dag
)

t2 = PostgresOperator(
    task_id='insert',
    postgres_conn_id='food_delivery_db',
    sql='''
    SELECT * FROM discounts;
    ''',
    dag=dag
)

t1 >> t2
