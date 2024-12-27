from datetime import timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

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
    'init',
    schedule_interval='@once',
    catchup=False,
    default_args=default_args,
    description='Load seed data for restaurants, orders, drivers, etc.',
)

create_tmp_tables = """
CREATE TABLE IF NOT EXISTS tmp_cities (
  id         SERIAL PRIMARY KEY,
  name       VARCHAR(64) NOT NULL,
  state      VARCHAR(64) NOT NULL,
  country    VARCHAR(64) NOT NULL
);

CREATE TABLE IF NOT EXISTS tmp_business_hours (
  id         SERIAL PRIMARY KEY,
  weekday    VARCHAR(64) NOT NULL,
  open_time  TIME NOT NULL,
  close_time TIME NOT NULL
);

CREATE TABLE IF NOT EXISTS tmp_restaurants (
  id                SERIAL PRIMARY KEY,
  created_at        TIMESTAMP NOT NULL,
  updated_at        TIMESTAMP NOT NULL,
  name              VARCHAR(64) NOT NULL,
  email             VARCHAR(64) UNIQUE NOT NULL,
  address           VARCHAR(64) NOT NULL,
  phone             VARCHAR(64) NOT NULL,
  city_id           INTEGER REFERENCES tmp_cities(id),
  business_hours_id INTEGER REFERENCES tmp_business_hours(id),
  description       TEXT
);

CREATE TABLE IF NOT EXISTS tmp_menus (
  id            SERIAL PRIMARY KEY,
  name          VARCHAR(64) NOT NULL,
  restaurant_id INTEGER REFERENCES tmp_restaurants(id),
  description   TEXT,
  UNIQUE (name, restaurant_id)
);

CREATE TABLE IF NOT EXISTS tmp_categories (
  id          SERIAL PRIMARY KEY,
  name        VARCHAR(64) NOT NULL,
  menu_id     INTEGER REFERENCES tmp_menus(id),
  description TEXT,
  UNIQUE (name, menu_id)
);

CREATE TABLE IF NOT EXISTS tmp_menu_items (
  id          SERIAL PRIMARY KEY,
  name        VARCHAR(64) NOT NULL,
  price       VARCHAR(64) NOT NULL,
  category_id INTEGER REFERENCES tmp_categories(id),
  description TEXT,
  UNIQUE (name, category_id)
);

CREATE TABLE IF NOT EXISTS tmp_discounts (
  id            SERIAL PRIMARY KEY,
  name          VARCHAR(64) NOT NULL,
  type          VARCHAR(32) NOT NULL,
  value         NUMERIC NOT NULL,
  start_date    TIMESTAMP NOT NULL,
  end_date      TIMESTAMP NOT NULL,
  is_active     BOOLEAN DEFAULT TRUE,
  description   TEXT
);

CREATE TABLE IF NOT EXISTS tmp_orders (
  id           SERIAL PRIMARY KEY,
  placed_on    TIMESTAMP NOT NULL,
  menu_item_id INTEGER REFERENCES tmp_menu_items(id),
  quantity     INTEGER NOT NULL,
  discount_id  INTEGER REFERENCES tmp_discounts(id),
  comment      TEXT
);

CREATE TABLE IF NOT EXISTS tmp_customers (
  id         SERIAL PRIMARY KEY,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  name       VARCHAR(64) NOT NULL,
  email      VARCHAR(64) UNIQUE NOT NULL,
  address    VARCHAR(64) NOT NULL,
  phone      VARCHAR(64) NOT NULL,
  city_id    INTEGER REFERENCES tmp_cities(id)
);

CREATE TABLE IF NOT EXISTS tmp_drivers (
  id                SERIAL PRIMARY KEY,
  created_at        TIMESTAMP NOT NULL,
  updated_at        TIMESTAMP NOT NULL,
  name              VARCHAR(64) NOT NULL,
  email             VARCHAR(64) UNIQUE NOT NULL,
  phone             VARCHAR(64) NOT NULL,
  car_make          VARCHAR(64) NOT NULL,
  car_model         VARCHAR(64) NOT NULL,
  car_year          VARCHAR(64) NOT NULL,
  car_color         VARCHAR(64) NOT NULL,
  car_license_plate VARCHAR(64) NOT NULL
);

CREATE TABLE IF NOT EXISTS tmp_order_status (
  id              SERIAL PRIMARY KEY,
  transitioned_at TIMESTAMP NOT NULL,
  status          VARCHAR(64),
  order_id        INTEGER REFERENCES tmp_orders(id),
  customer_id     INTEGER REFERENCES tmp_customers(id),
  restaurant_id   INTEGER REFERENCES tmp_restaurants(id),
  driver_id       INTEGER REFERENCES tmp_drivers(id)
);
"""

t1 = PostgresOperator(
    task_id='setup',
    postgres_conn_id='food_delivery_db',
    sql=create_tmp_tables,
    dag=dag,
)

seed_tmp_tables = """
-- Seed tmp_cities
INSERT INTO tmp_cities (name, state, country)
VALUES
  ('San Francisco', 'California', 'USA'),
  ('New York', 'New York', 'USA'),
  ('Austin', 'Texas', 'USA');

INSERT INTO tmp_business_hours (weekday, open_time, close_time)
VALUES
  ('Monday', '09:00:00', '21:00:00'),
  ('Tuesday', '09:00:00', '21:00:00'),
  ('Wednesday', '09:00:00', '21:00:00'),
  ('Thursday', '09:00:00', '21:00:00'),
  ('Friday', '09:00:00', '23:00:00'),
  ('Saturday', '10:00:00', '23:00:00'),
  ('Sunday', '10:00:00', '20:00:00');

INSERT INTO tmp_restaurants (created_at, updated_at, name, email, address, phone, city_id, business_hours_id, description)
VALUES
  (NOW(), NOW(), 'Golden Gate Eats', 'contact@ggeats.com', '123 Golden Gate St', '415-555-1234', 1, 1, 'A fusion of global cuisines'),
  (NOW(), NOW(), 'Empire State Dining', 'info@esdining.com', '456 Empire Ave', '212-555-5678', 2, 2, 'Classic New York comfort food');

INSERT INTO tmp_menus (name, restaurant_id, description)
VALUES
  ('Lunch Menu', 1, 'Available from 11 AM to 3 PM'),
  ('Dinner Menu', 1, 'Available from 5 PM to 10 PM'),
  ('All Day Menu', 2, 'Available throughout the day');

INSERT INTO tmp_categories (name, menu_id, description)
VALUES
  ('Appetizers', 1, 'Start your meal with small bites'),
  ('Main Courses', 1, 'Hearty and satisfying dishes'),
  ('Desserts', 2, 'Sweet treats to end your meal'),
  ('Beverages', 2, 'Refreshing drinks to complement your food');

INSERT INTO tmp_menu_items (name, price, category_id, description)
VALUES
  ('Spring Rolls', '8.50', 1, 'Crispy rolls filled with vegetables and served with dipping sauce'),
  ('Grilled Salmon', '18.00', 2, 'Perfectly grilled salmon with a lemon butter sauce'),
  ('Chocolate Cake', '6.50', 3, 'Rich and moist chocolate cake with a ganache topping'),
  ('Iced Tea', '3.00', 4, 'Freshly brewed iced tea served over ice');

INSERT INTO tmp_discounts (name, type, value, start_date, end_date, is_active, description)
VALUES
  ('Holiday Special', 'percentage', 10.0, '2024-12-01 00:00:00', '2024-12-31 23:59:59', TRUE, '10% off all orders during December'),
  ('First Order Discount', 'flat_amount', 5.0, '2024-01-01 00:00:00', '2024-12-31 23:59:59', TRUE, 'Get $5 off your first order');

INSERT INTO tmp_customers (created_at, updated_at, name, email, address, phone, city_id)
VALUES
  (NOW(), NOW(), 'Alice Johnson', 'alice.johnson@example.com', '789 Maple St', '555-123-4567', 1),
  (NOW(), NOW(), 'Bob Smith', 'bob.smith@example.com', '321 Elm Ave', '555-987-6543', 2);

INSERT INTO tmp_drivers (created_at, updated_at, name, email, phone, car_make, car_model, car_year, car_color, car_license_plate)
VALUES
  (NOW(), NOW(), 'John Doe', 'john.doe@delivery.com', '555-555-1212', 'Toyota', 'Camry', '2020', 'White', 'ABC123'),
  (NOW(), NOW(), 'Jane Roe', 'jane.roe@delivery.com', '555-555-3434', 'Honda', 'Civic', '2021', 'Black', 'XYZ456');

INSERT INTO tmp_orders (placed_on, menu_item_id, quantity, discount_id, comment)
VALUES
  (NOW(), 1, 2, 1, 'No peanuts, please'),
  (NOW(), 3, 1, NULL, 'Add extra chocolate sauce');

INSERT INTO tmp_order_status (transitioned_at, status, order_id, customer_id, restaurant_id, driver_id)
VALUES
  (NOW(), 'Placed', 1, 1, 1, 1),
  (NOW(), 'Delivered', 2, 2, 2, 2);
"""

t2 = PostgresOperator(
    task_id='seed',
    postgres_conn_id='food_delivery_db',
    sql=seed_tmp_tables,
    dag=dag,
)

t1 >> t2