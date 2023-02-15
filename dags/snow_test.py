from airflow import DAG
from datetime import datetime, timedelta
#from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'MY_DB'
SNOWFLAKE_SCHEMA = 'MY_SCHEMA'
SNOWFLAKE_WAREHOUSE= 'COMPUTE_WH'
SNOWFLAKE_ROLE = 'SYSADMIN'
SNOWFLAKE_TABLE= 'CUSTOMERS'
AWS_FILE_PATH = 's3://dbt-tutorial-public/jaffle_shop_customers.csv'


default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2023, 2, 12),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}



dag = DAG('send_data_to_snowflake',
          default_args=default_args,
          schedule_interval=timedelta(days=1),
          catchup=False)

create_table_sql = '''
        create or replace TABLE MY_DB.MY_SCHEMA.EMPLOYEE_airflow (
        ID NUMBER(38,0) NOT NULL autoincrement,
        NAME VARCHAR(50) NOT NULL,
        DEPARTMENT VARCHAR(50) NOT NULL,
        SALARY NUMBER(10,2) NOT NULL,
        HIRE_DATE DATE NOT NULL,
        primary key (ID)
         );
         '''

insert_data_sql = '''
       INSERT INTO MY_DB.MY_SCHEMA.EMPLOYEE_airflow (name,department,salary,hire_date)
        VALUES('jon2','it2',75000,'2020-01-01'),
        ('John Doe', 'IT', 75000, '2020-01-01'),
        ('Jane Doe', 'HR', 65000, '2021-03-01'),
        ('Bob Smith', 'Sales', 55000, '2022-06-01');

'''

create_table = SnowflakeOperator(
    task_id='create_table',
    sql = create_table_sql,
    snowflake_conn_id='snowflake_conn',
    dag=dag)

insert_into_table = SnowflakeOperator(
    task_id='insert_table',
    sql = insert_data_sql,
    snowflake_conn_id='snowflake_conn',
    dag=dag )


create_table >> insert_into_table

