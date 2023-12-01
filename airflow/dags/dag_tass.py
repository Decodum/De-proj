from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator 
import requests as req
import psycopg2
from airflow.models import Variable
from tass import fetch_rss_tass_initial, fetch_rss_tass_incremental

#import psycopg2-binary as ps2

#расположим функцию реквест модуля
pg_hostname = 'postgres'  
pg_port = '5432'  
pg_username = 'airflow'  
pg_pass = 'airflow'    
pg_db = 'airflow'    


default_args = {
    'owner': 'julia',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'start_date': datetime(2023, 10, 19),
    'template_searchpath': '/tmp'
}



# with DAG(dag_id='give_me_news',
#          default_args=default_args,
#          description="give_me_news DAG",
#          schedule_interval=timedelta(minutes=10),
#          tags=["bash", "Julia"],
#          catchup=False) as dag:
    
#     start = DummyOperator(task_id='start')
    
#     good_morning = BashOperator(
#         task_id='good_morning',
#         bash_command=f"echo 'give_me_news' && mkdir -p /tmp/test")
     
#     fetch_data_task = PythonOperator(
#         task_id="fetch_exchange_rate",
#         python_callable=fetch_rss_vedomosti_and_insert_to_db
        
#     )


#     end = DummyOperator(task_id='end')

#     start >> good_morning >> fetch_data_task >> end

    

# Остальные импорты и определение default_args оставляем без изменений

with DAG(dag_id='give_me_news_tass',
         default_args=default_args,
         description="give_me_news DAG",
         schedule_interval=timedelta(minutes=10),
         tags=["bash", "Julia"],
         catchup=False) as dag:
    
    start = DummyOperator(task_id='start')
    
    good_morning = BashOperator(
        task_id='good_morning',
        bash_command=f"echo 'give_me_news_tass' && mkdir -p /tmp/test")
     
    fetch_data_initial_task = PythonOperator(
        task_id="fetch_tass_initial",
        python_callable=fetch_rss_tass_initial
    )

    fetch_data_incremental_task = PythonOperator(
        task_id="fetch_tass_incremental",
        python_callable=fetch_rss_tass_incremental
    )

    end = DummyOperator(task_id='end')

    start >> good_morning >> [fetch_data_initial_task, fetch_data_incremental_task] >> end
