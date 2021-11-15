import os
import sys
from datetime import date, datetime
from datetime import timedelta

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from airflow import DAG
from airflow.operators.python import PythonOperator

from Nbu_API import Extractor
from db_work import csv

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

with DAG(
        'nbu_extract_data',
        default_args=default_args,
        description='data extraction',
        schedule_interval=timedelta(minutes=30),
        start_date=datetime(2021, 11, 10),
        catchup=False,
        tags=['example'],
) as dag:
    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = PythonOperator(
        task_id='extract_data',
        python_callable=Extractor.get_data,
        op_kwargs={'start_date': date(2021, 11, 1),
                   'end_date': datetime.now().date(),
                   'list_of_currencies': [],
                   'sleep_opt': 1},
        dag=dag
    )
    t2 = PythonOperator(

        task_id='insert_to_db',
        python_callable=csv.insert_into_db,
        op_kwargs={'data_for_insert': t1.output},
        dag=dag
    )

    t1 >> t2
