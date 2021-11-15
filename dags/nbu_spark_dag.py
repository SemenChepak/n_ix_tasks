import os
import sys
from datetime import datetime
from datetime import timedelta

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from airflow import DAG
from airflow.operators.python import PythonOperator

from Spark_obj import spark_op

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
        'nbu_spark',
        default_args=default_args,
        description='data extraction',
        schedule_interval=timedelta(minutes=30),
        start_date=datetime(2021, 11, 10),
        catchup=False,
        tags=['example'],
) as dag:
    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = PythonOperator(
        task_id='extract_data_from_db',
        python_callable=spark_op.read_from_db,
        dag=dag
    )
    t2 = PythonOperator(

        task_id='create_spark_data_frame',
        python_callable=spark_op.create_df,
        op_kwargs={'data_l': t1.output},
        dag=dag
    )
    t3 = PythonOperator(

        task_id='create_parts',
        python_callable=spark_op.create_parts,
        op_kwargs={'df': t2.output},
        dag=dag
    )
    t4 = PythonOperator(

        task_id='upload_directoryTo_S3',
        python_callable=spark_op.upload_directory,
        op_kwargs={'list_of_dirs': t3.output},
        dag=dag
    )

    t1 >> t2 >> t3 >> t4
