import json
import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


def extract():
    data_string = '{"1001":1, "1002":2,"1003":3}'
    return json.loads(data_string)

##xcom is a mechanism by which tasks communicate

def transform(ti):
    order_data_dict = ti.xcom.pull(task_ids="extract")
    total_order_value = sum(order_data_dict.values())
    return {"total_order_value : ", total_order_value}

def load(ti):
    total = ti.xcom_pull(task_ids="transform")["total_order_value"]
    print("Total order value : ", total)


with DAG(
        dag_id="old_etl_pipeline",
        schedule=None,
        start_date=pendulum.datetime(2023,1,1, tz='UTC'),
        catchup=False,
        tags=["example"],
        ) as dag:
    extract_task = PythonOperator(task_id="extract", python_callable=extract)
    transform_task = PythonOperator(task_id="transform", python_callable=transform)
    load_task = PythonOperator(task_id="load", python_callable=load)

    extract_task >> transform_task >> load_task




