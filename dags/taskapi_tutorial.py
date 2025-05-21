import json
import pendulum

from airflow.sdk import dag, task

@dag(
        schedule=None,
        start_date=pendulum.datetime(2021,1,1, tz='UTC'),
        catchup=False,
        tags=["example"],
        )
def taskflow_api_tutorial():

    @task()
    def extract():
        data_string = '{"1001":1, "1002":2,"1003":3}'
        order_data_dict = json.loads(data_string)

        return order_data_dict

    @task()
    def transform(order_data_dict: dict):
        total_order_value = 0

        for value in order_data_dict.values():
            total_order_value += value

        return {"total order value": total_order_value}

    @task()
    def load(total_order_value: float):
        print(f"Total order value is: {total_order_value:.2f}")



    order_data = extract()
    order_summary = transform(order_data)
    load(order_summary["total_order_value"])

taskflow_api_tutorial()


