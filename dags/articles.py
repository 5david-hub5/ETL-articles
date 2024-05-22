from airflow import DAG
from clickhouse_driver import Client
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import requests
import json


dag = DAG(
    dag_id="articles",
    start_date=datetime(2024,5,10),
    schedule_interval="@weekly"
)


def get_data_from_api():
    url = 'https://hacker-news.firebaseio.com/v0/item/19155826.json'
    response = requests.get(url)
    articles_json = response.json()
    
    with open('/opt/airflow/dags/data/articles_json.json', 'w') as write:
            json.dump(articles_json, write)

    df = pd.DataFrame(articles_json)
    df.to_csv('/opt/airflow/dags/data/articles_json.csv', index=False)

stage1 = PythonOperator(
    task_id="get_data_from_api",
    python_callable=get_data_from_api,
    dag=dag
)

stage1