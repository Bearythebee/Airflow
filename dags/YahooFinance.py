import pandas as pd
import requests
import json
from bs4 import BeautifulSoup
import re

from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
        'start_date': datetime(2023, 10, 7)
    }

stock_list = ['IWDA.L', 'EIMI.L']

header = {'Connection': 'keep-alive',
                   'Expires': '-1',
                   'Upgrade-Insecure-Requests': '1',
                   'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) \
                   AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36'
                   }

def get_stock_data(**kwargs):
    stock_list = kwargs['stock_list']
    for stock in stock_list:
        url = f'https://query1.finance.yahoo.com/v8/finance/chart/{stock}'
        response = requests.get(url,headers=header)
        if response.status_code == 200:
            data = json.loads(response.content)
            current_date = datetime.strftime(datetime.now(), '%Y%m%d')
            stock_name = re.sub('\.','',stock)
            with open(f"YahooFinance/data/{stock_name}_{current_date}.json", "w") as outfile:
                json.dump(data, outfile)

dag = DAG('pull_data',
         schedule_interval=None#'@daily',
         default_args=default_args,
         catchup=False)

start_op = DummyOperator(task_id='start_task',
                         dag=dag)



pull_stock = PythonOperator(task_id='pulldata',
                            python_callable=get_stock_data,
                            op_kwargs={'stock_list':stock_list},
                            dag=dag)
print_user = BashOperator(
            task_id='log_user',
            bash_command='echo Done', dag=dag)
end_op = DummyOperator(task_id='end_task',
                       dag=dag)

start_op >> pull_stock >> print_user >> end_op




