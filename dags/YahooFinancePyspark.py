import pandas as pd
import requests
import json
from bs4 import BeautifulSoup
import re
import os

from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, struct, concat, lit
from pyspark.sql.types import StringType, StructType, StructField, \
DoubleType,Row, FloatType, TimestampType, IntegerType
import pprint
import requests
import json


os.chdir('/home/barry/Airflow')

pp = pprint.PrettyPrinter(indent=4)


# Creating Spark Session
spark = SparkSession.builder. \
    master("local[4]"). \
    appName('YahooFinancePyspark').\
    config("spark.network.timeout", "3600s").\
    getOrCreate()

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

current_date = datetime.strftime(datetime.now(), '%Y%m%d')

schema = StructType([
    StructField('timestamp_unix',IntegerType(),True),
    StructField('ticker',StringType(),True),
    StructField('open',FloatType(),True),
    StructField('high',FloatType(),True),
    StructField('low',FloatType(),True),
    StructField('close',FloatType(),True),
    StructField('volume',IntegerType(),True)
    ])

def get_stock_data(**kwargs):
    stock_list = kwargs['stock_list']
    for stock in stock_list:
        url = f'https://query1.finance.yahoo.com/v8/finance/chart/{stock}'
        response = requests.get(url,headers=header)
        if response.status_code == 200:
            data = json.loads(response.content)
            stock_name = re.sub('\.','',stock)
            with open(f"YahooFinance/data/{stock_name}_{current_date}.json", "w") as outfile:
                json.dump(data, outfile)

def parse_json(main_df, file_path):

    with open(file_path,'r') as file:
        data = json.load(file)

    timestamp = data['chart']['result'][0]['timestamp']
    ticker = [data['chart']['result'][0]['meta']['symbol']]*len(timestamp)
    open_ = data['chart']['result'][0]['indicators']['quote'][0]['open']
    low_ = data['chart']['result'][0]['indicators']['quote'][0]['low']
    high_ = data['chart']['result'][0]['indicators']['quote'][0]['high']
    close_ = data['chart']['result'][0]['indicators']['quote'][0]['close']
    volume_ = data['chart']['result'][0]['indicators']['quote'][0]['volume']

    df = spark.createDataFrame(list(zip(timestamp,ticker,
                        open_,high_,low_,
                        close_,volume_)),
                    schema = schema)

    # df = pd.DataFrame(list(zip(timestamp,
    #                         open_,high_,low_,
    #                         close_,volume_)),
    #                 columns = ['timestamp','open','high','low','close','volume'])

    # df['ticker'] = ticker

    #return pd.concat([main_df,df],axis=0)
    return main_df.union(df)

@udf(returnType=TimestampType())
def parse_unix_timestamp(val):
    return datetime.fromtimestamp(val)

def merge_files():

    emptyRDD = spark.sparkContext.emptyRDD()
    
    main_df = spark.createDataFrame(emptyRDD,schema)

    for files in os.listdir('YahooFinance/data'):
        main_df = parse_json(main_df, 'YahooFinance/data/' + files)
    
    main_df = main_df.withColumn('timestamp',parse_unix_timestamp(col('timestamp_unix')))
    main_df = main_df.orderBy(col('ticker'),col('timestamp'))

    main_df.toPandas().to_excel(f'df_{current_date}.xlsx',index=False)

# merge_files()
# main_df['timestamp'] = main_df['timestamp'].apply(parse_unix_timestamp)
# print(main_df)
# main_df.to_excel(f'df_{current_date}.xlsx',index=False)

dag = DAG('pull_data_pyspark',
         schedule_interval=None,#'@daily',
         default_args=default_args,
         catchup=False)

start_op = DummyOperator(task_id='start_task',
                         dag=dag)


pull_stock = PythonOperator(task_id='pulldata',
                            python_callable=get_stock_data,
                            op_kwargs={'stock_list':stock_list},
                            dag=dag)

merge_data = PythonOperator(task_id='mergefiles',
                            python_callable=merge_files,
                            dag=dag)

print_user = BashOperator(
            task_id='log_user',
            bash_command='echo Done', dag=dag)
end_op = DummyOperator(task_id='end_task',
                       dag=dag)

start_op >> pull_stock >> merge_data >> print_user >> end_op




