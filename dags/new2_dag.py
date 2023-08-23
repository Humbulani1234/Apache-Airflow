import sys
import json
from datetime import datetime, timedelta

sys.path.append("/home/humbulani/airflow")

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.models import Variable

from load import INSERT_INTO_TABLE

http_address = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

sql_data = """INSERT INTO securities(Symbol, Security, SEC_filings, GICS_Sector,
            GICS_Sub_Industry, Headquaters_Location) VALUES (%s,%s,%s,%s,%s,%s)"""

sql_create = """CREATE TABLE IF NOT EXISTS securities(Symbol VARCHAR(255),
                                                      Security VARCHAR(255),
                                                      SEC_filings VARCHAR(255),
                                                      GICS_Sector VARCHAR(255),
                                                      GICS_Sub_Industry VARCHAR(255),
                                                      Headquaters_Location VARCHAR(255),
                                                      id INT AUTO_INCREMENT PRIMARY KEY);"""
  
with DAG (dagrun_timeout=timedelta(minutes=60),dag_id="mysql_dag", start_date=datetime(2023,3,15),
          schedule_interval=None, catchup=False, template_searchpath=['/home/humbulani/airflow/dags']) as dag:

    data = {'http_address':'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'}    
    start_pipeline = EmptyOperator(task_id="start_pipeline")    
    create_table = MySqlOperator(task_id="mysql_connection", sql=sql_create, mysql_conn_id="mysql_connection")
    task1 = PythonOperator(task_id="insert_data", python_callable=INSERT_INTO_TABLE,
                           op_kwargs=Variable.get("mysql_dag_settings", deserialize_json=True))    
    end_pipeline = EmptyOperator(task_id="end_pipeline")
    
start_pipeline >> create_table >> task1 >> end_pipeline

