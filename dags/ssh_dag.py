from airflow.decorators import dag
from datetime import datetime
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow import DAG

with DAG (dag_id='ssh', schedule_interval=None, start_date=datetime(2023,3,29), catchup=False) as dag:      
    t5 = SSHOperator(task_id='SSHOperator', ssh_conn_id='ssh_connection', command='echo "Text from SSH Operator"')

t5
