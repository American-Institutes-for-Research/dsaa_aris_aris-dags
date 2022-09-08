from datetime import timedelta, datetime
from pickle import TRUE
import airflow
import code_executer
from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators.email_operator import EmailOperator

SERVICE_GIT_DIR = 'C:\\ARIS\\autoDigest\\ipeds' # File housing ARIS repos on SAS server's C drive

# default args
default_args = {
    'owner': 'airflow',
    'email': ['mkruse@air.org', 'gchickering@air.org'],
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email_on_success' : True,
    'start_date': datetime.now() - timedelta(minutes=20),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Define Main DAG for CCD pipeline 
dag = DAG(dag_id='aris_email_trial',
          default_args=default_args,
        #   schedule_interval='0,10,20,30,40,50 * * * *',
          dagrun_timeout=timedelta(seconds=600))


def temp_print():
    print("we are in here")

temp_task = PythonOperator(
  task_id = "print_message",
  python_callable = temp_print,
  trigger_rule='all_success',
  dag = dag
)

send_email = EmailOperator( 
task_id='send_email', 
to='gchickering@air.org', 
subject='ingestion complete', 
html_content= 'Attached is the latest sales report', 
conn_id = 'smtp_default', 
dag=dag
)

temp_task >> send_email