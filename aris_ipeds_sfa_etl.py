from datetime import timedelta, datetime
from pickle import TRUE
import airflow
import code_executer
from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.python import ShortCircuitOperator

from airflow.contrib.hooks.ssh_hook import SSHHook
SERVICE_GIT_DIR = 'C:\\ARIS\\autoDigest\\ipeds' # File housing ARIS repos on SAS server's C drive

# default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['mtrihn@air.org', 'gchickering@air.org'],
    'email_on_failure': TRUE,
    'email_on_retry': False,
    'start_date': datetime.now() - timedelta(minutes=20),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Define Main DAG for CCD pipeline 
dag = DAG(dag_id='aris_ipeds_sfa_etl',
          default_args=default_args,
        #   schedule_interval='0,10,20,30,40,50 * * * *',
          dagrun_timeout=timedelta(seconds=600))

def sas_sfa():
    '''
    Purpose: execute ccd_nonfiscal_state_RE2.sas on command line to generate nonfiscal long data from ccd data 
    '''
    ssh = SSHHook(ssh_conn_id="svc_202205_sasdev")
    ssh_client = None
    print(ssh)
    try:
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        command = 'cd ' +  SERVICE_GIT_DIR + '\\SAS' + '\\d21'+ '\\SFA Survey SAS code'+' && FOR %I in (*.sas) DO sas %I'
        stdin, stdout, stderr = ssh_client.exec_command(command)
        out = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        print(out)
        print(error)
    finally:
        if ssh_client:
            ssh_client.close()

def mrt_SFA():
    '''
    Purpose: execute write_mrt.py on command line to generate mrt from nonfiscal long and write to database. 
    '''
    ssh = SSHHook(ssh_conn_id="svc_202205_sasdev")
    ssh_client = None
    print(ssh)
    try:
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        command = 'cd ' +  SERVICE_GIT_DIR + '\\DB-Generation' + ' && python write_mrt_sfa.py' 
        stdin, stdout, stderr = ssh_client.exec_command(command)
        out = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        print(out)
        print(error)
    finally:
        if ssh_client:
            ssh_client.close()   

def sas_log_check():

    ssh = SSHHook(ssh_conn_id="svc_202205_sasdev")
    ssh_client = None
    print(ssh)
    try:
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        command = 'cd ' +  SERVICE_GIT_DIR + '\\SAS' + '\\d21'+ 'python .\sas_parser.py "SFA Survey SAS code" '
        stdin, stdout, stderr = ssh_client.exec_command(command)
        out = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        print(out)
        print(error)
    finally:
        if ssh_client:
            ssh_client.close()   


# Generate Nonfiscal state from CCD Data with SAS
gen_sfa = PythonOperator(
    task_id='gen_sfa',
    python_callable=sas_sfa,
    dag=dag
)
gen_sfa_mrt = PythonOperator(
    task_id = 'load_mrt_sfa',
    python_callable=mrt_SFA,
    dag= dag
)

sas_log_parser = ShortCircuitOperator(
        task_id="check_sas_scripts",
        python_callable=sas_log_check,
        dag = dag
    )

# DAG Dependancy
#gen_sfa >> gen_sfa_mrt
gen_sfa >> sas_log_parser