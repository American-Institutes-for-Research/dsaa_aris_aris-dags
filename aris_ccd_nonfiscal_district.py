from datetime import timedelta, datetime
from pickle import TRUE
import airflow
import code_executer
from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
SERVICE_GIT_DIR = 'C:\\ARIS\\autoDigest\\ccd' # File housing ARIS repos on SAS server's C drive

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
dag = DAG(dag_id='aris_ccd_nonfiscal_district_etl',
          default_args=default_args,
        #   schedule_interval='0,10,20,30,40,50 * * * *',
          dagrun_timeout=timedelta(seconds=600))


def links():
    '''
    Purpose: execute ccd_data_list_downloader.py  on command line to generate list of CCD links
    '''
    ssh = SSHHook(ssh_conn_id="svc_202205_sasdev")
    ssh_client = None
    print(ssh)
    try:
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        command = 'cd ' +  SERVICE_GIT_DIR + ' && python ' + '\\IO\\ccd_data_list_downloader.py' 
        stdin, stdout, stderr = ssh_client.exec_command(command)
        out = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        print(out)
        print(error)
    finally:
        if ssh_client:
            ssh_client.close()


def dat():
    '''
    Purpose: execute ccd_data_downloader.py on command line to download CCD data 
    '''
    ssh = SSHHook(ssh_conn_id="svc_202205_sasdev")
    ssh_client = None
    print(ssh)
    try:
        
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        command = 'cd ' +  SERVICE_GIT_DIR + ' && python ' +  'IO\\ccd_data_downloader.py'
        stdin, stdout, stderr = ssh_client.exec_command(command)
        out = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        print(out)
        print(error)
    finally:
        if ssh_client:
            ssh_client.close()
def nonfiscal_district():
    '''
    Purpose: execute t318 SAS code 
    '''
    ssh = SSHHook(ssh_conn_id="svc_202205_sasdev")
    ssh_client = None
    print(ssh)
    try:
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        command = 'cd ' +  SERVICE_GIT_DIR + '\\SAS' + ' && sas ccd_nonfiscal_district'
        stdin, stdout, stderr = ssh_client.exec_command(command)
        out = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        print(out)
        print(error)
    finally:
        if ssh_client:
            ssh_client.close()
    #exe = code_executer(SERVICE_GIT_DIR , 'sas ccd_nonfiscal_district', 'sas')
    #exe.execute_command() 

def district_convert():
    '''
    Purpose: execute t318 SAS code 
    '''
    ssh = SSHHook(ssh_conn_id="svc_202205_sasdev")
    ssh_client = None
    print(ssh)
    try:
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        command = 'cd ' +  SERVICE_GIT_DIR + '\\SAS' + ' && sas ccd_district_convert'
        stdin, stdout, stderr = ssh_client.exec_command(command)
        out = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        print(out)
        print(error)
    finally:
        if ssh_client:
            ssh_client.close()
    #exe = code_executer(SERVICE_GIT_DIR , 'sas ccd_district_convert', 'sas')
    #exe.execute_command() 

def mrt_nonfiscal_district():
    '''
    Purpose: execute write_mrt.py on command line to generate mrt from nonfiscal long and write to database. 
    '''
    ssh = SSHHook(ssh_conn_id="svc_202205_sasdev")
    ssh_client = None
    print(ssh)
    try:
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        command = 'cd ' +  SERVICE_GIT_DIR + '\\DB-Generation' + ' && python write_mrt_nonfiscal_district.py' 
        stdin, stdout, stderr = ssh_client.exec_command(command)
        out = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        print(out)
        print(error)
    finally:
        if ssh_client:
            ssh_client.close()

# Download CCD Links 
download_links = PythonOperator(
    task_id='download_links',
    python_callable=links,
    dag=dag
)

# Download CCD Data 
download_dat = PythonOperator(
    task_id='download_dat',
    python_callable=dat,
    dag=dag
)


# Generate Nonfiscal district from CCD Data with SAS
gen_nonfiscal_district = PythonOperator(
    task_id='gen_nonfiscal_district',
    python_callable=nonfiscal_district,
    dag=dag
)


# Generate Nonfiscal district from CCD Data with SAS
gen_district_wide = PythonOperator(
    task_id='gen_district_wide',
    python_callable=district_convert,
    dag=dag
)

load_mrt_nonfiscal_district = PythonOperator(
    task_id = "load_mrt_nonfiscal_district",
    python_callable = mrt_nonfiscal_district,
    dag = dag
)

download_links >> download_dat >> gen_nonfiscal_district >> gen_district_wide >> load_mrt_nonfiscal_district  