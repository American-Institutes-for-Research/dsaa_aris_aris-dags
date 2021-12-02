from datetime import timedelta, datetime
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
SERVICE_GIT_DIR = 'C:\\ARIS' # File housing ARIS repos on SAS server's C drive

# default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime.now() - timedelta(minutes=20),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Define Main DAG for CCD pipeline 
dag = DAG(dag_id='aris_ccd_etl',
          default_args=default_args,
        #   schedule_interval='0,10,20,30,40,50 * * * *',
          dagrun_timeout=timedelta(seconds=600))


def links():
    '''
    Purpose: execute ccd_data_list_downloader.py  on command line to generate list of CCD links
    '''
    ssh = SSHHook(ssh_conn_id="sas1buehlere")
    ssh_client = None
    print(ssh)
    try:
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        command = 'cd ' +  SERVICE_GIT_DIR + ' && python ' + 'ccdSAS\\IO\\ccd_data_list_downloader.py' 
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
    ssh = SSHHook(ssh_conn_id="sas1buehlere")
    ssh_client = None
    print(ssh)
    try:
        
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        command = 'cd ' +  SERVICE_GIT_DIR + ' && python ' +  'ccdSAS\\IO\\ccd_data_downloader.py'
        stdin, stdout, stderr = ssh_client.exec_command(command)
        out = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        print(out)
        print(error)
    finally:
        if ssh_client:
            ssh_client.close()

def nonfiscal():
    '''
    Purpose: execute ccd_nonfiscal_state_RE2.sas on command line to generate nonfiscal long data from ccd data 
    '''
    ssh = SSHHook(ssh_conn_id="sas1buehlere")
    ssh_client = None
    print(ssh)
    try:
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        command = 'cd ' +  SERVICE_GIT_DIR + '\\ccdSAS\\SAS' + ' && sas ccd_nonfiscal_state_RE2'
        stdin, stdout, stderr = ssh_client.exec_command(command)
        out = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        print(out)
        print(error)
    finally:
        if ssh_client:
            ssh_client.close()

def nonfiscal_wide():
    '''
    Purpose: execute ccd_school_convert.sas on command line to generate nonfiscal wide data from nonfiscal long data. 
    '''
    ssh = SSHHook(ssh_conn_id="sas1buehlere")
    ssh_client = None
    print(ssh)
    try:
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        command = 'cd ' +  SERVICE_GIT_DIR + '\\ccdSAS\\SAS' + ' && sas ccd_school_convert'
        stdin, stdout, stderr = ssh_client.exec_command(command)
        out = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        print(out)
        print(error)
    finally:
        if ssh_client:
            ssh_client.close()

def fiscal():
    '''
    Purpose: execute ccd_fiscal_state.sas on command line to generate fiscal data from ccd data. 
    '''
    ssh = SSHHook(ssh_conn_id="sas1buehlere")
    ssh_client = None
    print(ssh)
    try:
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        command = 'cd ' +  SERVICE_GIT_DIR + '\\ccdSAS\\SAS' + ' && sas ccd_fiscal_state'
        stdin, stdout, stderr = ssh_client.exec_command(command)
        out = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        print(out)
        print(error)
    finally:
        if ssh_client:
            ssh_client.close()

def mrt():
    '''
    Purpose: execute write_mrt.py on command line to generate mrt from nonfiscal long and write to database. 
    '''
    ssh = SSHHook(ssh_conn_id="sas1buehlere")
    ssh_client = None
    print(ssh)
    try:
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        command = 'cd ' +  SERVICE_GIT_DIR + '\\ccdSAS\\DB-Generation' + ' && python write_mrt.py' 
        stdin, stdout, stderr = ssh_client.exec_command(command)
        out = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        print(out)
        print(error)
    finally:
        if ssh_client:
            ssh_client.close()

def hrt():
    '''
    Purpose: execute gen_hrt.py on command line to generate hrt files from mrt loaded in the database. 
    '''
    ssh = SSHHook(ssh_conn_id="sas1buehlere")
    ssh_client = None
    print(ssh)
    try:
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        command = 'cd ' +  SERVICE_GIT_DIR + '\\ccdSAS\\HRT' + ' && python gen_hrt.py -t 203.10 --xlsx_dir HRT' 
        ssh_client.exec_command(command)
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

# Generate Nonfiscal from CCD Data with SAS
gen_nonfiscal = PythonOperator(
    task_id='gen_nonfiscal',
    python_callable=nonfiscal,
    dag=dag
)

# Generate Nonfiscal wide file with SAS 
gen_nonfiscal_wide = PythonOperator(
    task_id='gen_nonfiscal_wide',
    python_callable=nonfiscal_wide,
    dag=dag
)

# Generate Fiscal Data from CCD Data with SAS
gen_fiscal = PythonOperator(
    task_id='gen_fiscal',
    python_callable=fiscal,
    dag=dag
)

# Create MRT and load to Database with Python 
load_mrt = PythonOperator(
    task_id='load_mrt',
    python_callable=mrt,
    dag=dag
)

# Generate HRT file 
gen_hrt = PythonOperator(
    task_id='gen_hrt',
    python_callable=hrt,
    dag=dag
)

# DAG Dependancy
download_links >> download_dat 
download_dat >> gen_nonfiscal >> gen_nonfiscal_wide >> load_mrt >> gen_hrt
gen_fiscal