from datetime import timedelta, datetime
from multiprocessing import connection
from pickle import TRUE
import airflow
import code_executer
from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator, ShortCircuitOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.sensors.python import PythonSensor

SERVICE_GIT_DIR = 'C:\\ARIS\\autoDigest\\ccd' # File housing ARIS repos on SAS server's C drive
year = "2021"
QC_Run = "False"

# default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['mtrihn@air.org', 'gchickering@air.org'],
    'email_on_failure': TRUE,
    'email_on_retry': False,
    'start_date': datetime.now() - timedelta(minutes=20),
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

# Define Main DAG for CCD pipeline 
dag = DAG(dag_id='aris_ccd_nonfiscal_state_etl',
          default_args=default_args,
        #   schedule_interval='0,10,20,30,40,50 * * * *',
          dagrun_timeout=timedelta(seconds=3600))

def connect_to_server(run_command):
    print(run_command)
    ssh = SSHHook(ssh_conn_id="svc_202205_sasdev")
    ssh_client = None
    print(ssh)
    try:
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        command = run_command 
        stdin, stdout, stderr = ssh_client.exec_command(command)
        out = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        print(out)
        print(error)
    finally:
        if ssh_client:
            ssh_client.close() 

def links():
    '''
    Purpose: execute ccd_data_list_downloader.py  on command line to generate list of CCD links
    '''
    command = 'cd ' +  SERVICE_GIT_DIR + ' && python ' + '\\IO\\ccd_data_list_downloader.py' 
    connect_to_server(command)


def dat():
    '''
    Purpose: execute ccd_data_downloader.py on command line to download CCD data 
    '''
    command = 'cd ' +  SERVICE_GIT_DIR + ' && python ' +  'IO\\ccd_data_downloader.py'
    connect_to_server(command)

def nonfiscal():
    '''
    Purpose: execute ccd_nonfiscal_state_RE2.sas on command line to generate nonfiscal long data from ccd data 
    '''
    command = 'cd ' +  SERVICE_GIT_DIR + '\\SAS' + ' && sas ".\ccd_nonfiscal_state - RE2.sas”  -set cnfyr "2020" -set cnfv “1a" '
    connect_to_server(command)

def mrt_nonfiscal_state():
    '''
    Purpose: execute write_mrt.py on command line to generate mrt from nonfiscal long and write to database. 
    '''
    command = 'cd ' +  SERVICE_GIT_DIR + '\\DB-Generation' + ' && python write_mrt_nonfiscal_state.py'
    connect_to_server(command)


def qc_sas_logs(qc_run):
    '''
    Purpose: check output of sas log files.
    '''
    if(qc_run == "False"):
        return False
    else:
        error_strings= ["Errors found"]
        main_flag = 0
        ssh = SSHHook(ssh_conn_id="svc_202205_sasdev")
        ssh_client = None
        print(ssh)
        try:
            ssh_client = ssh.get_conn()
            ssh_client.load_system_host_keys()
            command = 'cd ' +  SERVICE_GIT_DIR + '\\DB-Generation' + ' && python sas_parser.py' 
            print(command)
            stdin, stdout, stderr = ssh_client.exec_command(command)
            stdout.channel.recv_exit_status()
            lines = stdout.readlines()
            for line in lines:
                print(line.strip())
                if any(strings in line for strings in error_strings):
                    main_flag = 1
            error = stderr.read().decode().strip()
            print(error)
        finally:
            if ssh_client:
                ssh_client.close() 
                return(main_flag) 



def qc_sas_output(qc_run, year): 
    '''
    Purpose: check output of sas output files
    '''
    print(year)
    command = 'cd ' +  SERVICE_GIT_DIR + '\\DB-Generation' + ' && python qc_sas_output.py' + year + ' "nonfiscal"' 
    if(qc_run == "False"):
        return False
    else:
        connect_to_server(command)

def qc_database_linking(qc_database):
    '''
    Purpose: check output of sas output files
    '''
    if(qc_database == "False"):
        return False
    else:
        command = 'cd ' +  SERVICE_GIT_DIR + '\\DB-Generation' + ' && python qc_database.py" year "nonfiscal"' 
        connect_to_server(command)


# Download CCD Links 
# download_links = PythonOperator(
#     task_id='download_links',
#     python_callable=links,
#     dag=dag
# )

# # Download CCD Data 
# download_dat = PythonOperator(
#     task_id='download_dat',
#     python_callable=dat,
#     dag=dag
# )


# Generate Nonfiscal state from CCD Data with SAS
gen_nonfiscal = PythonOperator(
    task_id='gen_nonfiscal',
    python_callable=nonfiscal,
    trigger_rule='all_success',
    dag=dag
)

# load_mrt_nonfiscal_state = PythonOperator(
#     task_id = "load_mrt_nonfiscal_state",
#     python_callable = mrt_nonfiscal_state,
#     trigger_rule='all_success',
#     dag = dag
# )

##QC Steps
# qc_sas_logs = ShortCircuitOperator(
#     task_id='qc_sas_logs',
#     python_callable=qc_sas_logs,
#     op_kwargs= {"qc_run": QC_Run},
#     dag=dag
# )


# # Generate Nonfiscal state from CCD Data with SAS
# qc_sas_output = ShortCircuitOperator(
#     task_id='qc_sas_output',
#     python_callable= qc_sas_output,
#     op_kwargs= {"qc_run": QC_Run},
#     dag=dag
# )

# qc_database = ShortCircuitOperator(
#     task_id = "qc_database",
#     python_callable = qc_database_linking,
#     op_kwargs= {"qc_database": QC_Run},
#     trigger_rule='all_success',
#     dag = dag
# )


#download_links >> download_dat >>
gen_nonfiscal 
#>> qc_sas_logs >> qc_sas_output
#gen_nonfiscal >> load_mrt_nonfiscal_state >> qc_database

