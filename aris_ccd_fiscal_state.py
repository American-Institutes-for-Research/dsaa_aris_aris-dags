from datetime import timedelta, datetime
from multiprocessing import connection
from pickle import TRUE
import airflow
import code_executer
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
#from airflow.operators.python_operator import BranchPythonOperator
#from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.utils.edgemodifier import Label
import requests

SERVICE_GIT_DIR = 'C:\\ARIS\\autoDigest\\ccd' # File housing ARIS repos on SAS server's C drive
QC_Run = "False"
Download_Data = "False"

sas_variables = {'Year' : "2021",
                'Version':"0a" }
#Year = "2020"

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
dag = DAG(dag_id='aris_ccd_fiscal_state_etl',
          default_args=default_args,
        #   schedule_interval='0,10,20,30,40,50 * * * *',
          dagrun_timeout=timedelta(seconds=3600))


def check_airflow_to_azure():
    ssh = SSHHook(ssh_conn_id="svc_202205_sasdev")
    ssh_client = None
    print(ssh)
    try:
        ssh_client = ssh.get_conn()
        temp = ssh_client.load_system_host_keys()
        print(temp)

    finally:
        if ssh_client:
            ssh_client.close() 

def check_azure_to_database():
    command = 'cd ' +  SERVICE_GIT_DIR + '\\DB-Generation' + ' && python check_connections_azure_to_db.py' 
    error_strings= ["Closing db connection"]
    results = connect_to_server_qc(command, error_strings)
    print(results)
    if results == False:
        results = True
    else:
        results = False
    return (results)

def check_azure_to_NCES():
    command = 'cd ' +  SERVICE_GIT_DIR + '\\DB-Generation' + ' && python check_connections_azure_to_nces.py' 
    error_strings= ["Response [200]"]
    results = connect_to_server_qc(command, error_strings)
    print(results)
    if results == False:
        results = True
    else:
        results = False
    return (results)

   

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

def connect_to_server_qc(run_command,error_strings_list):
    '''
    Purpose: check output of sas log files.
    '''
    error_strings= error_strings_list
    main_flag = 0
    ssh = SSHHook(ssh_conn_id="svc_202205_sasdev")
    ssh_client = None
    print(ssh)
    try:
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        command = run_command
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
            if main_flag == 1:
                return(False)
            else:
                return(True) 
    
def links(Download_Data):
    '''
    Purpose: execute ccd_data_list_downloader.py  on command line to generate list of CCD links
    '''
    if(Download_Data == "False"):
        return True

    command = 'cd ' +  SERVICE_GIT_DIR  + '\\IO' + '&& python ccd_data_list_downloader.py'
    error_strings= ["Finished checking for links"]
    results = connect_to_server_qc(command, error_strings)
    print(results)
    if results == False:
        results = True
    else:
        results = False
    return (results)
     
    


def download_dat():
    '''
    Purpose: execute ccd_data_downloader.py on command line to download CCD data 
    '''
    error_strings= ["Finished downloading data from website"]
    command = 'cd ' +  SERVICE_GIT_DIR +   '\\IO' ' && python ccd_data_downloader.py'
    results = connect_to_server_qc(command, error_strings)
    print(results)
    if results == False:
        results = True
    else:
        results = False
    return (results)

def download_dodea_dat():
    '''
    Purpose: execute ccd_dodea_downloader.py on command line to download CCD data 
    '''
    command = 'cd ' +  SERVICE_GIT_DIR + '\\IO' ' && python ccd_dodea_downloader.py' 
    connect_to_server(command)

def ccd_edge_downloader():
    '''
    Purpose: execute ccd_edge_downloader.py on command line to download CCD data 
    '''
    command = 'cd ' +  SERVICE_GIT_DIR + '\\IO' ' && python ccd_edge_downloader.py' 
    connect_to_server(command)

def fiscal():
    '''
    Purpose: execute ccd_fiscal_state.sas on command line to generate fiscal data from ccd data. 
    '''

    command = 'cd ' +  SERVICE_GIT_DIR + '\\SAS' + ' && sas ccd_fiscal_state'
    connect_to_server(command)


def write_to_db(year): 
    '''
    Purpose: Write Output file to the db
    '''
    file = 'Output-CCD-ST-' + year + '.xlsx'
    print(file)
    command = 'cd ' +  SERVICE_GIT_DIR + '\\DB-Generation' + ' && python write_to_db.py ' + year + ' nonfiscal ' + file
    connect_to_server(command)    


def qc_sas_logs(qc_run):
    '''
    Purpose: check output of sas log files.
    '''
    if(qc_run == "False"):
        return True
    else:
        command = 'cd ' +  SERVICE_GIT_DIR + '\\DB-Generation' + ' && python sas_parser.py ccd_nonfiscal_state-RE2.log' 
        error_strings= ["Critical Errors"]
        results = connect_to_server_qc(command, error_strings)
        return (results)



def qc_sas_output(qc_run, year): 
    '''
    Purpose: check output of sas output files
    '''
    file = 'Output-CCD-ST-' + year + '.xlsx'
    print(file)
    command = 'cd ' +  SERVICE_GIT_DIR + '\\DB-Generation' + ' && python qc_sas_output.py ' + year + ' nonfiscal ' + file
    if(qc_run == "False"):
        return True
    else:
        connect_to_server(command)
        return True

def qc_database_linking(qc_run, year):
    ##This is the file to run through the script
    file = 'Output-CCD-ST-' + year + '.xlsx'

    if(qc_run == "False"):
        return True
    else:
        error_strings= ["Please resolve these duplicated values issue", "Discrepancy found between Sas output file and database value"]
        command = 'cd ' +  SERVICE_GIT_DIR + '\\DB-Generation' + ' && python qc_database.py ' +  year + ' nonfiscal ' + file
        results = connect_to_server_qc(command, error_strings)
        return (results)


def mrt_fiscal_state():
    '''
    Purpose: execute write_mrt.py on command line to generate mrt from nonfiscal long and write to database. 
    '''

    command = 'cd ' +  SERVICE_GIT_DIR + '\\DB-Generation' + ' && python write_mrt_fiscal_state.py' 
    error_strings = ['TypeError']
    results = connect_to_server_qc(command, error_strings)
    return (results)
    

############# Operators ##################

##Check Connections
check_airflow_to_azure = PythonOperator(
    task_id = "check_airflow_to_azure",
    python_callable = check_airflow_to_azure,
    trigger_rule='all_success',
    dag = dag
)

check_azure_to_database = ShortCircuitOperator(
    task_id = "check_azure_to_database",
    python_callable = check_azure_to_database,
    trigger_rule='all_success',
    dag = dag
)

check_azure_to_NCES = ShortCircuitOperator(
    task_id = "check_azure_to_NCES",
    python_callable = check_azure_to_NCES,
    trigger_rule='all_success',
    dag = dag
)

#Download CCD Links 
download_links = ShortCircuitOperator(
    task_id='download_links',
    python_callable=links,
    op_kwargs= {"Download_Data": Download_Data},
    dag=dag   
)

# # Download CCD Data 
download_data =  ShortCircuitOperator(
    task_id='download_data',
    python_callable=download_dat,
    dag=dag
)

# # Download Dodea Data 
download_dodea_data = PythonOperator(
    task_id='download_dodea_data',
    python_callable= download_dodea_dat,
    dag=dag
)
##Download Edge Data
download_edge_data = PythonOperator(
    task_id='download_edge_data',
    python_callable= ccd_edge_downloader,
    dag=dag
)




# Generate Fiscal Data from CCD Data with SAS
gen_fiscal = PythonOperator(
    task_id='gen_fiscal',
    python_callable=fiscal,
    dag=dag
)


#QC Steps
    #QC Sas Logs
qc_sas_logs = ShortCircuitOperator(
    task_id='qc_sas_logs',
    python_callable=qc_sas_logs,
    op_kwargs= {"qc_run": QC_Run},
    trigger_rule='all_success',
    dag=dag
)
    #QC Sas Output
qc_sas_output = ShortCircuitOperator(
    task_id='qc_sas_output',
    python_callable= qc_sas_output,
    op_kwargs= {"qc_run": QC_Run,
                  "year": sas_variables['Year']},
    trigger_rule='all_success',
    dag=dag
)

#QC Data written to DB
qc_database = ShortCircuitOperator(
    task_id = "qc_database",
    python_callable = qc_database_linking,
    op_kwargs= {"qc_run": QC_Run, 
                "year": sas_variables['Year']},
    trigger_rule='all_success',
    dag = dag
)


##Write Data to DB
write_to_db = PythonOperator(
    task_id='write_to_db',
    python_callable=write_to_db,
    op_kwargs= {"year": sas_variables['Year']},
    trigger_rule = "none_failed", 
    dag=dag
)

load_mrt_fiscal_state = PythonOperator(
    task_id = "load_mrt_fiscal_state",
    python_callable = mrt_fiscal_state,
    dag = dag
)

check_airflow_to_azure >>  Label("Checking Connections") >> check_azure_to_NCES >> check_azure_to_database >> Label("Check for New Links") >> download_links
download_links >> Label("Downloading Data") >> download_data >> download_dodea_data >> download_edge_data
download_edge_data >> Label("Running Sas Script") >> gen_fiscal >>  Label("QC Checks:Sas Output") >> qc_sas_logs >> qc_sas_output
qc_sas_output >>  Label("Write to DB") >> write_to_db >> Label("QC Check:Database")>> qc_database
qc_database >> Label("Create Tables") 