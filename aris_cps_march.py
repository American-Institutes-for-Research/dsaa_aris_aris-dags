from datetime import timedelta, datetime
from pickle import TRUE
from sqlite3 import connect
import airflow
import code_executer
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.utils.edgemodifier import Label


##To DO List
#Create Sas arguments list
#Implement all the ones I added (year, month, file) to 

SERVICE_GIT_DIR = 'C:\\ARIS\\autoDigest\\cps' # File housing ARIS repos on SAS server's C drive
QC_Run = "True"
year = "2020"
month = "March"
file = "501-80-CPS-MAR-2021-D21.txt"


# default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['mtrihn@air.org', 'gchickering@air.org'],
    'email_on_failure': TRUE,
    'email_on_retry': False,
    'start_date': datetime.now() - timedelta(minutes=20),
    'retries': 0,
    'retry_delay': timedelta(minutes=90),
}

sas_script_arguments = {
    "t501-80-CPS-MAR-2021-D21.sas":{}
                        }
# "t302-60-CPS-OCT2020.sas":{},
# "t104-10-CPS-MAR-2020.sas":{},
#     "t302-10_CPS-OCT2020.sas":{},
#     "t302-20_CPS-OCT2020.sas":{},
# "t501-50-CPS-MAR-2021-D21.sas":{},
#     "t501-60-70-CPS-MAR-2021-D21.sas":{},
#     "t501-80-CPS-MAR-2021-D21.sas":{},
#     "t501-85-90-CPS-MAR-2021-D21.sas":{}

dag = DAG(dag_id='aris_cps_march_etl',
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

def compile_sas_command(sas_arguments, sas_key):
    command_str = "sas " + sas_key
    sas_dict = sas_arguments[sas_key]
    for key in sas_dict:
        argument_str = " -set " + key + " " + sas_dict[key]
        command_str = command_str + argument_str
    return command_str

def compile_sas_scripts(sas_arguments):
    '''
    Purpose: execute all Survey Completion Sas Scripts 
    '''
    for sas_key in sas_arguments:
        sas_command = compile_sas_command(sas_arguments, sas_key)
        command = 'cd ' +  SERVICE_GIT_DIR + '\\SAS' + ' && ' + sas_command
        print(command)
        connect_to_server(command)

def qc_sas_logs(qc_run, file):
    '''
    Purpose: check output of sas log files.
    '''
    if(qc_run == "False"):
        return False
    else:
        command = 'cd ' +  SERVICE_GIT_DIR + '\\DB-Generation' + '&& python sas_parser.py '  + file
        error_strings= ["Critical Errors"]
        results = connect_to_server_qc(command, error_strings)
        return (results)


def qc_sas_output(qc_run, year, month, file): 
    '''
    Purpose: check output of sas output files
    '''
    command = 'cd ' +  SERVICE_GIT_DIR + '\\DB-Generation' + '&& python qc_sas_output.py ' + year  +  ' ' + month + ' '+ file 

    if(qc_run == "False"):
        return False
    else:
        connect_to_server(command)
        return True 


def write_to_db(year, month, file): 
    '''
    Purpose: Write Output file to the db
    '''
    
    command = 'cd ' +  SERVICE_GIT_DIR + '\\DB-Generation' + ' && python write_to_db.py ' + year  +  ' ' + month + ' '+ file 
    connect_to_server(command)    


def qc_database_linking(qc_run, year, month, file):

    if(qc_run == "False"):
        return False
    else:
        error_strings= ["Please resolve these duplicated values issue", "Discrepancy found between Sas output file and database value"]
        command = 'cd ' +  SERVICE_GIT_DIR + '\\DB-Generation' + ' && python qc_database.py '  + year  +  ' ' + month + ' '+ file 
        results = connect_to_server_qc(command, error_strings)
        return (results) 

def mrt_completion():
    '''
    Purpose: execute write_mrt.py on command line to generate mrt from nonfiscal long and write to database. 
    '''
    command = 'cd ' +  SERVICE_GIT_DIR + '\\DB-Generation' + ' && python write_mrt_cps.py' 
    connect_to_server(command)             

# Generate Nonfiscal state from CCD Data with SAS
run_sas_scripts = PythonOperator(
    task_id='run_sas_scripts',
    python_callable = compile_sas_scripts,
    op_kwargs= {"sas_arguments": sas_script_arguments},
    dag=dag
)

qc_sas_logs = ShortCircuitOperator(
    task_id='qc_sas_logs',
    python_callable=qc_sas_logs,
    op_kwargs= {"qc_run": QC_Run, "file":file},
    trigger_rule='all_success',
    dag=dag
)

qc_sas_output = ShortCircuitOperator(
    task_id='qc_sas_output',
    python_callable= qc_sas_output,
    op_kwargs= {"qc_run": QC_Run, "year":year, "month":month, "file":file},
    trigger_rule='all_success',
    dag=dag
)

##Write Data to DB
write_to_db = PythonOperator(
    task_id='write_to_db',
    python_callable=write_to_db,
    op_kwargs= {"year":year, "month":month, "file":file},
    trigger_rule = "none_failed", 
    dag=dag
)

# #QC Data written to DB
qc_database = ShortCircuitOperator(
    task_id = "qc_database",
    python_callable = qc_database_linking,
    op_kwargs= {"qc_run": QC_Run, 
                "year":year, "month":month, "file":file},
    trigger_rule='all_success',
    dag = dag
)

# load_completion_mrt = PythonOperator(
#     task_id='load_mrt_completion',
#     python_callable=mrt_completion,
#     dag=dag
# )

# DAG Dependancy
#gen_completion >> gen_completion_mrt

run_sas_scripts  >>  Label("QC Checks:Sas Output") >> qc_sas_logs >> qc_sas_output
qc_sas_output >>  Label("Write to DB")  >> write_to_db
write_to_db  >> Label("QC Check:Database") >> qc_database
# write_to_db  >> Label("QC Check:Database") >> qc_database >> Label("Create Tables") 
##load_completion_mrt  