from datetime import timedelta, datetime
from pickle import TRUE
import airflow
import code_executer
from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
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

sas_script_arguments = { "t318-30-IPEDS-d21.sas":
                        {"dataYear": "d22",
                        "year": "2019",
                        "cy_year": "2020" }
}

# Define Main DAG for CCD pipeline 
dag = DAG(dag_id='aris_ipeds_completion_etl',
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

def compile_sas_command(sas_arguments):
    
    for sas_key in sas_arguments:
        command_str = "sas" + sas_key 
        for key, value in sas_key:
            argument_str = " -set " + key + " " + value
            print(key , "->", value)
            command_str = command_str + argument_str
    print(command_str)



def sas_completion():
    '''
    Purpose: execute all Survey Completion Sas Scripts 
    '''
    
    command = 'cd ' +  SERVICE_GIT_DIR + '\\SAS' + '\\d21'+ '\\Completion Survey SAS code'+' && FOR %I in (*.sas) DO sas %I'
        

def mrt_completion():
    '''
    Purpose: execute write_mrt.py on command line to generate mrt from nonfiscal long and write to database. 
    '''
    ssh = SSHHook(ssh_conn_id="svc_202205_sasdev")
    ssh_client = None
    print(ssh)
    try:
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        command = 'cd ' +  SERVICE_GIT_DIR + '\\DB-Generation' + ' && python write_mrt_completion.py' 
        stdin, stdout, stderr = ssh_client.exec_command(command)
        out = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        print(out)
        print(error)
    finally:
        if ssh_client:
            ssh_client.close()            

# Generate Nonfiscal state from CCD Data with SAS
# gen_completion = PythonOperator(
#     task_id='gen_completion',
#     python_callable=sas_completion,
#     dag=dag
# )

# gen_completion_mrt = PythonOperator(
#     task_id='load_mrt_completion',
#     python_callable=mrt_completion,
#     dag=dag
# )

compile_sas = PythonOperator(
    task_id='compile_sas_commands',
    python_callable=compile_sas_command,
    op_kwargs= {"sas_arguments": sas_script_arguments},
    dag=dag
)

# DAG Dependancy
#gen_completion >> gen_completion_mrt