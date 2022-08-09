from datetime import timedelta, datetime
from pickle import TRUE
from sqlite3 import connect
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

sas_script_arguments = { 
                    "t318-30-IPEDS-d21.sas":
                        {"dataYear": "d22",
                        "year": "2020",
                        "cy_year": "2021" },

                    ##Update All These to new year
                    "t318-40-d21-MR.sas":
                        {"digest_year": "d22",
                        "schyear2": "2020-21",
                        "datayear2": "2021",
                        "schyear1": "2019-20",
                        "datayear1": "2020" },
                    "t318-40-d21-MR.sas": 
                        {"digest_year": "d22",
                        "schyear2": "2020-21",
                        "datayear2": "2021",
                        "schyear1": "2019-20",
                        "datayear1": "2020"
                        },
                    "t318-50-60-MR.sas":
                    {"digest_year": "d22",
                    "year": "2020-21",
                    "shortyear": "2021"
                    },
                    "t319-10-20-d21-MR.sas":
                    {"digest_year": "d22",
                    "cy":"2020-21",
                    "cy_long": "2021",
                    "py": "2019-20",
                    "py_long": "2020"
                    },
                    "t319-30-40-318-20-d21-MR.sas":
                    {"digest_year": "d22",
                    "cy":"2020-21",
                    "cy_long": "2021",
                    "py": "2019-20",
                    "py_long": "2020"   
                    },
                    "t320-10-t321-10-IPEDS-C-D21-MR.sas":
                    {"digest_year": "d22",
                    "year":"20-21",
                    "cy_long": "2021",
                    "pyyear": "19-20",
                    "py_long": "2020" 
                    }
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

def compile_sas_command(sas_arguments, sas_key):
    print(sas_arguments)
    command_str = "sas " + sas_key
    sas_dict = sas_arguments[sas_key]
    for key in sas_dict:
        argument_str = " -set " + key + " " + sas_dict[key]
        print(key , "->", sas_dict[key])
        command_str = command_str + argument_str

    return command_str
    # for sas_key, sas_args in sas_arguments.items():
    #     print(sas_key)
    #     command_str = "sas " + sas_key
    #     for key in sas_args:
    #         argument_str = " -set " + key + " " + sas_args[key]
    #         print(key , "->", sas_args[key])
    #         command_str = command_str + argument_str
    # print(command_str)



def sas_completion(sas_arguments):
    '''
    Purpose: execute all Survey Completion Sas Scripts 
    '''
    for sas_key in sas_arguments:
        sas_command = compile_sas_command(sas_arguments, sas_key)
        print(sas_command)
        command = 'cd ' +  SERVICE_GIT_DIR + '\\SAS' + '\\d21'+ '\\Completion Survey SAS code'+' && ' + sas_command
        print(command)
        connect_to_server(command)    

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
run_sas_scripts = PythonOperator(
    task_id='run_sas_scripts',
    python_callable=sas_completion,
    op_kwargs= {"sas_arguments": sas_script_arguments},
    dag=dag
)

# gen_completion_mrt = PythonOperator(
#     task_id='load_mrt_completion',
#     python_callable=mrt_completion,
#     dag=dag
# )

# compile_sas = PythonOperator(
#     task_id='compile_sas_commands',
#     python_callable=compile_sas_command,
#     op_kwargs= {"sas_arguments": sas_script_arguments},
#     dag=dag
# )

# DAG Dependancy
#gen_completion >> gen_completion_mrt

run_sas_scripts 