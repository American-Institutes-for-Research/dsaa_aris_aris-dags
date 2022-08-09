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

SERVICE_GIT_DIR = 'C:\\ARIS\\autoDigest\\ipeds' # File housing ARIS repos on SAS server's C drive
QC_Run = "True"


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
                        },
                    "t320-20-IPEDS-d21-MR.sas":
                        {"digest_year": "d22",
                        "year":"2020-21",
                        "cy_long": "2021",
                        "PYyear": "2019-20",
                        "py_long": "2020" 
                        },
                    "t321-30-d21-MR.sas":
                        {"digest_year": "d22",
                        "year":"2020-21",
                        "cy_long": "2021",
                        "pyyear": "2019-20",
                        "py_long": "2020" 
                        },
                    "t322-10-20-30-40-50-d21-MR.sas":
                        {"digest_year": "d22",
                        "schyear2": "2020-21",
                        "datayear2": "2021",
                        "schyear1": "2019-20",
                        "datayear1": "2020"
                        },
                    "t323-10-20-30-40-50-d21-MR.sas":
                        {"digest_year": "d22",
                        "schyear2": "2020-21",
                        "datayear2": "2021",
                        "schyear1": "2019-20",
                        "datayear1": "2020"
                        },
                    "t324-10-20-25-30-35-d21-MR.sas":
                        {"digest_year": "d22",
                        "schyear2": "2020-21",
                        "datayear2": "2021",
                        "schyear1": "2019-20",
                        "datayear1": "2020"
                        },
                    "t324-40-50-MRT.sas" :
                        {"digest_year": "d22",
                        "cy":"2020",
                        "cy_long": "2021",
                        "py": "2019",
                        "py_long": "2020" 
                        },
                    "t325-sup-d21-MR.sas" :
                        {"digest_year": "d22",
                        "cy":"2020-21",
                        "cy_long": "2021",
                        "py": "2019-20",
                        "py_long": "2020"
                        },
                    "table318-45-IPEDS-d21-MR.sas":
                        {"digest_year": "d22",
                        "cy":"2021",
                        "py": "2020",  
                        },
                    "table321-20-IPEDS-d21_MR.sas":
                        {"digest_year": "d22",
                        "year": "2020",
                        "PYyear": "2019",
                        "cy_long": "2021",
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



def sas_completion(sas_arguments):
    '''
    Purpose: execute all Survey Completion Sas Scripts 
    '''
    for sas_key in sas_arguments:
        sas_command = compile_sas_command(sas_arguments, sas_key)
        command = 'cd ' +  SERVICE_GIT_DIR + '\\SAS' + '\\d21'+ '\\Completion Survey SAS code'+' && ' + sas_command
        print(command)
        connect_to_server(command)  

def qc_sas_logs(qc_run):
    '''
    Purpose: check output of sas log files.
    '''
    if(qc_run == "False"):
        return False
    else:
        command = 'cd ' +  SERVICE_GIT_DIR + '\\DB-Generation' + ' && python sas_parser.py  d21 "Survey Completion SAS Code" '
        error_strings= ["Critical Errors"]
        results = connect_to_server_qc(command, error_strings)
        return (results)


def qc_sas_output(qc_run): 
    '''
    Purpose: check output of sas output files
    '''
    command = 'cd ' +  SERVICE_GIT_DIR + '\\DB-Generation' + ' && python qc_sas_output.py d22 Survey-Completion '
    if(qc_run == "False"):
        return False
    else:
        connect_to_server(command)
        return True  

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

qc_sas_logs = ShortCircuitOperator(
    task_id='qc_sas_logs',
    python_callable=qc_sas_logs,
    op_kwargs= {"qc_run": QC_Run},
    trigger_rule='all_success',
    dag=dag
)

qc_sas_output = ShortCircuitOperator(
    task_id='qc_sas_output',
    python_callable= qc_sas_output,
    op_kwargs= {"qc_run": QC_Run},
    trigger_rule='all_success',
    dag=dag
)

# gen_completion_mrt = PythonOperator(
#     task_id='load_mrt_completion',
#     python_callable=mrt_completion,
#     dag=dag
# )

# DAG Dependancy
#gen_completion >> gen_completion_mrt

run_sas_scripts  >>  Label("QC Checks:Sas Output") >> qc_sas_logs >> qc_sas_output