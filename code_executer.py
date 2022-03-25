from airflow.contrib.hooks.ssh_hook import SSHHook

class code_executer:
    def __init__(self, service_dir, file_ex, sub_dir):
        self.dir = service_dir
        self.command =  f'cd {service_dir}\\{sub_dir} && {file_ex}' 
        self.type = type 

    def execute_command(self): 
        ssh = SSHHook(ssh_conn_id="sas1chickeringg")
        ssh_client = None
        print(ssh)
        try:
            ssh_client = ssh.get_conn()
            ssh_client.load_system_host_keys()
            stdin, stdout, stderr = ssh_client.exec_command(self.command)
            out = stdout.read().decode().strip()
            error = stderr.read().decode().strip()
            print(out)
            print(error)
        finally:
            if ssh_client:
                ssh_client.close()