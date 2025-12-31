import json
import argparse
import subprocess
import paramiko
from scp import SCPClient
import shutil

ntp_path = "./tools/ntp"
def make_ntp_conf(ntp_args):
    target_path = "/tmp/ntp.conf.dhcp"

    if ntp_args.mode == "remote":
        server_line = f"server {ntp_args.master} iburst minpoll 2 maxpoll 2\n"
        file_path = f"{ntp_path}/base_ntp.conf"
        # Read the existing content
        with open(file_path, 'r') as f:
            content = f.readlines()

        # Prepend the new server line
        content.insert(0, server_line)

        # Write back to the file
        with open(target_path, 'w') as f:
            f.writelines(content)

    if ntp_args.mode == "local":
        shutil.copyfile(f"{ntp_path}/ntp_auth.conf", "/tmp/ntp.conf.dhcp.master")

        server_line = f"server {ntp_args.master} iburst minpoll 2 maxpoll 2\n"
        file_path = f"{ntp_path}/ntp_auth_client.conf"

        # Read the existing content
        with open(file_path, 'r') as f:
            content = f.readlines()

        content.insert(0, server_line)
        # Write back to the file
        with open(target_path, 'w') as f:
            f.writelines(content)

    elif ntp_args.mode == "default":
        shutil.copyfile(f"{ntp_path}/ntp.conf.dhcp", target_path)

def extrapolate_ip_from_host(host):
    ip_text = subprocess.run(f"nslookup {host}", shell=True, capture_output=True, text=True)

    # Parse the output
    lines = ip_text.stdout.splitlines()

    address_after_name = None
    found_name = False

    for line in lines:
        if line.startswith('Name:'):
            found_name = True
        elif found_name and line.strip().startswith('Address:'):
            address_after_name = line.split('Address:')[1].strip()
            break

    assert address_after_name is not None, f"Could not find the address for host {host}"

    return address_after_name

def exec_ssh_command(ssh_client, command):
    stdin, stdout, stderr =  ssh_client.exec_command(command)
    output = stdout.read().decode().strip()
    error = stderr.read().decode().strip()
    exit_status = stdout.channel.recv_exit_status()

    return exit_status, output, error

def update_ntp_config(target_ip, local_file, target_file, master=False, bad_clock=False):
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(target_ip, username="root")

    scp_client = SCPClient(ssh_client.get_transport())

    scp_client.put(local_file, target_file)

    exit_status, output, error = exec_ssh_command(ssh_client, "systemctl restart ntp")

    if exit_status == 0:
        print(f"Successfull Update on host {target_ip}")
    else:
        print(f"Error on updating host {target_ip}")
        print(f"    Output: {output}")
        print(f"    Error: {error}")

    if master:
        exit_status, output, error = exec_ssh_command(ssh_client, 'date -s "$(date -u)"')

        if exit_status == 0:
            print(f"Successfull Stepped master {target_ip} Clock")
        else:
            print(f"Error on updating host {target_ip}")
            print(f"    Output: {output}")
            print(f"    Error: {error}")

        if bad_clock:
            scp_client.put(f"{ntp_path}/ntp_host_unstable_script.sh", "/tmp/ntp_host_unstable_script.sh")
            ssh_client.exec_command("/tmp/ntp_host_unstable_script.sh")

    scp_client.close()
    ssh_client.close()

def clean_ntp(target_ip):
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(target_ip, username="root")

    stdin, stdout, stderr =  ssh_client.exec_command("systemctl stop ntp")
    output = stdout.read().decode().strip()
    error = stderr.read().decode().strip()
    exit_status = stdout.channel.recv_exit_status()

    if exit_status == 0:
        print(f"Successfull Stopped NTP on host {target_ip}")
    else:
        print(f"Error on updating host {target_ip}")
        print(f"    Output: {output}")
        print(f"    Error: {error}")
def find_host_match(prefix, items):
    for s in items:
        if s.startswith(prefix):
            return s
    return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize machines in grid5000 cluster")

    parser.add_argument(
        "user", help="Username in Grid5000"
    )

    parser.add_argument("--job_id", help="Optional job id to specify which to get the information from")

    parser.add_argument(
        'mode',
        type=str,
        choices=['remote', 'default', 'local'],
        help='NTP mode type'
    )

    parser.add_argument(
        '--master',
        type=str,
        help="IP address of remote machine to sync NTP (required if remote is set)"
    )

    parser.add_argument(
        '--clean',
        action="store_true", help="Stop all NTP configs"
    )

    parser.add_argument(
        '--restart-master',
        action="store_true", help="Solely restarts the local Master node"
    )

    parser.add_argument(
        '--bad-host',
        action="store_true", help="Actives script on local NTP host to act badly"
    )

    args = parser.parse_args()

    if (args.mode == "remote" or args.mode == "local") and not args.master:
        parser.error("argument --master is missing in remote mode")

    # Get Hosts

    if args.job_id is not None:
        job_info_txt = subprocess.run(f"oarstat -j {args.job_id} -J", shell=True, capture_output=True, text=True)

    else:
        job_info_txt = subprocess.run(f"oarstat -u {args.user} -J", shell=True, capture_output=True, text=True)

    # Parse output as JSON
    try:
        job_info_json = json.loads(job_info_txt.stdout)
    except json.JSONDecodeError as e:
        print("Failed to parse JSON:", e)
        exit(-1)

    assert len(job_info_json) == 1, "Assuming only a single job exists in the json"
    job_id = list(job_info_json.keys())[0]

    host_machines = job_info_json[job_id]["assigned_network_address"]

    ip_addresses = [extrapolate_ip_from_host(host) for host in host_machines]

    # Create NTP Configuration Files
    make_ntp_conf(args)

    if args.clean:
        for i, host in enumerate(ip_addresses):
            clean_ntp(host)


    # Config paths
    local_file_path = "/tmp/ntp.conf.dhcp"
    target_file_path = "/run/ntp.conf.dhcp"

    # In Local mode, issue special configuration for local master node
    if args.mode == "local":
        master_host = find_host_match(args.master, host_machines)
        assert master_host is not None, f"Master machine {args.master} not in host machines"
        local_master_ip = extrapolate_ip_from_host(master_host)
        update_ntp_config(local_master_ip, "/tmp/ntp.conf.dhcp.master", target_file_path, master=True, bad_clock=args.bad_host)
        ip_addresses.remove(local_master_ip)

        if args.restart_master:
            exit(1)

    # Update the NTP of all other nodes
    for i, host in enumerate(ip_addresses):
        update_ntp_config(host, local_file_path, target_file_path)