import json
import argparse
import subprocess
import paramiko
from scp import SCPClient

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize machines in grid5000 cluster")

    parser.add_argument(
        "user", help="Username in Grid5000"
    )


    args = parser.parse_args()

    # Get Hosts

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

    ip_addresses = []
    for host in host_machines:
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

        ip_addresses.append(address_after_name)



    # For each host, copy the start_up script and execute it

    for i, host in enumerate(ip_addresses):
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(host, username="root")

        ssh_client.exec_command("rm -rf /tmp/detock_*")
        ssh_client.exec_command("rm -rf /tmp/glog_*")

        ssh_client.close()