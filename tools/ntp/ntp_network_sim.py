import json
import argparse
import paramiko
import subprocess

def extract_ip_from_host(host):
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
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize machines in grid5000 cluster")

    parser.add_argument(
        "user", help="Username in Grid5000"
    )

    parser.add_argument("ntp-host", help="NTP host name information to remove it from calculations")
    parser.add_argument("host-region", help="Region of NTP host, passed as an index")
    parser.add_argument("num_regions", type=int, help="Number of regions in Grid5000 deployment")
    parser.add_argument("num_partitions", type=int, help="Number of partitions at each region")
    parser.add_argument("num_clients", type=int, help="Number of clients at each region")


    parser.add_argument("--job_id", help="Optional job id to specify which to get the information from")

    args = parser.parse_args()

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

    num_machines = args.num_regions * args.num_partitions

    region_mapping = []
    region_ssh_clients = []

    local_num_parts = 0
    local_num_clients = 0
    local_num_regions = 1

    machine = 0
    # Set up all IP and ssh client information

    for r in range(args.num_regions):

        region_parts = []
        local_region_ssh = []
        for p in range(args.num_partitions):
            ip = extract_ip_from_host(host_machines[machine])

            region_parts.append(ip)

            # Create SSH client
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(ip, username="root")
            local_region_ssh.append(ssh_client)

            machine += 1

        region_mapping.append(region_parts)
        region_ssh_clients.append(local_region_ssh)


        # Skip client machines
        machine += args.num_clients

    # Obtain information about the NTP server host
    host_ip = extract_ip_from_host(args.ntp_host)

    # Region Delay map
    DELAY = [
        [0.1, 6, 33, 38, 74, 87, 106, 99],
        [6, 0.1, 38, 43, 66, 80, 99, 94],
        [33, 38, 0.1, 6, 101, 114, 92, 127],
        [38, 43, 6, 0.1, 105, 118, 86, 132],
        [74, 66, 101, 105, 0.1, 16, 36, 64],
        [87, 80, 114, 118, 16, 0.1, 36, 74],
        [106, 99, 92, 86, 36, 36, 0.1, 46],
        [99, 94, 127, 132, 64, 74, 46, 0.1],
    ]

    # Create NETEM script for each one



