import json
import argparse
import subprocess
import paramiko
from statistics import mean
import time
from datetime import datetime
import math
import threading
"""
NTP 
"""
def parse_ntpq_output(out):
    # Remove leading NTP selection symbol (*, +, -, etc)
    if out[0] in "*#o+x.-":
        out = out[1:].lstrip()

    # Tokenize by whitespace
    parts = out.split()

    if len(parts) != 10 or parts[0] == "remote":
        return None, None, None, None

    active_master = parts[1]

    off = float(parts[-2])
    jit = float(parts[-1])
    reach = parts[-4]
    return active_master, off, jit, reach

def ntp_quality(ssh_clients, stop_event, local_deployment, debug_print=False):
    offset_samples = []
    jitter_samples = []
    master_sources = []
    max_diff_samples = []

    if debug_print:
        print("\n=== NTP Summary ===\n")

    start_time = datetime.now()

    try:
        while not stop_event.is_set():
            if debug_print:
                print(f"--- Polling at {datetime.now().strftime('%H:%M:%S')} ---")
            local_offset = []
            local_jitter = []
            for host, client in ssh_clients:
                stdin, stdout, stderr = client.exec_command("ntpq -p")
                output = stdout.read().decode()

                for line in output.splitlines():
                    # Detect the selected peer: line starts with '*'
                    if not line.startswith("*") and not local_deployment:
                        continue

                    master, offset, jitter, reach = parse_ntpq_output(line)

                    if master is None:
                        continue
                    if debug_print:
                        if reach != "377":
                            print(f"[{host}] SYNCHING Master={master} Offset={offset} ms Jitter={jitter} ms")
                        else:
                            print(f"[{host}] Master={master} Offset={offset} ms Jitter={jitter} ms")

                    local_offset.append(offset)
                    local_jitter.append(jitter)

                    offset_samples.append(offset)
                    jitter_samples.append(jitter)
                    master_sources.append(master)

            if local_offset:
                max_diff = max(local_offset) - min(local_offset)
                max_diff_samples.append(max_diff)
                if debug_print:
                    print(f"Max Pairwise Diff: {max_diff}")

            if debug_print:
                print("")
            time.sleep(5)
    except KeyboardInterrupt:
        # graceful shutdown
        if debug_print:
            print("\n\n=== CTRL+C detected — stopping the test ===\n")
        stop_event.set()

    finally:
        end_time = datetime.now()
        duration = end_time - start_time

        # Cleanup SSH
        for _, client in ssh_clients:
            client.close()

    return duration, offset_samples, jitter_samples, max_diff_samples

def process_ntp_quality(duration, offset_samples, jitter_samples, max_diff_samples, print_results = False):
    average_offset = None
    average_jitter = None
    average_max_diff = None
    # Averages
    if offset_samples:
        average_offset = mean(offset_samples)
    if jitter_samples:
        average_jitter = mean(jitter_samples)
    if max_diff_samples:
        average_max_diff = mean(max_diff_samples)

    if print_results:
        print("=== NTP Final Test Report ===")
        print(f"Duration: {duration}")
        print(f"Total samples collected: {len(offset_samples)}")

        print(f"Average offset: {average_offset:.3f} ns")
        print(f"Average jitter: {average_jitter:.3f} ms")
        print(f"Average Max Diff: {average_max_diff:.3f} ms")
        print("\nDone.\n")

    """
    # Master check
    unique_masters = set(master_sources)
    if len(unique_masters) == 1:
        print(f"All machines used SAME master: {list(unique_masters)[0]}")
    else:
        print("WARNING: Machines used DIFFERENT masters:")
        for m in unique_masters:
            print(f" - {m}")

    # Averages
    if offset_samples:
        print(f"Average offset: {mean(offset_samples):.3f} ms")
    else:
        print("No offset data collected")

    if jitter_samples:
        print(f"Average jitter: {mean(jitter_samples):.3f} ms")
    else:
        print("No jitter data collected")

    if max_diff_samples:
        print(f"Average Max Diff: {mean(max_diff_samples):.3f} ms")
    else:
        print("No max diff collected")

    print("\nDone.\n")
    """


"""
PTP
"""

def parse_pmc_output(out):
    lines = out.splitlines()

    assert len(lines) == 5, f"Output returned wrong size: got {len(lines)}, expected size 5"

    offset_line = lines[3]
    parts = offset_line.split()

    assert len(parts) == 2, f"Offset line is incorrect size: {len(parts)} != 2: {offset_line}"
    assert parts[0] == "offsetFromMaster",  f"Offset line is incorrect format: {offset_line}"

    return float(parts[1])

def calculate_jitter(data):
    if len(data) < 2:
        return None

    n = len(data)
    mean = sum(data) / n
    variance = sum((x - mean) ** 2 for x in data) / n
    jitter = math.sqrt(variance)

    return jitter

def ptp_quality(ssh_clients, stop_event, debug_print=False):
    command = "pmc -u -b 0 'GET CURRENT_DATA_SET'"
    offset_samples = []
    jitter_samples = []

    max_diff_samples = []

    if debug_print:
        print("\n=== PTP Summary ===\n")
    start_time = datetime.now()

    per_host_offset = {}

    # Initialize per_host_offset for jitter calculations

    for host, _ in ssh_clients:
        per_host_offset[host] = []

    try:
        while not stop_event.is_set():
            if debug_print:
                print(f"--- Polling at {datetime.now().strftime('%H:%M:%S')} ---")
            local_offset = []
            local_jitter = []
            for host, client in ssh_clients:
                stdin, stdout, stderr = client.exec_command(command)
                output = stdout.read().decode()

                offset = parse_pmc_output(output)
                per_host_offset[host].append(offset)
                jitter = calculate_jitter(per_host_offset[host])
                if debug_print:
                    print(f"[{host}] Offset={offset} ns Jitter={jitter} ns")

                local_offset.append(offset)
                offset_samples.append(offset)
                if jitter:
                    local_jitter.append(jitter)
                    jitter_samples.append(jitter)

            if local_offset:
                max_diff = max(local_offset) - min(local_offset)
                max_diff_samples.append(max_diff)
                if debug_print:
                    print(f"Max Pairwise Diff: {max_diff} ns")

            if debug_print:
                print("")
            time.sleep(1)

    except KeyboardInterrupt:
        # graceful shutdown
        if debug_print:
            print("\n\n=== CTRL+C detected — stopping the test ===\n")
        stop_event.set()

    finally:
        end_time = datetime.now()
        duration = end_time - start_time

    return duration, offset_samples, jitter_samples, max_diff_samples


def process_ptp_quality(duration, offset_samples, jitter_samples, max_diff_samples, print_results = False):

    average_offset = None
    average_jitter = None
    average_max_diff = None


    # Averages
    if offset_samples:
        average_offset = mean(offset_samples)
    if jitter_samples:
        average_jitter = mean(jitter_samples)
    if max_diff_samples:
        average_max_diff = mean(max_diff_samples)

    if print_results:
        print("=== PTP Final Test Report ===")
        print(f"Duration: {duration}")
        print(f"Total samples collected: {len(offset_samples)}")

        print(f"Average offset: {average_offset:.3f} ns")
        print(f"Average jitter: {average_jitter:.3f} ns")
        print(f"Average Max Diff: {average_max_diff:.3f} ns")
        print("\nDone.\n")

    return average_offset, average_jitter, average_max_diff



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize machines in grid5000 cluster")

    parser.add_argument(
        "user", help="Username in Grid5000"
    )

    parser.add_argument("num_regions", type=int, help="Number of regions in Grid5000 deployment")
    parser.add_argument("num_partitions", type=int, help="Number of partitions at each region")
    parser.add_argument("num_clients", type=int, help="Number of clients at each region")

    parser.add_argument("--poll-interval", default=5, help="Time between NTP polls")

    parser.add_argument("--job_id", help="Optional job id to specify which to get the information from")

    parser.add_argument("--ntp-host", help="Optional ntp-host information to remove it from calculations")

    parser.add_argument("--local-deployment", action="store_true", help="Special rules for local deployment of ntp")
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

    ip_addresses = []

    local_num_parts = 0
    local_num_clients = 0
    local_num_regions = 1

    machine = 0
    for r in range(args.num_regions):

        for p in range(args.num_partitions):
            host = host_machines[machine]
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

            machine += 1

        for c in range(args.num_clients):
            machine += 1

    # For each host, run ntpq command and extract offset and jitter

    clients = []

    for i, host in enumerate(ip_addresses):
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(host, username="root")

        clients.append((host, ssh_client))

    stop_event = threading.Event()

    duration, offset_samples, jitter_samples, max_diff_samples = ntp_quality(clients, stop_event, True, True)
    process_ntp_quality(duration, offset_samples, jitter_samples, max_diff_samples, True)
    #ptp_quality(clients, stop_event)
