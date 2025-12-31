import json
import argparse
import subprocess

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create a config for emulab")

    parser.add_argument(
        "--regions", "-reg", default=2, type=int, help="Number of regions"
    )

    parser.add_argument(
        "--replicas", "-rep", default=1, type=int, help="Number of replicas"
    )

    parser.add_argument(
        "--partitions", "-p", default=4, type=int, help="Number of partitions"
    )

    parser.add_argument(
        "--clients", "-c", default=1, help="Number of clients"
    )

    parser.add_argument(
        "--settings", "-s", help="Path to the base settings file"
    )

    parser.add_argument(
        "--user", "-u", help="Username in Emulab"
    )

    parser.add_argument(
        "--sample", default=100, help="Sampling rate"
    )

    parser.add_argument(
        "--output", "-o", default="experiments/geozip.json", help="Output path"
    )

    parser.add_argument(
        "--dev", "-d", help="Device to which apply the network delays (if such exist)"
    )

    parser.add_argument(
        "--order", action='store_true', help="Requires an order node"
    )

    parser.add_argument(
        "--scaling_delay", action='store_true', help="Flag to create a scaling experiment delay set. "
                                                     "Ignore the existing delay mapping and creates a base one"
    )

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

    # Check if we have enough machines for desired config
    required_hosts = args.regions * (args.replicas * args.partitions + args.clients)
    if args.order:
        required_hosts+=1

    assert len(ip_addresses) >= required_hosts, \
        f"Current Job does not have enough resources. Required: {required_hosts}. Job's hosts: {len(ip_addresses)}"

    # Get base config
    base_config = json.load(open(args.settings))

    experiment = {
        "username" : args.user,
        "sudo" : args.user,
        "dev" : args.dev,
        "sample": args.sample,
        "deployment_env": "grid5000"
    }

    reg_names = [f"{i}" for i in range(args.regions)]

    experiment["regions"] = reg_names

    # use1, use2, apse1, apse2, euw1, euw2, apne1, apne2


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

    if not args.scaling_delay:
        experiment["delays"] = DELAY
    else:
        scaling_delay = []
        for i in range(len(reg_names)):
            reg_delay = []
            for j in range(len(reg_names)):
                reg_delay.append(5 if i != j else 0.1)

            scaling_delay.append(reg_delay)

        experiment["delays"] = scaling_delay

    assert len(experiment["delays"]) >= args.regions, "Missing network delay information for number of provided regions"
    # Priority of multi region access
    # All these values are default, if you wish to emulate a different network change these by hand
    distance_ranking = {}

    for i in range(args.regions):

        # Get the subset of regions in this experiment
        delays = experiment["delays"][i][0:args.regions]

        # Associate the region (i.e. the index) to the latency
        delay_zip = [(reg_names[j], delays[j]) for j in range(len(delays))]

        # Remove its own region
        delay_zip.pop(i)

        # Sort by delay
        sorted_delays = sorted(delay_zip, key=lambda x: x[1])

        # Extract the ranking
        distance_ranking[reg_names[i]] = [reg_name[0] for reg_name in sorted_delays]
        assert reg_names[i] not in distance_ranking[reg_names[i]], "The region cannot have its own name in the distance ranking"

    experiment["distance_ranking"] = distance_ranking

    experiment["num_replicas"] = {}
    experiment["shrink_mh_orderer"] = {}
    experiment["servers_public"] = {}
    experiment["servers_private"] = {}
    experiment["clients"] = {}

    # Fill the ips in order
    current_ip = 0
    for i, reg in enumerate(reg_names):

        experiment["num_replicas"][reg] = args.replicas
        experiment["shrink_mh_orderer"][reg] = True

        experiment["servers_public"][reg] = []
        experiment["servers_private"][reg] = []
        experiment["clients"][reg] = []

        for j in range(args.partitions):
            experiment["servers_public"][reg].append(ip_addresses[current_ip])
            experiment["servers_private"][reg].append(ip_addresses[current_ip])
            current_ip += 1

        for j in range(args.clients):
            experiment["clients"][reg].append(ip_addresses[current_ip])
            current_ip += 1

    if args.order:
        experiment["order"] =  {"public" : ip_addresses[current_ip],
                                "private" : ip_addresses[current_ip]}

    experiment.update(base_config)

    json.dump(experiment, open(args.output, "w"), indent = 6)