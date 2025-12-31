import json
import argparse


def verify_deployment_config(config):
    region_names = [reg for reg in config if reg != "order"]

    num_machines = 0

    for reg in region_names:
        region_config = config[reg]

        assert "machines" in region_config, f"Region config for Region {reg} is missing num_replicas information"
        assert "num_clients" in region_config, f"Region config for Region {reg} is missing num_clients information"

        if num_machines == 0:
            num_machines = len(region_config["machines"]) - region_config["num_clients"]
            assert num_machines > 0, f"Region {reg} has not enough machines to match the required config"

        else:
            assert len(region_config[
                           "machines"]) - region_config["num_clients"] == num_machines, f"Region {reg} has different number of machines then previous regions.All regions must have the same number of machines"

        for machine in region_config["machines"]:
            assert len(
                machine) == 2, f"Region {reg} has wrong machine setup. All machines must have 2 ips: A public and a private one"

        assert "num_replicas" in region_config, f"Region config for Region {reg} is missing num_replicas information"
        assert "network_delays" in region_config, f"Region config for Region {reg} is missing network_delays information"

        for r in region_names:
            assert r in region_config[
                "network_delays"], f"Region {reg} is missing network delay information regarding Region {r}"

    if "order" in config:
        assert "public" in config["order"], "Order config does not have public ip"
        assert "private" in config["order"], "Order config does not have private ip"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create an experiments configuration json")

    parser.add_argument(
        "--deployment", "-d", help="Path to the base json containing the deployment information"
    )

    parser.add_argument(
        "--settings", "-s", help="Path to the experiment setting file"
    )

    parser.add_argument(
        "--user", "-u", help="Username on deployments machines. It must be the same username across all machines"
    )

    parser.add_argument(
        "--sudo", help="Sudo password on deployments machines. It must be the same username across all machines."
    )

    parser.add_argument(
        "--sample", default=10, help="Sampling rate of transactions"
    )

    parser.add_argument(
        "--output", "-o", default="experiments/geozip.json", help="Output path for experiment configuration"
    )

    args = parser.parse_args()

    # Get deployment configuration

    deployment_config = json.load(open(args.deployment))

    verify_deployment_config(deployment_config)

    # Get base config
    base_config = json.load(open(args.settings))

    experiment = {
        "username": args.user,
        "sudo": args.sudo,
        "sample": args.sample
    }

    # Extract regions
    experiment["regions"] = [reg for reg in deployment_config if reg != "order"]

    # Setup base configuration
    experiment["distance_ranking"] = {}
    experiment["num_replicas"] = {}
    experiment["shrink_mh_orderer"] = {}
    experiment["servers_public"] = {}
    experiment["servers_private"] = {}
    experiment["clients"] = {}

    # Extract information from each region
    for reg in experiment["regions"]:
        region_config = deployment_config[reg]
        experiment["num_replicas"][reg] = region_config["num_replicas"]
        experiment["shrink_mh_orderer"][reg] = region_config["shrink_mh_orderer"]
        num_servers = len(region_config["machines"]) - region_config["num_clients"]

        experiment["servers_public"][reg] = [ips[0] for ips in region_config["machines"][:num_servers]]
        experiment["servers_private"][reg] = [ips[1] for ips in region_config["machines"][:num_servers]]
        experiment["clients"][reg] = [ips[0] for ips in region_config["machines"][num_servers:]]

        delay_zip = [(r, region_config["network_delays"][r]) for r in region_config["network_delays"] if r != reg]
        sorted_delays = sorted(delay_zip, key=lambda x: x[1])
        experiment["distance_ranking"][reg] = [r[0] for r in sorted_delays]

    if "order" in deployment_config:
        experiment["order"] = deployment_config["order"]

    experiment.update(base_config)
    json.dump(experiment, open(args.output, "w"), indent = 6)


"""

    experiment["regions"] = reg_names

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

    assert len(DELAY) >= args.regions, "Missing network delay information for number of provided regions"
    # Priority of multi region access
    # All these values are default, if you wish to emulate a different network change these by hand
    distance_ranking = {}

    for i in range(args.regions):

        # Get the subset of regions in this experiment
        delays = DELAY[i][0:args.regions]

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

    for i, reg in enumerate(reg_names):

        experiment["num_replicas"][reg] = args.replicas
        experiment["shrink_mh_orderer"][reg] = False

        experiment["servers_public"][reg] = [f"10.10.1.{ip}" for ip in range(3+args.clients+i*(args.partitions+args.clients), 3+args.clients+i*(args.partitions+args.clients) + args.partitions)]
        experiment["servers_private"][reg] = [f"10.10.1.{ip}" for ip in range(3+args.clients+i*(args.partitions+args.clients), 3+args.clients+i*(args.partitions+args.clients) + args.partitions)]

        experiment["clients"][reg] = [f"10.10.1.{ip}" for ip in range(3+i*(args.partitions+args.clients), 3+i*(args.partitions+args.clients) + args.clients)]


    experiment["order"] =  "Order"

    experiment.update(base_config)

    json.dump(experiment, open(args.output, "w"), indent = 6)
    """
