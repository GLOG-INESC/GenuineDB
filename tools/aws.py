import boto3
import json
import logging
import time
import copy

from subprocess import Popen, PIPE

from common import Command, initialize_and_run_commands
from tabulate import tabulate

LOG = logging.getLogger("aws")

MAX_RETRIES = 6

INSTALL_DOCKER_COMMAND = (
    "curl -fsSL https://get.docker.com -o get-docker.sh && "
    "sh get-docker.sh && "
    "sudo usermod -aG docker ubuntu "
)


def shorten_output(out):
    MAX_LINES = 10
    lines = out.split("\n")
    if len(lines) < MAX_LINES:
        return out
    removed_lines = len(lines) - MAX_LINES
    return "\n".join([f"... {removed_lines} LINES ELIDED ..."] + lines[:MAX_LINES])


def install_docker(instance_ips):
    # Install Docker
    installer_procs = []
    for region, ips in instance_ips.items():
        for ip in ips:
            command = [
                "ssh",
                "-o",
                "StrictHostKeyChecking no",
                f"ubuntu@{ip}",
                INSTALL_DOCKER_COMMAND,
            ]
            LOG.info('%s [%s]: Running command "%s"', region, ip, command)
            process = Popen(command, stdout=PIPE, stderr=PIPE)
            installer_procs.append((region, ip, process))

    for region, ip, p in installer_procs:
        p.wait()
        out, err = p.communicate()
        out = shorten_output(out.decode().strip())
        err = shorten_output(err.decode().strip())
        message = f"{region} [{ip}]: Done"
        if out:
            message += f"\nSTDOUT:\n\t{out}"
        if err:
            message += f"\nSTDERR:\n\t{err}"
        LOG.info(message)


def print_instance_ips(instance_ips, label="IP ADDRESSES"):
    print(f"\n================== {label} ==================\n")
    print(json.dumps(instance_ips, indent=2))


def print_slog_config_fragment(instance_public_ips, instance_private_ips, num_clients):
    print("\n================== SLOG CONFIG FRAGMENT ==================\n")
    slog_configs = []
    for region in instance_public_ips:
        public_ips = instance_public_ips[region]
        private_ips = instance_private_ips[region]
        private_server_ips = private_ips[num_clients:]
        client_ips, public_server_ips = (
            public_ips[:num_clients],
            public_ips[num_clients:],
        )

        private_server_ips_str = [f'  addresses: "{ip}"' for ip in private_server_ips]
        public_server_ips_str = [
            f'  public_addresses: "{ip}"' for ip in public_server_ips
        ]
        clients = [f'  client_addresses: "{ip}"' for ip in client_ips]
        slog_configs.append(
            "regions: {\n"
            + "\n".join(private_server_ips_str)
            + "\n"
            + "\n".join(public_server_ips_str)
            + "\n"
            + "\n".join(clients)
            + "\n}"
        )

    print("\n".join(slog_configs))


class AWSCommand(Command):
    def add_arguments(self, parser):
        parser.add_argument(
            "-r", "--regions", nargs="*", help="Run this script on these regions only"
        )


class CreateSpotClusterCommand(AWSCommand):

    NAME = "spot"
    HELP = "Create a spot clusters from given configurations"

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument("config", help="Configuration file for spot cluster")
        parser.add_argument(
            "--clients", type=int, default=1, help="Number of client machines"
        )
        parser.add_argument(
            "--capacity",
            type=int,
            default=None,
            help="Overwrite target capacity in the config",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Run the command without actually creating the clusters",
        )

    def initialize_and_do_command(self, args):
        all_configs = {}
        with open(args.config) as f:
            all_configs = json.load(f)

        if not all_configs:
            LOG.error("Empty config")
            exit()

        assert "InstanceType" in all_configs["configs"], "InstanceType must be set before launching machines"
        instance_type = all_configs["configs"]["InstanceType"]

        zip_orderer_region = all_configs["configs"].get("ZipOrderedRegion", "")

        regions = [
            r
            for r in all_configs
            if (r != "default" and r != "configs" and (not args.regions or r in args.regions))
        ]

        for r in regions:

            all_configs[r]["LaunchTemplateConfigs"][0]["Overrides"] = [{"InstanceType" : instance_type}]

        if args.capacity is not None:
            all_configs["default"]["TargetCapacity"] = args.capacity

        LOG.info(
            "Requesting %d spot instances at: %s",
            all_configs["default"]["TargetCapacity"],
            regions,
        )

        if args.dry_run:
            return

        # Request spot fleets
        spot_fleet_requests = {}
        for region in regions:
            # Apply region-specific configs to the default config
            config = copy.deepcopy(all_configs["default"])
            config.update(all_configs[region])

            if region == zip_orderer_region:
                config["TargetCapacity"] = config["TargetCapacity"]+1

                if "OnDemandTargetCapacity" in config:
                    config["OnDemandTargetCapacity"] = config["OnDemandTargetCapacity"]+1

            ec2 = boto3.client("ec2", region_name=region)

            response = ec2.request_spot_fleet(SpotFleetRequestConfig=config)
            request_id = response["SpotFleetRequestId"]
            LOG.info("%s: Spot fleet requested. Request ID: %s", region, request_id)

            spot_fleet_requests[region] = {
                "request_id": request_id,
                "config": config,
            }

        # Wait for the fleets to come up
        instance_ids = {}
        for region in regions:
            if region not in spot_fleet_requests:
                LOG.warning("%s: No spot fleet request found. Skipping", region)
                continue

            ec2 = boto3.client("ec2", region_name=region)

            request_id = spot_fleet_requests[region]["request_id"]
            config = spot_fleet_requests[region]["config"]
            target_capacity = config["TargetCapacity"]

            region_instances = []
            wait_time = 4
            attempted = 0
            while len(region_instances) < target_capacity and attempted < MAX_RETRIES:
                try:
                    response = ec2.describe_spot_fleet_instances(
                        SpotFleetRequestId=request_id
                    )
                    region_instances = response["ActiveInstances"]
                except Exception as e:
                    LOG.exception(region, e)

                LOG.info(
                    "%s: %d/%d instances started",
                    region,
                    len(region_instances),
                    target_capacity,
                )
                if len(region_instances) >= target_capacity:
                    break
                time.sleep(wait_time)
                wait_time *= 2
                attempted += 1

            if len(region_instances) >= target_capacity:
                LOG.info("%s: All instances started", region)
                instance_ids[region] = [
                    instance["InstanceId"] for instance in region_instances
                ]
            else:
                LOG.error("%s: Fleet failed to start", region)

        # Wait until status check is OK then collect IP addresses
        instance_public_ips = {}
        instance_private_ips = {}
        for region in regions:
            if region not in instance_ids:
                LOG.warning("%s: Skip fetching IP addresses", region)
                continue

            ec2 = boto3.client("ec2", region_name=region)
            ids = instance_ids[region]

            try:
                LOG.info(
                    "%s: Waiting for OK status from %d instances", region, len(ids)
                )
                status_waiter = ec2.get_waiter("instance_status_ok")
                status_waiter.wait(InstanceIds=ids)

                LOG.info("%s: Collecting IP addresses", region)
                response = ec2.describe_instances(InstanceIds=ids)
                instance_public_ips[region] = []
                instance_private_ips[region] = []
                for r in response["Reservations"]:
                    instance_public_ips[region] += [
                        i["PublicIpAddress"].strip() for i in r["Instances"]
                    ]
                    instance_private_ips[region] += [
                        i["PrivateIpAddress"].strip() for i in r["Instances"]
                    ]
            except Exception as e:
                LOG.exception(region, e)

        print_instance_ips(instance_public_ips, "PUBLIC IP ADDRESSES")
        print_instance_ips(instance_private_ips, "PRIVATE IP ADDRESSES")
        print_slog_config_fragment(
            instance_public_ips, instance_private_ips, args.clients
        )

class CreateEC2ClusterCommand(AWSCommand):

    NAME = "ec2"
    HELP = "Create a EC2 fleet clusters from given configurations"

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument("config", help="Configuration file for spot cluster")
        parser.add_argument(
            "--clients", type=int, default=1, help="Number of client machines"
        )
        parser.add_argument(
            "--capacity",
            type=int,
            default=None,
            help="Overwrite target capacity in the config",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Run the command without actually creating the clusters",
        )

    def initialize_and_do_command(self, args):

        all_configs = {}
        with open(args.config) as f:
            all_configs = json.load(f)

        if not all_configs:
            LOG.error("Empty config")
            exit()

        assert "InstanceType" in all_configs["configs"], "InstanceType must be set before launching machines"
        instance_type = all_configs["configs"]["InstanceType"]

        zip_orderer_region = all_configs["configs"].get("ZipOrderedRegion", "")

        regions = [
            r
            for r in all_configs
            if (r != "default" and r != "configs" and (not args.regions or r in args.regions))
        ]

        for r in regions:
            all_configs[r]["LaunchTemplateConfigs"][0]["Overrides"] = [{"InstanceType" : instance_type}]

        if args.capacity is not None:
            all_configs["default"]["TargetCapacitySpecification"]["TotalTargetCapacity"] = args.capacity
            all_configs["default"]["TargetCapacitySpecification"]["OnDemandTargetCapacity"] = args.capacity

        LOG.info(
            "Requesting %d spot instances at: %s",
            all_configs["default"]["TargetCapacitySpecification"]["TotalTargetCapacity"],
            regions,
        )

        if args.dry_run:
            return

        # Request spot fleets
        ec2_fleet_requests = {}

        for region in regions:
            # Apply region-specific configs to the default config
            config = copy.deepcopy(all_configs["default"])
            config.update(all_configs[region])

            if region == zip_orderer_region:
                config["TargetCapacitySpecification"]["TotalTargetCapacity"] = config["TargetCapacitySpecification"]["TotalTargetCapacity"]+1
                config["TargetCapacitySpecification"]["OnDemandTargetCapacity"] = config["TargetCapacitySpecification"]["OnDemandTargetCapacity"]+1

            ec2 = boto3.client("ec2", region_name=region)


            response = ec2.create_fleet(**config)
            request_id = response["FleetId"]
            LOG.info("%s: EC2 fleet requested. Request ID: %s", region, request_id)

            ec2_fleet_requests[region] = {
                "request_id": request_id,
                "config": config,
            }

        # Wait for the fleets to come up
        instance_ids = {}
        for region in regions:
            if region not in ec2_fleet_requests:
                LOG.warning("%s: No spot fleet request found. Skipping", region)
                continue

            ec2 = boto3.client("ec2", region_name=region)

            request_id = ec2_fleet_requests[region]["request_id"]
            config = ec2_fleet_requests[region]["config"]
            target_capacity = config["TargetCapacitySpecification"]["TotalTargetCapacity"]

            region_instances = []
            wait_time = 4
            attempted = 0
            while len(region_instances) < target_capacity and attempted < MAX_RETRIES:
                try:
                    response = ec2.describe_fleet_instances(
                        FleetId=request_id
                    )
                    region_instances = response["ActiveInstances"]
                except Exception as e:
                    LOG.exception(region, e)

                LOG.info(
                    "%s: %d/%d instances started",
                    region,
                    len(region_instances),
                    target_capacity,
                )
                if len(region_instances) >= target_capacity:
                    break
                time.sleep(wait_time)
                wait_time *= 2
                attempted += 1

            if len(region_instances) >= target_capacity:
                LOG.info("%s: All instances started", region)
                instance_ids[region] = [
                    instance["InstanceId"] for instance in region_instances
                ]
            else:
                LOG.error("%s: Fleet failed to start", region)

        # Wait until status check is OK then collect IP addresses
        instance_public_ips = {}
        instance_private_ips = {}
        for region in regions:
            if region not in instance_ids:
                LOG.warning("%s: Skip fetching IP addresses", region)
                continue

            ec2 = boto3.client("ec2", region_name=region)
            ids = instance_ids[region]

            try:
                LOG.info(
                    "%s: Waiting for OK status from %d instances", region, len(ids)
                )
                status_waiter = ec2.get_waiter("instance_status_ok")
                status_waiter.wait(InstanceIds=ids)

                LOG.info("%s: Collecting IP addresses", region)
                response = ec2.describe_instances(InstanceIds=ids)
                instance_public_ips[region] = []
                instance_private_ips[region] = []
                for r in response["Reservations"]:
                    instance_public_ips[region] += [
                        i["PublicIpAddress"].strip() for i in r["Instances"]
                    ]
                    instance_private_ips[region] += [
                        i["PrivateIpAddress"].strip() for i in r["Instances"]
                    ]
            except Exception as e:
                LOG.exception(region, e)

        print_instance_ips(instance_public_ips, "PUBLIC IP ADDRESSES")
        print_instance_ips(instance_private_ips, "PRIVATE IP ADDRESSES")
        print_slog_config_fragment(
            instance_public_ips, instance_private_ips, args.clients
        )



class DestroySpotClusterCommand(AWSCommand):

    NAME = "stop"
    HELP = "Destroy a spot clusters from given configurations"

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Run the command without actually destroying the clusters",
        )

    def initialize_and_do_command(self, args):
        for region in args.regions:
            ec2 = boto3.client("ec2", region_name=region)
            response = ec2.describe_spot_fleet_requests()
            spot_fleet_requests_ids = [
                config["SpotFleetRequestId"]
                for config in response["SpotFleetRequestConfigs"]
                if config["SpotFleetRequestState"]
                in ["submitted", "active", "modifying"]
            ]
            LOG.info("%s: Cancelling request IDs: %s", region, spot_fleet_requests_ids)
            if not args.dry_run:
                if len(spot_fleet_requests_ids) > 0:
                    ec2.cancel_spot_fleet_requests(
                        SpotFleetRequestIds=spot_fleet_requests_ids,
                        TerminateInstances=True,
                    )

class DestroyEC2ClusterCommand(AWSCommand):

    NAME = "stop_ec2"
    HELP = "Destroy a ec2 clusters from given configurations"

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Run the command without actually destroying the clusters",
        )

    def initialize_and_do_command(self, args):
        for region in args.regions:
            ec2 = boto3.client("ec2", region_name=region)
            response = ec2.describe_fleets()

            fleet_ids_to_cancel = [
                fleet["FleetId"]
                for fleet in response["Fleets"]
                if fleet["FleetState"] in ["active", "submitted"]
            ]
            LOG.info("%s: Cancelling request IDs: %s", region, fleet_ids_to_cancel)
            if not args.dry_run:
                if len(fleet_ids_to_cancel) > 0:
                    ec2.delete_fleets(
                        FleetIds=fleet_ids_to_cancel,
                        TerminateInstances=True,
                    )


class InstallDockerCommand(AWSCommand):
    NAME = "docker"
    HELP = "Install docker on all running instances"

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument("--addresses", "-a", nargs="*", help="IP addresses")
        parser.add_argument(
            "--clients", type=int, default=1, help="Number of client machines"
        )
        parser.add_argument("--type", nargs="*", help="Filter instances by type")
        parser.add_argument("--role", nargs="*", help="Filter instances by tagged role")
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="List the instances to install Docker",
        )

    def initialize_and_do_command(self, args):
        if args.addresses is None and not args.regions:
            LOG.error("Must provide at least one region with --regions")
            return

        instance_public_ips = {}
        instance_private_ips = {}

        if args.addresses is not None:
            instance_public_ips["addresses"] = args.addresses
            instance_private_ips["addresses"] = []
        else:
            filters = [{"Name": "instance-state-name", "Values": ["running"]}]
            if args.type:
                filters.append({"Name": "instance-type", "Values": args.type})
            if args.role:
                filters.append({"Name": "tag:role", "Values": args.role})

            for region in args.regions:
                ec2 = boto3.client("ec2", region_name=region)
                try:
                    running_instances = ec2.describe_instances(Filters=filters)
                    LOG.info("%s: Collecting IP addresses", region)
                    instance_public_ips[region] = []
                    instance_private_ips[region] = []
                    for r in running_instances["Reservations"]:
                        instance_public_ips[region] += [
                            i["PublicIpAddress"].strip() for i in r["Instances"]
                        ]
                        instance_private_ips[region] += [
                            i["PrivateIpAddress"].strip() for i in r["Instances"]
                        ]
                except Exception as e:
                    LOG.exception(region, e)

        if not args.dry_run:
            install_docker(instance_public_ips)

        print_instance_ips(instance_public_ips, "PUBLIC IP ADDRESSES")
        print_instance_ips(instance_private_ips, "PRIVATE IP ADDRESSES")
        print_slog_config_fragment(
            instance_public_ips, instance_private_ips, args.clients
        )


class ListInstancesCommand(AWSCommand):
    NAME = "ls"
    HELP = "List instances and their states"

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument(
            "--state",
            default="",
            choices=["pending", "running", "terminated", "stopping", "stopped"],
            nargs="*",
            help="Filter instances by state",
        )
        parser.add_argument("--type", nargs="*", help="Filter instances by type")

    def initialize_and_do_command(self, args):
        if not args.regions:
            LOG.error("Must provide at least one region with --regions")
            return

        filters = []
        if args.state:
            filters.append({"Name": "instance-state-name", "Values": args.state})

        if args.type:
            filters.append({"Name": "instance-type", "Values": args.type})

        info = []
        for region in args.regions:
            ec2 = boto3.client("ec2", region_name=region)
            try:
                instances = ec2.describe_instances(Filters=filters)
                for r in instances["Reservations"]:
                    for i in r["Instances"]:
                        info.append(
                            [
                                i["InstanceId"],
                                i.get("PublicIpAddress", ""),
                                i.get("PrivateIpAddress", ""),
                                i["State"]["Name"],
                                i["Placement"]["AvailabilityZone"],
                                i["InstanceType"],
                                ",".join(
                                    [sg["GroupName"] for sg in i["SecurityGroups"]]
                                ),
                                i["KeyName"],
                            ]
                        )
            except Exception as e:
                LOG.exception(region, e)

        print(
            tabulate(
                info,
                headers=[
                    "ID",
                    "Public IP",
                    "Private IP",
                    "State",
                    "Availability Zone",
                    "Type",
                    "Security group",
                    "Key",
                ],
            )
        )

class CreateExperimentCommand(AWSCommand):
    NAME = "config"
    HELP = "Create a JSON configuration file for deployment from Spot Clusters"

    # Network latencies measured on November 25th 2025
    NETWORK_DELAYS = {
        "us-east-1" : {
            "us-east-1" : 0.1,
            "us-east-2" : 6,
            "eu-west-1" : 34,
            "eu-west-2" : 38,
            "ap-northeast-1" : 74,
            "ap-northeast-2" : 91,
            "ap-southeast-1" : 107,
            "ap-southeast-2" : 99
        },

        "us-east-2" : {
            "us-east-1" : 6,
            "us-east-2" : 0.1,
            "eu-west-1" : 39,
            "eu-west-2" : 43,
            "ap-northeast-1" : 67,
            "ap-northeast-2" : 85,
            "ap-southeast-1" : 102,
            "ap-southeast-2" : 93
        },

        "eu-west-1" : {
            "us-east-1" : 34,
            "us-east-2" : 39,
            "eu-west-1" : 0.1,
            "eu-west-2" : 5.1,
            "ap-northeast-1" : 101,
            "ap-northeast-2" : 119,
            "ap-southeast-1" : 87,
            "ap-southeast-2" : 127
        },

        "eu-west-2" : {
            "us-east-1" : 38,
            "us-east-2" : 43,
            "eu-west-1" : 5.1,
            "eu-west-2" : 0.1,
            "ap-northeast-1" : 106,
            "ap-northeast-2" : 124,
            "ap-southeast-1" : 82,
            "ap-southeast-2" : 132
        },

        "ap-northeast-1" : {
            "us-east-1" : 74,
            "us-east-2" : 67,
            "eu-west-1" : 101,
            "eu-west-2" : 106,
            "ap-northeast-1" : 0.1,
            "ap-northeast-2" : 16.5,
            "ap-southeast-1" : 34.1,
            "ap-southeast-2" : 51
        },

        "ap-northeast-2" : {
            "us-east-1" : 91,
            "us-east-2" : 85,
            "eu-west-1" : 119,
            "eu-west-2" : 124,
            "ap-northeast-1" : 16.5,
            "ap-northeast-2" : 0.1,
            "ap-southeast-1" : 35,
            "ap-southeast-2" : 75
        },

        "ap-southeast-1" : {
            "us-east-1" : 107,
            "us-east-2" : 102,
            "eu-west-1" : 87,
            "eu-west-2" : 82,
            "ap-northeast-1" : 34.1,
            "ap-northeast-2" : 35,
            "ap-southeast-1" : 0.1,
            "ap-southeast-2" : 46
        },

        "ap-southeast-2" : {
            "us-east-1" : 99,
            "us-east-2" : 93,
            "eu-west-1" : 127,
            "eu-west-2" : 132,
            "ap-northeast-1" : 51,
            "ap-northeast-2" : 75,
            "ap-southeast-1" : 46,
            "ap-southeast-2" : 0.1
        },
    }

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument("config", help="Template Configuration file")
        parser.add_argument("output", help="Path to output resulting configuration json")
        parser.add_argument("private_key", help="Path to private key")

        parser.add_argument("--target-dir", default="/home/ubuntu/GeoZip/build/", help="Path to target dir with executable")
        parser.add_argument(
            "--clients", type=int, default=1, help="Number of client machines"
        )
        parser.add_argument(
            "--order-region", default="", help="Region to which to deploy the ordering service"
        )


    def initialize_and_do_command(self, args):
        instance_public_ips = {}
        instance_private_ips = {}

        base_config = json.load(open(args.config))


        for region in args.regions:
            ec2 = boto3.client("ec2", region_name=region)

            instance_public_ips[region] = []
            instance_private_ips[region] = []

            try:
                instances = ec2.describe_instances(Filters=[
                    {"Name": "instance-state-name", "Values": ["running"]}
                ])
                for r in instances["Reservations"]:
                    for i in r["Instances"]:
                        instance_public_ips[region].append(i.get("PublicIpAddress", ""))
                        instance_private_ips[region].append(i.get("PrivateIpAddress", ""))

                if region == args.order_region:
                    assert len(instance_public_ips[region]) > args.clients+1, f"Deployments are not big enough for requested number of clients and ordering service in {region}:\nRequested: {args.clients}\nAvailable: {len(instance_public_ips[region])}"
                else:
                    assert len(instance_public_ips[region]) > args.clients, f"Deployments are not big enough for requested number of clients in {region}:\nRequested: {args.clients}\nAvailable: {len(instance_public_ips[region])}"

            except Exception as e:
                LOG.exception(region, e)

        experiment = {"username": "ubuntu", "sudo": "ubuntu", "dev": "", "sample": 100, "deployment_env": "aws",
                      "regions": args.regions, "num_replicas": {}, "shrink_mh_orderer": {}, "servers_public": {},
                      "servers_private": {}, "clients": {}, "private_key": args.private_key, "target_dir": args.target_dir}


        DELAY = []
        distance_ranking = {}
        for r1 in args.regions:

            experiment["num_replicas"][r1] = 1
            experiment["shrink_mh_orderer"][r1] = True

            if r1 == args.order_region:

                experiment["order"] = {
                    "public"  : instance_public_ips[r1].pop(),
                    "private" : instance_private_ips[r1].pop()
                }

            experiment["servers_public"][r1] = instance_public_ips[r1][args.clients:]
            experiment["servers_private"][r1] = instance_private_ips[r1][args.clients:]
            experiment["clients"][r1] = instance_public_ips[r1][:args.clients]


            r1_delays = []

            r1_distance_ranking = []

            for r2 in args.regions:
                r1_delays.append(self.NETWORK_DELAYS[r1][r2])

                if r1 != r2:
                    r1_distance_ranking.append((r2, self.NETWORK_DELAYS[r1][r2]))

            sorted_r1_distance_ranking = sorted(r1_distance_ranking, key=lambda x: x[1])

            distance_ranking[r1] = [reg[0] for reg in sorted_r1_distance_ranking]

            DELAY.append(r1_delays)

        experiment["delays"] = DELAY
        experiment["distance_ranking"] = distance_ranking

        experiment.update(base_config)
        json.dump(experiment, open(args.output, "w"), indent = 6)

if __name__ == "__main__":
    initialize_and_run_commands(
        "AWS utilities",
        [
            CreateSpotClusterCommand,
            CreateEC2ClusterCommand,
            DestroySpotClusterCommand,
            DestroyEC2ClusterCommand,
            InstallDockerCommand,
            ListInstancesCommand,
            CreateExperimentCommand
        ],
    )
