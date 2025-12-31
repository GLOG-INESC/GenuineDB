import argparse
import threading
from concurrent.futures.thread import ThreadPoolExecutor
from time import sleep

import csv
import itertools
import json
import os
import logging
import shlex
from collections import defaultdict
from tempfile import gettempdir
import time
from multiprocessing import Process
from pprint import pprint

import google.protobuf.text_format as text_format
from typing import List, Type

import base_admin as admin
from proto.configuration_pb2 import Configuration, Region
from connection import SSHConnection
from ntp.ntp_quality import ntp_quality, ptp_quality, process_ptp_quality, process_ntp_quality
import paramiko

LOG = logging.getLogger("experiment")

docker_clients = {}

def generate_config(
    settings: dict,
    template_path: str,
    orig_num_partitions: int,
    num_log_mangers: int,
        orig_num_regions: int
):
    config = Configuration()
    with open(template_path, "r") as f:
        text_format.Parse(f.read(), config)


    regions = settings["regions"]
    if orig_num_regions is not None:
        if len(regions) < orig_num_regions:
            raise RuntimeError(
                f"Not enough Regions ({len(regions)} < {orig_num_regions})"
            )
        regions = regions[0:orig_num_regions]

    regions_ids = {name: id for id, name in enumerate(regions)}

    num_partitions = orig_num_partitions
    num_regions = len(regions_ids)

    for r in regions:
        region = Region()

        public_ips = settings["servers_public"][r]
        private_ips = settings["servers_private"][r]

        if num_partitions is None:
            num_partitions = len(public_ips)

        if len(public_ips) < num_partitions:
            raise RuntimeError(
                f"Not enough public ips for region '{r}' ({len(public_ips)} < {num_partitions})"
            )

        if len(private_ips) < num_partitions:
            raise RuntimeError(
                f"Not enough private ips for region '{r}' ({len(private_ips)} < {num_partitions})"
            )

        servers_private = [
            addr.encode() for addr in private_ips[:num_partitions]
        ]
        region.addresses.extend(servers_private)

        servers_public = [
            addr.encode() for addr in public_ips[:num_partitions]
        ]
        region.public_addresses.extend(servers_public)

        clients = [addr.encode() for addr in settings["clients"][r]]
        region.client_addresses.extend(clients)

        distance_ranking = []
        for other_r in settings["distance_ranking"][r]:
            if other_r in regions_ids:
                distance_ranking.append(str(other_r) if isinstance(other_r, int) else str(regions_ids[other_r]))

        """
        distance_ranking = [
            str(other_r) if isinstance(other_r, int) else str(regions_ids[other_r])
            for other_r in settings["distance_ranking"][r]
        ]
        """

        region.distance_ranking = ",".join(distance_ranking)

        if "num_replicas" in settings:
            region.num_replicas = settings["num_replicas"].get(r, 1)
        else:
            region.num_replicas = 1

        if "shrink_mh_orderer" in settings:
            region.shrink_mh_orderer = settings["shrink_mh_orderer"].get(r, False)

        region.sync_replication = settings.get("local_sync_replication", False)

        config.regions.append(region)

    config.num_partitions = num_partitions

    if num_log_mangers is not None:
        config.num_log_managers = num_log_mangers

    # Quick hack to change the number of keys based on number of partitions
    # for the scalability experiment
    if orig_num_regions is not None or orig_num_partitions is not None:
        config.simple_partitioning.num_records = 125000000*num_regions*num_partitions

    # Glog: Order address
    if "order" in settings:
        config.order_address.public_address  = settings["order"]["public"]
        config.order_address.private_address = settings["order"]["private"]

    if "ntp_host" in settings:
        ntp_host_info = settings["ntp_host"]
        config.ntp_host.address = ntp_host_info["address"]
        config.ntp_host.region = ntp_host_info["region"]
        config.ntp_host.jitter = ntp_host_info["jitter"]
        config.ntp_host.loss = 0
        config.ntp_host.reordering = 0

    # Add network delay information

    if "delays" in settings:
        # When deploying scaling experiments, ignore the set values
        if orig_num_regions is not None:
            for r in range(len(regions)):
                for latency in range(len(regions)):
                    config.network_latency.append(5.0 if r != latency else 0.1)
        else:
            assert len(settings["delays"]) >= len(regions), "Delay mapping size must match number of regions"
            for r in range(len(regions)):
                for latency in range(len(regions)):
                    config.network_latency.append(settings["delays"][r][latency])


    config_filename, config_ext = os.path.splitext(os.path.basename(template_path))
    if orig_num_partitions is not None:
        config_filename += f"-P{orig_num_partitions}"

    if orig_num_regions is not None:
        config_filename += f"-R{orig_num_regions}"

    print(config_filename)
    config_path = os.path.join(gettempdir(), f"{config_filename}{config_ext}")
    with open(config_path, "w") as f:
        text_format.PrintMessage(config, f)


    return config_path


def cleanup(username: str, config_path: str, image: str, connection: str):
    LOG.info("STOP ANY RUNNING EXPERIMENT")
    # fmt: off
    admin.main(
        [
            "benchmark",
            config_path,
            "--connection", connection,
            "--user", username,
            "--image", image,
            "--cleanup",
            "--clients", "1",
            "--txns", "0",
        ]
    )


    # fmt: on

def reset_ptp(settings: dict, config_path: str):

    assert "deployment_env" in settings, "Settings must explicitly mention the deployment environment"

    if settings["deployment_env"] == "grid5000":
        admin.main(
            [
                "reset_ptp",
                config_path,
                "--user", settings["username"],
             ]
        )
        #give time for clocks to update
        sleep(30)

def start_server(settings: dict, config_path: str, connection: str, image: str, build_dir="detock_fr", binary="slog", prof=False, resync=False):
    LOG.info("START SERVERS")

    server_args = [
        "start",
        config_path,
        "--connection", connection,
        "--user", settings["username"],
        "--image", image,
        "--build-dir", build_dir,
        "--bin", binary
    ]

    if resync:
        server_args = server_args + ["--resync"]

    if prof:
        server_args = server_args + ["--prof"]

    if settings["deployment_env"] == "aws":
        server_args += ["--private-key-path", settings["private_key"]]
        server_args += ["--skip-sync"]

    if "host_dir" in settings:
        server_args += ["--host-dir", settings["host_dir"]]

    if "target_dir" in settings:
        server_args += ["--target-dir", settings["target_dir"]]

    admin.main(server_args)

    LOG.info("WAIT FOR ALL SERVERS TO BE ONLINE")
    admin.main([
        "collect_server",
        config_path,
        "--connection", connection,
        "--user", settings["username"],
        "--image", image,
        "--sudo", settings["sudo"],
        "--flush-only",
        "--no-pull"
    ])
    LOG.info("COMPLETED COLLECT SERVERS")

def start_order(settings: dict, username: str, config_path: str, connection: str, image: str,  build_dir="glog_fr", binary="zip_order", prof=False):
    LOG.info("START ORDER")

    order_args = [
        "start_zip",
        config_path,
        "--connection", connection,
        "--user", username,
        "--image", image,
        "--build-dir", build_dir,
        "--bin", binary,
    ]

    if prof:
        order_args = order_args + ["--prof"]

    if settings["deployment_env"] == "aws":
        order_args += ["--private-key-path", settings["private_key"]]
        order_args += ["--skip-sync"]

    if "target_dir" in settings:
        order_args += ["--target-dir", settings["target_dir"]]

    admin.main(order_args)
    # TODO - add stuff to continue only when active

def clean_containers(settings: dict, config_path: str, connection: str, build_dir: str):

    clean_args = [
        "clean",
        config_path,
        "--connection", connection,
        "--user", settings["username"],
        "--build-dir", build_dir,
    ]

    if settings["deployment_env"] == "aws":
        clean_args += ["--private-key-path", settings["private_key"]]
        clean_args += ["--skip-sync"]

    if "host_dir" in settings:
        clean_args += ["--host-dir", settings["host_dir"]]

    admin.main(clean_args)

def stop_containers(username: str, config_path: str, connection: str):
    admin.main([
        "stop",
        config_path,
        "--connection", connection,
        "--user", username
    ])
def collect_client_data(username: str, config_path: str, out_dir: str, tag: str):
    admin.main(
        ["collect_client", config_path, tag, "--user", username, "--out-dir", out_dir]
    )

def collect_server_data(
    username: str, config_path: str, image: str, out_dir: str, tag: str, sudo: str
):
    # fmt: off
    admin.main(
        [
            "collect_server",
            config_path,
            "--tag", tag,
            "--user", username,
            "--image", image,
            "--out-dir", out_dir,
            "--sudo", sudo,
            # The image has already been pulled when starting the servers
            "--no-pull",
        ]
    )
    # fmt: on

def collect_server_prof(
        username: str, config_path: str, image: str, out_dir: str, tag: str, binary: str
):

    # fmt: off
    admin.main(
        [
            "collect_server_prof",
            config_path,
            "--tag", tag,
            "--user", username,
            "--image", image,
            "--out-dir", out_dir,
            # The image has already been pulled when starting the servers
            "--no-pull",
            "--bin", binary
        ]
    )
    # fmt: on

def collect_data(
    username: str,
    config_path: str,
    image: str,
    out_dir: str,
    tag: str,
    no_client_data: bool,
    no_server_data: bool,
    profiling: bool,
    sudo: str,
    binary: str
):
    if not no_client_data:
        LOG.info("COLLECT CLIENT DATA")
        collect_client_data(username, config_path, out_dir, tag)

    if not no_server_data:
        LOG.info("COLLECT SERVER DATA")
        collect_server_data(username, config_path, image, out_dir, tag, sudo)

    if profiling:
        LOG.info("COLLECT PROFILING DATA")

        collect_server_prof(username, config_path, image, out_dir, tag, binary)



def combine_parameters(params, default_params, workload_settings):

    common_values = {}
    ordered_value_lists = []
    for p in params:
        if p in workload_settings:
            value_list = workload_settings[p]
            if isinstance(value_list, list):
                ordered_value_lists.append([(p, v) for v in value_list])
            else:
                common_values[p] = value_list

    combinations = [
        dict(v) for v in
        itertools.product(*ordered_value_lists)
    ]

    # Apply combinations inclusion
    if "include" in workload_settings:
        patterns = workload_settings["include"]
        # Resize extra to be the same size as combinations
        extra = [{} for _ in range(len(combinations))]
        # List of extra combinations
        new = []
        for p in patterns:
            is_new = True
            for c, e in zip(combinations, extra):
                overlap_keys = p.keys() & c.keys()
                if all([c[k] == p[k] for k in overlap_keys]):
                    is_new = False
                    e.update(
                        {k:p[k] for k in p if k not in overlap_keys}
                    )
            if is_new:
                new.append(p)

        for c, e in zip(combinations, extra):
            c.update(e)
        combinations += new

    # Apply combinations exclusion
    if "exclude" in workload_settings:
        patterns = workload_settings["exclude"]
        combinations = [
            c for c in combinations if
            not any([c.items() >= p.items() for p in patterns])
        ]

    # Populate common values and check for missing/unknown params
    params_set = set(params)
    for c in combinations:
        c.update(common_values)
        for k, v in default_params.items():
            if k not in c:
                c[k] = v

        missing = params_set - c.keys()
        if missing:
            raise KeyError(f"Missing required param(s) {missing} in {c}")

        unknown = c.keys() - params_set
        if unknown:
            raise KeyError(f"Unknown param(s) {unknown} in {c}")

    return combinations

def clean_processes():
    admin.main(["cleanup_admin"])

def get_dir_size_gb(path: str) -> float:
    total_size = 0

    for root, dirs, files in os.walk(path):
        for name in files:
            try:
                file_path = os.path.join(root, name)
                total_size += os.path.getsize(file_path)
            except (FileNotFoundError, PermissionError):
                # Some files may disappear or be inaccessible
                pass

    return total_size / (1024 ** 3)   # bytes → GB

class ExperimentExtension:

    OTHER_PARAMS = []
    @classmethod
    def pre_run_hook(cls, _settings: dict, _dry_run: bool):
        pass

    @classmethod
    def post_config_gen_hook(cls, _settings: dict, _config_path: str, _experiment_name: str, _dry_run: bool, _connection: str, _image="", _build_dir=""):
        pass

    @classmethod
    def pre_run_per_val_hook(cls, _settings: dict, _config_path: str, _val: dict, _dry_run: bool):
        pass

    @classmethod
    def post_run_per_val_hook(cls, _settings: dict, _config_path: str, _val: dict, _dry_run: bool):
        pass

    @classmethod
    def post_run_cleanup_hook(cls, settings: dict, config_path: str, connection: str, build_dir: str):
        pass

    @classmethod
    def post_everything_clean(cls, settings: dict, config_path: str, build_dir: str):
        pass

class Experiment:
    """
    A base class for an experiment.

    An experiment consists of a settings.json file and config files.

    A settings.json file has the following format:
    {
        "username": string,  // Username to ssh to the machines
        "sample": int,       // Sample rate, in percentage, of the measurements
        "regions": [string], // Regions involved in the experiment
        "distance_ranking": { string: [string] }, // Rank of distance to all other regions from closest to farthest for each region
        "num_replicas": { string: int },          // Number of replicas in each region
        "servers_public": { string: [string] },   // Public IP addresses of all servers in each region
        "servers_private": { string: [string] },  // Private IP addresses of all servers in each region
        "clients": { string: [string] },          // Private IP addresses of all clients in each region
        // The objects from this point correspond to the experiments. Each object contains parameters
        // to run for an experiment. The experiment is run for all cross-combinations of all these parameters.
        <experiment name>: {
            "servers": [ { "config": string, "image": string } ], // A list of objects containing path to a config file and the Docker image used
            "workload": string,                                   // Name of the workload to use in this experiment

            // Parameters of the experiment. All possible combinations of the parameters 
            // will be generated and possibly modified by "exclude" and "include". This is
            // similar to Github's matrix:
            //      https://docs.github.com/en/actions/using-jobs/using-a-matrix-for-your-jobs#excluding-matrix-configurations
            <parameter1>: [<parameter1 values>]
            <parameter2>: [<parameter2 values>]
            ...
            "exclude": [
                { <parameterX>: <parameterX value>, ... },
                ...
            ]
            "include": [
                { <parameterY>: <parameterY value>, ... },
                ...
            ]
        }
    }
    """

    # Global registry of extensions
    extensions: List[Type] = []

    NAME = ""
    # Parameters of the workload
    WORKLOAD_PARAMS = []
    # Parameters of the benchmark tool and the environment other than the 'params' argument
    OTHER_PARAMS = [
        "generators",
        "clients",
        "txns",
        "duration",
        "rate_limit",
        "num_partitions",
        "num_regions"
    ]
    DEFAULT_PARAMS = {
        "generators": 2,
        "rate_limit": 0,
        "txns": 0,
        "num_partitions": None,
        "num_regions": None
    }


    # ====== Registration API ======
    @classmethod
    def register_extension(cls, ext_cls: Type):
        """Register an extension class that defines any hook(s)."""
        if not issubclass(ext_cls, ExperimentExtension):
            raise TypeError(f"{ext_cls.__name__} must inherit from ExperimentExtension")

        LOG.info(f"Registering extension: {ext_cls.__name__}")
        cls.extensions.append(ext_cls)

        cls.OTHER_PARAMS += ext_cls.OTHER_PARAMS

    # ====== Multiplexing logic ======
    @classmethod
    def _call_extensions(cls, hook_name: str, *args, **kwargs):
        """Call all registered extensions that define this hook."""
        for ext in cls.extensions:
            hook = getattr(ext, hook_name, None)
            if callable(hook):
                LOG.debug(f"Calling {hook_name} from {ext.__name__}")
                hook(*args, **kwargs)


    @classmethod
    def pre_run_hook(cls, _settings: dict, _dry_run: bool):
        cls._call_extensions("pre_run_hook", _settings, _dry_run)

    @classmethod
    def post_config_gen_hook(cls, _settings: dict, _config_path: str, _experiment_name: str, _dry_run: bool, _connection: str, _image="", _build_dir=""):
        cls._call_extensions("post_config_gen_hook", _settings, _config_path, _experiment_name, _dry_run, _connection, _image, _build_dir)

    @classmethod
    def pre_run_per_val_hook(cls, _settings: dict, _config_path: str, _val: dict, _dry_run: bool):
        cls._call_extensions("pre_run_per_val_hook", _settings, _config_path, _val, _dry_run)

    @classmethod
    def post_run_per_val_hook(cls, _settings: dict, _config_path: str, _val: dict, _dry_run: bool):
        cls._call_extensions("post_run_per_val_hook", _settings, _config_path, _val, _dry_run)

    @classmethod
    def post_run_cleanup_hook(cls, _settings: dict, _config_path: str, _connection: str, _build_dir: str):
        cls._call_extensions("post_run_cleanup_hook", _settings, _config_path, _connection, _build_dir)
        clean_processes()



    @classmethod
    def post_everything_clean(cls, _settings: dict, _config_path: str, _build_dir: str):
        cls._call_extensions("post_everything_clean", _settings, _config_path, _build_dir)
        clean_containers(_settings, _config_path, args.connection, _build_dir)

    @classmethod
    def run(cls, args):
        try:
            settings_dir = os.path.dirname(args.settings)
            with open(args.settings, "r") as f:
                settings = json.load(f)

            # Git Personal Access Token used for AWS builds
            if hasattr(args, "git_PAT"):
                print("Update GIT PAT")
                settings["GitHubPAT"] = args.git_PAT

            LOG.info('============ PRE EXPERIMENT HOOK ============')
            cls.pre_run_hook(settings, args.dry_run)
            LOG.info('============      COMPLETE       ============')

            workload_settings = settings[cls.NAME]
            params = cls.OTHER_PARAMS + cls.WORKLOAD_PARAMS

            all_values = combine_parameters(params, cls.DEFAULT_PARAMS, workload_settings)

            num_regs_to_values = defaultdict(dict)

            for v in all_values:
                r = v["num_regions"]

                if r not in num_regs_to_values:
                    num_regs_to_values[r] = defaultdict(list)

                num_regs_to_values[r][v["num_partitions"]].append(v)


            num_log_managers = workload_settings.get("num_log_managers", None)


            for server in workload_settings["servers"]:
                template_path = os.path.join(settings_dir, server["config"])
                # Special config that contains all server ip addresses

                cleanup_config_path = generate_config(settings, template_path, None, num_log_managers, None)

                # Create special sub-directory for results of this build type

                for num_regs, parts in num_regs_to_values.items():
                    for num_partitions, values in parts.items():


                        # Cleanup before each trial
                        config_path = generate_config(
                            settings,
                            template_path,
                            num_partitions,
                            num_log_managers,
                            num_regs
                        )
                        LOG.info('============ GENERATED CONFIG "%s" ============', config_path)

                        LOG.info('============ CLEAN PREVIOUS EXECS ============')

                        LOG.info('                    CLEAN SERVERS ============')

                        clean_containers(settings, config_path, args.connection, server.get("build_dir"))

                        LOG.info('                 CLEAN BENCHMARKS ============')

                        cleanup(settings["username"], cleanup_config_path, server["image"], args.connection)

                        LOG.info('============ POST CONFIG SETUP ============')

                        cls.post_config_gen_hook(settings, config_path, cls.NAME, args.dry_run, args.connection, server["image"], server["build_dir"])


                        LOG.info('============    START SERVERS   ============')

                        if not args.skip_starting_server:
                            start_server(
                                settings,
                                config_path,
                                args.connection,
                                server["image"],
                                server.get("build_dir", "detock_fr"),
                                server.get("binary", "slog"),
                                args.prof,
                                True
                            )

                        config_name = os.path.splitext(os.path.basename(server["config"]))[0]
                        if num_regs is not None:
                            config_name += f"-R{num_regs}"
                        if num_partitions is not None:
                            config_name += f"-P{num_partitions}"

                        LOG.info('============    BEGIN EXPERIMENTS   ============')


                        exp_info = {
                            "system": server["system"],
                            "replication": server["replication"],
                            "execution": server["execution"],
                            "config": config_name,
                            "exec_type": workload_settings["workload"]
                        }

                        cls._run_benchmark(
                            args,
                            server["image"],
                            settings,
                            config_path,
                            config_name,
                            values,
                            server.get("build_dir"),
                            server.get("binary", "slog"),
                            server["server_reset"],
                            cleanup_config_path,
                            exp_info
                        )

                        cls.post_run_cleanup_hook(settings, config_path, args.connection, server.get("build_dir"))
        finally:
            settings_dir = os.path.dirname(args.settings)
            with open(args.settings, "r") as f:
                settings = json.load(f)
            # Cleanup before each trial
            workload_settings = settings[cls.NAME]
            template_path = os.path.join(settings_dir, workload_settings["servers"][0]["config"])

            config_path = generate_config(
                settings,
                template_path,
                None,
                1,
                None
            )
            cls.post_everything_clean(settings, config_path, "")


    @classmethod
    def _run_benchmark(cls, args, image, settings, config_path, config_name, values, build_dir, binary, reset_server=False, cleanup_config="", exp_info={}):
        out_dir = os.path.join(
            args.out_dir, cls.NAME if args.name is None else args.name
        )

        # Update out_dir to write to the build folder
        #out_build_dir = os.path.join(out_dir, build_dir)

        #if not os.path.exists(out_build_dir):
        #    os.makedirs(out_build_dir)

        # Ensure it doesnt overrun the number of experiments, wait for the backlock to be finished
        while True:

            result_dir_size = get_dir_size_gb(args.out_dir)
            processing_dir_size = get_dir_size_gb("/home/rasoares/parquet_tmp")
            if result_dir_size + processing_dir_size < 65:
                LOG.info(f"Current result directory size: {result_dir_size} / {processing_dir_size}")
                break
            else:
                LOG.info(f"Result directory is too full ({result_dir_size} / {processing_dir_size}). Awaiting processing of results")
                sleep(5)


        sample = settings.get("sample", 10)
        trials = settings.get("trials", 1)
        workload_settings = settings[cls.NAME]
        params = cls.OTHER_PARAMS + cls.WORKLOAD_PARAMS

        tag_keys = args.tag_keys
        if tag_keys is None:
            # Only use keys that have varying values
            tag_keys = [
                k for k in params if
                any([v[k] != values[0][k] for v in values])
            ]

        # Already pre-existing experiments
        files = []

        if args.processing_dir:
            if os.path.exists(args.processing_dir):
                files = [entry.name for entry in os.scandir(args.processing_dir)]

        executed = 0
        for v, val in enumerate(values):
            cls.pre_run_per_val_hook(settings, config_path, val, args.dry_run)

            for t in range(trials):
                start_time = time.time()

                tag = config_name

                if build_dir:
                    tag += f"_{build_dir}"
                tag_suffix = "".join([f"{k}{val[k]}" for k in tag_keys])
                if tag_suffix:
                    tag += f"-{tag_suffix}"
                if trials > 1:
                    tag += f"-{t}"

                if tag in files:
                    LOG.info(f"Experiment {tag} has already been executed")
                    continue

                params = ",".join(f"{k}={val[k]}" for k in cls.WORKLOAD_PARAMS)

                LOG.info("'============    RUN BENCHMARK   ============'")
                # fmt: off
                benchmark_args = [
                    "benchmark",
                    config_path,
                    "--build-dir", build_dir,
                    "--connection", args.connection,
                    "--user", settings["username"],
                    "--image", image,
                    "--workload", workload_settings["workload"],
                    "--clients", f"{val['clients']}",
                    "--rate", f"{val['rate_limit']}",
                    "--generators", f"{val['generators']}",
                    "--txns", f"{val['txns']}",
                    "--duration", f"{val['duration']}",
                    "--sample", f"{sample}",
                    "--event-sample", f"{1}",
                    "--seed", f"{args.seed}",
                    "--params", params,
                    "--tag", tag,
                    # The image has already been pulled in the cleanup step
                    "--no-pull",
                ]

                if "target_dir" in settings:
                    benchmark_args += ["--target-dir", settings["target_dir"]]

                # fmt: on
                admin.main(benchmark_args)

                LOG.info("'============    COLLECT DATA   ============'")
                collect_data(
                    settings["username"],
                    config_path,
                    image,
                    out_dir,
                    tag,
                    args.no_client_data,
                    args.no_server_data,
                    args.prof,
                    settings["sudo"],
                    binary
                )

                additional_params = ["jitter", "warehouses", "txn_issuing_delay"]

                for p in additional_params:
                    if p in val:
                        exp_info[p] = val[p]

                json_output_path = os.path.join(out_dir, tag, "finished.json")
                with open(json_output_path, "w") as f:
                    json.dump(exp_info, f, indent=4)

                executed += 1

                if reset_server and not (v+1 == len(values) and t+1 == trials):

                    if not cleanup_config:
                        raise RuntimeError(
                            "Cleanup Config must be set to properly reset servers after execution"
                        )

                    cleanup(settings["username"], cleanup_config, image, args.connection)
                    clean_containers(settings, cleanup_config, args.connection, build_dir)

                    if not args.skip_starting_server:
                        if binary != "slog":
                            start_order(settings,
                                        settings["username"],
                                        config_path,
                                        args.connection,
                                        image,
                                        build_dir)
                            time.sleep(5)

                        start_server(
                                settings,
                                config_path,
                                args.connection,
                                image,
                                build_dir,
                                binary,
                                args.prof
                            )

                final_time = time.time() - start_time
                LOG.info(f"'============    Experiment Complete in {final_time:.2f} seconds ============'")

            cls.post_run_per_val_hook(settings, config_path, val, args.dry_run)





            # IF CLEANUP, reset the servers


        if args.dry_run:
            pprint([{ k:v for k, v in p.items() if k in tag_keys} for p in values])

"""
Workload Experiments
"""
class YCSBExperiment(Experiment):
    NAME = "ycsb"
    WORKLOAD_PARAMS = [
        "writes",
        "records",
        "hot_records",
        "mp_parts",
        "mh_homes",
        "mh_zipf",
        "hot",
        "mp",
        "mh",
        "rw"
    ]
    DEFAULT_PARAMS = {**Experiment.DEFAULT_PARAMS, **{
        "writes": 10,
        "records": 10,
        "hot_records": 2,
        "mp_parts": 2,
        "mh_homes": 2,
        "mh_zipf": 1,
        "rw": 100
    }}

class ZipfYCSBExperiment(Experiment):
    NAME = "ycsb-zipf"
    WORKLOAD_PARAMS = [
        "records",
        "mh_homes",
        "mh_zipf",
        "zipf",
        "mh",
        "mix",
        "fi"
    ]

    DEFAULT_PARAMS = {**Experiment.DEFAULT_PARAMS, **{
        "records": 4,
        "mh_homes": 2,
        "mh_zipf": 1,
        "mix" : "0_0_100",
        "fi" : 0
    }}

    OTHER_PARAMS = Experiment.OTHER_PARAMS + ["txn_issuing_delay"]

    @classmethod
    def pre_run_per_val_hook(cls, settings: dict, config_path: str, val: dict, dry_run: bool):
        super().pre_run_per_val_hook(settings, config_path, val, dry_run)
        if "txn_issuing_delay" in val:
            # Update txn_issuing_delay in config
            config = Configuration()
            with open(config_path, "r") as f:
                text_format.Parse(f.read(), config)

            config.txn_issuing_delay = val["txn_issuing_delay"]

            with open(config_path, "w") as f:
                text_format.PrintMessage(config, f)

class TPCCExperiment(Experiment):
    NAME = "tpcc"
    WORKLOAD_PARAMS = ["mh_zipf", "sh_only"]

    OTHER_PARAMS = Experiment.OTHER_PARAMS + ["warehouses"]

    @classmethod
    def pre_run_per_val_hook(cls, settings: dict, config_path: str, val: dict, dry_run: bool):
        super().pre_run_per_val_hook(settings, config_path, val, dry_run)

        assert "warehouses" in val, "Experiments must have the warehouses setting"

        # Update warehouses in config
        config = Configuration()
        with open(config_path, "r") as f:
            text_format.Parse(f.read(), config)

        assert config.HasField("tpcc_partitioning"), " Configuration must have tpcc partitioning"

        config.tpcc_partitioning.warehouses = val["warehouses"]

        with open(config_path, "w") as f:
            text_format.PrintMessage(config, f)

"""
Extensions
"""

class PtpRestartExtension(ExperimentExtension):
    def post_run_cleanup_hook(cls, settings: dict, config_path: str, connection: str, build_dir: str):
        reset_ptp(settings, config_path)

class NetworkDelayExtension(ExperimentExtension):

    FILE_NAME = "netem_jitter"

    DELAY = [
        [0.1, 6,        30.5, 33, 33.5, 37.5, 74, 86.5],
        [6, 0.1,        25, 26, 38.5, 42.5, 66, 80],
        [30.5, 25,      0.1, 10, 68, 72.5, 53.5, 67],
        [33, 26,        10, 0.1, 63.5, 64, 47.5, 62],
        [33.5, 38.5,    68, 63.5, 0.1, 5.5, 101, 114.5],
        [37.5, 42.5,    72.5, 64, 5.5, 0.1, 104.5, 118],
        [74, 66,        53.5, 47.5, 101, 104.5, 0.1, 16],
        [86.5, 80,      67, 62, 114.5, 118, 16, 0.1]
    ]

    OTHER_PARAMS = ExperimentExtension.OTHER_PARAMS + ["jitter"]

    @classmethod
    def run_netem_script(cls, settings: dict, config_path: str, filename: str):
        LOG.info("Executing Netems")
        admin.main(
            [
                "execute_netem",
                config_path,
                "--user", settings["username"],
                "--out", filename + ".sh",
                "--sudo", settings["sudo"],
                "--deployment", settings["deployment_env"],
                         ]
        )

        LOG.info("Successfully Executed Netems")



    @classmethod
    def post_run_cleanup_hook(cls, settings: dict, config_path: str, connection: str, build_dir: str):
        admin.main(
            [
                "cleanup_netem",
                config_path,
                "--user", settings["username"],
                "--sudo", settings["sudo"],
                "--dev", settings["dev"],
                "--deployment", settings["deployment_env"]
            ]
        )

        clean_containers(settings, config_path, connection, build_dir)

    @classmethod
    def post_config_gen_hook(cls, settings: dict, config_path: str, _experiment_name: str, dry_run: bool, connection: str, image="", _build_dir=""):
        LOG.info("Generating Netems")

        workload_settings = settings[_experiment_name]

        if "jitter" not in workload_settings:
            raise KeyError(f"Missing required key: jitter")

        # Recreate the delay map
        if "delays" in settings:
            delay_map = settings["delays"]
        else:
            delay_map = cls.DELAY

        delay_path = os.path.join(gettempdir(), cls.FILE_NAME + ".csv")
        with open(delay_path, "w") as f:
            writer = csv.writer(f)
            writer.writerows(delay_map)

        jitters = workload_settings["jitter"]

        for j in jitters:
            command = [
                "gen_netem",
                config_path,
                delay_path,
                "--user", settings["username"],
                "--out", f"{cls.FILE_NAME}_{j}.sh",
                "--jitter", str(j),
                "--dev", settings["dev"]
            ]
            if "jitter_constant" in workload_settings:
                LOG.info("Applying constant jitter")
                command += ["--jitter-constant"]

            # fmt: off
            admin.main(
                command
            )
            # fmt: on
        LOG.info("Successfully Generated Netems")
    @classmethod
    def pre_run_per_val_hook(cls, settings: dict, config_path: str, val: dict, dry_run: bool):
        jitter = val["jitter"]
        if not dry_run:
            cls.run_netem_script(settings, config_path, f"{cls.FILE_NAME}_{jitter}")
            time.sleep(5)

class GlogExtension(ExperimentExtension):
    @classmethod
    def post_config_gen_hook(cls, _settings: dict, _config_path: str, _experiment_name: str, _dry_run: bool, connection: str, image="", build_dir=""):
        start_order(
            _settings,
            _settings["username"],
            _config_path,
            connection,
            image,
            build_dir,
            "zip_order"
        )

class AWSExtension(ExperimentExtension):
    @classmethod
    def post_config_gen_hook(cls, _settings: dict, _config_path: str, _experiment_name: str, _dry_run: bool, _connection: str, _image="", _build_dir=""):
        LOG.info("Updating AWS Builds")
        admin.main(
            [
                "aws_update_build",
                _config_path,
                "--git-pat", _settings["GitHubPAT"],
                "--skip-sync"
            ]
        )

class ClockSyncQuality(ExperimentExtension):

        STOP_EVENT = threading.Event()

        SERVER_CONNECTIONS = []
        SETUP = False

        FUTURE = None

        EXECUTOR = ThreadPoolExecutor(max_workers=1)

        IS_NTP = True
        @classmethod
        def setup_connections(cls, settings):

            for region in settings["regions"]:
                for public_ip in settings["servers_public"][region]:
                    ssh_connection = paramiko.SSHClient()
                    ssh_connection.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                    ssh_connection.connect(public_ip, username="root")

                    cls.SERVER_CONNECTIONS.append((public_ip, ssh_connection))

        @classmethod
        def pre_run_per_val_hook(cls, _settings: dict, _config_path: str, _val: dict, _dry_run: bool):
            LOG.info("SETUP PTP MEASUREMENTS")
            if not cls.SETUP:
                cls.setup_connections(_settings)

            if cls.IS_NTP:
                cls.FUTURE = cls.EXECUTOR.submit(ntp_quality, cls.SERVER_CONNECTIONS, cls.STOP_EVENT, False)
            else:
                cls.FUTURE = cls.EXECUTOR.submit(ptp_quality, cls.SERVER_CONNECTIONS, cls.STOP_EVENT)
        @classmethod
        def post_run_per_val_hook(cls, settings: dict, config_path: str, _val: dict, _dry_run: bool):
            # End measurement

            if not cls.FUTURE:
                print("Error: No Thread found")
                return

            cls.STOP_EVENT.set()

            duration, offset_samples, jitter_samples, max_diff_samples = cls.FUTURE.result()

            if cls.IS_NTP:
                process_ntp_quality(duration, offset_samples, jitter_samples, max_diff_samples, True)
            else:
                process_ptp_quality(duration, offset_samples, jitter_samples, max_diff_samples, True)
        @classmethod
        def post_everything_clean(cls, _settings: dict, _config_path: str, _build_dir: str):
            # Close connections
            for host, con in cls.SERVER_CONNECTIONS:
                con.close()

if __name__ == "__main__":

    EXPERIMENTS = {
        "ycsb": YCSBExperiment(),
        "tpcc": TPCCExperiment(),
        "ycsb-zipf": ZipfYCSBExperiment(),
    }

    EXTENSIONS = {
        "network_delay" : NetworkDelayExtension,
        "glog": GlogExtension,
        "aws": AWSExtension,
        "ptp": PtpRestartExtension,
        "clock_sync" : ClockSyncQuality
    }

    parser = argparse.ArgumentParser(description="Run an experiment")
    parser.add_argument(
        "experiment", choices=EXPERIMENTS.keys(), help="Name of the experiment to run"
    )
    parser.add_argument("--extensions", nargs="*", default=[], help="List of extensions to use"),
    parser.add_argument(
        "--settings", "-s", default="experiments/settings.json", help="Path to the settings file"
    )
    parser.add_argument(
        "--out-dir", "-o", default=".", help="Path to the output directory"
    )
    parser.add_argument(
        "--name", "-n", help="Override name of the experiment directory"
    )
    parser.add_argument(
        "--connection", default="ssh", help="Type of connection to host machines. "
    )

    parser.add_argument(
        "--tag-keys",
        nargs="*",
        help="Keys to include in the tag",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Check the settings and generate configs without running the experiment",
    )
    parser.add_argument(
        "--skip-starting-server", action="store_true", help="Skip starting server step"
    )
    parser.add_argument(
        "--no-client-data", action="store_true", help="Don't collect client data"
    )
    parser.add_argument(
        "--no-server-data", action="store_true", help="Don't collect server data"
    )

    parser.add_argument(
        "--processing-dir", help="Path where processed experiments lie. "
                                 "This path is used to detect and skip repeated experiments after they have been processed by the result_processing script."
    )

    parser.add_argument(
        "--prof", action="store_true", help="Run with gprofng and retrieve profile information"
    )

    parser.add_argument(
        "--git-PAT", help="Github Personal Access Token for AWS builds"
    )

    parser.add_argument("--seed", default=-1, help="Seed for the random engine")
    args = parser.parse_args()

    if args.dry_run:

        def noop(cmd):
            print(f"\t{shlex.join(cmd)}\n")

        admin.main = noop

    experiment = EXPERIMENTS[args.experiment]

    if "extensions" in args:
        for ext in args.extensions:
            experiment.register_extension(EXTENSIONS[ext])

    experiment.run(args)
