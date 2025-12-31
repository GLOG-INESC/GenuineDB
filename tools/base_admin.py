#!/usr/bin/python3
"""Admin tool

This tool is used to control a cluster of SLOG servers. For example,
starting a cluster, stopping a cluster, getting status, and more.
"""
import collections
import ipaddress
import itertools
import logging
import os
import shutil
import time
import tarfile
import io
import math
from email.policy import default

import docker
import google.protobuf.text_format as text_format
import sys

from datetime import datetime
from multiprocessing.dummy import Pool
from concurrent.futures import ThreadPoolExecutor

from functools import partial
from typing import Dict, List, Tuple

from docker.models.containers import Container
from paramiko.ssh_exception import PasswordRequiredException

from common import Command, initialize_and_run_commands
from netem import gen_netem_script
from proto.configuration_pb2 import Configuration, Region

import subprocess
from connection import SSHConnection, DockerConnection
from constants import *

import random

remote_processes = {}

LOG = logging.getLogger("admin")



RemoteProcess = collections.namedtuple(
    "RemoteProcess",
    [
        "connection",
        "public_address",
        "private_address",
        "region",
        "replica",
        "partition",
        "name"
    ],
)


def public_addresses(reg: Region):
    if reg.public_addresses:
        return reg.public_addresses
    return reg.addresses


def private_addresses(reg: Region):
    return reg.addresses


def cleanup_container(
    client: docker.DockerClient,
    name: str,
    addr="",
) -> None:
    """
    Cleans up a container with a given name.
    """
    try:
        c = client.containers.get(name)
        c.remove(force=True)
        LOG.info(
            '%sCleaned up container "%s"',
            f"{addr}: " if addr else "",
            name,
        )
    except:
        pass

def stop_container(
        client: docker.DockerClient,
        name: str,
        addr="",
) -> None:
    """
    Stops a container with a given name.
    """
    try:
        c = client.containers.get(name)
        c.kill()
        LOG.info(
            '%Killed container "%s"',
            f"{addr}: " if addr else "",
            name,
        )
    except:
        pass

def get_container_status(client: docker.DockerClient, name: str) -> str:
    if client is None:
        return "network unavailable"
    else:
        try:
            c = client.containers.get(name)
            return c.status
        except docker.errors.NotFound:
            return "container not started"
        except:
            pass
    return "unknown"


def wait_for_containers(containers: List[Tuple[Container, str]]) -> None:
    """
    Waits until all given containers stop.
    """
    for c, addr in containers:
        res = c.wait()
        if res["StatusCode"] == 0:
            LOG.info("%s: Done", addr)
        else:
            LOG.error(
                "%s: Finished with non-zero status (%d). "
                'Check the logs of the container "%s" for more details',
                addr,
                res["StatusCode"],
                c.name,
            )


def parse_envs(envs: List[str]) -> Dict[str, str]:
    """Parses a list of environment variables

    This function transform a list of strings such as ["env1=a", "env2=b"] into:
    {
        env: a,
        env: b,
    }

    """
    if envs is None:
        return {}
    env_var_tuples = [env.split("=") for env in envs]
    return {env[0]: env[1] for env in env_var_tuples}


def fetch_data(machines, user, tag, out_path):
    """Fetch data from remote machines

    @param machines  list of machine info dicts. Each dict has the following format:
                     {
                        'address': address of the machine,
                        'name': name of directory containing the
                                fetched data for this machine
                      }
    @param user      username used to ssh to the machines
    @param tag       tag of the data to fetch
    @param output    directory containing the fetched data
    """

    if os.path.exists(out_path):
        shutil.rmtree(out_path, ignore_errors=True)
        LOG.info("Removed existing directory: %s", out_path)

    os.makedirs(out_path)
    LOG.info(f"Created directory: {out_path}")

    commands = []

    data_path = os.path.join(HOST_DATA_DIR, tag)

    def extract_data(remote_proc):
        data_tar_file = f'{remote_proc.name}.tar.gz'
        data_tar_path = os.path.join(HOST_DATA_DIR, data_tar_file)
        out_tar_path = os.path.join(out_path, data_tar_file)
        out_final_path = os.path.join(out_path, remote_proc.name)

        os.makedirs(out_final_path, exist_ok=True)
        # First, TAR the results
        remote_proc.connection.issue(f"tar -czf {data_tar_path} -C {data_path} .", True)

        # Then, extract the results
        remote_proc.connection.extract(data_tar_path, out_path)

        # Finally, extract those values locally

        return subprocess.Popen(
            ["tar", "-xzf", out_tar_path, "-C", out_final_path]
        )

    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(extract_data, machines))

        for r in results:
            r.wait()

    """
    for m in machines:
        addr = m["address"]
        data_path = os.path.join(HOST_DATA_DIR, tag)
        data_tar_file = f'{m["name"]}.tar.gz'
        data_tar_path = os.path.join(HOST_DATA_DIR, data_tar_file)
        out_tar_path = os.path.join(out_path, data_tar_file)
        out_final_path = os.path.join(out_path, m["name"])

        os.makedirs(out_final_path, exist_ok=True)
        cmd = (
            f'{SSH} {user}@{addr} "tar -czf {data_tar_path} -C {data_path} ." && '
            f"rsync -vh --inplace {user}@{addr}:{data_tar_path} {out_path} && "
            f"tar -xzf {out_tar_path} -C {out_final_path}"
        )
        commands.append(f"({cmd}) & ")

    LOG.info("Executing commands:\n%s", "\n".join(commands))
    os.system("".join(commands) + " wait")
    """

def fetch_profiling(machines, user, out_path):
    """Fetch data from remote machines

    @param machines  list of machine info dicts. Each dict has the following format:
                     {
                        'address': address of the machine,
                        'name': name of directory containing the
                                fetched data for this machine
                      }
    @param user      username used to ssh to the machines
    @param tag       tag of the data to fetch
    @param output    directory containing the fetched data
    """
    if os.path.exists(out_path):
        shutil.rmtree(out_path, ignore_errors=True)
        LOG.info("Removed existing directory: %s", out_path)

    os.makedirs(out_path)
    LOG.info(f"Created directory: {out_path}")

    commands = []
    for m in machines:
        addr = m["address"]
        prof_tar_file = f'{m["name"]}_prof.tar.gz'
        data_tar_path = os.path.join(PROF_DATA_DIR, prof_tar_file)
        out_tar_path = os.path.join(out_path, prof_tar_file)
        out_final_path = os.path.join(out_path, m["name"])

        os.makedirs(out_final_path, exist_ok=True)
        cmd = (
            f'{SSH} {user}@{addr} "tar -czf {data_tar_path} -C {PROF_DATA_DIR} ." && '
            f"rsync -vh --inplace {user}@{addr}:{data_tar_path} {out_path} && "
            f"tar -xzf {out_tar_path} -C {out_final_path}"
        )
        commands.append(f"({cmd}) & ")

    LOG.info("Executing commands:\n%s", "\n".join(commands))
    os.system("".join(commands) + " wait")

def add_known_hosts(addr):
    command = f"ssh-keyscan -H {addr} >> ~/.ssh/known_hosts"
    try:
        subprocess.run(command, shell=True, check=True)
    except:
        LOG.info("[ERROR] Unable to run ssh-keyscan:")
def new_docker_client(user, addr):
    """
    Gets a new Docker client for a given address.
    """

    return docker.DockerClient(
        base_url=f"ssh://{user}@{addr}",
        timeout=300
    )
class AdminCommand(Command):
    """Base class for a command.

    This class contains the common implementation of all commands in this tool
    such as parsing of common arguments, loading config file, and pulling new
    SLOG image from docker repository.

    All commands must extend from this class. A command may or may not override
    any method but it must override and implement the `do_command` method as
    well as all class variables to describe the command.
    """

    CONFIG_FILE_REQUIRED = True

    def __init__(self):
        self.config = None
        self.config_name = None

    def add_arguments(self, parser):
        parser.add_argument(
            "config",
            nargs="?",
            default="",
            metavar="config_file",
            help="Path to a config file",
        )

        parser.add_argument(
            "--connection", choices=["ssh", "docker"], default="ssh", help="Type of connection to host machines. "
                                                                              "SSH has every operation executing through ssh connection."
                                                                              "Docker has a docker container running the application"
        )

        parser.add_argument(
            "--no-pull", action="store_true", help="Skip image pulling step"
        )
        parser.add_argument(
            "--image", default=SLOG_IMG, help="Name of the Docker image to use"
        )
        parser.add_argument(
            "--user", "-u", default=USER, help="Username of the target machines"
        )

        parser.add_argument(
            "--host-dir", type=str, default=HOST_SLOG_DIR, help="Directory in host machine containing Slog code"
        )
        parser.add_argument(
            "--target-dir", type=str, default=TARGET_SLOG_DIR, help="Directory in target machine to hold executables"
        )

        parser.add_argument(
            "--private-key-path", type=str, help="Path to private key for ssh connection"
        )

        parser.add_argument(
            "--skip-sync", action="store_true", help="Skip explicit synchronization of build folder"
        )

    def initialize_and_do_command(self, args):
        # The initialization phase is broken down into smaller methods so
        # that subclasses can override them with different behaviors
        try:
            self.load_config(args)
            self.init_remote_processes(args)
            self.initialize(args)
            # Perform the command
            self.do_command(args)

        except Exception as e:
            LOG.info(f"Error: {e}")
            self.clean_connections()
            raise e

    def load_config(self, args):
        self.config_name = os.path.basename(args.config)
        with open(args.config, "r") as f:
            self.config = Configuration()
            text_format.Parse(f.read(), self.config)

    def init_remote_processes(self, args):
        # Create a docker client for each node
        self.remote_procs = remote_processes

        self.init_server_remote_processes(args)
        self.init_clients_remote_processes(args)
        self.init_order_remote_processes(args)

    def initialize(self,args):
        """
        Initialize the hosts with the required dependencies for the command

        :param args:
        :return:
        """
        if not hasattr(self, 'remote_procs') or len(self.remote_procs) == 0:
            LOG.info('No processes to initialize')
            return

        connections = []
        # Merge all lists
        for _, rp_list in self.remote_procs.items():
            connections += [connection for connection, *_ in rp_list]

        with ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(lambda c: c.initialize(args), connections))


    def do_command(self, args):
        raise NotImplementedError

    def clean_connections(self):

        if hasattr(self, "remote_procs"):
            for _, rp_list in self.remote_procs.items():
                for connection, *_ in rp_list:
                    connection.close()




    ##############################
    #       Helper methods
    ##############################
    def init_server_remote_processes(self, args):
        if "regions" not in self.remote_procs:
            self.remote_procs["regions"] = []
            for reg, reg_info in enumerate(self.config.regions):
                # Region Connections
                pub_addresses = public_addresses(reg_info)
                priv_addresses = private_addresses(reg_info)
                for rep in range(reg_info.num_replicas):
                    for p in range(self.config.num_partitions):
                        idx = rep*self.config.num_partitions + p
                        pub_addr = pub_addresses[idx]
                        priv_addr = priv_addresses[idx]
                        #add_known_hosts(pub_addr)

                        if args.connection == "docker":
                            connection = DockerConnection(args, pub_addr)
                        else:
                            connection = SSHConnection(args, pub_addr)

                        self.remote_procs["regions"].append(RemoteProcess(connection, pub_addr, priv_addr, reg, rep, p, f"{reg}-{p}"))
    def init_clients_remote_processes(self, args):
        if "clients" not in self.remote_procs:
            self.remote_procs["clients"] = []

            for reg, reg_info in enumerate(self.config.regions):
                num_addresses = len(reg_info.client_addresses)
                # Calculate Client Addresses
                step = max((num_addresses + 1) // reg_info.num_replicas, 1)
                i = 0
                for rep in range(reg_info.num_replicas - 1):
                    for j in range(step):
                        if i < num_addresses:
                            addr = reg_info.client_addresses[i]
                            add_known_hosts(addr)
                            if args.connection == "docker":
                                connection = DockerConnection(args, addr)
                            else:
                                connection = SSHConnection(args, addr)
                            self.remote_procs["clients"].append(RemoteProcess(connection, addr, None, reg, rep, j, f"{reg}-{j}"))
                            i += 1
                j = 0
                while i < num_addresses:
                    addr = reg_info.client_addresses[i]
                    add_known_hosts(addr)

                    if args.connection == "docker":
                        connection = DockerConnection(args, addr)
                    else:
                        connection = SSHConnection(args, addr)

                    self.remote_procs["clients"].append(RemoteProcess(connection, addr, None, reg, reg_info.num_replicas - 1, j, f"{reg}-{j}"))
                    i += 1
                    j += 1


    def init_order_remote_processes(self, args):
        if self.config.order_address:
            if "order" not in self.remote_procs:
                add_known_hosts(self.config.order_address.public_address)

                if args.connection == "docker":
                    connection = DockerConnection(args, self.config.order_address.public_address)
                else:
                    connection = SSHConnection(args, self.config.order_address.public_address)

                self.remote_procs["order"] = [RemoteProcess(connection, self.config.order_address.public_address, self.config.order_address.private_address, 0, 0, 0, "order")]

            self.order_process = self.remote_procs["order"][0]

class SyncCommand(AdminCommand):

    NAME = "sync"
    HELP = "Sync configuration file with servers"


class StartCommand(AdminCommand):

    NAME = "start"
    HELP = "Start an SLOG cluster"

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument(
            "--bin", default="slog", help="Name of the binary file to run"
        )
        parser.add_argument(
            "-e",
            nargs="*",
            help="Environment variables to pass to the container. For example, "
            "use -e GLOG_v=1 to turn on verbose logging at level 1.",
        )

        parser.add_argument(
            "--build-dir", "-b", help="Build directory containing executables"
        )

        parser.add_argument(
            "--prof",
            action="store_true",
            help="Activate application profiling"
        )

        parser.add_argument(
            "--resync",
            action="store_true",
            help="Resync executables and configs of servers"
        )

    def init_remote_processes(self, args):
        super().init_remote_processes(args)

        if args.resync:
            # Re-sync the connections to copy the right files
            for category in self.remote_procs:
                for remote_proc in self.remote_procs[category]:
                    remote_proc.connection.sync(args)

    def do_command(self, args):
        if len(self.remote_procs["regions"]) == 0:
            raise Exception("ERROR: Starting without initializing remote processes")

        # Prepare a command to update the config file
        config_text = text_format.MessageToString(self.config)
        config_path = os.path.join(CONTAINER_DATA_DIR, self.config_name)
        sync_config_cmd = f"echo '{config_text}' > {config_path}"


        # Clean up everything first so that the old running session does not
        # mess up with broker synchronization of the new session
        def clean_up(remote_proc):
            connection, *_ = remote_proc
            if args.connection == "docker":
                connection.clean(SLOG_CONTAINER_NAME)
            else:
                connection.clean(name=args.bin, target_build_dir=args.target_dir)

        LOG.info("Cleaning Servers")
        with ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(clean_up, self.remote_procs["regions"]))
        LOG.info("Finished Cleaning Servers")

        rmdir_cmd = f"rm -rf {PROF_DATA_DIR}"
        mkdir_cmd = f"mkdir -p {PROF_DATA_DIR}"

        subprocess.run(["rm", "-rf", "/var/tmp/prof"])
        subprocess.run(["mkdir", "-p", "/var/tmp/prof"])


        def start_server(enumerate_remote_proc):
            i, remote_proc = enumerate_remote_proc
            connection, pub_address, priv_address, *_ = remote_proc

            prof_cmd = ""
            stay_alive_cmd = ""
            if args.prof:
                print(f"Profiling application for server {pub_address}")
                prof_cmd = f"gprofng collect app -o {args.bin}_{pub_address}.er -y SIGUSR1,r"
                stay_alive_cmd = ""

            target_dir = ""
            if args.connection == "ssh":
                target_dir = args.target_dir

            shell_cmd = (
                f"{target_dir}/{args.build_dir}/{args.bin} "
                f"--config {config_path} "
                f"--address {priv_address} "
                f"--data-dir {CONTAINER_DATA_DIR} "
            )
            print(shell_cmd)
            # Synchronously issues the config creation command
            connection.issue(sync_config_cmd, True)

            command = f"ulimit -n 65535; nohup /bin/sh -c '{prof_cmd} {shell_cmd} {stay_alive_cmd}' >{target_dir}/{args.build_dir}/{args.bin}.log 2>&1 &"

            if args.connection == "docker":
                docker_command = [command, SLOG_CONTAINER_NAME, args.image]
                if args.e:
                    docker_command += ["-e"] + args.e
                connection.issue(docker_command)
            else:
                connection.issue(command)

            command = f'while [ ! -f "{target_dir}/{args.build_dir}/{args.bin}.log" ]; do sleep 1; done;'
            connection.issue(command, True)


        LOG.info("Starting Servers")

        with ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(start_server, enumerate(self.remote_procs["regions"])))

        LOG.info("Completed Server Start")



class StartZipClusterCommand(StartCommand):
    NAME = "start_zip"
    HELP = "Starts a Ziplog cluster"

    def do_command(self, args):
        if self.order_process is None:
            print("ERROR: Starting Zip Cluster without initializing Order. \n Make sure an adress is initialized to it")
            raise Exception

        # Prepare a command to update the config file
        config_text = text_format.MessageToString(self.config)
        config_path = os.path.join(CONTAINER_DATA_DIR, self.config_name)

        sync_config_cmd = f"echo '\\''{config_text}'\\'' > {config_path}"

        # Clean up everything first so that the old running session does not
        # mess up with broker synchronization of the new session
        def clean_up(remote_proc):
            connection, *_ = remote_proc
            if args.connection == "docker":
                connection.clean(ZIPLOG_CONTAINER_NAME)
            else:
                connection.clean(name=args.bin, target_build_dir=args.target_dir)

        clean_up(self.order_process)

        def start_server(remote_proc):
            connection, pub_address, priv_address, *_ = remote_proc

            prof_cmd = ""
            stay_alive_cmd = ""
            if args.prof:
                prof_cmd = f"gprofng collect app -o {priv_address}.er"
                stay_alive_cmd = " || echo 'Staying Alive to extract profiling data' && tail -f /dev/null"

            target_dir = ""
            if args.connection == "ssh":
                target_dir = args.target_dir

            shell_cmd = (
                f"{target_dir}/{args.build_dir}/{args.bin} "
                f"--config {config_path} "
                f"--address {priv_address} "
                f"--data-dir {CONTAINER_DATA_DIR} "
            )

            command = f"nohup /bin/sh -c '{sync_config_cmd} && {prof_cmd} {shell_cmd}' >{target_dir}/{args.build_dir}/{args.bin}.log 2>&1 &"

            if args.connection == "docker":
                docker_command = [command, ZIPLOG_CONTAINER_NAME, args.image]
                if args.e:
                    docker_command += ["-e"] + args.e
                connection.issue(docker_command)
            else:
                connection.issue(command, sync=True)

            LOG.info("%s: Synced config and ran command: %s", pub_address, shell_cmd)

        start_server(self.order_process)

        #super().do_command(args)



class StopCommand(AdminCommand):

    NAME = "stop"
    HELP = "Stop an SLOG cluster"

    def do_command(self, args):
        if len(self.remote_procs) == 0:
            return

        def stop_server(container_name, remote_proc):
            connection, *_ = remote_proc
            if args.connection == "docker":
                connection.stop(container_name)
            else:
                connection.stop()

        stop_partial = partial(stop_server, SLOG_CONTAINER_NAME)
        with Pool(processes=len(self.remote_procs["regions"])) as pool:
            pool.map(stop_partial, self.remote_procs["regions"])

        if hasattr(self, 'order_process'):
            stop_server(ZIPLOG_CONTAINER_NAME, self.order_process)

class CleanCommand(AdminCommand):

    NAME = "clean"
    HELP = "Remove all SLOG/ZIPLOG containers"
    def add_arguments(self, parser):
        super().add_arguments(parser)

        parser.add_argument(
            "--build-dir", "-b", help="Build directory containing executables"
        )

        parser.add_argument(
            "--clean-connection", action="store_true", help="Scrub connections as well"
        )


    def do_command(self, args):
        if len(self.remote_procs) == 0:
            LOG.info("No processes to clean, returning")
            return

        def clean_up(connection):
            connection.clean(self.config_name, args.target_dir)
            if args.clean_connection:
                connection.close()

        LOG.info("Starting clean up")
        connection_list = []

        for remote_proc in self.remote_procs["regions"]:
            connection_list.append(remote_proc.connection)

        for remote_proc in self.remote_procs["clients"]:
            connection_list.append(remote_proc.connection)

        if hasattr(self, 'order_process'):
            connection_list.append(self.order_process.connection)

        with ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(clean_up, connection_list))

        LOG.info("Clean up complete")

class BenchmarkCommand(AdminCommand):

    NAME = "benchmark"
    HELP = "Spawn distributed clients to run benchmark"

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument(
            "--txns",
            type=int,
            required=True,
            help="Number of transactions generated per benchmark machine",
        )
        parser.add_argument(
            "--duration",
            type=int,
            default=0,
            help="How long the benchmark is run in seconds",
        )
        parser.add_argument(
            "--tag", help="Tag of this benchmark run. Auto-generated if not provided"
        )
        parser.add_argument(
            "--workload",
            "-wl",
            default="basic",
            help="Name of the workload to run benchmark with",
        )
        parser.add_argument("--params", default="", help="Parameters of the workload")
        parser.add_argument(
            "--rate",
            type=int,
            default=0,
            help="Maximum number of transactions sent per second"
        )
        parser.add_argument(
            "--clients",
            type=int,
            default=0,
            help="Number of clients sending synchronized txns"
        )
        parser.add_argument(
            "--generators",
            type=int,
            default=1,
            help="Number of threads for each benchmark machine",
        )
        parser.add_argument(
            "--sample",
            type=int,
            default=10,
            help="Percent of sampled transactions to be written to result files",
        )
        parser.add_argument(
            "--event-sample",
            type=int,
            default=10,
            help="Percent of sampled transaction events to be written to result files",
        )

        parser.add_argument(
            "--txn-profiles",
            action="store_true",
            help="Output the profile of the sampled txns",
        )
        parser.add_argument(
            "-e",
            nargs="*",
            help="Environment variables to pass to the container. For example, "
            "use -e GLOG_v=1 to turn on verbose logging at level 1.",
        )
        parser.add_argument(
            "--seed",
            type=int,
            default=-1,
            help="Seed for the randomization in the benchmark. Set to -1 for random seed",
        )
        parser.add_argument(
            "--cleanup",
            action="store_true",
            help="Clean up all running benchmarks then exit",
        )

        parser.add_argument(
            "--prof",
            action="store_true",
            help="Profile benchmark "
        )

        parser.add_argument(
            "--build-dir", "-b", help="Build directory containing executables"
        )

    def do_command(self, args):
        # Prepare a command to update the config file
        config_text = text_format.MessageToString(self.config)
        config_path = os.path.join(CONTAINER_DATA_DIR, self.config_name)
        sync_config_cmd = f"echo '\\''{config_text}'\\'' > {config_path}"


        if args.tag:
            tag = args.tag
        else:
            tag = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

        out_dir = os.path.join(CONTAINER_DATA_DIR, tag)



        # Clean up everything
        def clean_up(remote_proc):
            connection, pub_addr, *_ = remote_proc

            if args.connection == "docker":
                connection.clean(BENCHMARK_CONTAINER_NAME)
                connection.clean("benchmark_test")
            else:
                connection.clean(name="benchmark", target_build_dir=args.target_dir)

            # Test for benchmark starting delay
            command = "/bin/sh -c echo"
            start_time = time.time()

            if args.connection == "docker":
                connection.issue([command, "benchmark_test", "-r", True])
                elapsed_time = time.time() - start_time
                connection.status()
            else:
                # with SSH, the ready time is always 0
                elapsed_time = 0

            LOG.info("%s: Removed old data directory", pub_addr)
            return elapsed_time


        with ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(clean_up, self.remote_procs["clients"]))

            LOG.info(
                "Delay per client: %s",
                {
                    self.remote_procs["clients"][i].public_address: f"{results[i]:.2f}"
                    for i in range(len(self.remote_procs["clients"]))
                },
            )

        if args.cleanup:
            return

        def benchmark_runner(enumerate_benchmark):
            i, proc = enumerate_benchmark
            connection, addr, _, reg, rep, *_ = proc

            rmdir_cmd = f"rm -rf {out_dir}"
            mkdir_cmd = f"mkdir -p {out_dir}"
            print(f"Duration: {args.duration}")
            prof_cmd = ""
            if args.prof:
                LOG.info("Running benchmark with profiling")
                prof_cmd = f"gprofng collect app -o {out_dir}.er"

            target_dir = ""
            if args.connection == "ssh":
                target_dir = args.target_dir

            shell_cmd = (
                f"{target_dir}/{args.build_dir}/benchmark "
                f"--config {config_path} "
                f"--region {reg} "
                f"--replica {rep} "
                f"--data-dir {CONTAINER_DATA_DIR} "
                f"--out-dir {out_dir} "
                f"--duration {args.duration} "
                f"--wl {args.workload} "
                f'--params "{args.params}" '
                f"--txns {args.txns} "
                f"--generators {args.generators} "
                f"--sample {args.sample} "
                f"--event_sample {args.event_sample} "
                f"--seed {args.seed} "
                f"--txn_profiles={args.txn_profiles} "
                f"--rate {args.rate} "
                f"--clients {args.clients} "
            )

            print(shell_cmd)
            delay = max(results) - results[i] #+ random.uniform(0.0, 4.0)
            LOG.info(
                "%s: Delay for %f seconds before running the benchmark", addr, delay
            )
            command = f"/bin/sh -c '{sync_config_cmd} && {rmdir_cmd} && {mkdir_cmd} && {prof_cmd} {shell_cmd} ' > {target_dir}/{args.build_dir}/benchmark.log 2>&1"

            time.sleep(delay)

            if args.connection == "docker":
                connection.issue([command, BENCHMARK_CONTAINER_NAME, "-e", args.e])
            else:
                connection.issue(command)


        with ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(benchmark_runner, enumerate(self.remote_procs["clients"])))

        def benchmark_status(proc):
            connection, *_ = proc
            connection.status()

        with ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(benchmark_status, self.remote_procs["clients"]))

class CollectClientCommand(AdminCommand):

    NAME = "collect_client"
    HELP = "Collect benchmark data from the clients"

    def add_arguments(self, parser):
        parser.add_argument(
            "config",
            metavar="config_file",
            help="Path to a config file",
        )
        parser.add_argument("tag", help="Tag of the benchmark data")
        parser.add_argument(
            "--out-dir", default="", help="Directory to put the collected data"
        )
        parser.add_argument(
            "--user", "-u", default=USER, help="Username of the target machines"
        )

    def pull_slog_image(self, _):
        pass

    def do_command(self, args):

        client_out_dir = os.path.join(args.out_dir, args.tag, "client")
        machines = [
            {"address": c, "name": f"{i}-{j}"}
            for i, r in enumerate(self.config.regions)
            for j, c in enumerate(r.client_addresses)
        ]
        fetch_data(self.remote_procs["clients"], args.user, args.tag, client_out_dir)

class CollectServerCommand(AdminCommand):

    NAME = "collect_server"
    HELP = "Collect metrics data from the servers"

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument("--tag", default="test", help="Tag of the metrics data")
        parser.add_argument(
            "--out-dir", default="", help="Directory to put the collected data"
        )
        parser.add_argument("--sudo", help="Password for sudo access")
        group = parser.add_mutually_exclusive_group()
        group.add_argument(
            "--flush-only",
            action="store_true",
            help="Only trigger flushing metrics to disk",
        )
        group.add_argument(
            "--download-only", action="store_true", help="Only download the data files"
        )

    def pull_slog_image(self, _):
        pass

    def do_command(self, args):

        if len(self.remote_procs["regions"]) == 0:
            raise Exception("ERROR: No Servers to connect from")

        if not args.download_only:


            addresses = [a for r in self.config.regions for a in public_addresses(r)]

            out_dir = os.path.join(HOST_DATA_DIR, args.tag)
            config_path = os.path.join(HOST_DATA_DIR, self.config_name)

            command = f"rm -rf {out_dir}; mkdir -p {out_dir}; cp {config_path} {out_dir}"

            LOG.info("Attempting Cleanout")
            def clean_out_dir(remote_proc):
                connection, *_ = remote_proc

                connection.issue(command, True)

            with ThreadPoolExecutor(max_workers=10) as executor:
                results = list(executor.map(clean_out_dir, self.remote_procs["regions"]))

            LOG.info("Finished Cleanout")

            docker_client = docker.from_env()

            if not args.no_pull:
                LOG.info("Pulling image")
                docker_client.images.pull(args.image)
                LOG.info("Finished pulling")

            def trigger_flushing_metrics(enum_address):
                i, address = enum_address
                out_dir = os.path.join(CONTAINER_DATA_DIR, args.tag)
                container_name = f"{SLOG_CLIENT_CONTAINER_NAME}_{i}"

                cleanup_container(docker_client, container_name)
                try:
                    docker_client.containers.run(
                        args.image,
                        name=f"{SLOG_CLIENT_CONTAINER_NAME}_{i}",
                        command=[
                            "/bin/sh",
                            "-c",
                            f"client metrics {out_dir} --host {address} --port {self.config.server_port}",
                        ],
                        network_mode="host",
                        remove=True,
                    )
                except Exception as e:
                    LOG.info(f"Failed client metrics to host {address}")
                    raise

                LOG.info("%s: Triggered flushing metrics to disk", address)

            with ThreadPoolExecutor(max_workers=10) as executor:
                results = list(executor.map(trigger_flushing_metrics, enumerate(addresses)))

        if args.flush_only:
            LOG.info("Finished flushing")
            return

        server_out_dir = os.path.join(args.out_dir, args.tag, "server")
        machines = [
            {"address": a, "name": f"{r}-{p}"}
            for r, reg in enumerate(self.config.regions)
            for p, a in enumerate(public_addresses(reg))
        ]
        LOG.info("Fetching data")

        fetch_data(self.remote_procs["regions"], args.user, args.tag, server_out_dir)
        LOG.info("Finished fetching data")

class CollectServerProfilingCommand(AdminCommand):

    NAME = "collect_server_prof"
    HELP = "Collect metrics data from the servers"

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument("--tag", default="test", help="Tag of the metrics data")
        parser.add_argument(
            "--out-dir", default="", help="Directory to put the collected data"
        )
        group = parser.add_mutually_exclusive_group()
        group.add_argument(
            "--flush-only",
            action="store_true",
            help="Only trigger flushing metrics to disk",
        )
        group.add_argument(
            "--download-only", action="store_true", help="Only download the data files"
        )
        parser.add_argument("--bin", help="Binary of the executed system")

        parser.add_argument(
            "--build-dir", "-b", help="Build directory containing executables"
        )

    def pull_slog_image(self, _):
        pass

    def do_command(self, args):
        server_out_dir = os.path.join(args.out_dir, args.tag, "server")

        def get_profile_server_containers(remote_proc):
            connection, pub_address, priv_address, *_ = remote_proc

            prof_path = f"/home/{args.user}/{args.bin}_{pub_address}.er"
            command = f"pgrep -u {args.user} -f {args.target_dir}/{args.build_dir}/{args.bin} | xargs -r kill -s SIGUSR1"

            connection.issue(command, True)
            connection.issue(f"gprofng archive -a on {prof_path}", True)

        LOG.info("Fetching profiling")

        with ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(get_profile_server_containers, self.remote_procs["regions"]))

        LOG.info("Finished fetching data")



class GenNetEmCommand(AdminCommand):

    NAME = "gen_netem"
    HELP = "Generate netem scripts for every server"

    def add_arguments(self, parser):
        parser.add_argument(
            "config",
            metavar="config_file",
            help="Path to a config file",
        )
        parser.add_argument(
            "latency",
            help="Path to a csv file containing a matrix of one-way latency between regions",
        )
        parser.add_argument(
            "--user", "-u", default=USER, help="Username of the target machines"
        )
        parser.add_argument("--out", "-o", default="netem.sh")
        parser.add_argument("--dev", default="roce0")

        parser.add_argument("--jitter", type=float, default=0, help="Delay jitter in ms")
        parser.add_argument("--jitter-constant", action="store_true",
                            help="Informs that the jitter value is a constant and not a percentage")

        parser.add_argument(
            "--offset", type=int, default=0, help="Extra delay added to all links ms"
        )
        parser.add_argument("--dry-run", action="store_true")

    def init_remote_processes(self, args):
        pass

    def pull_slog_image(self, _):
        pass

    def do_command(self, args):
        regions_pub = []
        regions_priv = []
        
        if len(self.config.regions) == 1:
            n = self.config.num_partitions
            pub_addresses = public_addresses(self.config.regions[0])
            priv_addresses = private_addresses(self.config.regions[0])
            regions_pub = [pub_addresses[i:i+n] for i in range(0, len(pub_addresses), n)]
            regions_priv = [priv_addresses[i:i+n] for i in range(0, len(priv_addresses), n)]
        else:
            regions_pub = [public_addresses(reg) for reg in self.config.regions]
            regions_priv = [private_addresses(reg) for reg in self.config.regions]

        assert len(regions_pub) == len(regions_priv)

        latency = []
        with open(args.latency, "r") as f:

            for i, line in enumerate(f):
                if i > len(regions_pub)-1:
                    break

                arr = list(map(float, line.split(",")))

                assert len(arr) >= len(regions_pub), "Number of latencies must be higher than the number of regions"

                latency.append(arr[0:len(regions_pub)])

        assert len(latency) == len(regions_pub), "Number of regions must match config"

        commands = []
        preview = []

        ntp_host = None

        if self.config.HasField("ntp_host"):
            ntp_host = self.config.ntp_host
        for i, r_from in enumerate(regions_pub):
            netems = []
            filters = []
            for j, r_to in enumerate(regions_priv):
                if i == j:
                    continue

                link_latency = latency[i][j] + args.offset
                jitter = [link_latency * args.jitter, args.jitter][args.jitter_constant]

                netems.append(f"delay {link_latency}ms {jitter}ms")
                filters.append([ip for ip in r_to])

            if ntp_host:
                link_latency = 100 + args.offset
                jitter = ntp_host.jitter

                netems.append(f"delay {link_latency}ms {jitter}ms loss {ntp_host.loss}% reorder {ntp_host.reordering}%")
                filters.append([ntp_host.address])

            script = gen_netem_script(netems, args.dev, filters)

            file_name = args.out

            # TODO - Clean and make neat exceptions
            if args.out.endswith(".sh"):
                prefix = args.out[:-3]

                file_name = f"{prefix}_{i}.sh"
            else:
                sys.exit("NETEM file name must end with a .sh")


            # Write file name in tmp

            file_path = f"/tmp/{file_name}"
            with open(file_path, "w") as f:
                f.write(script)

            preview.append((i, script))

            for ip in r_from:
                commands.append(
                        f'(scp "{file_path}" {args.user}@{ip}:/tmp && {SSH} {args.user}@{ip} "chmod +x {file_path}") & '
                    )

        if ntp_host is not None:
            netems = []
            filters = []
            for j, r_to in enumerate(regions_priv):
                if ntp_host.region == j:
                    continue

                link_latency = 100 + args.offset
                jitter = ntp_host.jitter

                netems.append(f"delay {link_latency}ms {jitter}ms loss {ntp_host.loss}% reorder {ntp_host.reordering}%")
                filters.append([ip for ip in r_to])

            script = gen_netem_script(netems, args.dev, filters)

            file_name = args.out

            # TODO - Clean and make neat exceptions
            if args.out.endswith(".sh"):
                prefix = args.out[:-3]

                file_name = f"{prefix}_ntp_host.sh"
            else:
                sys.exit("NETEM file name must end with a .sh")


            # Write file name in tmp

            file_path = f"/tmp/{file_name}"
            with open(file_path, "w") as f:
                f.write(script)

            preview.append((i, script))

            commands.append(
                f'(scp "{file_path}" {args.user}@{ntp_host.address}:/tmp && {SSH} {args.user}@{ntp_host.address} "chmod +x {file_path}") & '
            )


        if not args.dry_run:
            os.system(" ".join(commands) + " wait")

class ExecuteNetEmCommand(AdminCommand):

    NAME = "execute_netem"
    HELP = "Execute the netem scripts for every server"

    def create_command(self, args, sufix):
        file_name = args.out
        # TODO - Clean and make neat exceptions
        if args.out.endswith(".sh"):
            prefix = args.out[:-3]
            file_name = f"{prefix}_{sufix}.sh"
        else:
            sys.exit("NETEM file name must end with a .sh")

        command = None

        if args.deployment == "grid5000":
            command = f"/tmp/{file_name}"

        elif args.deployment == "emulab":
            command = f"echo {args.sudo} | sudo -S ./{file_name}"

        assert command is not None, f"Missing command instructions for deployment {args.deployment}"

        return command

    def add_arguments(self, parser):
        parser.add_argument(
            "config",
            metavar="config_file",
            help="Path to a config file",
        )
        parser.add_argument(
            "--user", "-u", default=USER, help="Username of the target machines"
        )
        parser.add_argument("--out", "-o", default="netem.sh")
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--sudo", help="Password for sudo access")
        parser.add_argument("--deployment", help="Deployment environment, needed for different kinds of setup")

    def init_remote_processes(self, _):
        pass

    def pull_slog_image(self, _):
        pass

    def do_command(self, args):
        regions_pub = []
        regions_priv = []

        if len(self.config.regions) == 1:
            n = self.config.num_partitions
            pub_addresses = public_addresses(self.config.regions[0])
            priv_addresses = private_addresses(self.config.regions[0])
            regions_pub = [pub_addresses[i:i+n] for i in range(0, len(pub_addresses), n)]
            regions_priv = [priv_addresses[i:i+n] for i in range(0, len(priv_addresses), n)]
        else:
            regions_pub = [public_addresses(reg) for reg in self.config.regions]
            regions_priv = [private_addresses(reg) for reg in self.config.regions]

        assert len(regions_pub) == len(regions_priv)

        commands = []
        preview = []
        user = args.user
        if args.deployment == "grid5000":
            user = "root"

        for i, r_from in enumerate(regions_pub):
            command = self.create_command(args, i)

            for ip in r_from:
                commands.append(
                    f'({SSH} {user}@{ip} "{command}") & '
                )

        if self.config.ntp_host:
            command = self.create_command(args, "ntp_host")
            commands.append(
                f'({SSH} {user}@{self.config.ntp_host.address} "{command}") & '
            )

        if not args.dry_run:
            subprocess.run("".join(commands) + " wait",
                           shell=True,
                           stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL)

        LOG.info("Finished netem execute")

class ResetPTPCommand(AdminCommand):

    NAME = "reset_ptp"
    HELP = "Reset the PTP deployment on grid5000"

    def add_arguments(self, parser):
        parser.add_argument(
            "config",
            metavar="config_file",
            help="Path to a config file",
        )
        parser.add_argument(
            "--user", "-u", default=USER, help="Username of the target machines"
        )
        parser.add_argument("--dry-run", action="store_true")

    def init_remote_processes(self, _):
        pass

    def pull_slog_image(self, _):
        pass

    def do_command(self, args):
        regions_pub = []
        regions_priv = []

        if len(self.config.regions) == 1:
            n = self.config.num_partitions
            pub_addresses = public_addresses(self.config.regions[0])
            priv_addresses = private_addresses(self.config.regions[0])
            regions_pub = [pub_addresses[i:i+n] for i in range(0, len(pub_addresses), n)]
            regions_priv = [priv_addresses[i:i+n] for i in range(0, len(priv_addresses), n)]
        else:
            regions_pub = [public_addresses(reg) for reg in self.config.regions]
            regions_priv = [private_addresses(reg) for reg in self.config.regions]

        assert len(regions_pub) == len(regions_priv)

        commands = []

        for i, r_from in enumerate(regions_pub):
            command = f"/tmp/ptp_script.sh"
            user = "root"

            for ip in r_from:
                commands.append(
                    f'({SSH} {user}@{ip} "{command}") & '
                )

        if not args.dry_run:
            subprocess.run("".join(commands) + " wait",
                           shell=True,
                           stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL)

        LOG.info("Reset PTP")


class CleanupNetEmCommand(AdminCommand):

    NAME = "cleanup_netem"
    HELP = "Cleanup the netem scripts for every server"

    def add_arguments(self, parser):
        parser.add_argument(
            "config",
            metavar="config_file",
            help="Path to a config file",
        )
        parser.add_argument(
            "--user", "-u", default=USER, help="Username of the target machines"
        )
        parser.add_argument("--dev", default="roce0")
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--sudo", help="Password to execute sudo")
        parser.add_argument("--deployment", help="Deployment environment, needed for different kinds of setup")

    def init_remote_processes(self, _):
        pass

    def pull_slog_image(self, _):
        pass

    def do_command(self, args):
        regions_pub = []
        regions_priv = []

        if len(self.config.regions) == 1:
            n = self.config.num_partitions
            pub_addresses = public_addresses(self.config.regions[0])
            priv_addresses = private_addresses(self.config.regions[0])
            regions_pub = [pub_addresses[i:i+n] for i in range(0, len(pub_addresses), n)]
            regions_priv = [priv_addresses[i:i+n] for i in range(0, len(priv_addresses), n)]
        else:
            regions_pub = [public_addresses(reg) for reg in self.config.regions]
            regions_priv = [private_addresses(reg) for reg in self.config.regions]

        assert len(regions_pub) == len(regions_priv)

        commands = []
        user = args.user
        command = None

        if args.deployment == "grid5000":
            command = f"tc qdisc del dev {args.dev} root 2> /dev/null"
            user = "root"

        elif args.deployment == "emulab":
            command = f"echo {args.sudo} | sudo -S tc qdisc del dev {args.dev} root 2> /dev/null"

        assert command is not None, f"Missing command instructions for deployment {args.deployment}"

        for i, r_from in enumerate(regions_pub):
            for ip in r_from:
                commands.append(
                    f'({SSH} {user}@{ip} "{command}") & '
                )

        LOG.info(commands)
        if not args.dry_run:
            os.system("".join(commands) + " wait")
        LOG.info("Finished netem cleanup")

class CleanupAdmin(AdminCommand):

    NAME = "cleanup_admin"
    HELP = "Cleanup all state regarding admin"

    def initialize_and_do_command(self, args):
        self.do_command(args)
    def add_arguments(self, parser):
        pass

    def init_remote_processes(self, _):
        pass

    def pull_slog_image(self, _):
        pass

    def do_command(self, args):
        if remote_processes:
            print("CLEANING PROCESSING")
            for _, rp_list in remote_processes.items():
                for connection, *_ in rp_list:
                    connection.close()
        print("CLEAN")
        remote_processes.clear()

class UpdateBuildCommand(AdminCommand):

    NAME = "aws_update_build"
    HELP = "Pull new updates and make new build"

    def add_arguments(self, parser):
        super().add_arguments(parser)

        parser.add_argument(
            "--git-pat", help="Github Personal Access Token"
        )
        parser.add_argument(
            "--git-rep", "-rep", default="GeoZip", help="Name of the git repository"
        )
        parser.add_argument(
            "--git-user", default="JRafaelSoares", help="Github Username"
        )

    def do_command(self, args):
        def update_build(args, connection):
            # Prepare commands to run non-interactively
            commands = f"""
        cd {args.git_rep}
        # Update remote URL to embed PAT
        git remote set-url origin https://{args.git_user}:{args.git_pat}@github.com/{args.git_user}/{args.git_rep}.git
        # Pull latest changes
        git restore .
        git pull
        # Run build script
        ./emulab/build_all.sh
        """

            print(commands)

            connection.issue(commands, True)

        connection_list = []

        for remote_proc in self.remote_procs["regions"]:
            connection_list.append(remote_proc.connection)

        for remote_proc in self.remote_procs["clients"]:
            connection_list.append(remote_proc.connection)

        if hasattr(self, 'order_process'):
            connection_list.append(self.order_process.connection)

        update_partial = partial(update_build, args)
        with ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(update_partial, connection_list))

def main(args):
    start_time = time.time()
    initialize_and_run_commands(
        "Controls deployment and experiment of SLOG",
        [
            BenchmarkCommand,
            CollectClientCommand,
            CollectServerCommand,
            CollectServerProfilingCommand,
            StartCommand,
            StartZipClusterCommand,
            StopCommand,
            CleanCommand,
            GenNetEmCommand,
            ExecuteNetEmCommand,
            CleanupNetEmCommand,
            CleanupAdmin,
            ResetPTPCommand,
            UpdateBuildCommand
        ],
        args,
    )
    LOG.info("Elapsed time: %.1f sec", time.time() - start_time)


if __name__ == "__main__":
    import sys

    main(sys.argv[1:])
