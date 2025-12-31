import os.path
from sys import stdin

import paramiko
import argparse

from paramiko.channel import Channel

import constants
import docker
import logging
from typing import Dict, List, Tuple
from scp import SCPClient, SCPException
import glob
import time
from dataclasses import dataclass
from io import StringIO

LOG = logging.getLogger("admin")

class Connection:
    def connect(self):
        pass

    def initialize(self, args):
        pass

    def stop(self, name):
        pass

    def clean(self, name, config_name, target_build_dir):
        """
        Cleans any current execution

        :param name: Docker container name for docker executions
        :param config_name: Name of the used config file
        :param target_build_dir: Directory containing executables
        """
        pass

    def unimportant_issue(self, command):
        pass

    def issue(self, command, sync):
        pass

    def status(self, logs, path_to_logs):
        pass

    def close(self):
        pass

    def sync(self, args):
        pass

    def extract(self, target_path, local_path):
        """
        Extracts file from remote machine in target_path to local directory in local_path

        :param target_path: Path to target file extraction
        :param local_path: Path to local file location
        """
        pass

@dataclass
class CommandChannels:
    c_stdin: Channel
    c_stdout: Channel
    c_stderr: Channel

class SSHConnection(Connection):
    """
    Class representing an ssh connection to a machine.
    Keeps a paramiko ssh client for a given machine

    """

    def __init__(self, args, addr):
        self.user = args.user
        self.addr = addr
        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.scp_client = None
        self.initialized = False
        if args.private_key_path:
            # Check if file exists
            if not os.path.exists(args.private_key_path):
                raise Exception(f"Private Key does not exist! {args.private_key_path}")

            private_key_file = open(args.private_key_path, 'r')
            keyfile = StringIO(private_key_file.read())
            self.key = paramiko.RSAKey.from_private_key(keyfile)
        self.connect()

        self.current_command = None
        self.current_commands = {}
        self.command_id = 0



    def connect(self):
        """
        Initializes the connection with the given address
        """
        if hasattr(self, "key"):
            self.ssh_client.connect(self.addr, username=self.user, pkey=self.key)
        else:
            self.ssh_client.connect(self.addr, username=self.user)

        self.scp_client = SCPClient(self.ssh_client.get_transport())

        LOG.info(f"Created new SSH connection to {self.addr}")

    def initialize(self, args):
        """
        Ensures the connected machine has all the required dependencies to run the command

        :param args:
        :return:
        """
        if not self.initialized:
            self.sync(args)
            self.initialized = True
            print("Initialized")


    def stop(self, name=None):
        #TODO - Currently there isnt a good way to stop, so just cleans it
        if name is None:
            name = []
        self.clean(name=name)

    def clean(self, config_name="", name=None, target_build_dir=""):
        #TODO - There are more cleaning than closing any existing channel, like tmp files

        commands = []
        if target_build_dir:
            commands.append(f"pgrep -u {self.user} -f {target_build_dir} | xargs -r kill")

        if config_name:
            prefix = config_name.split('.')[0]
            commands.append(f'rm -rf /tmp/{prefix}_*;')

        for command in commands:
            stdin, stdout, stderr = self.ssh_client.exec_command(command)

            status = stdout.channel.recv_exit_status()

            out = stdout.read().decode()
            err = stderr.read().decode()

            if out:
                LOG.info(f"Result: {out}")
            if err:
                raise Exception(f"Error in command {command}:  {err}")

            #if status != 0 or out or err:
            #    LOG.info(f"Error in command {command}")

    def unimportant_issue(self, command):
        #LOG.info(f"[UNINMPORTAT] Issuing command to {self.addr}")
        assert self.ssh_client is not None and self.ssh_client.get_transport() is not None and self.ssh_client.get_transport().is_active(), "SSH client is not connected"

        command_stdin, stdout, stderr = self.ssh_client.exec_command(command)


        exit_status = stdout.channel.recv_exit_status()

        if exit_status == 1:
            LOG.info(f"ERROR: {stderr.read().decode()}")


    def issue(self, command, sync=False):
        """
        Issues a command to the host machine via ssh

        :param command: The command to be executed
        :param sync: Synchronization flag. If on, issue waits for the return
        """

        stdin, stdout, stderr = self.ssh_client.exec_command(command)

        if sync:
            if stdout.channel.recv_exit_status() == 1:
                LOG.info(f"ERROR: Out: {stdout.read().decode()}, stderr: {stderr.read().decode()}")
        else:
            self.current_command = CommandChannels(stdin, stdout, stderr)

        """
        assert self.channel is None, "Channel is already assigned a long running command"
        LOG.info(f"Issuing command to {self.addr}")
        self.channel = self.ssh_client.get_transport().open_session()
        self.channel.exec_command(command)

        if self.channel.exit_status_ready():
            LOG.info(f"ERROR: {self.channel.recv_stderr(1024)}")

        if sync:
            if self.channel.recv_exit_status() == 1:
                LOG.info(f"ERROR: {self.channel.recv_stderr(1024)}")
        """

    def status(self, logs=False, path_to_logs="/tmp/status.log"):
        """
        Waits until the container is finished
        """

        if self.current_command is not None:
            if logs:
                with open(path_to_logs, "w") as f:
                    while not self.current_command.c_stdout.channel.exit_status_ready():
                        output = self.current_command.recv(1024).decode()  # Read in chunks
                        f.write(output)
                    f.close()

            status = self.current_command.c_stdout.channel.recv_exit_status()

            if status == 0:
                LOG.info("Completed sucessfully")
            else:
                LOG.info("Returned error, read the log")

            self.current_command = None
            """
            start = time.time()

            if logs:
                with open(path_to_logs, "w") as f:
                    try:
                        while True:
                            while self.current_command.c_stdout.channel.recv_ready():
                                output = self.current_command.recv(1024).decode()
                                f.write(output)

                            if self.current_command.c_stdout.channel.exit_status_ready():
                                break

                            if time.time() - start > 180:
                                self.current_command.c_stdout.channel.close()
                                raise TimeoutError(f"Command exceeded 180 seconds")

                            time.sleep(0.1)

                        status = self.current_command.c_stdout.channel.recv_exit_status()
                    except Exception as e:
                        status = -1
                        LOG.info("ERROR: Timeout was issued")

                    f.close()



            if status == 0:
                LOG.info("Completed sucessfully")
            else:
                LOG.info("Returned error, read the log")

            self.current_command = None
        """
        else:
            LOG.info("No channel to get exit status from!")

    def close(self):
        LOG.info("CLOSING SSH CLIENT")
        self.scp_client.close()
        self.ssh_client.close()
        self.initialized = False

    def extract(self, target_path, local_path):
        if not self.initialized:
            raise Exception("Error: Connection must be initialized to extract results")

        self.scp_client.get(target_path, local_path)


    def sync(self, args):
        """
        Synchronize with the end machine the required files and executables

        :param args: Must include:
            host_dir             -> Directory of the source code
            target_dir           -> Directory at the target machine where the files will be copied
            build_dir            -> Directory from which the executables must be extracted from
        """

        if not args.skip_sync:
            try:
                command_stdin, stdout, stderr = self.ssh_client.exec_command(f"/bin/sh -c 'rm -rf {args.target_dir} && mkdir {args.target_dir} && mkdir {args.target_dir}/{args.build_dir}'")
                exit_status = stdout.channel.recv_exit_status()

                if exit_status == 1:
                    LOG.info(f"ERROR: {stderr.read().decode()}")

                execs = glob.glob(f"{args.host_dir}/{args.build_dir}/*")
                for exec in execs:
                    self.scp_client.put(exec, f"{args.target_dir}/{args.build_dir}", recursive=True)
                """
                files = glob.glob(f"{args.host_dir}/examples/*.conf")
                for file in files:
                    self.scp_client.put(file, args.target_dir, recursive=True)

                self.scp_client.put(f"{args.host_dir}/tools", f"{args.target_dir}/tools", recursive=True)
                """
            except Exception as e:
                LOG.error(f"Failed to copy files: {e}")

        self.initialized = True



class DockerConnection(Connection):
    """
       Class representing a docker connection.
       Keeps a paramiko ssh client for a given machine
   """
    def __init__(self, args, addr):
        # The args are already parsed from the top function, so no need to make a parser here
        self.addr = addr
        self.user = args.user

        self.client = None
        self.container = None
        self.connect()

    def connect(self):
        """
        Initializes the connection with the given address
        """

        self.client = docker.DockerClient(
            base_url=f"ssh://{self.user}@{self.addr}",
            timeout=300
        )
        LOG.info(f"Created new Docker connection to {self.addr}")


    def initialize(self, args):
        """
        Ensures that the host machine has all required docker information
        :param args:
        :return:
        """

        if args.no_pull:
            return

        self.client.images.pull(args.image)

    def stop(self, name):
        if self.client is not None:
            try:
                self.client.containers.get(name).stop(timeout=0)
                LOG.info(
                    '%sCleaned up container "%s"',
                    f"{self.addr}: " if self.addr else "",
                    name,
                )
            except docker.errors.NotFound:
                pass
        else:
            LOG.info("Client was never initialized")

    def clean(self, name, config_name=""):
        if self.client is not None:
            try:
                self.client.containers.get(name).remove(force=True)
                LOG.info(
                    '%sCleaned up container "%s"',
                    f"{self.addr}: " if self.addr else "",
                    name,
                )
            except docker.errors.NotFound:
                pass
        else:
            LOG.info("Client was never initialized")

    def issue(self, args, sync=False):
        parser = argparse.ArgumentParser(description="Issuing command arguments")
        parser.add_argument(
            "command", type=str, help="Command executing on container start"
        )
        parser.add_argument(
            "name", type=str, help="Container name"
        )
        parser.add_argument(
            "image", type=str, help="Docker Image to use"
        )
        parser.add_argument(
            "-e",
            nargs="*",
            help="Environment variables to pass to the container. For example, "
                 "use -e GLOG_v=1 to turn on verbose logging at level 1.",
        )
        parser.add_argument(
            "--remove", "-r",
            default=False,
            help="Remove flag determining if one should remove the container after execution"
        )
        args = parser.parse_args(args)

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

        if self.client is not None:
            self.container = self.client.containers.run(
                        args.image,
                        name=args.name,
                        command=args.command,
                        # Mount a directory on the host into the container
                        mounts=[constants.SLOG_DATA_MOUNT],
                        # Expose all ports from container to host
                        network_mode="host",
                        # Avoid hanging this tool after starting the server
                        detach=True,
                        remove=args.remove,
                        environment=parse_envs(args.e),
                    )
        else:
            LOG.error("Docker client was not initialized!")
        pass

    def status(self):
        """
        Waits until the container is finished
        """
        if self.container is not None:
            res = self.container.wait()
            if res["StatusCode"] == 0:
                LOG.info("%s: Done", self.addr)
            else:
                LOG.error(
                    "%s: Finished with non-zero status (%d). "
                    'Check the logs of the container "%s" for more details',
                    self.addr,
                    res["StatusCode"],
                    self.container.name,
                )

    def close(self):
        if self.client is not None:
            self.client.close()
