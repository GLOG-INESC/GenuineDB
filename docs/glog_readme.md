# GenuineDB

GenuineDB is the first partially replicated, geo-distributed data store which provides **latency-genuine** performance. 

Its codebase is based on that of [Detock](https://github.com/umd-dslam/Detock/tree/master/module). The following README and guide are heavily based on the original codebase guides.

This repository contains an experimental implementations of the system, along with the instructions to build and deploy the system.

### Disclaimer:

GenuineDB was deployed and evaluated in the [Grid'5000 testbench](https://www.grid5000.fr/w/Grid5000:Home), which requires a Grid'5000 account.
For artifact evaluation, you may request an account following [these instructions](https://www.grid5000.fr/w/Grid5000:Get_an_account#:~:text=regarding%20your%20collaboration.-,Access%20for%20a%20conference%20peer%20review,-Access%20to%20Grid).
The team behind GenuineDB is available for any questions or concerns, and you may contact the author for further support:
Rafael Soares - joao.rafael.pinto.soares@tecnico.ulisboa.pt


## How to Build

The following guide has been tested on Debian 11.11 with GCC 10.2.1 and CMake 3.18.4


First, install the build tools by running the `install-deps.sh` script inside GenuineDB directory:
```
sudo ./install-deps.sh
```

Then, run `tools/build_all.sh` to build all system versions 

```
./tools/build_all.sh
```

Next, you will need to create a python venv using the following command:

```
python3 -m venv $PATH
source $PATH/bin/activate
pip3 install -r tools/requirements.txt
```

## How to Run

We provide instructions for three different deployment scenarios:
1. A generic deployment scenario, where a user provides a set of IP addresses to which to deploy GenuineDB on;
2. Grid'5000 Deployment
3. AWS Deployment

# GenuineDB on a Generic Cluster

The following guide shows how to run GenuineDB on a cluster of multiple machines.

First, you will need to fill the base setting describing your deployment. A template is provided in `experiments/base_settings.json`

Below is a detailed description of the json and each field:

<details>
<summary>Description of `base_setting.json` fields </summary>


```json
{
  "region_name" : { 
    
    # Server and Client IPs
    "machines" : [  
      ["public_ip", "private_ip"],
      ["public_ip_2", "private_ip_2"]
    ],
  
    "shrink_mh_orderer": true,
    "num_replicas": 1,
  
    # Number of machines to use as client machines
    # It will select the last N machines as client machines
    "num_clients": 1,

    # One way Network delays from `region_name` to all other regions
    "network_delays": { 
      "region_name": 10,
      "region_name_2": 20
    }
  },
  # GenuineDB Coordinator machine IPs
  "order": {
    "public" : "public_ip",
    "private": "private_ip"
  }
}
```
</details>

NOTE: If your deployment involves network link that is over 100ms round-trip time, for better performance, run the following commands on each node to increase the network buffer:

```
sudo sysctl -w net.core.rmem_max=10485760
sudo sysctl -w net.core.wmem_max=10485760
```

## Configuration Setup
Once filled, you can select one of the prepared experiment setups found in the `experiments` directory and create the configuration file running the command:

```
python3 tools/generic_config_creator.py -d <BASE_SETTING_JSON> -s <EXPERIMENT_JSON> -u <USERNAME> --sudo <SUDO_PASSWORD> --output <OUTPUT_PATH>
```

Where:
- `<BASE_SETTINGS_JSON>`: Path to the base settings file
- `<EXPERIMENT_JSON>`: Path to the experiment scenario. The experiments used in the paper can be found in `experiments/grid5000/microbenchmarks` in each directory.
- `<USERNAME>`: Username to which to SSH to each machine. The username must be the same for all machines
- `<SUDO_USER>`: Sudo password for the given user. Sudo is required to introduce network delays
- `<OUTPUT_JSON>`: Output file path


## Run Experiment
Once the experiment json has been created, you may run the experiment with the command:

```
python3 tools/run_experiment.py ycsb-zipf -n <EXP_DIRECTORY_NAME> --extensions <EXTENSIONS> --processing-dir <PROCESSING_DIR> --settings <EXPERIMENT_JSON> --out-dir <OUTPUT_DIR> --tag <TAGS> --connection ssh
```

⚠️ **WARNING**: Before executing, make sure to modify the the flag `<HOST_SLOG_DIR>` in the file `tools/constants.py` to match the location of your built executors.

Where:
- `<EXP_DIRECTORY_NAME>`: Name of the resulting directory from executing this experiment 
- `<EXTENSIONS>`: Extensions to be added to the experiment. There are five key extensions:
  - `glog` : Extension to deploy the Coordinator module required by GenuineDB;
  - `network_delay` : Extension to emulate geo-distributed environment by applying the network delays described in the `base_settings.json`. Network emulation is done via Linux Traffic Controller, and thus requires sudo permissions on all machines. Furthermore, it requires users to specify which network device to apply the delays in the base settings.
  - `aws` : Extension to deploy GenuineDB to a EC2 AWS Cluster. Further details on this extension will be provided in [AWS Deployment](#aws-deployment).
  - `clock_sync` : Extension to record clock synchronization accuracy during experiments.

- `<PROCESSING_DIR>`: Directory containing the processed experimental statistics. Required to avoid disk overflows, as described later.
- `<EXPERIMENT_JSON>`: Path to the experiment json generated from the previous command
- `<OUTPUT_DIR>`: Path where experiment results will be written to
- `<TAGS>`: Set of tags to be added to the directory name to explicitly identify the experiment. Tags must be fields from the experiment configuration previously selected in `<EXPERIMENT_JSON>`. Common tags include `clients`, `mh`, `wl:zipf`, and others.

Processing Your Results:

Once the experiment is done, you can process its results into multiple useful statistics.
We recommend processing the results in parallel with execution, as logs from particularly large experiments (specially scalability ones) can be 10's of Gigabytes in size.

Information on Analysis is found in here:


# Grid'5000 Deployment

Grid'5000 allows users to schedule jobs and reserve machines for large scale deployments, with some clusters holding up to a hundred powerfull machines.

In this section, we describe in detail how you may replicate our microbenchmark experimental setup.

### Request Grid'5000 Account

As a Conference Peer reviewer, you may request a special Grid'5000 account by [following these instructions](https://www.grid5000.fr/w/Grid5000:Get_an_account#:~:text=regarding%20your%20collaboration.-,Access%20for%20a%20conference%20peer%20review,-Access%20to%20Grid)

### Starting a Job

The experiments of our paper were all deployed in the **Nancy** datacenter in the **gros** cluster.
The choice of datacenter and cluster is relevant, as it is the largest cluster supporting PTP deployment and has enough machines for our scalability microbenchmarks.

Different experiments require different number of machines. More specifically, we separate the experiments into two types of deployments:
- Non-Scalability Experiments - These encompasses most of our microbenchmarks and require **18 total machines**;
- Scalability Experiments - Scalability experiments (region scalability and partition scalability) require respectively **98 and 74** machines each. These experiments must be run at night or during the weekend to respect the [Grid'5000 Usage Policy](https://www.grid5000.fr/w/Grid5000:UsagePolicy). We include special detail on how to setup the night/weekend experiments automatically.

Before starting a job, you must ensure that there are enough resources for our experiments. You may do so by checking [Nancy's Status Page](https://intranet.grid5000.fr/oar/Nancy/drawgantt-svg/).

If enough machines are available, you may start a job to reserve the machines. Different deployments have different reservation needs, which we describe in detail below:

<details>
<summary>Non-Scalability Experiments Reservation</summary>

Grid'5000 Usage Policy restricts the duration of resource reservations during daytime (9am to 7pm France time) on weekdays.
Thankfully, Non-Scalability Experiments are small enough that one may schedule a job for the whole duration of the day.

To schedule a reservation, connect to Nancy's frontend and run the command:

```
oarsub -t deploy -p gros -l host=18,walltime=9:45:00 -r '<YYYY-MM-DD> 9:00:01'
```

Where `<YYYY-MM-DD>` is the date of the reservation. 

⚠️ **WARNING**: Reservations can only be made **24h before reservation start**, so do plan ahead.

</details>

<details>
<summary>Scalability Experiments Reservation</summary>

Due to their size, Scalability Experiments must be scheduled for either the nighttime on weekdays (7pm to 9am France time) or during the weekend, where machines may be reserved without restriction.

To schedule the scalability experiment, connect to Nancy's frontend and run the command:

Nighttime:
```
oarsub -t deploy -p gros -l host=98,walltime=13:45:00 -r '<YYYY-MM-DD> 19:00:01'
```

Where `<YYYY-MM-DD>` is the date of the reservation.

⚠️ **WARNING**: Reservations can only be made **24h before reservation start**, so do plan ahead.

We suggest starting with the Non-scalability experiments to ensure the successful deployment and experiment execution.
Scalability experiments should only take one night and will not require any interaction once deployed.

</details>

### Machine Initialization

Upon the start of your reservation, you may check if it's ready by connecting to Nancy's frontend and running the command:

```
oarstat -u <USER> 
```

Where `<USER>` is your Grid'5000 username. The output should come in the form of:

``` 
Job id     Name           User           Submission Date     S Queue
---------- -------------- -------------- ------------------- - ----------
5997814                   rasoares       2025-12-27 20:18:26 R default
```

The experiment is ready once its Status (`S`) is set to Ready (`R`). Once its ready, copy the `Job id` and run the command:

``` 
oarsub -C <JOB_ID>
```

Once connected, you must setup the machines according to a Kadeploy image. The image used for our experimental setup is publicly available in Grid'5000 and may be accessed by running:
``` 
kadeploy3 genuineDb
```

This will start the setup of all machines. This step may take a while to finish. Upon successful termination, the command should output a list of machines like so:
``` 
The deployment is successful on nodes
gros-86.nancy.grid5000.fr
...
```

Take note of the identifier of the last machine, which we will be using as our experiment frontend. The identifier is in the form `gros-<MACHINE_NUMBER>`.

Once kadeploy finishes execution, initialize the PTP clock synchronization deployment by running the command:
``` 
python3 grid5000/grid5000_initialize_machines.py <USER> eno1
```

Where `<USER>` is your Grid'5000 username.

Note: Clock Synchronization experiments require a different setup, which we will describe further ahead. This setup is used for all experiments other than clock synchronization.

### Configuration Setup 

Once all machines have been initialized, create the necessary experiment json files. 
To create each experimental config, you can run the command:
```
python3 grid5000/grid5000_config_creator.py -reg <NUM_REGS> -p <NUM_PARTITIONS> -s <EXPERIMENT_PATH> -u <USER> -d eno1 -o <OUTPUT_PATH> --order
```
Where:
- `<NUM_REGS>` - Number of regions to emulate. Except for scalability experiments, the number of regions is always 8.
- `<NUM_PARTITIONS>` - Number of partitions per region. Except for scalability experiments, the number of partitions is always 1.
- `<EXPERIMENT_PATH>` - Experiment JSON path. Microbenchmark experiments are located in `experiments/grid5000/microbenchmarks`, and we include the 6 experiments executed in the paper:
  - `mh.json` - To measure the _One-Shot Transaction Performance_ and _Global and Local Transaction Latency_ (Figures 5 and 6).
  - `glog_variants.json` - To evaluate GenuineDB against other Multi-Leader ordering variants (Figure 7).
  - `costs.json` - To measure the network overhead costs of Sups in GenuineDB (Figure 8). Between executions, the user must manually update the `min_slots: 24000` entry in GenuineDB's configuration file (`experiments/grid5000/microbenchmarks/glog.conf`) to modify the issuing rate.
  - `clock.json` - To measure the impact of clock synchronization accuracy on GenuineDB performance (Table 2). Clock synchronization requires special deployment configuration before deployment, which we elaborate [further ahead](#clock-synchronization-experiments).
  - `jitter.json` - To measure the impact of network instability in each system (Figure 9).
  - `region_scalability.json` - To evaluate GenuineDB's scalability with increasing number of Regions (Figure 10).
  - `partition_scalability.json` - To evaluate GenuineDB's scalability with increasing number of Partitions (Figure 10).
- `<USER>` - Username to use for SSH connection to each machine;
- `<OUTPUT_PATH>` - Target path to write the resulting configuration.


### Result Processing Setup

Experiment logs can take a significant ammount of storage, which is specially limited by default in Grid'5000. 

First, we suggest the user to request a storage quota extension for Nancy's homedir.
To request this extension, the user must:
1. Access [Grid'5000 homepage](https://www.grid5000.fr/w/Grid5000:Home).
2. Click **[User account > Manage Account](https://api.grid5000.fr/ui/account)**.
3. Once logged in, click on **Homedir quotas**.
4. On the bottom of the **Quotas** visualization, click on **Request quota extension**.
5. In the dropdowns, select the **Nancy** site and **100GB**. 

To reduce the storage load and analysis times, we process the experiments in parallel with experiment execution.

Open a new terminal (or use a terminal multiplexer like [tmux](https://github.com/tmux/tmux/wiki)) to connect to Nancy's frontend and connect to the experiment frontend obtained from the `kadeploy3` command.
``` 
ssh gros-<MACHINE_NUMBER>
```

There, navigate to the Analysis directory and run the command:
```
python3 result_processing.py -c -l 
```

The python script will run in parallel with the experiments, processing them one-by-one and depositing the results in directory X.

### Begin Experiments

Once the result processing script is online, you may begin the experimental evaluation.
Connect to the experiment frontend obtained from the `kadeploy3` command:
``` 
ssh gros-<MACHINE_NUMBER>
```

And run the experiment configuration as depicted earlier on the [Generic Cluster Deployment](#run-experiment).

### Clock Synchronization Experiments

For the Clock Synchronization experiment, a special setup is required during setup of the Grid'5000 environment.

In the [Machine Initialization Step](#machine-initialization), you must run a different command:

``` 
python3 grid5000/grid5000_initialize_machines.py <USER> eno1 --no-ptp
```

Then, we manually setup one of the two NTP's setups.

For NTP using Google's NTP server (`time.google.com`), run the command:
``` 
python3 tools/ntp/ntp_update_remote.py <USER> remote --master "time.google.com"
```

Where `<USER>` is your Grid'5000 username.

For simulating the BadClock setup, run the command:

``` 
python3 tools/ntp/ntp_update_remote.py <USER> local --master gros-<MACHINE_NUMBER>
```
Where `<USER>` is your Grid'5000 username and `gros-<MACHINE_NUMBER>` is experiment frontend node obtained from the `kadeploy3` command. 

After any of these commands, you must wait for clock synchronization to complete before starting your experiments.
To do so, run the command:
``` 
python3 tools/ntp/ntp_quality.py <USER> 8 1 1
```

The command will poll the clock synchronization accuracy for each server machine.
Wait until the `offset` parameters stabilize, which should take around 2 minutes.

Once stabilized, you may run the experiment.

## AWS Deployment

We additionally include scripts on how to setup our AWS deployment.
Many of our setup scripts rely on the [AWS CLI](https://aws.amazon.com/cli/) and require the user to install and authenticate before usage.
You may find instructions [here](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-quickstart.html).

NOTE:

Our experiments were deployed on `r5.4xlarge` machines, whose resources are larger than those permitted by the default quotas provided to a new AWS account.
The user may have to first request [Quota Extensions](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-resource-limits.html) to its resources, more specifically the Service/Quotas:

- **Amazon Elastic Compute Cloud (Amazon EC2)** - Running On-Demand Standard (A, C, D, H, I, M, R, T, Z) instances
- **Amazon Elastic Compute Cloud (Amazon EC2)** - All Standard (A, C, D, H, I, M, R, T, Z) Spot Instance Requests

(While the Spot instances are not required, they may be useful to attempt to save credits during execution).

From our experience, as we used a freshly created account, AWS EC2 Support did not grant us the requested quotas with AWS EC2 Free Tier.
You may consider upgrading your account to [Business Support+](https://us-east-1.console.aws.amazon.com/support/plans/home?region=us-east-1#/compare) for faster handling of quota extensions

### Setting Up AWS Cross-Region Communication

Our experiments are run across 8 AWS EC2 regions (`us-east-1`, `us-east-2`, `eu-west-1`, `eu-west-2`, `ap-northeast-1`,  `ap-northeast-2`, `ap-southeast-1`, `ap-southeast-2`)

To allow communication between EC2 regions using private IP's (thus significantly reducing the communication costs), the user must first undergo extensive setup.

We have automatized the process and include our used setup scripts. 
The user may also base himself on its contents if it wishes to modify and/or do the setup manually.

Virtual Public Cloud Mesh:

For communication across regions, we have deployed a point-to-point VPC Mesh, connecting each region to all other regions in a point to point fashion.

To setup the VPC, run the script:
``` 
./tools/aws/aws_setup.sh
```
Note - If the user wishes, it can select other EC2 regions by modifying the `REGIONS` variable in `tools/aws/aws_setup.sh`.

### Setting up Lunch Template

After setting up the cross-region communication, the user must create a [Launch Template](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/create-launch-template.html) in each region.

A key step in this process is the creation of an [Amazon Machine Image](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AMIs.html) (or AMI). 
You may create an AMI following [this guide](https://docs.aws.amazon.com/toolkit-for-visual-studio/latest/user-guide/tkv-create-ami-from-instance.html) and installing the required dependencies as instructed in the [How to Build](#how-to-build) section.
Once an AMI is created in a single region, you may automatize the copying and creation of the templates in each region by running the script `tools/aws/ami_setup.sh`.

You must modify the script's configuration to your own setup, most notably:

- `SOURCE_REGION` - Region where the AMI is located;
- `SOURCE_AMI_ID` - ID of the created AMI. You may find this ID in AWS console, by clicking **Images > AMIs**
- `KEY_PAIR_NAME` - Name of the user's EC2 Key Pair to connect to machines via `ssh`. For ease of usage, we recommend using the same key across all regions, but requires its manual creation and copying at each target region.
- `SUBNETS` - Map containing the VPC and Subnet id created from the `aws_setup.sh` script at each region.

### Launching EC2 fleet

Once the Lunch Templates have been setup, you may setup a EC2 Cluster configuration file to automatize the creation and destruction of the EC2 cluster.

To do so, you must first update the provided configuration file `tools/aws/geozip_ec2_cluster_config.json`. 
Notably, for each region, you must update the `LaunchTemplateId` and `Version` with those of your newly created Launch Templates.

Note - We suggest the user begins by testing its launch templates in a simple 2 region deployment to ensure the setup was successful.
A template test configuration file is available in `tools/aws/geozip_ec2_cluster_config_test.json`

Once you have confirmed all previous setup steps were successful, you may deploy your EC2 cluster by running the  command:
``` 
python3 tools/aws.py ec2 <CONFIG_JSON>
```

Where `<CONFIG_JSON>` is the path to the EC2 cluster configuration file.

Later, to destroy the cluster, you can run the command:
``` 
python3 tools/aws.py stop_ec2 --regions <REGIONS> 
```

Where `<REGIONS>` is a list of regions with deployed EC2 clusters, in the form of `us-east-1 us-east-2 eu-west-1 eu-west-2 ap-northeast-1 ap-northeast-2 ap-southeast-1 ap-southeast-2`.

### Creating Configuration file

Similarly to the Grid'5000 deployment, we include AWS adapted scripts to create the experiment's configuration files.
You may do so by running the command:

``` 
python3 tools/aws.py config ./experiments/aws/tpcc.json <OUTPUT_CONFIG_PATH> <AWS_PEM_PATH> --order-region <ORDER_REGION> --regions <REGIONS>
```

Where:
- `<OUTPUT_CONFIG_PATH>` - Target path to write the experiments .json file.
- `<AWS_PEM_PATH>` - Path to the AWS .pem ssh key to allow SSH connection;
- `<ORDER_REGION>` - Region where GenuineDB's Coordinator will be located;
- `<REGIONS>` - Regions where an EC2 Cluster were deployed and to be used by the experiments

### Running the Experiment

Finally, you may run the TPC-C Experiment by running the command:
``` 
python3 tools/run_experiment.py tpcc -n aws_tpcc --settings <OUTPUT_CONFIG_PATH>  --out-dir <RESULT_DIR> --tag clients warehouses --connection ssh --extensions glog
```

Where:
- `<OUTPUT_CONFIG_PATH>` - Path of the previously created configuration file.
- `<RESULT_DIR>` - Path to directory where the experiment logs will be written to.

Similarly to previous experiments, the logs need to be processed to extract the required statistics.



## Experiments
        
Experiment data and code to generate the figures in the paper can be found in https://github.com/umd-dslam/DetockAnalysis



## WARNINGS

In tools/constants.py, gotta change the host dir