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

⚠️ **WARNING**: Before executing, make sure to modify the flag `<HOST_SLOG_DIR>` in the file `tools/constants.py` to match the location of your built executors.

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

## Processing Your Results:

Once the experiment is done, you can process its results into multiple useful statistics.

You can find the steps to process the results in [here](result_analysis.md).

