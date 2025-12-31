# AWS Deployment

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
You may create an AMI following [this guide](https://docs.aws.amazon.com/toolkit-for-visual-studio/latest/user-guide/tkv-create-ami-from-instance.html) and installing the required dependencies as instructed in the [How to Build](../README.md) section.
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

You can find said instructions [here](result_analysis.md).