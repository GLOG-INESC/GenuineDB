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
1. [A generic deployment scenario](docs/generic_cluster.md), where a user provides a set of IP addresses to which to deploy GenuineDB on;
2. [Grid'5000 Deployment](docs/grid5000.md)
3. [AWS Deployment](docs/aws.md)

