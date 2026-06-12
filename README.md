# RageDB

[![Documentation](https://img.shields.io/badge/documentation-black)](https://ragedb.com)
[![Slack](https://img.shields.io/badge/slack-purple)](https://ragedb.slack.com)
[![Twitter](https://img.shields.io/twitter/follow/rage_database.svg?style=social&label=Follow)](https://twitter.com/intent/follow?screen_name=rage_database)
![C++](https://github.com/ragedb/ragedb/workflows/Coverage/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/ragedb/ragedb/badge.svg?branch=main)](https://coveralls.io/github/ragedb/ragedb?branch=main)
[![Technical Debt](https://sonarcloud.io/api/project_badges/measure?project=ragedb_ragedb&metric=sqale_index)](https://sonarcloud.io/summary/new_code?id=ragedb_ragedb)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=ragedb_ragedb&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=ragedb_ragedb)
[![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=ragedb_ragedb&metric=vulnerabilities)](https://sonarcloud.io/summary/new_code?id=ragedb_ragedb)
[![Bugs](https://sonarcloud.io/api/project_badges/measure?project=ragedb_ragedb&metric=bugs)](https://sonarcloud.io/summary/new_code?id=ragedb_ragedb)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=ragedb_ragedb&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=ragedb_ragedb)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=ragedb_ragedb&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=ragedb_ragedb)
[![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=ragedb_ragedb&metric=code_smells)](https://sonarcloud.io/summary/new_code?id=ragedb_ragedb)

[<p align="center"><img src="https://raw.githubusercontent.com/ragedb/ragedb.github.io/main/images/mascot-logo.webp" alt="ragedb mascot" width="660"/></p>](https://ragedb.com)
<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=53459147-2981-4e5a-97bd-d40d0faa0954" />

In Memory Property Graph Server using the Shared Nothing design from [Seastar](https://seastar.io).

[RageDB](https://ragedb.com):

- Faster than a speeding bullet train
- Connect from anywhere via HTTP
- Use the Lua programming language to query
- Apache License, version 2.0

Bring up the main website and documentation [ragedb.com](https://ragedb.com) while you spin up an instance on docker right now:


## Docker

    docker pull dockerhub.ragedb.com/ragedb/ragedb
    docker run -u 0 -p 127.0.0.1:7243:7243 --name ragedb -t dockerhub.ragedb.com/ragedb/ragedb:latest --cap-add=sys_nice

If you are running Docker on a Mac or Windows Host, you may see this error message:

    WARNING: unable to mbind shard memory; performance may suffer:

Run Docker on a Linux host for the best performance.

## Terraform

- This is work in progress, if you can make this better please help!
- Generate an SSH key if you don't already have one with `ssh-keygen -t rsa -b 4096`.
- [Install Terraform](https://learn.hashicorp.com/tutorials/terraform/install-cli)
- [Install the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html).
- [Configure the AWS CLI with an access key ID and secret access key](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html).

### Variables

#### your_region
- Which AWS Region. The options are [here](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html#concepts-available-regions).
- E.g. `eu-west-2`.

#### your_public_key
- This will be in `~/.ssh/id_rsa.pub` by default.

### Steps
- Run `terraform init`.
- Run `terraform apply`.
- Wait a few minutes for the code to compile and the server to spin up.
- Copy the IP output by the previous command into your browser http://x.x.x.x:/7243
- Do Graphy Stuff.
- Irrecoverably shut everything down with `terraform destroy`.

This will bring up an [r5.2xlarge](https://aws.amazon.com/ec2/instance-types/r5/) with 4 cores set to 1 thread per core with 100 GB of space.


## HTTP API

RageDB exposes a rich HTTP API for schema management, node and relationship CRUD operations, path traversing, degree and neighbor queries, and Lua scripting. For the complete HTTP API endpoint reference and usage examples, see [API.md](API.md).


## Building

RageDB uses Seastar which only runs on *nix servers (no windows or mac) so use your local linux desktop or use EC2. A compiler supporting C++23 (such as GCC 12 or newer) is required.

On EC2 launch an instance:

    Step 1: Choose an Amazon Machine Image
    Ubuntu Server 24.04 LTS (HVM), SSD Volume Type (as it comes with GCC 13 by default, providing full C++23 support)

    Step 2: Choose Instance Type
    r5.2xlarge

    Step 3: Configure Instance
    Specify CPU options
    Threads per core = 1

    Step 4: Add Storage
    100 GiB

    Launch

Once the instance is running, connect to it and start a "screen" session, then follow these steps:

First let's update and upgrade to the latest versions of local software:

    sudo apt-get update && sudo apt-get dist-upgrade

Install Seastar (this will take a while, that's why we are using screen):

    git clone -b seastar-25.05.0 https://github.com/scylladb/seastar.git
    cd seastar
    sudo ./install-dependencies.sh
    # Note: On older Ubuntu 22.04, install gcc-12/g++-12 and configure with:
    # ./configure.py --mode=release --prefix=/usr/local --compiler=g++-12 --c-compiler=gcc-12 --without-tests --without-apps --without-demos
    ./configure.py --mode=release --prefix=/usr/local
    sudo ninja -C build/release install

Install Additional Dependencies

    sudo apt-get install -y ccache python3-pip

Install conan

    pip install --user conan
    sudo ln -s ~/.local/bin/conan /usr/bin/conan



Install LuaJIT

    sudo apt-get install -y luajit luajit-5.1-dev

Install RageDB

    git clone https://github.com/ragedb/ragedb.git
    cd ragedb
    mkdir build
    cd build
    # On Ubuntu 22.04 with GCC 12, run:
    # CC=gcc-12 CXX=g++-12 cmake .. -DCMAKE_BUILD_TYPE=Release
    cmake .. -DCMAKE_BUILD_TYPE=Release
    cmake --build . --target ragedb
    cd bin
    sudo ./ragedb

### Troubleshooting

If you get errors regarding conan locks, run:

    conan remove --locks

If you get errors like:
    
    Creation of perf_event based stall detector failed, falling back to posix timer: std::system_error

Then this should fix it: 

    echo 0 > /proc/sys/kernel/perf_event_paranoid

To make it permanent edit /etc/sysctl.conf by adding:

    kernel.perf_event_paranoid = 0

If you get errors about aio-max-nr you'll want to increase it:

    sudo echo 88208 > /proc/sys/fs/aio-max-nr

We allocate 11026 aio slots per shard (10000 + 1024 + 2) so 8 shards = 88208

To make it permanent edit /etc/sysctl.conf by adding:

    fs.aio-max-nr = 88208

### Todos

    - Command Logging (started)
    - Recovering (started)
    - Snapshots
    - Replication
    - Metrics
    - Everthing Else
    - Take over the world

### Reactor Stalls

    - When running KHop queries on combining the RoaringBitmaps using |=
    - When importing data on the power of two growth of the tsl sparse_map
    - In GetToKeysFromRelationshipsInCSV
    - In PartitionRelationshipsInCSV 


### Missing Features that can be added "easily"
    - Allow Node and Relationship Type handlers to take a json map defining the property keys and data types
    - Allow additional data types: 8, 16 and 32 bit integers, 32 bit floats, byte, list of bytes, nested types 
    - NodeTypes and RelationshipTypes should allow type deletion and type id reuse
    - Allow property type conversions (int to double, string to int, int to int array, etc).

### PVS Commands

    pvs-studio-analyzer trace -- make
    pvs-studio-analyzer analyze
    plog-converter -a GA:1,2 -t tasklist  PVS-Studio.log
