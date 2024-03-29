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

### Schema

#### Restore Graph

    :POST /db/{graph}/restore

#### Delete Graph

    :DELETE /db/{graph}/schema

#### Get Node Types

    :GET /db/{graph}/schema/nodes

#### Get a Node Type

    :GET /db/{graph}/schema/nodes/{type}

#### Create a Node Type

    :POST /db/{graph}/schema/nodes/{type}

#### Delete a Node Type

    :DELETE /db/{graph}/schema/nodes/{type}

#### Get Relationship Types

    :GET /db/{graph}/schema/relationships

#### Get a Relationship Type

    :GET /db/{graph}/schema/relationships/{type}

#### Create a Relationship Type

    :POST /db/{graph}/schema/relationships/{type}

#### Delete a Relationship Type

    :DELETE /db/{graph}/schema/relationships/{type}

RageDB currently supports booleans, 64-bit integers, 64-bit doubles, strings and lists of the preceding data types:

    boolean, integer, double, string, boolean_list, integer_list, double_list, string_list

#### Get a Node Property Type

    :GET /db/{graph}/schema/nodes/{type}/properties/{property}

#### Create a Node Property Type

    :POST /db/{graph}/schema/nodes/{type}/properties/{property}/{data_type}

#### Delete a Node Property Type

    :DELETE /db/{graph}/schema/nodes/{type}/properties/{property}

#### Get a Relationship Property Type

    :GET /db/{graph}/schema/relationships/{type}/properties/{property}

#### Create a Relationship Property Type

    :POST /db/{graph}/schema/relationships/{type}/properties/{property}/{data_type}

#### Delete a Relationship Property Type

    :DELETE /db/{graph}/schema/relationships/{type}/properties/{property}

### Nodes

#### Get All Nodes

    :GET /db/{graph}/nodes?limit=100&skip=0

#### Get All Nodes of a Type

    :GET /db/{graph}/nodes/{type}?limit=100&skip=0

#### Get A Node By Type and Key

    :GET /db/{graph}/node/{type}/{key}

#### Get A Node By Id

    :GET /db/{graph}/node/{id}

#### Create A Node

    :POST /db/{graph}/node/{type}/{key}
    JSON formatted Body: {properties}

#### Delete A Node By Type and Key

    :DELETE /db/{graph}/node/{type}/{key}

#### Delete A Node By Id

    :DELETE /db/{graph}/node/{id}

#### Find Nodes

    :POST /db/{graph}/nodes/{type}/{property}/{operation}?limit=100&skip=0 {json value}

### Node Properties

#### Get the Properties of a Node By Type and Key

    :GET /db/{graph}/node/{type}/{key}/properties

#### Get the Properties of a Node By Id

    :GET /db/{graph}/node/{id}/properties

#### Reset the Properties of a Node By Type and Key

    :POST /db/{graph}/node/{type}/{key}/properties
    JSON formatted Body: {properties}

#### Reset the Properties of a Node By Id

    :POST /db/{graph}/node/{id}/properties
    JSON formatted Body: {properties}

#### Set some Properties of a Node By Type and Key

    :PUT /db/{graph}/node/{type}/{key}/properties
    JSON formatted Body: {properties}

#### Set some Properties of a Node By Id

    :PUT /db/{graph}/node/{id}/properties
    JSON formatted Body: {properties}

#### Delete the Properties of a Node By Type and Key

    :DELETE /db/{graph}/node/{type}/{key}/properties

#### Delete the Properties of a Node By Id

    :DELETE /db/{graph}/node/{id}/properties

#### Get a Property of a Node By Type and Key

    :GET /db/{graph}/node/{type}/{key}/property/{property}

#### Get a Property of a Node By Id

    :GET /db/{graph}/node/{id}/property/{property}

#### Create a Property of a Node By Type and Key

    :PUT /db/{graph}/node/{type}/{key}/property/{property}
    JSON formatted Body: {property}

#### Create a Property of a Node By Id

    :PUT /db/{graph}/node/{id}/property/{property}
    JSON formatted Body: {property}

#### Delete a Property of a Node By Type and Key

    :DELETE /db/{graph}/node/{type}/{key}/property/{property}

#### Delete a Property of a Node By Id

    :DELETE /db/{graph}/node/{id}/property/{property}

### Relationships

#### Get All Relationships

    :GET /db/{graph}/relationships?limit=100&skip=0

#### Get All Relationships of a Type

    :GET /db/{graph}/relationships/{type}?limit=100&skip=0

#### Get A Relationship

    :GET /db/{graph}/relationship/{id}

#### Create A Relationship By Node Types

    :POST /db/{graph}/node/{type_1}/{key_1}/relationship/{type_2}/{key_2}/{rel_type}
    JSON formatted Body: {properties}

#### Create A Relationship By Node Ids

    :POST /db/{graph}/node/{id_1}/relationship/{id_2}/{rel_type}
    JSON formatted Body: {properties}

#### Delete A Relationship

    :DELETE /db/{graph}/relationship/{id}

#### Find Relationships

    :POST /db/{graph}/relationships/{type}/{property}/{operation}?limit=100&skip=0 {json value}

#### Get the Relationships of a Node By Node Type

    :GET /db/{graph}/node/{type}/{key}/relationships
    :GET /db/{graph}/node/{type}/{key}/relationships/{direction [all, in, out]} 
    :GET /db/{graph}/node/{type}/{key}/relationships/{direction [all, in, out]}/{type TYPE_ONE}
    :GET /db/{graph}/node/{type}/{key}/relationships/{direction [all, in, out]}/{type(s) TYPE_ONE&TYPE_TWO}

#### Get the Relationships of a Node By Node Id

    :GET /db/{graph}/node/{id}/relationships
    :GET /db/{graph}/node/{id}/relationships/{direction [all, in, out]} 
    :GET /db/{graph}/node/{id}/relationships/{direction [all, in, out]}/{type TYPE_ONE}
    :GET /db/{graph}/node/{id}/relationships/{direction [all, in, out]}/{type(s) TYPE_ONE&TYPE_TWO}

### Relationship Properties

#### Get the Properties of a Relationship

    :GET /db/{graph}/relationship/{id}/properties

#### Reset the Properties of a Relationship

    :POST /db/{graph}/relationship/{id}/properties
    JSON formatted Body: {properties}

#### Set some Properties of a Relationship

    :PUT /db/{graph}/relationship/{id}/properties
    JSON formatted Body: {properties}

#### Delete the Properties of a Relationship

    :DELETE /db/{graph}/relationship/{id}/properties

#### Get a Property of a Relationship

    :GET /db/{graph}/relationship/{id}/property/{property}

#### Create a Property of a Relationship

    :PUT /db/{graph}/relationship/{id}/property/{property}
    JSON formatted Body: {property}

#### Delete a Property of a Relationship

    :DELETE /db/{graph}/relationship/{id}/property/{property}

### Node Degrees

#### Get the Degree of a Node By Node Type

    :GET /db/{graph}/node/{type}/{key}/degree
    :GET /db/{graph}/node/{type}/{key}/degree/{direction [all, in, out]} 
    :GET /db/{graph}/node/{type}/{key}/degree/{direction [all, in, out]}/{type TYPE_ONE}
    :GET /db/{graph}/node/{type}/{key}/degree/{direction [all, in, out]}/{type(s) TYPE_ONE&TYPE_TWO}

#### Get the Degree of a Node By Node Id

    :GET /db/{graph}/node/{id}/degree
    :GET /db/{graph}/node/{id}/degree/{direction [all, in, out]} 
    :GET /db/{graph}/node/{id}/degree/{direction [all, in, out]}/{type TYPE_ONE}
    :GET /db/{graph}/node/{id}/degree/{direction [all, in, out]}/{type(s) TYPE_ONE&TYPE_TWO}

### Node Neighbors

#### Get the Neighbors of a Node By Node Type

    :GET /db/{graph}/node/{type}/{key}/neighbors
    :GET /db/{graph}/node/{type}/{key}/neighbors/{direction [all, in, out]} 
    :GET /db/{graph}/node/{type}/{key}/neighbors/{direction [all, in, out]}/{type TYPE_ONE}
    :GET /db/{graph}/node/{type}/{key}/neighbors/{direction [all, in, out]}/{type(s) TYPE_ONE&TYPE_TWO}

#### Get the Neighbors of a Node By Node Id

    :GET /db/{graph}/node/{id}/neighbors
    :GET /db/{graph}/node/{id}/neighbors/{direction [all, in, out]} 
    :GET /db/{graph}/node/{id}/neighbors/{direction [all, in, out]}/{type TYPE_ONE}
    :GET /db/{graph}/node/{id}/neighbors/{direction [all, in, out]}/{type(s) TYPE_ONE&TYPE_TWO}

### Lua

    :POST db/{graph}/lua
    STRING formatted Body: {script}

The script must end in one or more values that will be returned in JSON format inside an Array.
Within the script the user can access to graph functions. For example:

    -- Get some things about a node
    a = NodeGetId("Node","Max")
    b = NodeTypesGetCount()
    c = NodeTypesGetCount("Node")
    d = NodeGetProperty("Node", "Max", "name")
    e = NodeGetProperty(a, "name")
    a, b, c, d, e

A second example:

    -- get the names of nodes I have relationships with
    names = {}
    ids = NodeGetLinks("Node", "Max")
    for k=1,#ids do
        v = ids[k]
        table.insert(names, NodeGetProperty(v:getNodeId(), "name"))
    end
    names



## Building

RageDB uses Seastar which only runs on *nix servers (no windows or mac) so use your local linux desktop or use EC2.

On EC2 launch an instance:

    Step 1: Choose an Amazon Machine Image
    Ubuntu Server 20.04 LTS(HVM), SSD Volume Type - ami-09e67e426f25ce0d7

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

    git clone https://github.com/scylladb/seastar.git
    cd seastar
    sudo ./install_dependencies.sh
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
