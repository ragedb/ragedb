# RageDB

In Memory Property Graph Server using the Shared Nothing design from [Seastar](https://seastar.io).

The [RageDB](https://ragedb.com) server can host multiple Graphs. The graphs are accessible via a REST API (see below).
Each Graph is split into multiple Shards. One Shard per Core of the server.
Shards communicate by explicit message passing. Nodes and Relationships have internal and external ids.
The external ids embed the type as well as which Shard they belong to.
The internal ids are pointers into vectors that hold the data of each Node and Relationship.
The Relationship ids are replicated to both incoming and outgoing Nodes.
The Relationship Object (and properties) belong to the Outgoing Node (and shard).
Each Node must have a singular Type and unique Key on creation which the server stores in a map for retrieval.
External and Internal Ids are assigned upon creation for both Nodes and Relationships.

Along side an HTTP API, RageDB also has a Lua http endpoint that allows users to send complex queries.
These queries are interpreted by LuaJIT, compiled and executed within a Seastar Thread that allows blocking.
By not having a "query language" we avoid parsing, query planning, query execution and a host of [problems](https://maxdemarzi.com/2020/05/25/declarative-query-languages-are-the-iraq-war-of-computer-science/).

## Docker

    docker run -u 0 -p 127.0.0.1:7243:7243 --name ragedb -t ragedb/ragedb:latest --cap-add=sys_nice

If you are running Docker on a Mac or Windows Host, you may see this error message:

    WARNING: unable to mbind shard memory; performance may suffer:

Run Docker on a Linux host for the best performance.


## HTTP API

### Schema

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

    :GET /db/{graph}/nodes?limit=100&offset=0

#### Get All Nodes of a Type

    :GET /db/{graph}/nodes/{type}?limit=100&offset=0

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

    :GET /db/{graph}/relationships?limit=100&offset=0

#### Get All Relationships of a Type

    :GET /db/{graph}/relationships/{type}?limit=100&offset=0

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
    c = NodeTypesGetCountByType("Node")
    d = NodePropertyGet("Node", "Max", "name")
    e = NodePropertyGetById(a, "name")
    a, b, c, d, e

A second example:

    -- get the names of nodes I have relationships with
    names = {}
    ids = NodeGetRelationshipsIds("Node", "Max")
    for k=1,#ids do
        v = ids[k]
        table.insert(names, NodePropertyGetById(v.node_id, "name"))
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

### Troubleshooting

If you get errors regarding conan locks, run:

    conan remove --locks

### Missing Features that can be added "easily"
    - Allow Node and Relationship Type handlers to take a json map defining the property keys and data types
    - Allow additional data types: 8, 16 and 32 bit integers, 32 bit floats, byte, list of bytes, nested types 
    - NodeTypes and RelationshipTypes should allow type deletion and type id reuse
    - Allow property type conversions (int to double, string to int, int to int array, etc).

### PVS Commands

    pvs-studio-analyzer trace -- make
    pvs-studio-analyzer analyze
    plog-converter -a GA:1,2 -t tasklist  PVS-Studio.log
