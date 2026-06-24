# HTTP API

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

### GQL & Full-Text Search (FTS)

RageDB supports GQL (Graph Query Language) queries, including high-performance full-text search capabilities powered by a native, partitioned `iresearch` integration. For the complete reference of GQL features, grammar compliance, and catalog schema controls, see [GQL.md](GQL.md).

#### Running a GQL Query

To execute a GQL query, send a `POST` request to the `/db/{graph}/gql` endpoint with the GQL query string as the body:

    :POST /db/{graph}/gql
    Body: MATCH p IN SEARCH Product.description FOR 'databases' YIELD p, score RETURN p.name, score

#### Full-Text Search Indexing DDL

You can create full-text indexes on node or relationship property fields to search matching terms:

*   **Create Full-Text Index**:
    ```cypher
    CREATE FULLTEXT INDEX Product.description
    CREATE FULLTEXT INDEX WORKS_AT.since
    ```
*   **Show Indexes** (returns JSON array of active indexes with `"kind": "fulltext"` for FTS indexes):
    ```cypher
    SHOW INDEXES
    ```
*   **Drop Full-Text Index**:
    ```cypher
    DROP INDEX Product.description
    ```

#### Full-Text Search Queries

Full-text search queries match property terms and yield the matched entity alongside a BM25 relevance `score`.

*   **Exact match**:
    ```cypher
    MATCH p IN SEARCH Product.description FOR 'databases' YIELD p, score RETURN p.name, score
    ```
*   **Fuzzy match** (supports Levenshtein distance matching up to 2 edits using `~` or fuzzy options):
    ```cypher
    MATCH p IN SEARCH Product.description FOR 'databas~' OPTIONS { fuzzy: 'true' } YIELD p, score RETURN p.name, score
    ```
