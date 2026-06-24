# Property Indexing in RageDB

RageDB supports partitioned, shared-nothing property indexing on both node and relationship properties to accelerate point lookups and range queries.

---

## What Can and Cannot Be Indexed

### Supported Property Types
You can create property indexes on **scalar** property types:
- `boolean`
- `integer` (64-bit signed integer)
- `double` (64-bit double precision float)
- `string` (UTF-8 encoded string)

### How Each Type is Indexed
- **`boolean`**: Indexed using two compressed **`roaring::Roaring64Map`** bitmaps (one for `true`, one for `false`). This provides $O(1)$ add/remove complexity, ultra-low memory overhead, and allows hardware-accelerated set operations (e.g. fast bitwise AND intersection).
- **`integer` & `double`**: Indexed using **`std::multimap`** which maintains sorted order of numerical values to support logarithmic point searches and fast range boundary iterations.
- **`string`**: Indexed using **`unodb::art_db`** (Adaptive Radix Tree) mapping binary encoded strings to heap-allocated duplicate vectors, optimizing prefix matches and lexicographical lookups.

### Unsupported Property Types
Currently, the following types **cannot** be indexed:
- List/Vector properties (`boolean_list`, `integer_list`, `double_list`, `string_list`).
- Vector embeddings (similarity search / vector search).

---

## Internals and Query Routing

Property indexes are partitioned across Seastar cores (shards). Each property index entry is routed to a specific shard based on the hash of the property's value. 

Because of this value-based partitioning, query execution behaves differently for exact lookups versus range lookups.

### 1. Exact Queries (`Operation::EQ`)
When performing an equality query (`EQ`), the engine uses the property value to calculate the exact shard where the matching entry must reside:
$$\text{Target Shard} = \text{CalculateShardId}(\text{type}, \text{property}, \text{value})$$

*   **Routing**: The query is routed directly to that single Seastar shard (bypassing all other shards).
*   **Performance**: Highly optimized point lookup with $O(1)$ routing and fast search times inside the shard's radix tree (for strings) or multimap (for numbers). Bypasses linear scans and avoids scatter-gather network/inter-core coordination overhead.

### 2. Range Queries (`GT`, `GTE`, `LT`, `LTE`, `NEQ`)
Since property indexes are partitioned by hashing the property values, entries are distributed across all shards, and sorted ordering is only preserved locally on each shard.
*   **Routing**: Range queries are executed as **scatter-gather** operations. The engine invokes the query on **all Seastar cores in parallel**.
*   **Performance**: Each shard performs a range scan on its local index (`std::multimap` for scalar numbers), and the peered coordinator aggregates and slices the combined results (using `skip` and `limit`).

---

## Index Management APIs

You can create or delete indexes dynamically using either the HTTP REST API or Lua scripts.

### 1. HTTP REST API

#### Create a Node Property Index
```http
POST /db/{graph}/schema/nodes/{type}/properties/{property}/index
```
*   **Success Status**: `201 Created`
*   **Failure Status**: `304 Not Modified` (if index already exists or type is invalid)

#### Delete a Node Property Index
```http
DELETE /db/{graph}/schema/nodes/{type}/properties/{property}/index
```
*   **Success Status**: `204 No Content`
*   **Failure Status**: `304 Not Modified`

#### Create a Relationship Property Index
```http
POST /db/{graph}/schema/relationships/{type}/properties/{property}/index
```

#### Delete a Relationship Property Index
```http
DELETE /db/{graph}/schema/relationships/{type}/properties/{property}/index
```

#### List Node Property Indexes
```http
GET /db/{graph}/schema/nodes/indexes
```
*   **Response**: JSON mapping node labels to vectors of indexed properties: `{"Person": ["name"]}`

#### List Node Property Indexes for a Specific Type
```http
GET /db/{graph}/schema/nodes/{type}/indexes
```
*   **Response**: JSON array of indexed properties for the target type: `["name"]`

#### List Relationship Property Indexes
```http
GET /db/{graph}/schema/relationships/indexes
```
*   **Response**: JSON mapping relationship types to vectors of indexed properties: `{"ACTED_IN": ["since"]}`

#### List Relationship Property Indexes for a Specific Type
```http
GET /db/{graph}/schema/relationships/{type}/indexes
```
*   **Response**: JSON array of indexed properties for the target type: `["since"]`

---

### 2. Lua API

From Lua scripts or console, use the following built-in functions:

```lua
-- Create node property index
NodeIndexCreate("Person", "name")

-- Delete node property index
NodeIndexDelete("Person", "name")

-- Create relationship property index
RelationshipIndexCreate("Knows", "since")

-- Delete relationship property index
RelationshipIndexDelete("Knows", "since")

-- Show all node indexes (returns table mapping NodeType -> List of properties)
NodeIndexesGet()

-- Show node indexes for a specific node type (returns table list of properties)
NodeIndexesGet("Person")

-- Show all relationship indexes (returns table mapping RelationshipType -> List of properties)
RelationshipIndexesGet()

-- Show relationship indexes for a specific relationship type (returns table list of properties)
RelationshipIndexesGet("Knows")
```

---

### 3. GQL DDL Commands

You can manage and view indexes directly using GQL commands:

#### Create Index
```cypher
-- Create a node property index
CREATE INDEX Person.name;

-- Create a relationship property index
CREATE INDEX WORKS_AT.since;
```

#### Drop Index
```cypher
-- Drop a node property index
DROP INDEX Person.name;

-- Drop a relationship property index
DROP INDEX WORKS_AT.since;
```

#### Show Indexes
```cypher
-- Show all active indexes
SHOW INDEXES;

-- Show indexes filtered by label
SHOW INDEXES ON Person;
```

When running `SHOW INDEXES`, the engine returns a JSON array of active indexes:
```json
[
  {"type": "Node", "label": "Person", "property": "name"},
  {"type": "Relationship", "label": "WORKS_AT", "property": "since"}
]
```

---

## Full-Text Search Indexing (FTS)

RageDB supports native, high-performance full-text search indexing on both node and relationship properties.

### Storage & Execution Architecture

FTS indexing is backed by standard, zero-dependency **Apache 2.0 `iresearch`** (statically linked).
*   **Shared-Nothing Multi-Segment Partitioning**: Each Seastar core runs its own isolated, single-threaded instance of `iresearch::memory_directory`.
*   **Zero Cross-Core Contention**: Text analysis, tokenization (using the standard segmentation tokenizer), and indexing happen concurrently and locally within each CPU core.
*   **Top-K Scatter-Gather Search**: Queries are broadcasted to all shards, matching documents are scored using BM25, and local top-K results are returned. The peered coordinator performs a fast heap-merge to return the globally sorted top-100 results.

### Full-Text Search Management (GQL DDL)

*   **Create Full-Text Index**:
    ```cypher
    CREATE FULLTEXT INDEX Person.bio;
    CREATE FULLTEXT INDEX WORKS_AT.description;
    ```
*   **Show Indexes**:
    ```cypher
    SHOW INDEXES;
    ```
    Full-text indexes are indicated by `"kind": "fulltext"` in the returned JSON structure.
*   **Drop Full-Text Index**:
    ```cypher
    DROP INDEX Person.bio;
    ```

---

## Querying via Full-Text Search

Full-text queries are executed inside `MATCH` blocks using the `SEARCH` operator. FTS searches return the matching entity and a relevance score.

### 1. Exact Term Search
```cypher
MATCH p IN SEARCH Person.bio FOR 'database'
YIELD p, score
RETURN p.name, score;
```

### 2. Fuzzy Search (Levenshtein Distance)
Fuzzy matching supports Levenshtein edit distance (up to distance of `2`) by using the `~` suffix or specifying the `fuzzy: 'true'` option.
```cypher
MATCH p IN SEARCH Person.bio FOR 'databas~' OPTIONS { fuzzy: 'true' }
YIELD p, score
RETURN p.name, score;
```
