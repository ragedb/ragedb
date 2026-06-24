# GQL Query Execution Plans

This document provides a guide to understanding the GQL execution plans and the Query Plan Cache in RageDB.

---

## Commands

RageDB GQL supports the following commands to inspect execution plans and manage the cache:

1.  **`EXPLAIN`**:
    Generates and returns the query plan tree without executing the query itself. Useful for inspecting the execution structure and verifying indexing or join decisions.
2.  **`PROFILE`**:
    Generates the query plan, executes the query, tracks operational metrics (processing times and actual row counts for each phase), and overlays them on the query plan.
3.  **`CALL CLEAR CACHE`**:
    Resets the Query Plan Cache across all sharded cores. This is useful for manual cache clearing, though RageDB will automatically invalidate the cache whenever a schema operation (e.g. `CREATE NODE TYPE`) is executed.

---

## Output Fields

The execution plan is returned as a JSON array of row objects conforming to standard GQL query results.

| Field | Returned By | Description |
| :--- | :--- | :--- |
| **`Operator`** | `EXPLAIN` & `PROFILE` | The execution operation name. Indented with ASCII connectors (`├─`, `└─`) to represent hierarchy. |
| **`Details`** | `EXPLAIN` & `PROFILE` | Extra context (like matched node/edge types, predicates, aliases, or join conditions). |
| **`Outputs`** | `EXPLAIN` & `PROFILE` | List of variables that are bound and output at this stage of the query. |
| **`Cache`** | `EXPLAIN` & `PROFILE` | Displays `"HIT"` if the execution plan came from the cache, or `"MISS"` if it was parsed and optimized. |
| **`Actual Rows`** | `PROFILE` only | The actual number of records/bindings produced by this operator during query execution. |
| **`Time (ms)`** | `PROFILE` only | The exact time in milliseconds spent executing this operator. |

---

## Example 1: `EXPLAIN` Query Plan

### Query
```gql
EXPLAIN MATCH (p:Person)-[:FRIEND]->(friend) RETURN p.name, friend.name
```

### JSON Plan Output
```json
[
  {
    "Operator": "ProduceResults",
    "Details": "p.name, friend.name",
    "Outputs": "p.name, friend.name",
    "Cache": "HIT"
  },
  {
    "Operator": "└─ Expand / Traverse",
    "Details": "(p:Person)-[FRIEND]->(friend)",
    "Outputs": "p, friend",
    "Cache": "HIT"
  },
  {
    "Operator": "   └─ Scan / Traverse",
    "Details": "(p:Person)",
    "Outputs": "p",
    "Cache": "HIT"
  }
]
```

### Explanation of the Steps
1.  **`Scan / Traverse`**: Scans the database for all nodes matching the label/type `Person`. Binds the matching nodes to variable `p`.
2.  **`Expand / Traverse`**: Starting from the bound variable `p`, traverses outgoing relationship edges of type `FRIEND` and binds the target neighbor nodes to variable `friend`.
3.  **`ProduceResults`**: Projects the properties `p.name` and `friend.name` into the final returned format.

---

## Example 2: `PROFILE` Query Plan

### Query
```gql
PROFILE MATCH (p:Person)-[:FRIEND]->(friend) RETURN p.name, friend.name
```

### JSON Plan Output
```json
[
  {
    "Operator": "ProduceResults",
    "Details": "p.name, friend.name",
    "Outputs": "p.name, friend.name",
    "Actual Rows": 1,
    "Time (ms)": 0.052,
    "Cache": "MISS"
  },
  {
    "Operator": "└─ Expand / Traverse",
    "Details": "(p:Person)-[FRIEND]->(friend)",
    "Outputs": "p, friend",
    "Actual Rows": 1,
    "Time (ms)": 0.243,
    "Cache": "MISS"
  },
  {
    "Operator": "   └─ Scan / Traverse",
    "Details": "(p:Person)",
    "Outputs": "p",
    "Actual Rows": 2,
    "Time (ms)": 0.118,
    "Cache": "MISS"
  }
]
```

### Explanation of the Metrics
*   **`Scan / Traverse`**: Scanned 2 `Person` nodes in `0.118 ms`.
*   **`Expand / Traverse`**: From those 2 starting nodes, traversed outgoing `FRIEND` relationships, outputting 1 matched pattern binding in `0.243 ms`.
*   **`ProduceResults`**: Received 1 record and outputted it in `0.052 ms`.

---

## Cache Administration: `CALL CLEAR CACHE`

### Query
```gql
CALL CLEAR CACHE
```

### JSON Output
```json
{
  "status": "cache cleared"
}
```

### Explanation
When executed, this command clears all parsed and optimized GQL execution plans stored in the thread-local query caches across all Seastar shards (CPU cores). Future queries will result in a cache `"MISS"` on their first subsequent run until they are parsed, optimized, and cached again.
