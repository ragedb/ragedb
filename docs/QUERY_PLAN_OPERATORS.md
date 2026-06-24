# GQL Query Plan Operators

This document describes the query plan operators supported and executed by the RageDB GQL engine. When running `EXPLAIN` or `PROFILE` on a GQL query, these operators represent the different nodes in the query execution tree.

---

## Operator Categories

| Operator | Type | Description |
| :--- | :--- | :--- |
| [Scan / Traverse](#scan--traverse) | Match | Scan starting nodes of a pattern (e.g. by label or property filters) when no prior bindings exist. |
| [Expand / Traverse](#expand--traverse) | Match | Traverses relationships and adjacent nodes from an already bound variable. |
| [OptionalExpand](#optionalexpand) | Match | Traverses relationships/adjacent nodes in `OPTIONAL MATCH` clauses (variables may evaluate to null). |
| [NaturalJoin](#naturaljoin) | Join | Performed to join independent execution branches or parallel match statements based on shared variables. |
| [LeftOuterJoin](#leftouterjoin) | Join | Left-outer joins matching results with the main result stream (used for `OPTIONAL MATCH`). |
| [Filter](#filter) | Logic | Filters intermediate rows according to conditions specified in `WHERE` clauses. |
| [Write](#write) | Mutation | Performs write actions, such as node/relationship inserts, updates, or deletions. |
| [Aggregate](#aggregate) | Aggregation | Groups records and computes aggregate functions (e.g. `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`). |
| [Distinct](#distinct) | Logic | Filters out duplicate records from the query stream. |
| [Sort](#sort) | Logic | Sorts intermediate records according to sorting keys and directions. |
| [Limit](#limit) | Logic | Restricts the total number of records passing through. |
| [ProduceResults](#produceresults) | Result | Formats the final projection and returns the columns defined in the `RETURN` clause. |
| [Union](#union) | Set | Set union of two subqueries, removing duplicates. |
| [UnionAll](#unionall) | Set | Set union of two subqueries, preserving duplicates. |
| [Intersect](#intersect) | Set | Set intersection of two subqueries, removing duplicates. |
| [IntersectAll](#intersectall) | Set | Set intersection of two subqueries, preserving duplicates. |
| [SchemaOperation](#schemaoperation) | Utility | Executes schema definitions (e.g. creating or dropping node/relationship types). |

---

## Detailed Explanations

### Scan / Traverse
*   **Purpose**: The starting point of pattern matching.
*   **Behavior**: Scans nodes of a specific type/label or using node-specific index details.
*   **Plan Representation**: Typically sits at the leaves of the execution tree.

### Expand / Traverse
*   **Purpose**: Follows relationships from a known vertex to find adjacent vertices matching the pattern.
*   **Behavior**: Takes input bindings, uses relationship-direction details, and traverses graph topologies.

### OptionalExpand
*   **Purpose**: Follows relationships optionally.
*   **Behavior**: If the pattern is not found, it yields `null` values for newly introduced variables instead of discarding the parent row.

### NaturalJoin
*   **Purpose**: Integrates two streams of variables by matching shared fields.
*   **Behavior**: Implemented as hash joins or loop joins depending on size, matching column keys and filtering non-matching keys.

### LeftOuterJoin
*   **Purpose**: Preserves all rows on the left side of the join, stiching matching rows from the right side.
*   **Behavior**: Pads unmatched right-side variables with `null`.

### Filter
*   **Purpose**: Evaluates boolean expressions for each row.
*   **Behavior**: Discards rows for which the expression evaluates to `false` or `null`.

### Write
*   **Purpose**: Runs mutations on the graph.
*   **Behavior**: Invokes write tasks (e.g., node/edge insertion/properties/removal) and pipes resulting variables forward.

### Aggregate
*   **Purpose**: Condenses multiple rows into summary metrics.
*   **Behavior**: Groups by non-aggregated return variables and computes aggregates on grouped values.

### Distinct
*   **Purpose**: Removes duplicate elements from the stream.
*   **Behavior**: Uses a hash set to track seen rows and filters repeats.

### Sort
*   **Purpose**: Restructures row ordering.
*   **Behavior**: Buffers matching rows, sorts them using specified properties/comparators, and streams them out sorted.

### Limit
*   **Purpose**: Controls maximum record yield.
*   **Behavior**: Stops executing upstream once the limit count is met.

### ProduceResults
*   **Purpose**: Formats the final result set.
*   **Behavior**: Selects and renames output columns/variables to align with the return requirements of the GQL query.

### Union / UnionAll / Intersect / IntersectAll
*   **Purpose**: Perform relational set algebra on the outputs of two separate query parts.
*   **Behavior**: Combines or intersects subquery results.

### SchemaOperation
*   **Purpose**: Modifies the graph database schemas.
*   **Behavior**: Bypasses standard match/read phases to manipulate database types directly.
