# Graph Query Language (GQL) Support in RageDB

RageDB features a native C++ Graph Query Language (GQL) execution engine built on top of Seastar's thread-per-core asynchronous architecture. This dialect is designed to be highly compliant with the official ISO GQL specification.

---

## 1. Syntax & Features

### MATCH & Projections
- **MATCH / OPTIONAL MATCH**: Thread-safe peered shard traversal read logic.
- **SEARCH (Full-Text Search)**: Querying string properties using the `SEARCH` operator within the `MATCH` pattern (e.g. `MATCH p IN SEARCH Product.description FOR 'databases' YIELD p, score`). Supports options like `{ fuzzy: 'true' }` for Levenshtein-based matching.
- **WHERE Filtering**: Supports logical conjunctive precedence (`NOT` > `AND` > `OR`) and standard comparisons (`=`, `!=`, `<`, `<=`, `>`, `>=`).
- **RETURN**: Projection return list with optional aliasing (`AS alias`).
- **ORDER BY & LIMIT**: Sorting and pagination at the query and set operation levels.

### Data Modifications
Our data modification operations map directly to the official GQL grammar rules:

| Command | Syntax Example | Description |
| :--- | :--- | :--- |
| **INSERT** | `INSERT (p:Person {name: 'Alice', age: 30})` | Creates new nodes and relationships. |
| **SET** | `SET p.age = 45` | Updates a property on a node or relationship. |
| **REMOVE** | `REMOVE p.age` | Deletes a property from a node or relationship. |
| **DELETE** | `DELETE p` / `DETACH DELETE p` | Removes nodes and edges from the graph. |

### Aggregations & Grouping
- Standard aggregate functions (`COUNT(*)`, `COUNT(expr)`, `SUM`, `AVG`, `MIN`, `MAX`) with numeric type coercion.
- Implicit grouping based on non-aggregate return projections.
- Correct default output on empty result sets (e.g. returning a single row of `[{"count(*)": 0, "sum(...)": null}]`).

### Set Operations
- **Supported operators**: `UNION`, `UNION ALL`, `INTERSECT`, and `INTERSECT ALL`.
- **Precedence**: `INTERSECT` binds tighter than `UNION`.
- Merged results support top-level sorting and pagination.

---

## 2. Catalog & Schema Controls (DDL)

To align with RageDB's native schema constraints, the GQL engine supports direct Data Definition Language (DDL) queries within the current graph context:

### Node Type Management
* **Create Node Type**:
  - `CREATE NODE TYPE Person`
  - `CREATE NODE TYPE Person (name STRING, age INTEGER)`
* **Drop Node Type**:
  - `DROP NODE TYPE Person`
* **Alter Node Type**:
  - `ALTER NODE TYPE Person ADD weight DOUBLE`
  - `ALTER NODE TYPE Person DROP age`

### Relationship Type Management
* **Create Relationship Type**:
  - `CREATE RELATIONSHIP TYPE FRIEND_OF`
  - `CREATE RELATIONSHIP TYPE FRIEND_OF (roles STRING_LIST, rating DOUBLE)`
* **Drop Relationship Type**:
  - `DROP RELATIONSHIP TYPE FRIEND_OF`
* **Alter Relationship Type**:
  - `ALTER RELATIONSHIP TYPE FRIEND_OF ADD date STRING`
  - `ALTER RELATIONSHIP TYPE FRIEND_OF DROP rating`

### Supported Data Types
GQL DDL keywords are case-insensitive and support standard type names along with convenient shorthand aliases:
- `STRING` -> `"string"`
- `INTEGER` / `INT` -> `"integer"`
- `DOUBLE` -> `"double"`
- `BOOLEAN` / `BOOL` -> `"boolean"`
- `STRING_LIST` -> `"string_list"`
- `INTEGER_LIST` / `INT_LIST` -> `"integer_list"`
- `DOUBLE_LIST` -> `"double_list"`
- `BOOLEAN_LIST` / `BOOL_LIST` -> `"boolean_list"`

### Index Management

#### Indexable Data Types
RageDB supports indexing of specific scalar data types depending on the index type:
*   **Standard Property Index (`CREATE INDEX`)**:
    *   `BOOLEAN`: Indexed via compressed `roaring::Roaring64Map` bitmaps.
    *   `INTEGER` & `DOUBLE`: Indexed via `std::multimap`.
    *   `STRING`: Indexed via `unodb::art_db` (Adaptive Radix Tree).
*   **Full-Text Search Index (`CREATE FULLTEXT INDEX`)**:
    *   `STRING`: Indexed via Apache `iresearch` using standard text segmentation.
*   *Note*: List types (`STRING_LIST`, `INTEGER_LIST`, `DOUBLE_LIST`, `BOOLEAN_LIST`) cannot be indexed.

* **Create Index**:
  - Property Index: `CREATE INDEX Person.name`
  - Full-Text Index: `CREATE FULLTEXT INDEX Person.bio`
* **Drop Index**:
  - `DROP INDEX Person.name` (drops standard or full-text indexes)
* **Show Indexes**:
  - `SHOW INDEXES` (returns list of all indexes; full-text indexes are marked with `"kind": "fulltext"`)
  - `SHOW INDEXES ON Person` (filters indexes by a specific label/type)

---

## 3. ISO GQL Grammar Compliance Status

Refer to the official ISO GQL ANTLR4 grammar specifications:
* **Match & Traversals**: 100% compliant with standard path pattern lists.
* **Writes**: 100% compliant with data modification statements.
* **Set Operations**: 100% compliant with query conjunction set operators.
* **DDL**: Mapped GQL's structural schema definitions directly to node/relationship types and constraint controls.

---

## 4. Unsupported GQL Features

While RageDB supports the core operational subsets of GQL required for graph traversals, data updates, aggregations, and schema definitions, the following specifications from the full ISO GQL grammar are currently **not supported**:

### Procedural & Control Flow
- **`LET` Variable Definitions**: General-purpose procedural assignment blocks (e.g. `LET x = value`) are not supported (use `AS` aliases in projections instead).
- **`FOR` Loops**: Procedural loop iteration blocks (e.g. `FOR item IN list`) are not supported.
- **`CALL` Procedures**: Calling built-in or custom named procedure scopes (e.g. `CALL myProc(x) YIELD y`) is not supported.
- **`NEXT` Chaining**: Piping multiple independent linear query blocks sequentially using the `NEXT` statement.

### Sessions & Transactions
- **Session Configuration**: Session-level commands like `SESSION SET SCHEMA`, `SESSION RESET`, and `SESSION CLOSE`.
- **Transaction Commands**: Transaction boundary markers like `START TRANSACTION`, `COMMIT`, and `ROLLBACK` (RageDB transactions are handled natively at the HTTP request or Seastar sharding level).

### Advanced Pattern Matching
- **Path Variables**: Binding whole traversed paths to path variables (e.g. `p = (a)-[e]->(b)`).
- **Match Modes & Keep Clauses**: Special path matching modes (e.g. repeatable path elements, shortest path search selectors, and path filtration clauses).
- **SQL-like SELECT Syntax**: The grammar defines a SQL-styled `SELECT * FROM ...` query format. RageDB focuses strictly on the standard GQL `MATCH ... RETURN` query structure.
