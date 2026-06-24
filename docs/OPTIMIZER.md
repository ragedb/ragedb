# Walkthrough: GQL Execution Engine Optimizations in RageDB

We are implementing AST-level execution engine optimizations in RageDB's native GQL engine.

---

## Completed Phase 1: Limit & Top-K Push Down

Avoid traversing and processing the entire graph when only a small subset of rows is requested via the `LIMIT` clause.

### Changes Made
1. **Limit Applicability Check**: We updated `execute_query_internal` in [GqlExecutor.cpp](file:///home/maxdemarzi/ragedb/src/gql/GqlExecutor.cpp) to detect when a `LIMIT` pushdown is safe (i.e. when no aggregates or sorting are present) and pass it to the execution path.
2. **Downstream Limit Propagation**: The limit is now pushed down through factorized joins, path traversals, variable-length relationship expansions, and single-node start index queries to abort execution paths early once the required number of rows is matched.

---

## Completed Phase 2: Advanced Filter Push Down (Range/Inequality)

Extend pushdowns to non-equality comparison predicates (e.g. `p.age > 21` or `p.status != 'inactive'`) to leverage shard-local indexes and filter traversed paths earlier.

### Changes Made

#### 1. AST Generalization in [GqlAst.h](file:///home/maxdemarzi/ragedb/src/gql/GqlAst.h)
- Included `../graph/Operation.h` in the AST header.
- Defined the `PropertyFilter` structure consisting of the property name, `Operation` enum type, and comparison value.
- Added a `std::vector<PropertyFilter> property_filters` field to both `PatternNode` and `PatternEdge` to hold pushed-down predicates.

#### 2. General Predicate Extraction in [GqlOptimizer.cpp](file:///home/maxdemarzi/ragedb/src/gql/GqlOptimizer.cpp)
- Replaced `extract_eq_predicates` with `extract_filters` to support binary operators representing `EQ`, `NE`, `LT`, `LE`, `GT`, and `GE`.
- Handles operand mirroring (e.g., `21 < p.age` translates to `p.age > 21` by inverting `LT` to `GT`).
- Pushes extracted filters into node and edge patterns' `property_filters` vectors and simplifies/removes them from the GQL WHERE expression.

#### 3. Execution Engine Updates in [GqlExecutor.cpp](file:///home/maxdemarzi/ragedb/src/gql/GqlExecutor.cpp)
- **Unified Start Node Index Queries**: Updated `get_start_nodes` to collect both inline equality properties and pushed-down property filters. If multiple filters are present, we issue concurrent index queries (e.g. `FindNodesPeered` using the filter's native comparison `Operation` operator) to shards, and intersect their IDs using the Worst-Case-Optimal `leapfrogJoin`.
- **Traversal Constraints Matching**: Added checks in `traverse_step` and `traverse_var_len_async` to validate traversed node and relationship properties against the pushed-down `property_filters` using `matches_filters`.

#### 4. Filter Evaluation in [GqlValue.cpp](file:///home/maxdemarzi/ragedb/src/gql/GqlValue.cpp) and [GqlValue.h](file:///home/maxdemarzi/ragedb/src/gql/GqlValue.h)
- Implemented `matches_filters` to check a property map against a vector of comparison filters, matching each operator accordingly (`EQ`, `NEQ`, `LT`, `LTE`, `GT`, `GTE`) by invoking the native `compare_properties` function.

---

## Completed Phase 3: Projection Pruning

Avoid loading or retaining properties on intermediate node and relationship results if they are not projected or referenced by expressions in the query.

### Changes Made

1. **Property Pruning Support**:
   - Added `pruneProperties(const std::set<std::string>& keys)` to [Node.cpp](file:///home/maxdemarzi/ragedb/src/graph/Node.cpp) and [Relationship.cpp](file:///home/maxdemarzi/ragedb/src/graph/Relationship.cpp) to delete unreferenced key-value property entries from retrieved objects.

2. **Static Expression Analysis**:
   - Added `ProjectionPruner` and `collect_accessed_properties` to [GqlExecutor.cpp](file:///home/maxdemarzi/ragedb/src/gql/GqlExecutor.cpp).
   - Statically analyzes all fields referenced in the `RETURN` items, global `WHERE` conditions, `ORDER BY` specifiers, write-operation expressions (`SET`, etc.), and pattern-inline constraints/filters.
   - Identifies which variables are used as whole objects (where pruning is skipped) versus those where only a subset of properties are accessed.

3. **Pruner Propagation**:
   - Propagated `ProjectionPruner` to `get_start_nodes`, `traverse_step`, `traverse_path_pattern`, and `traverse_var_len_async` to proactively discard unprojected properties from shard-returned results before intermediate rows are combined/returned.

---

## Verification Results

### Automated Tests
We updated the optimizer test suite in [GqlOptimizerTests.cpp](file:///home/maxdemarzi/ragedb/test/gql/GqlOptimizerTests.cpp) and query execution tests in [GqlExecutorTests.cpp](file:///home/maxdemarzi/ragedb/test/gql/GqlExecutorTests.cpp):
1. **GQL Optimizer Range / Inequality Pushdown**: Verifies that range and inequality comparisons are successfully extracted by the optimizer, mapped to the correct operator, and stored in `property_filters`.
2. **Query Execution with Range / Inequality Pushdown**: Runs queries using `>`, `<`, and `!=` property comparisons, verifying that only matching vertices are traversed and returned.
3. **Projection Pruning**: Runs queries that select a single property on matching nodes, ensuring properties not needed are pruned.

---

## Completed Phase 4: Top-K Push Down

Optimizes `ORDER BY` + `LIMIT` patterns by replacing full sorts in memory with partial sorts.

### Changes Made

1. **Combined Result Sorting**:
   - Modified `sort_combined_result` in [GqlExecutor.cpp](file:///home/maxdemarzi/ragedb/src/gql/GqlExecutor.cpp) to accept an optional `limit` parameter.
   - If a limit is present and is smaller than the row count, it utilizes `std::partial_sort` to sort only the top-K elements in $O(N \log K)$ time instead of fully sorting the whole vector in $O(N \log N)$ time.

2. **Grouped / Aggregate Sorting**:
   - Updated the grouped result sorting in `execute_query_internal` in [GqlExecutor.cpp](file:///home/maxdemarzi/ragedb/src/gql/GqlExecutor.cpp) to use `std::partial_sort` and resize the vector when a limit is present.

3. **Flat Row Sorting**:
   - Updated the flat row sorting path in `execute_query_internal` in [GqlExecutor.cpp](file:///home/maxdemarzi/ragedb/src/gql/GqlExecutor.cpp) to use `std::partial_sort` and resize the vector when a limit is present.

---

## Completed Phase 5: Relationship Count / Node Degree Optimization

Optimizes GQL queries of the form `MATCH (p)-[:TYPE]->(f) RETURN p, count(f)` by completely bypassing edge/relationship traversal. It maps direction and relationship types to the node's pattern, asynchronously queries shard-level precalculated degree metadata during node retrieval, and rewrites the return aggregate from `COUNT(f)` to `SUM(p._degree_p_opt)`.

### Changes Made

1. **AST Extensions in [GqlAst.h](file:///home/maxdemarzi/ragedb/src/gql/GqlAst.h)**:
   - Added `DegreePopulateInfo` struct mapping the target property name, relationship types, and direction.
   - Extended `PatternNode` with a `std::vector<DegreePopulateInfo> degree_opt_info` field to store optimization metadata.

2. **AST Optimizer Pass in [GqlOptimizer.cpp](file:///home/maxdemarzi/ragedb/src/gql/GqlOptimizer.cpp)**:
   - Identifies candidate single-hop queries where the counted node and traversal edge are not referenced elsewhere.
   - Pre-assigns explicit aliases on the `RETURN` items (e.g. `count(f)`) to preserve the output column schema transparently.
   - Rewrites aggregate `COUNT(f)` or `COUNT(*)` functions to `SUM(start_node._degree_prop)`.
   - Populates `degree_opt_info` on the start node pattern and truncates the MATCH pattern to remove the edge and end node.

3. **Asynchronous Degree Retrieval in [GqlExecutor.cpp](file:///home/maxdemarzi/ragedb/src/gql/GqlExecutor.cpp)**:
   - Modified `get_start_nodes` to check `degree_opt_info`.
   - If present, queries shard degrees asynchronously using `graph.shard.local().NodeGetDegreePeered`.
   - Stores the degree count directly as a temporary, query-local property on the retrieved `Node` DTO objects using the public `Node::setProperty(prop, val)` method.
   - Directly initializes the future using ternary expressions to bypass Seastar future's lack of a default constructor.
   - Captures `degree_opt_info` by value and vector node pointers by value to prevent dangling references/SIGSEGVs during asynchronous execution.

4. **Dynamic Schema Validation Bypass in [GqlTypechecker.cpp](file:///home/maxdemarzi/ragedb/src/gql/GqlTypechecker.cpp)**:
   - Bypasses schema property type checking for optimization-injected dynamic properties starting with `_degree_` and returns `GqlType::INTEGER`.

5. **Test Harness Stability Guard in [GqlExecutorTests.cpp](file:///home/maxdemarzi/ragedb/test/gql/GqlExecutorTests.cpp)**:
   - Introduced `GraphStopGuard` utilizing RAII to guarantee `graph.Stop().get()` is cleanly executed during stack unwinding. This prevents Seastar's `~sharded()` destructor from aborting and crashing the Catch2 test runner with a SIGABRT on test assertion failures.

---

## Completed Phase 6: Aggregate Key Dependency Optimization

Optimizes hash-aggregation by pruning functionally dependent property lookups from the grouping keys when their corresponding node or relationship variable is already a grouping key.

### Changes Made

1. **Functional Dependency Detection**:
   - Inside `execute_query_internal` in [GqlExecutor.cpp](file:///home/maxdemarzi/ragedb/src/gql/GqlExecutor.cpp), we identify all return expressions that represent plain variables (e.g. `p` of type `VARIABLE`) and collect their names into a `std::set<std::string> group_variables`.
   - When building the list of `grouping_keys` for hash-aggregation, any expression representing a property lookup (e.g. `p.name` of type `PROPERTY_LOOKUP`) whose base variable exists in `group_variables` is pruned.

2. **Transparent Row Evaluation**:
   - The final projection still evaluates the full original return expressions (like `p.name`) using the group's representative row context. Since all rows in the group share the same node ID for the variable `p`, resolving `p.name` on the representative row is correct, while the grouping comparison and hashing overhead is reduced.

---

## Completed Phase 7: Accumulator Hash Join Optimization

Replaces the nested-loop join algorithm inside GqlExecutor's intermediate join step with a Worst-Case-Optimal Accumulator Hash Join algorithm when shared variables are present.

### Changes Made

1. **Hashing Helpers**:
   - Added `PropertyHash` and `GqlValueHash` functors in [GqlExecutor.cpp](file:///home/maxdemarzi/ragedb/src/gql/GqlExecutor.cpp) to cleanly hash GQL types (variant-based property lookups, node IDs, relationship IDs, list identifiers).
   - Added `GqlValueVectorHash` and `GqlValueVectorEqual` to support hashing and checking equality of `std::vector<GqlValue>` keys, which allows multi-key joins.

2. **Optimized Join Selection**:
   - Modified `join_flat_rows_variable` in [GqlExecutor.cpp](file:///home/maxdemarzi/ragedb/src/gql/GqlExecutor.cpp).
   - Dynamically intersects row schemas to identify shared join variables (ignoring internal edge/node trackers).
   - If no variables are shared, it falls back to a clean, pre-allocated Cartesian product (nested loops).
   - If shared variables exist, it identifies the smaller table (build side), constructs an in-memory hash table mapping the shared keys to row indices, and streams the larger table (probe side) against it. This reduces join complexity from $O(|L| \times |R|)$ to $O(|L| + |R|)$.

---

## Completed Phase 8: Unnecessary Join Removal

Identifies and removes redundant join operations at the AST/Optimizer level (e.g., duplicate `MATCH` or `OPTIONAL MATCH` clauses).

### Changes Made

1. **Equivalence Comparators**:
   - Implemented `is_equivalent_label_expr`, `is_equivalent_properties`, and `is_equivalent_pattern` inside an anonymous namespace in [GqlOptimizer.cpp](file:///home/maxdemarzi/ragedb/src/gql/GqlOptimizer.cpp).
   - These perform deep structural equality checks across labels, properties, and path nodes/edges (including direction, repetitions, and constraints).

2. **Pruning Loop**:
   - Modified `GqlOptimizer::optimize` in [GqlOptimizer.cpp](file:///home/maxdemarzi/ragedb/src/gql/GqlOptimizer.cpp) to run a duplicate detection pass.
   - It iterates through all `MATCH` statements and removes duplicate patterns. If a duplicate match is non-optional and the kept match is optional, it promotes the kept match to non-optional to preserve constraints.

---

## Completed Phase 9: Order By Push Down Optimization

Completely skips sorting steps on query execution results if the initial node scan retrieved from the storage layer can be sorted by the requested key.

### Changes Made

1. **Sort Parameter Push Down**:
   - Extended `get_start_nodes` in [GqlExecutor.cpp](file:///home/maxdemarzi/ragedb/src/gql/GqlExecutor.cpp) with parameters `sort_property`, `sort_ascending`, and `sort_by_id`.
   - When node retrieval is done (and after asynchronous degree properties are loaded if applicable), the start nodes vector is sorted directly in `get_start_nodes` before returning.
   
2. **Flag Propagation**:
   - Added `is_sorted` boolean flag to both `IntermediateResult` and `QueryResult` in [GqlExecutor.cpp](file:///home/maxdemarzi/ragedb/src/gql/GqlExecutor.cpp).
   - `traverse_path_pattern`, `traverse_match_statement`, and `execute_match_chain_factorized` receive and propagate the sort parameters to the first match step and set the `is_sorted` flag on the returned results.
   - `natural_join` and `left_outer_join` propagate `is_sorted` from the left input if no variables are shared (preserving the original order during Cartesian products).

3. **Bypassing Final Sort**:
   - In `execute_query_internal` and `GqlExecutor::execute` in [GqlExecutor.cpp](file:///home/maxdemarzi/ragedb/src/gql/GqlExecutor.cpp), the query sorting blocks are bypassed if the result is already marked as sorted.
   - If a `LIMIT` is present, it directly resizes the sorted rows without sorting first.

4. **Correct NULL/monostate Property Ordering**:
   - Updated `compare_properties` in [GqlValue.cpp](file:///home/maxdemarzi/ragedb/src/gql/GqlValue.cpp) to correctly order `std::monostate` (representing missing/NULL values) before any non-monostate property types.

---

## Completed Phase 10: Correlated Subquery Unnesting

Decorrelates EXISTS subqueries by translating nested loops into standardized relational join operations (left outer joins/semi-joins) that can be globally optimized.

### Changes Made

1. **Parser & Lexer Extensions**:
   - Added the `EXISTS` keyword and `TokenType::EXISTS` in [GqlLexer.h](file:///home/maxdemarzi/ragedb/src/gql/GqlLexer.h) and [GqlLexer.cpp](file:///home/maxdemarzi/ragedb/src/gql/GqlLexer.cpp).
   - Added the `ExpressionKind::EXISTS` and the `ExistsExpr` AST structure in [GqlAst.h](file:///home/maxdemarzi/ragedb/src/gql/GqlAst.h).
   - Parsed EXISTS subqueries (`EXISTS { MATCH ... [WHERE ...] }`) inside `GqlParser::parse_primary` in [GqlParser.cpp](file:///home/maxdemarzi/ragedb/src/gql/GqlParser.cpp).

2. **Scoped Typechecking**:
   - Updated `check_expression` in [GqlTypechecker.cpp](file:///home/maxdemarzi/ragedb/src/gql/GqlTypechecker.cpp) to typecheck nested `EXISTS` statements using a cloned, scoped environment copy. This allows subqueries to resolve outer (correlated) variables without bleeding subquery-defined variables back to the outer scope.

3. **AST Decorrelation Rewrite**:
   - Implemented a static helper method `unnest_subqueries_in_expr` in [GqlOptimizer.cpp](file:///home/maxdemarzi/ragedb/src/gql/GqlOptimizer.cpp) and integrated it into the optimization pipeline.
   - For each `ExistsExpr` inside the global WHERE clause, projected items, or ORDER BY clause:
     - Identifies variables introduced in the subquery matches that do not exist in the outer query matches. If no new variables exist (a degenerate subquery), generates a unique anonymous placeholder variable (e.g. `_exists_var_N`).
     - Designates one new variable as the existence check `target_variable`.
     - Appends the subquery matches to the main query matches as `OPTIONAL MATCH` statements (left outer joins).
     - Traverses the subquery's optional `where_expr` and pushes down any extractable property filters directly to the subquery matches' node/edge patterns, simplifying the subquery's `where_expr`.
     - Recursively processes any nested subqueries.

4. **Runtime Semi-Join Correctness**:
   - Stored original query variables in the `outer_vars` field of `GqlQuery` before unnesting.
   - Evaluated the `EXISTS` expression node inside `evaluate_expression` in [GqlValue.cpp](file:///home/maxdemarzi/ragedb/src/gql/GqlValue.cpp) by checking that the subquery's `target_variable` is bound and not NIL, and evaluating the simplified subquery `where_expr` if present.
   - Deduplicated intermediate rows by `outer_vars` inside `execute_query_internal` in [GqlExecutor.cpp](file:///home/maxdemarzi/ragedb/src/gql/GqlExecutor.cpp) if subquery unnesting occurred. This ensures that multiple matches in the subquery's outer join do not duplicate the outer query's row cardinality (retaining correct semi-join/anti-semi-join semantics).

---

## Completed Phase 11: Factorization Rewriter

Optimize multi-match query patterns that share a single central variable (e.g. `MATCH (a)-[:FRIEND]->(b) MATCH (b)-[:KNOWS]->(c) MATCH (b)-[:FRIEND]->(d)`) by partitioning the execution tree and evaluating branches independently, then merging them via factorized `natural_join` rather than flattening them early into flat rows.

### Changes Made

1. **Star-Join Candidate Discovery**:
   - Implemented `find_star_join_candidate` in [GqlExecutor.cpp](file:///home/maxdemarzi/ragedb/src/gql/GqlExecutor.cpp) to detect star joins by checking for shared variables across remaining matches and ensuring other variables are disjoint.

2. **Recursive Factorized Tree Execution**:
   - Extended `execute_match_chain_factorized` in [GqlExecutor.cpp](file:///home/maxdemarzi/ragedb/src/gql/GqlExecutor.cpp) to partition matches relative to a central variable.
   - For already bound variables, executes each branch statement independently, and merges the resulting factorized sub-trees using a lazy natural join.
   - For not-yet-bound variables, executes the first branch to bind the variable, then proceeds recursively.

3. **Lifetime Safety & Parameter Move Semantics**:
   - Converted the `matches` parameter of `execute_match_chain_factorized` from a reference (`const std::vector<MatchStatement>&`) to value (`std::vector<MatchStatement>`) to prevent dangling references in asynchronous `.then()` Seastar continuations when local/temporary vectors went out of scope.
   - Used `std::move` to transfer ownership of matches vectors into the recursive and lambda-bound execution steps.

---

## Completed Phase 12: GqlExecutor Refactoring & Modularization

To improve code readability, maintainability, and clean separation of concerns, the large `src/gql/GqlExecutor.cpp` (~2300 lines) was refactored into logical components located under a new `src/gql/executor/` subfolder.

### Changes Made

1. **Modular Code Extraction**:
   - **[FactorNode](file:///home/maxdemarzi/ragedb/src/gql/executor/FactorNode.h)**: Defines `FactorNodeType`, `FactorNode`, and `IntermediateResult` classes/structs representing factorized nodes in the join graph.
   - **[JoinHelpers](file:///home/maxdemarzi/ragedb/src/gql/executor/JoinHelpers.h)**: Houses hashing functors (`PropertyHash`, `GqlValueHash`, `GqlValueVectorHash`, `GqlValueVectorEqual`), natural join, left outer join, and other flat/factorized join helper utilities.
   - **[ExpressionEvaluator](file:///home/maxdemarzi/ragedb/src/gql/executor/ExpressionEvaluator.h)**: Houses AST expression evaluation, functional dependency checking, and aggregate detection functions (`has_aggregates`, `find_aggregates`, etc.).
   - **[PathTraverser](file:///home/maxdemarzi/ragedb/src/gql/executor/PathTraverser.h)**: Contains path traversal routines (`get_start_nodes`, `traverse_step`, `traverse_var_len_async`, `traverse_path_pattern`, `traverse_match_statement`), the recursive `execute_match_chain_factorized` star-join rewriter, and its associated `StarJoinCandidate` helper struct.

2. **Clean Driver Separation**:
   - Rewrote **[GqlExecutor.cpp](file:///home/maxdemarzi/ragedb/src/gql/GqlExecutor.cpp)** to import the modularized headers. It now acts as a high-level query execution orchestrator (parsing variables, managing locks, resolving groupings, executing return projections, and coordinating calls to the sub-modules).

3. **Build Configuration Updating**:
   - Added all new files to CMake targets in **[CMakeLists.txt](file:///home/maxdemarzi/ragedb/CMakeLists.txt)** and **[test/CMakeLists.txt](file:///home/maxdemarzi/ragedb/test/CMakeLists.txt)**.

---

## Verification Results

### Automated Tests
We updated the query execution tests in [GqlExecutorTests.cpp](file:///home/maxdemarzi/ragedb/test/gql/GqlExecutorTests.cpp) and optimizer tests in [GqlOptimizerTests.cpp](file:///home/maxdemarzi/ragedb/test/gql/GqlOptimizerTests.cpp):
1. **GQL Optimizer Range / Inequality Pushdown**: Verifies that range and inequality comparisons are successfully extracted by the optimizer, mapped to the correct operator, and stored in `property_filters`.
2. **Query Execution with Range / Inequality Pushdown**: Runs queries using `>`, `<`, and `!=` property comparisons, verifying that only matching vertices are traversed and returned.
3. **Projection Pruning**: Runs queries that select a single property on matching nodes, ensuring properties not needed are pruned.
4. **Top-K Push Down**: Runs queries sorting matches by a property descending and applying a limit, asserting that only the top sorted element is correctly returned.
5. **Relationship count degree optimization query**: Validates that matching a single-hop relationship count query correctly truncates the AST pattern, retrieves the correct degree from the shard indices, and returns the correct count matching the GQL engine's expectation.
6. **Aggregate key dependency optimization query**: Runs a query grouping by both a node variable `p` and its property `p.name` (e.g. `MATCH (p:Person) RETURN p, p.name, count(*)`), verifying that grouping and projection are executed successfully and output is correct.
7. **Join query utilizing accumulator hash join**: Runs a multi-match pattern query that joins on a shared variable `b` (`MATCH (a)-[:FRIEND]->(b) MATCH (b)-[:KNOWS]->(c) RETURN ...`), verifying that the accumulator hash join correctly unifies matching records.
8. **Cartesian product join query**: Runs a query with disjoint match patterns (`MATCH (a:Person) MATCH (b:Robot) RETURN ...`), verifying that the Cartesian product fallback paths unify all rows correctly.
9. **GQL Optimizer redundant match pattern removal**: Verifies that duplicate `MATCH` patterns are correctly pruned down to a single MATCH pattern.
10. **GQL Optimizer redundant optional match promotion**: Verifies that duplicate matches containing mixed optional/non-optional constraints are promoted to a single non-optional MATCH statement and redundant ones are removed.
11. **Order By Push Down**: Validates sorting of start nodes by a property ascending, descending, and by node variable (internal ID) ascending, ensuring the final output matches correct sorted expectations and that sorting is correctly pushed down and bypassed.
12. **GQL Optimizer Correlated Subquery Unnesting**: Verifies that EXISTS subqueries are unnested, appended as OPTIONAL MATCHes, and subquery filters are correctly pushed down.
13. **GQL Execution Correlated Subqueries**: Validates execution of unnested EXISTS, EXISTS with complex filters, and NOT EXISTS subqueries on a test graph, asserting correct matched output and deduplication.
14. **Star join utilizing factorization rewriter**: Validates execution of factorized star-joins on multiple patterns sharing a single central variable, verifying correctness of lazy join and flattening.

All tests compile and pass successfully:
- **Build Command**: `cmake --build build_debug --target tests -j8`
- **Test Executable**: `./build_debug/test/tests`

Output:
```
All tests passed (1474 assertions in 52 test cases)
```

---

## Completed Phase 13: PathTraverser Modularization

To further improve code decoupling and maintainability, `src/gql/executor/PathTraverser.cpp` and `src/gql/executor/PathTraverser.h` were logically split. We extracted independent helper functionalities into decoupled sub-modules:

### Changes Made

1. **ProjectionPruner Module**:
   - Extracted `ProjectionPruner` struct and its static `collect_accessed_properties` helper method to its own standalone files: [ProjectionPruner.h](file:///home/maxdemarzi/ragedb/src/gql/executor/ProjectionPruner.h) and [ProjectionPruner.cpp](file:///home/maxdemarzi/ragedb/src/gql/executor/ProjectionPruner.cpp). This isolates property-level optimization and static query analyses with zero dependencies on traversal routines.

2. **StarJoinRewriter Module**:
   - Extracted `StarJoinCandidate` and `execute_match_chain_factorized` recursively to [StarJoinRewriter.h](file:///home/maxdemarzi/ragedb/src/gql/executor/StarJoinRewriter.h) and [StarJoinRewriter.cpp](file:///home/maxdemarzi/ragedb/src/gql/executor/StarJoinRewriter.cpp). This separates query-plan level tree factorization, joins, and rewriting logic from raw traversals.

3. **Streamlined PathTraverser**:
   - Kept only core path traversals and start node lookup routines inside [PathTraverser.h](file:///home/maxdemarzi/ragedb/src/gql/executor/PathTraverser.h) and [PathTraverser.cpp](file:///home/maxdemarzi/ragedb/src/gql/executor/PathTraverser.cpp).

4. **Updated Query Driver & Configurations**:
   - Updated [GqlExecutor.cpp](file:///home/maxdemarzi/ragedb/src/gql/GqlExecutor.cpp) to import the new headers.
   - Registered the new files in both target builds inside [CMakeLists.txt](file:///home/maxdemarzi/ragedb/CMakeLists.txt) and [test/CMakeLists.txt](file:///home/maxdemarzi/ragedb/test/CMakeLists.txt).
