# Semantic Query Optimization in RageDB

RageDB includes a native **Semantic Query Optimizer (SQO)** that uses registered schema constraints and mathematical axioms to rewrite, simplify, or prune query execution trees before sharded traversal begins.

By resolving predicate satisfiability and relation structures at compile-time, RageDB eliminates redundant joins and traversal steps, short-circuiting execution when queries are mathematically guaranteed to yield zero results.

---

## 1. Mathematical Foundations & Implementation Mapping

The optimizer uses several mathematical axioms and techniques to prove query rewrites:

### Phase 1: Range Contradiction Pruning
* **Mathematical Axiom**: **Totality and Transitivity of Partially Ordered Sets (Posets)** $(S, \le)$.
  - *Reflexivity*: $a \le a$
  - *Anti-symmetry*: $a \le b \land b \le a \implies a = b$
  - *Transitivity*: $a \le b \land b \le c \implies a \le c$
  - *Totality*: $\forall a, b \in S$, either $a \le b$ or $b \le a$
* **GQL Query**:
  ```gql
  MATCH (x:Person) WHERE x.age = -5 RETURN x.name
  ```
* **Optimizer Mapping**:
  - We represent query filters as bounding intervals over a totally ordered set (integers/real numbers).
  - Catalog check constraints define the **impossible regions** for specific label attributes.
  - If the query interval for a variable is a subset of the impossible region (i.e. query interval $\cap$ valid region $= \emptyset$), the query is unsatisfiable.
  - The optimizer marks `query.no_op = true` and the executor short-circuits to return an empty result set immediately.
* **Code Location**: [ContradictionPruner.cpp](file:///home/maxdemarzi/ragedb/src/gql/optimizer/ContradictionPruner.cpp) (`semantic_pruning_pass`) and [GqlExecutor.cpp](file:///home/maxdemarzi/ragedb/src/gql/GqlExecutor.cpp) (`execute_query_internal` check).

### Phase 2: Join Elimination / Pruning
* **Mathematical Axiom**: **Referential Mappings & Existential Mappings**.
  - A constraint of the form $\forall s \in \text{Source}, \exists t \in \text{Target s.t. } (s) \xrightarrow{\text{REL}} (t)$ guarantees relationship existence.
* **GQL Query**:
  ```gql
  MATCH (s:Shipment)-[:SHIPPED_FROM]->(l:Location) RETURN s.name
  ```
* **Optimizer Mapping**:
  - If a GQL constraint specifies that a relation is mandatory (e.g. `MATCH (s:Shipment) WHERE NOT EXISTS { MATCH (s)-[:SHIPPED_FROM]->(:Location) } RETURN s` returns empty), then any query containing `MATCH (s:Shipment)-[:SHIPPED_FROM]->(l:Location)` can eliminate the join to `Location` if `l` is neither filtered nor projected in the query.
  - The pattern is rewritten to a simple single-node match `(s:Shipment)`.
* **Code Location**: [JoinEliminator.cpp](file:///home/maxdemarzi/ragedb/src/gql/optimizer/JoinEliminator.cpp) (`semantic_join_elimination_pass`).

### Phase 3: Multi-Variable Relational Predicate Reasoning
* **Mathematical Axiom**: **Strict Inequalities and Directed Acyclic Graph (DAG) Cycle Contradictions**.
  - A strict poset relation ($a < b$) is irreflexive: $\nexists a$ such that $a < a$.
  - Therefore, any directed graph of inequalities containing a cycle with at least one strict edge is unsatisfiable.
* **GQL Query**:
  ```gql
  MATCH (a:PosetNode), (b:PosetNode), (c:PosetNode) WHERE a.age < b.age AND b.age <= c.age AND c.age < a.age RETURN a.age
  ```
* **Optimizer Mapping**:
  - The optimizer extracts all inequalities between query variables (e.g. `a.age < b.age AND b.age <= c.age AND c.age < a.age`).
  - It constructs an inequality graph and runs the Floyd-Warshall transitive closure algorithm.
  - If a strict cycle (self-loop with strict flag) is detected (e.g. `a.age < a.age`), a contradiction is proved and `query.no_op = true` is set.
* **Code Location**: [ContradictionPruner.cpp](file:///home/maxdemarzi/ragedb/src/gql/optimizer/ContradictionPruner.cpp) (`relational_pruning_pass`).

### Phase 4: Algebraic Query Rewrites
* **Mathematical Axiom**: **Commutative Semiring Distributivity** $(K, \oplus, \otimes, 0, 1)$.
  - Join maps to product ($\otimes$), union/aggregation maps to sum ($\oplus$).
  - Distributive Axiom: $a \otimes (b \oplus c) = (a \otimes b) \oplus (a \otimes c)$.
* **GQL Query**:
  ```gql
  MATCH (p:Person)-[:FRIEND]->(f:FriendNode) RETURN p.name, sum(p.age * f.age)
  ```
* **Optimizer Mapping**:
  - When aggregating a multiplied expression: $\sum (A \times B)$.
  - If $A$ is a grouping key or depends solely on grouping variables (constant within each aggregate group), $A$ is factored out of the summation:
    $$\sum (A \times B) = A \times \sum B$$
  - The optimizer rewrites `sum(p.age * f.age)` to `p.age * sum(f.age)`. This reduces multiplication overhead to occur once per group rather than once per joined row.
* **Code Location**: [AlgebraicRewriter.cpp](file:///home/maxdemarzi/ragedb/src/gql/optimizer/AlgebraicRewriter.cpp) (`algebraic_rewriter_pass`).

### Phase 4.5: Algebraic Path Count Rewrite
* **Mathematical Axiom**: **Matrix-Vector Multiplication Associativity** $(A^k \cdot \mathbf{1} = A \cdot (A \cdot (\dots (A \cdot \mathbf{1})\dots)))$.
  - BFS path/walk expansion scales exponentially $O(|V| \cdot d^k)$ with hops.
  - Decomposing the walk count into iterative local degree sums scales linearly $O(k \cdot |E|)$.
* **GQL Query**:
  ```gql
  MATCH (p:Person)-[:FRIEND]->{3}(f) RETURN p.name, count(f)
  ```
  or
  ```gql
  MATCH (p:Person)-[:FRIEND]->(a)-[:FRIEND]->(b)-[:FRIEND]->(f) RETURN p.name, count(f)
  ```
* **Optimizer Mapping**:
  - The optimizer detects path-counting queries of length $k$ (represented via variable-length hops or explicit chained relationships) where intermediate and target variables are only referenced inside a `count()` aggregation.
  - The query pattern is truncated to a 1-node match `(p:Person)` and execution maps to the `AlgebraicPathCountJoin` operator.
  - The operator executes Seastar-peered iterative degree updates:
    $$\mathbf{v}_m[u] = \sum_{v \in Neigh(u)} \mathbf{v}_{m-1}[v]$$
  - Final walk counts are bound directly to output row variables, bypassing traversal expansion entirely.
* **Code Location**: [AlgebraicRewriter.cpp](file:///home/maxdemarzi/ragedb/src/gql/optimizer/AlgebraicRewriter.cpp) (`algebraic_rewriter_pass`), [PlanBuilder.cpp](file:///home/maxdemarzi/ragedb/src/gql/executor/PlanBuilder.cpp) (`build_match_plan`), and [PathTraverser.cpp](file:///home/maxdemarzi/ragedb/src/gql/executor/PathTraverser.cpp) (`propagate_path_counts`).

### Phase 5: Cardinality-Constrained Traversal Short-Circuiting
* **Mathematical Axiom**: Cardinality bounds on functions and relations.
  - If a relationship type is constrained to be at most 1-to-1 or $N$-to-1 (e.g. max-cardinality of outgoing edges of type $R$ is $C$), then the size of the image set is bounded: $|R(u)| \le C$.
* **GQL Query**:
  ```gql
  MATCH (s:Shipment)-[:SHIPPED_FROM]->(w:Warehouse) RETURN s.name, w.name
  ```
* **Optimizer Mapping**:
  - The optimizer detects cardinality constraints registered in the virtual catalog (e.g. `CREATE CONSTRAINT ShipmentOriginMaxCard AS MATCH (s:Shipment)-[:SHIPPED_FROM]->(w1) MATCH (s)-[:SHIPPED_FROM]->(w2) WHERE w1 != w2 RETURN s` which implies `SHIPPED_FROM` outdegree $\le 1$).
  - It sets `max_cardinality_limit = 1` on the `SHIPPED_FROM` pattern edge.
  - The traverser truncates neighbor lists to 1 when scanning edges, short-circuiting traversal and avoiding redundant shard communications.
* **Code Location**: [CardinalityShortCircuiter.cpp](file:///home/maxdemarzi/ragedb/src/gql/optimizer/CardinalityShortCircuiter.cpp) (`semantic_cardinality_limit_pass`) and [PathTraverser.cpp](file:///home/maxdemarzi/ragedb/src/gql/executor/PathTraverser.cpp) (`traverse_step` and `traverse_var_len_async`).

### Phase 6: Subsumption / Query Containment Pruning
* **Mathematical Axiom**: **Subsumption / Query Containment**.
  - A query pattern $Q_1$ is subsumed by another pattern $Q_2$ (denoted $Q_1 \sqsubseteq Q_2$) if for any database instance $D$, the results $Q_1(D) \subseteq Q_2(D)$.
  - If the variables bound in $Q_2$ are not projected or referenced outside the match pattern, and $Q_1$ imposes equal or stricter constraints over an isomorphic path structure originating from the same variable, then the execution of $Q_2$ is redundant and its match can be pruned entirely.
* **GQL Query**:
  ```gql
  MATCH (p:Person)-[:FRIEND]->(f1:Person) WHERE f1.age > 30
  MATCH (p)-[:FRIEND]->(f2:Person) WHERE f2.age > 20
  RETURN p.name
  ```
* **Optimizer Mapping**:
  - The optimizer detects isomorphic `MATCH` patterns originating from the same node variable (e.g., `p`).
  - It maps the variables in the patterns and computes interval intersections.
  - For target nodes and relationships, it checks label hierarchy compatibility and ensures that the filters on `f2` ($age > 20$) are a superset of/subsume those on `f1` ($age > 30$).
  - Since the variable `f2` is a dead-end (not projected, not sorted, not referenced in `WHERE` outside of simple range filters), the match of `f2` is redundant: any node `p` that has a friend older than 30 is guaranteed to have a friend older than 20.
  - The second MATCH is pruned from the query execution tree.
* **Code Location**: [SubsumptionPruner.cpp](file:///home/maxdemarzi/ragedb/src/gql/optimizer/SubsumptionPruner.cpp) (`semantic_subsumption_pass`).

---

## 2. Bypassing the Semantic Optimizer

Queries can dynamically skip the semantic query optimizer using prefix hints.

### Syntax Options
1. **Keyword Prefix**:
   ```gql
   NO_SEMANTIC MATCH (p:Person) WHERE p.age < -5 RETURN p
   ```
2. **Comment Directive**:
   ```gql
   /* no_semantic */ MATCH (p:Person) WHERE p.age < -5 RETURN p
   ```

When either prefix is matched, the parser sets `query.skip_semantic = true`. The optimizer checks this flag and exits immediately, bypassing all semantic passes.

---

## 3. Performance & Cache Hits

The semantic query optimizer is fully compatible with the RageDB query cache (`GqlQueryCache`).

* **Cold Query Execution**:
  - The raw GQL string is parsed.
  - Semantic optimizer runs passes (proving range/cycle satisfiability and factoring terms).
  - The pre-optimized AST is saved to the thread-local query cache.
* **Hot Query Execution**:
  - The pre-optimized AST is retrieved directly from the query cache.
  - Compilation, semantic analysis, and optimization passes are skipped entirely, going straight to sharded execution.
* **Dynamic Invalidation**:
  - Adding or dropping catalog constraints (e.g. `CREATE CONSTRAINT` or `DROP_CONSTRAINT`) automatically invalidates/flushes the query cache on all reactor threads to ensure outdated execution plans are never reused.

---

## 4. Optimization Performance Benchmarks

The following benchmark table compares the execution latency of GQL queries optimized by the Semantic Query Optimizer under three states (averaged over 50 iterations on a scaled-up graph of 1,000 shipments, 10,000 friendship edges, and 15 cycle nodes):
1. **Cold (Cache Miss)**: First execution where constraints are checked and the query plan is rewritten.
2. **Hot (Cache Hit)**: Subsequent executions retrieving the pre-optimized query plan directly from the cache.
3. **Unoptimized (Bypassed)**: Execution where semantic optimization is explicitly skipped via `NO_SEMANTIC`.

| Optimization Pass | Cold (Cache Miss) | Hot (Cache Hit) | Unoptimized (Bypassed) | Speedup (Hot vs Unopt) |
|---|---|---|---|---|
| **Phase 1: Contradiction Pruning** | 0.1369 ms | 0.0076 ms | 0.0963 ms | **12.7x** |
| **Phase 2: Join Elimination** | 9.0763 ms | 8.8437 ms | 38.6477 ms | **4.4x** |
| **Phase 3: Relational Cycle Pruning** | 0.1983 ms | 0.0163 ms | 15.3529 ms | **939.8x** |
| **Phase 4: Algebraic Sum Rewrite** | 8.1633 ms | 7.9157 ms | 17.0607 ms | **2.16x** |
| **Phase 4.5: Algebraic Path Count Rewrite** | 11.4990 ms | 11.4741 ms | 164.7380 ms | **14.35x** |
| **Phase 5: Cardinality Short-Circuit** | 5.5328 ms | 5.1012 ms | 355.4320 ms | **69.67x** |

### Key Performance Insights
* **Contradiction Pruning (Phases 1 & 3)**: Pruning query execution trees at compile-time when constraints are violated avoids unnecessary database scans and filters. Relational cycle pruning (Phase 3) short-circuits Cartesian product traversal completely, leading to a **930x+ speedup**.
* **Join Elimination (Phase 2)**: Bypassing sharded join hops to `Location` nodes across 1,000 active shipments saves physical networking and index lookup overhead, cutting traversal execution latency from 38.65 ms to 8.84 ms (**4.4x speedup**).
* **Algebraic Rewrite (Phase 4)**: Factorization pushes independent variables out of the sum aggregation across 10,000 friendship edges. This avoids performing **9,995 property lookups** and **9,995 multiplication operations**, saving **9.14 ms** of CPU execution time per query (**2.16x speedup**).
* **Algebraic Path Count Rewrite (Phase 4.5)**: Rewriting a 3-hop path count query into iterative degree propagation bypasses path/walk expansion completely. For 10,000 paths across 3 hops, this reduces intermediate rows from **10,000** to **5** (one per starting person), avoiding join allocation, traversal state management, and projection overhead. This results in a massive **14.3x speedup**, slashing execution latency from 164.74 ms to 11.47 ms (**saving 153.26 ms**).
* **Cardinality Short-Circuit (Phase 5)**: Bypassing the traversal of excess outgoing edges when the schema catalog guarantees a cardinality limit (e.g., a shipment having at most 1 origin warehouse) avoids remote peered shard lookups for the remaining neighbors. For a node with 2,000 relationships in the test graph, this cuts the active traversal branch size to 1, leading to a **69.67x speedup**, slashing latency from 355.43 ms to 5.10 ms.

---

## 5. Modular Optimizer Architecture

To maintain code readability and extensibility as more optimization rules are introduced, the monolithic `GqlOptimizer.cpp` is divided into separate optimization passes within the [optimizer](file:///home/maxdemarzi/ragedb/src/gql/optimizer) directory.

### Directory Layout & Component Roles

1. **[OptimizerUtils](file:///home/maxdemarzi/ragedb/src/gql/optimizer/OptimizerUtils.h)**:
   - Shared internal structures such as `Interval` (representing bounds over ordered sets) and `VarInfo` (representing label, intervals, and degree optimizations per variable).
   - Utility functions for AST inspection, variable mapping collection, label subsumption check, and value evaluation.
2. **[ContradictionPruner](file:///home/maxdemarzi/ragedb/src/gql/optimizer/ContradictionPruner.h)**:
   - *Phase 1 (Contradiction Pruning)*: Checks single-variable bounding intervals against schema catalog impossible regions.
   - *Phase 3 (Poset Relational Cycle Pruning)*: Builds a directed inequality graph over multivariable relational filters and runs Floyd-Warshall cycle detection to prove unsatisfiability.
3. **[JoinEliminator](file:///home/maxdemarzi/ragedb/src/gql/optimizer/JoinEliminator.h)**:
   - *Phase 2 (Join Elimination)*: Inspects mandatory relationship schema catalog constraints and prunes redundant structural joins where target variables are dead ends.
4. **[AlgebraicRewriter](file:///home/maxdemarzi/ragedb/src/gql/optimizer/AlgebraicRewriter.h)**:
   - *Phase 4 (Algebraic Sum Rewrite)*: Applies distributivity by factoring terms out of summation operators.
   - *Phase 4.5 (Algebraic Path Count Rewrite)*: Detects multi-hop path traversal count expressions and rewrites them into linear-time degree sum equations.
5. **[CardinalityShortCircuiter](file:///home/maxdemarzi/ragedb/src/gql/optimizer/CardinalityShortCircuiter.h)**:
   - *Phase 5 (Cardinality Short-Circuiting)*: Resolves schema cardinality upper bounds (1-to-1 or N-to-1) to inject early traversal termination limits on match steps.
6. **[SubsumptionPruner](file:///home/maxdemarzi/ragedb/src/gql/optimizer/SubsumptionPruner.h)**:
   - *Phase 6 (Subsumption / Query Containment)*: Identifies duplicate or redundant isomorphic query traversal paths originating from the same query node variable, pruning them if their constraints are completely subsumed by another path.
