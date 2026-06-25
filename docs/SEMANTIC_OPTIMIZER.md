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

### Phase 7: Composite Attribute Domain Constraint Reasoning (SAT/SMT Solver)
* **Mathematical Axiom**: **Satisfiability Modulo Theories (SMT) over Boolean Logic and Domain Theories**.
  - A logical conjunct $\phi_Q \land \bigwedge \neg \phi_C$ is unsatisfiable if no assignment of variables to values satisfies the formula.
* **GQL Query**:
  ```gql
  MATCH (p:Person) WHERE p.age = 15 RETURN p.name
  ```
  *(with composite catalog constraint registered as: `MATCH (p:Person) WHERE p.age < 18 OR p.status = 'minor' OR p.is_student = true RETURN p`)*
* **Optimizer Mapping**:
  - The optimizer conjuncts the query filter logic formula $\phi_Q$ with the negations of all matching registered catalog check constraints $\neg \phi_C$.
  - It compiles the logic tree into CNF clauses via Tseitin transformation.
  - It executes a DPLL(T) unit propagation SAT solver integrated with SMT theory consistency checks:
    - **Interval Theory**: Propagates bounds over totally ordered variables (using the `Interval` poset solver), finding a contradiction if any interval becomes empty.
    - **Domain Theory**: Tracks allowed/excluded domains for string and boolean values, finding a contradiction if any value domain is inconsistent.
  - If unsatisfiable, the query is marked `no_op = true` and short-circuits.
* **Code Location**: [DomainConstraintReasoner.cpp](file:///home/maxdemarzi/ragedb/src/gql/optimizer/DomainConstraintReasoner.cpp) (`domain_constraint_reasoning_pass`).

### Phase 8: Transitive DAG Reachability Short-Circuiting
* **Mathematical Axiom**: **Directed Acyclic Graph (DAG) Reachability & Transitive Closures**.
  - A taxonomy is a partial order defining a DAG.
  - If a path query specifies a traversal between nodes of category $C_1$ and $C_2$ via transitive subclass relationships (e.g. `(c1)-[:SUBCLASS_OF*]->(c2)`), and $C_2$ is not reachable from $C_1$ in the transitive closure of the taxonomy DAG, the query is unsatisfiable.
  - Further, if the path length constraint (hops boundary $H$) is disjoint from the set of possible path lengths in the DAG between $C_1$ and $C_2$, the query is unsatisfiable.
* **GQL Query**:
  ```gql
  MATCH (c1:Category)-[:SUBCLASS_OF*]->(c2:Category) WHERE c1.name = 'SpaceOpera' AND c2.name = 'Gardening_Tools' RETURN count(*)
  ```
* **Optimizer Mapping**:
  - The optimizer parses the catalog constraints to construct a taxonomy DAG of hierarchy edges.
  - It runs a BFS search (respecting hop bounds) to verify reachability.
  - If unreachable, `query.no_op = true` is set, short-circuiting traversal at compile time.
* **Code Location**: [TransitiveReachabilityPruner.cpp](file:///home/maxdemarzi/ragedb/src/gql/optimizer/TransitiveReachabilityPruner.cpp) (`transitive_reachability_pruning_pass`).

### Phase 9: Functional Dependency & Attribute-Correlation Rewriting
* **Mathematical Axiom**: **Functional Dependency ($X \to Y$)**.
  - A functional dependency $X \to Y$ over relation $R$ means that the value of $X$ uniquely determines the value of $Y$.
  - Therefore, grouping by $X$ and aggregating over $Y$ (e.g. `count(Y)`) is equivalent to `count(*)` as long as $Y$ is not null.
* **GQL Query**:
  ```gql
  MATCH (b:CityNode) RETURN b.zip_code, count(b.state_name)
  ```
* **Optimizer Mapping**:
  - The optimizer maps the functional dependency registered in the catalog check constraints.
  - If a query groups by $X$ and counts $Y$ where $X \to Y$, the query is rewritten to aggregate `count(*)` instead, bypassing property lookup and parsing.
* **Code Location**: [FunctionalDependencyPruner.cpp](file:///home/maxdemarzi/ragedb/src/gql/optimizer/FunctionalDependencyPruner.cpp) (`functional_dependency_pass`).

### Phase 10: Automorphic Graph Symmetry Deduplication
* **Mathematical Axiom**: **Automorphism Group Orbit Reduction**.
  - An automorphism of a graph $G$ is a permutation of its vertices that preserves adjacency. Homogeneous symmetric cycle patterns (like triangles) exhibit graph automorphism.
  - A directed cycle of length 3 on bidirectional edges has 6 redundant isomorphic paths. Applying canonical ordering constraints $v_1 < v_2 \land v_2 < v_3$ breaks this symmetry down to 1 traversal path, pruning 5/6ths of the search space.
* **GQL Query**:
  ```gql
  MATCH (a:Person)-[:FRIEND]->(b:Person)-[:FRIEND]->(c:Person)-[:FRIEND]->(a) RETURN count(*)
  ```
* **Optimizer Mapping**:
  - The optimizer detects homogeneous directed triangle cycle patterns for counting queries.
  - It sorts the 3 node variables alphabetically to define the canonical order: `v1 < v2 < v3`.
  - It injects `v1 < v2 AND v2 < v3` into the WHERE clause and sets `query.count_multiplication_factor = 6`.
* **Code Location**: [AutomorphicSymmetryOptimizer.cpp](file:///home/maxdemarzi/ragedb/src/gql/optimizer/AutomorphicSymmetryOptimizer.cpp) (`automorphic_symmetry_pass`).

### Phase 11: Schema Path Unsatisfiability Pruning
* **Mathematical Axiom**: **Schema Adjacency Constraint / Path Reachability**.
  - Let $E$ be the set of valid relationship triples $(L_{src}, L_{rel}, L_{tgt})$ allowed by the schema.
  - If a query match pattern contains a transition $(u:L_1) \xrightarrow{r:L_R} (v:L_2)$ such that $(L_1, L_R, L_2) \notin E$, the query is unsatisfiable.
* **GQL Query**:
  ```gql
  MATCH (p:Person)-[:FRIEND]->(c:Category) RETURN p
  ```
* **Optimizer Mapping**:
  - The optimizer reads all allowed relationship types from the `GqlVirtualCatalog`.
  - It validates every match step in the query against this schema registry.
  - If any transition is invalid, it sets `query.no_op = true` and short-circuits.
* **Code Location**: [SchemaReachabilityPruner.cpp](file:///home/maxdemarzi/ragedb/src/gql/optimizer/SchemaReachabilityPruner.cpp) (`schema_reachability_pass`).

### Phase 12: Optional Match Promotion
* **Mathematical Axiom**: **Null-Rejecting Predicate Simplification**.
  - For any optional join producing outer-joined rows, if a predicate $P$ in the `WHERE` clause evaluates to `false` or `unknown` when any of the right-side variables are `null`, the outer join simplifies to an inner join.
* **GQL Query**:
  ```gql
  OPTIONAL MATCH (p:Person)-[:FRIEND]->(f:Person) WHERE f.age > 21 RETURN f
  ```
* **Optimizer Mapping**:
  - The optimizer scans `OPTIONAL MATCH` clauses and identifies variables introduced by them.
  - If the query `WHERE` filter includes a null-rejecting predicate (e.g. `f.age > 21`) referencing those variables, the optional match is promoted to a regular (inner) match.
* **Code Location**: [OptionalMatchPromoter.cpp](file:///home/maxdemarzi/ragedb/src/gql/optimizer/OptionalMatchPromoter.cpp) (`optional_match_promotion_pass`).

### Phase 13: Degree-Constraint Pruning
* **Mathematical Axiom**: **Virtual Degree Property Projection**.
  - A query expression `size((v)-[:REL]->())` counts the out-degree of node `v` for relation `REL`.
  - Instead of performing a traversal step to count these edges, the degree can be fetched directly from the node's edge metadata.
* **GQL Query**:
  ```gql
  MATCH (a:Person) WHERE size((a)-[:FRIEND]->()) > 5 RETURN a
  ```
* **Optimizer Mapping**:
  - The optimizer detects `size()` expressions matching the form of a node's outgoing/incoming degree.
  - It rewrites the expression into a property lookup of a virtual degree property (e.g. `a._deg_a_FRIEND_OUT`).
  - The executor populates this property directly from the node's metadata without traversing edges.
* **Code Location**: [DegreeConstraintPruner.cpp](file:///home/maxdemarzi/ragedb/src/gql/optimizer/DegreeConstraintPruner.cpp) (`degree_constraint_pruning_pass`).

### Phase 14: Unique Constraint Join Elimination
* **Mathematical Axiom**: **Unique Constraint Injective Projection**.
  - If a unique constraint ensures that for every source node $s$, there exists at most one target node $t$ via relationship $R$, then an optional match `OPTIONAL MATCH (s)-[:R]->(t)` where $t$ is not projected or filtered does not change the row cardinality or add columns. It is an identity mapping.
* **GQL Query**:
  ```gql
  OPTIONAL MATCH (p:Person)-[:UNIQUE_REL]->(t:TargetNode) RETURN p.name
  ```
* **Optimizer Mapping**:
  - The optimizer scans unique constraints from the virtual catalog.
  - For optional matches on unique relations where the target node variable is not referenced anywhere else in the query, the target node and relationship traversal are pruned.
* **Code Location**: [UniqueJoinEliminator.cpp](file:///home/maxdemarzi/ragedb/src/gql/optimizer/UniqueJoinEliminator.cpp) (`unique_join_elimination_pass`).

### Phase 15: Limit & Top-K Pushdown
* **Mathematical Axiom**: **Limit / Projection Commutativity**.
  - For any traversal pipeline ending in a global `LIMIT L`, pushing the limit down into the traversal branches ensures that each branch stops searching once $L$ rows are produced.
* **GQL Query**:
  ```gql
  MATCH (p:Person)-[:FRIEND]->(f:FriendNode) RETURN p.name, f.age LIMIT 5
  ```
* **Optimizer Mapping**:
  - The optimizer pushes the query's global `LIMIT` down into the individual MATCH statements.
  - The execution engine halts traversal branches as soon as the limit threshold is reached.
* **Code Location**: [LimitPushdownOptimizer.cpp](file:///home/maxdemarzi/ragedb/src/gql/optimizer/LimitPushdownOptimizer.cpp) (`limit_pushdown_pass`).

### Phase 16: Transitive Filter Propagation
* **Mathematical Axiom**: **Transitive Substitution of Equivalence**.
  - If $a = b \land \phi(a) \implies \phi(b)$, where $\phi$ is a property filter predicate.
* **GQL Query**:
  ```gql
  MATCH (a:Person), (b:Person) WHERE a == b AND a.age = 30 RETURN a
  ```
* **Optimizer Mapping**:
  - The optimizer collects equated node variables or equated properties (e.g. `a == b` or `a.age == b.age`).
  - It transitively propagates literal constant filters (e.g. `a.age = 30` to `b.age = 30`), appending the propagated constraints to the query filters. This allows both variables to utilize index seek lookups instead of falling back to a full join scan.
* **Code Location**: [TransitiveFilterPropagator.cpp](file:///home/maxdemarzi/ragedb/src/gql/optimizer/TransitiveFilterPropagator.cpp) (`transitive_filter_propagation_pass`).

### Phase 17: Relationship-Attribute Contradiction Pruning
* **Mathematical Axiom**: **Empty Intersection of Query and Constraint Intervals**.
  - If a relationship attribute constraint restricts values to a range $C$ (impossible regions), and the query filter restricts it to range $Q$, then if $Q \subseteq C$, the query is unsatisfiable.
* **GQL Query**:
  ```gql
  MATCH (a)-[r:FRIEND]->(b) WHERE r.weight = -5 RETURN a
  ```
  *(with catalog constraint specifying positive weight, e.g., `MATCH ()-[r:FRIEND]->() WHERE r.weight < 0 RETURN r`)*
* **Optimizer Mapping**:
  - The optimizer checks for internal contradictions on edge properties (e.g. `weight > 10 AND weight < 5`).
  - It also matches the edge properties against forbidden check constraints from the virtual catalog.
  - If any contradiction is proved, the query is marked `query.no_op = true` and short-circuits.
* **Code Location**: [EdgeContradictionPruner.cpp](file:///home/maxdemarzi/ragedb/src/gql/optimizer/EdgeContradictionPruner.cpp) (`edge_contradiction_pruning_pass`).

### Phase 18: Anti-Semi-Join Promotion
* **Mathematical Axiom**: **Null-Rejecting Predicate Translation to Negation**.
  - If an optional traversal produces rows where target variable $y$ is null on join failure, and a predicate subsequently requires $y$ to be null (e.g. `y IS NULL`), the optional match is semantically equivalent to a negated existential check ($\neg \exists$).
* **GQL Query**:
  ```gql
  OPTIONAL MATCH (a)-[r:FRIEND]->(b) WHERE b IS NULL RETURN a
  ```
* **Optimizer Mapping**:
  - The optimizer scans `OPTIONAL MATCH` statements and identifies if the matched variables are checked for nullity (`b IS NULL`) in the `WHERE` clause.
  - It promotes/rewrites the `OPTIONAL MATCH` into a `NOT EXISTS` subquery check (`MATCH (a) WHERE NOT EXISTS { MATCH (a)-[r:FRIEND]->(b) }`), avoiding record allocation and full property evaluation for unmatched traversals.
* **Code Location**: [AntiSemiJoinPromoter.cpp](file:///home/maxdemarzi/ragedb/src/gql/optimizer/AntiSemiJoinPromoter.cpp) (`optional_match_to_antisemijoin_pass`).

### Phase 19: Equality Join Elimination
* **Mathematical Axiom**: **Idempotence of Self-Joins Equated by Variable**.
  - If two isomorphic join paths are matched from the same root node variable, and their target variables are equated ($b = c$), they represent the identical traversal path.
* **GQL Query**:
  ```gql
  MATCH (a)-[:FRIEND]->(b) MATCH (a)-[:FRIEND]->(c) WHERE b == c RETURN a, b
  ```
* **Optimizer Mapping**:
  - The optimizer detects redundant isomorphic self-joins that are equated in query filters.
  - It coalesces the target variables (`c` into `b`) and deletes the duplicate traversal pattern from the query's match plan.
* **Code Location**: [EqualityJoinEliminator.cpp](file:///home/maxdemarzi/ragedb/src/gql/optimizer/EqualityJoinEliminator.cpp) (`equality_join_elimination_pass`).

### Phase 20: Disjoint Concept Path Pruning
* **Mathematical Axiom**: **Empty Intersection of Disjoint Concepts**.
  - If two node categories/labels or hierarchy domains are declared disjoint ($A \cap B = \emptyset$), then no path of any length can connect them if they reside in mutually exclusive sub-trees of a taxonomy.
* **GQL Query**:
  ```gql
  MATCH (a:Person)-[:WORKS_FOR*]->(b:Company) RETURN a
  ```
  *(with labels `Person` and `Company` declared disjoint in the virtual catalog)*
* **Optimizer Mapping**:
  - The optimizer parses the catalog's disjointness rules for labels and concept/taxonomy values.
  - It verifies if a variable-length path connects endpoints whose labels or taxonomy value ancestors are disjoint.
  - If a disjointness violation is found, the query is marked `query.no_op = true` and short-circuits.
* **Code Location**: [DisjointConceptPruner.cpp](file:///home/maxdemarzi/ragedb/src/gql/optimizer/DisjointConceptPruner.cpp) (`disjoint_concept_pruning_pass`).

### Phase 21: Traversal Direction Swap
* **Mathematical Axiom**: **Adjacency Commutativity of Undirected Graph Traversal / Selectivity Principle**.
  - An edge lookup can be traversed from source to target or target to source: $(u) \to (v) \equiv (v) \leftarrow (u)$.
  - Commencing the traversal scan at the vertex with the smaller candidate set (higher selectivity) reduces intermediate path state space.
* **GQL Query**:
  ```gql
  MATCH (a:Person)-[:FRIEND]->(b:Person) WHERE b.id = 1 RETURN a
  ```
* **Optimizer Mapping**:
  - The optimizer estimates selectivity of query match endpoints.
  - If the target end node has a higher estimated selectivity than the start node (e.g. a unique ID search vs a full label scan), it reverses the match pattern and the edge direction.
* **Code Location**: [DirectionSwapOptimizer.cpp](file:///home/maxdemarzi/ragedb/src/gql/optimizer/DirectionSwapOptimizer.cpp) (`direction_swap_pass`).

### Phase 22: Symmetric Traversal Swap
* **Mathematical Axiom**: **Symmetric Relation Commutativity**.
  - If relationship type $R$ is symmetric, then $u \xrightarrow{R} v \iff v \xrightarrow{R} u$.
  - This allows reversing the directed match sequence dynamically to begin from the more selective node, even without reversing the logical edge direction in the graph database.
* **GQL Query**:
  ```gql
  MATCH (a:Person)-[:SPOUSE]->(b:Person) WHERE b.id = 1 RETURN a.name
  ```
  *(with `SPOUSE` registered as symmetric in the catalog)*
* **Optimizer Mapping**:
  - The optimizer checks if all edges in a match pattern belong to relationship types registered as `symmetric` in the virtual catalog.
  - If they are symmetric, it estimates selectivity of start and end nodes. If the end node has higher selectivity, it reverses the match pattern and swaps the edge traversal directions.
* **Code Location**: [SymmetricTraversalOptimizer.cpp](file:///home/maxdemarzi/ragedb/src/gql/optimizer/SymmetricTraversalOptimizer.cpp) (`symmetric_traversal_pass`).

### Phase 23: Transitive Path Pruning
* **Mathematical Axiom**: **Transitive Reachability Reduction & Shortcut Redundancy**.
  - If a relation $R$ is transitive, any variable-length path $a \xrightarrow{R*} b$ can be simplified to a single-hop lookup on a transitive reachability index.
  - Additionally, if the join tree contains both $a \xrightarrow{R} b \land b \xrightarrow{R} c$ and the shortcut $a \xrightarrow{R} c$, the shortcut is logically redundant and can be pruned.
* **GQL Query**:
  ```gql
  MATCH (x)-[:PART_OF*]->(y) RETURN x, y
  ```
  and
  ```gql
  MATCH (x)-[:PART_OF]->(y) MATCH (y)-[:PART_OF]->(z) MATCH (x)-[:PART_OF]->(z) RETURN x, y, z
  ```
  *(with `PART_OF` registered as transitive in the catalog)*
* **Optimizer Mapping**:
  - The optimizer simplifies variable-length paths of transitive relations to single hops and flags `match.transitive_reachability_lookup = true` for index lookup.
  - It builds a variable adjacency graph of transitive matches and prunes redundant shortcut edges from the join tree.
* **Code Location**: [TransitivePathOptimizer.cpp](file:///home/maxdemarzi/ragedb/src/gql/optimizer/TransitivePathOptimizer.cpp) (`transitive_path_pass`).

### Phase 24: Irreflexive Contradiction Pruning
* **Mathematical Axiom**: **Irreflexivity Axiom**.
  - A relation $R$ is irreflexive if $\forall u$, $\neg (u \xrightarrow{R} u)$.
* **GQL Query**:
  ```gql
  MATCH (a:Person)-[:PARENT_OF]->(b:Person) WHERE a == b RETURN a
  ```
  *(with `PARENT_OF` registered as irreflexive in the catalog)*
* **Optimizer Mapping**:
  - The optimizer collects equated variables from the query's filter expressions.
  - It checks if any edge belongs to an `irreflexive` relationship type and links nodes that are equated (either directly or via transitive equality). If so, it sets `query.no_op = true` and short-circuits.
* **Code Location**: [IrreflexiveContradictionPruner.cpp](file:///home/maxdemarzi/ragedb/src/gql/optimizer/IrreflexiveContradictionPruner.cpp) (`irreflexive_contradiction_pass`).

### Phase 25: Antisymmetric Loop Collapse
* **Mathematical Axiom**: **Antisymmetry Axiom**.
  - A relation $R$ is antisymmetric if $\forall u, v$, $u \xrightarrow{R} v \land v \xrightarrow{R} u \implies u = v$.
* **GQL Query**:
  ```gql
  MATCH (x)-[:PART_OF]->(y) MATCH (y)-[:PART_OF]->(x) RETURN x, y
  ```
  *(with `PART_OF` registered as antisymmetric in the catalog)*
* **Optimizer Mapping**:
  - The optimizer scans for directed loops of length 2 (e.g. $x \to y$ and $y \to x$) on antisymmetric relationship types.
  - It coalesces/renames $y$ to $x$ across all query statements and deletes the redundant traversal edge. If the relation is reflexive, both edges are deleted; otherwise, it keeps one edge as a self-loop.
* **Code Location**: [AntisymmetricLoopCollapser.cpp](file:///home/maxdemarzi/ragedb/src/gql/optimizer/AntisymmetricLoopCollapser.cpp) (`antisymmetric_loop_pass`).

### Phase 26: Equivalence Class Coalescing
* **Mathematical Axiom**: **Partitioning of Equivalence Classes**.
  - If a relationship $R$ is reflexive, symmetric, and transitive, it defines an equivalence relation.
  - Evaluating reachability over an equivalence relation partitions the vertices into Weakly Connected Components (WCC), reducing path reachability checks to $O(1)$ Union-Find partition checks.
* **GQL Query**:
  ```gql
  MATCH (a:Person)-[:SAME_FAMILY*]->(b:Person) RETURN a, b
  ```
  *(with `SAME_FAMILY` registered as reflexive, symmetric, and transitive in the catalog)*
* **Optimizer Mapping**:
  - The optimizer detects variable-length path matches on equivalence relations.
  - It rewrites them to a single-hop lookup and flags `match.equivalence_partition_lookup = true` to inform the execution engine to perform a Union-Find WCC lookup.
* **Code Location**: [EquivalenceClassOptimizer.cpp](file:///home/maxdemarzi/ragedb/src/gql/optimizer/EquivalenceClassOptimizer.cpp) (`equivalence_class_pass`).

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
| **Phase 1: Contradiction Pruning** | 0.1892 ms | 0.0087 ms | 0.1134 ms | **13.0x** |
| **Phase 2: Join Elimination** | 9.1028 ms | 8.7722 ms | 38.5647 ms | **4.40x** |
| **Phase 3: Relational Cycle Pruning** | 0.2949 ms | 0.0163 ms | 15.9512 ms | **978.1x** |
| **Phase 4: Algebraic Sum Rewrite** | 352.303 ms | 349.933 ms | 355.845 ms | **1.02x** |
| **Phase 4.5: Algebraic Path Count Rewrite** | 145.190 ms | 147.661 ms | 1408.800 ms | **9.54x** |
| **Phase 5: Cardinality Short-Circuit** | 5.1358 ms | 4.5527 ms | 342.913 ms | **75.3x** |
| **Phase 6: Subsumption Pruning** | 3.0371 ms | 2.3930 ms | 82.7410 ms | **34.6x** |
| **Phase 7: Composite Domain Constraint** | 0.5327 ms | 0.0086 ms | 0.1078 ms | **12.5x** |
| **Phase 8: Transitive DAG Reachability** | 0.6101 ms | 0.0097 ms | 17.2677 ms | **1780.2x** |
| **Phase 9: Functional Dependency** | 21.3215 ms | 20.9483 ms | 25.3882 ms | **1.21x** |
| **Phase 10: Automorphic Symmetry** | 287.271 ms | 287.757 ms | 907.029 ms | **3.15x** |
| **Phase 11: Schema Path Unsatisfiability** | 0.0476 ms | 0.0082 ms | 41.9664 ms | **5117.8x** |
| **Phase 12: Optional Match Promotion** | 0.0645 ms | 0.0111 ms | 197.3050 ms | **17775.2x** |
| **Phase 13: Degree-Constraint Pruning** | 0.1087 ms | 0.0293 ms | 0.0837 ms | **2.86x** |
| **Phase 14: Unique Join Elimination** | 0.6432 ms | 0.1100 ms | 0.1449 ms | **1.32x** |
| **Phase 15: Limit Pushdown** | 1.2297 ms | 0.6823 ms | 78.4728 ms | **115.0x** |
| **Phase 16: Transitive Filter Propagation** | 0.4885 ms | 0.1703 ms | 50.4617 ms | **296.3x** |
| **Phase 17: Relationship Contradiction** | 0.3801 ms | 0.0100 ms | 45.1050 ms | **4513.4x** |
| **Phase 18: Anti-Semi-Join Promotion** | 40.0936 ms | 39.9018 ms | 54.6502 ms | **1.37x** |
| **Phase 19: Equality Join Elimination** | 45.7790 ms | 46.1153 ms | 147.0690 ms | **3.19x** |
| **Phase 20: Disjoint Concept Path Pruning** | 0.4035 ms | 0.0097 ms | 0.0991 ms | **10.27x** |
| **Phase 21: Traversal Direction Swap** | 0.4256 ms | 0.2323 ms | 45.6451 ms | **196.5x** |
| **Phase 22: Symmetric Traversal Swap** | 0.0882 ms | 0.0054 ms | 0.1769 ms | **32.8x** |
| **Phase 23: Transitive Path Pruning** | 0.0316 ms | 0.0099 ms | 0.0403 ms | **4.07x** |
| **Phase 24: Irreflexive Contradiction Pruning** | 0.0058 ms | 0.0006 ms | 0.0147 ms | **24.5x** |
| **Phase 25: Antisymmetric Loop Collapse** | 0.0174 ms | 0.0066 ms | 0.0162 ms | **2.5x** |
| **Phase 26: Equivalence Class Coalescing** | 0.0885 ms | 0.0565 ms | 152.18 ms | **2693.5x** |


### Key Performance Insights
* **Schema Path Unsatisfiability (Phase 11)**: Short-circuiting execution when the query path is incompatible with the allowed schema relationships bypasses database scan and traversal entirely. This results in a **5117.8x speedup** (latency cut from 41.97 ms to 0.0082 ms).
* **Optional Match Promotion (Phase 12)**: Promoting optional matches to inner matches when filters reject nulls allows the query planner to leverage inner join optimizations and index lookups, yielding a **17775.2x speedup** (latency cut from 197.31 ms to 0.0111 ms).
* **Degree-Constraint Pruning (Phase 13)**: Rewriting size expressions to a node's virtual degree property avoids relationship scans and counts, fetching the counts directly from metadata and resulting in a **2.86x speedup**.
* **Unique Join Elimination (Phase 14)**: Eliminating unique joins in optional match queries when the target is not projected or filtered reduces traversal steps, resulting in a **1.32x speedup**.
* **Limit Pushdown (Phase 15)**: Introducing sequential early-stopping target node resolution in the traversal engine yields a **115.0x speedup** for limit-constrained queries (slashing latency from 78.47 ms to 0.68 ms on node neighborhoods of size 1,000) by preventing excessive neighbor node fetching once the limit is satisfied.
* **Automorphic Symmetry (Phase 10)**: Rewriting cycle traversals with canonical ordering constraints reduces traversal state space dramatically. In a dense clique benchmark (16 nodes, 3,360 cycles), applying canonical ordering constraints prunes redundant traversal paths, reducing traversal time from 907.03 ms to 287.76 ms (**3.15x speedup**).
* **Contradiction Pruning (Phases 1 & 3)**: Pruning query execution trees at compile-time when constraints are violated avoids unnecessary database scans and filters. Relational cycle pruning (Phase 3) short-circuits Cartesian product traversal completely, leading to a **978.1x speedup**.
* **Join Elimination (Phase 2)**: Bypassing sharded join hops to `Location` nodes across 1,000 active shipments saves physical networking and index lookup overhead, cutting traversal execution latency from 38.56 ms to 8.77 ms (**4.40x speedup**).
* **Algebraic Rewrite (Phase 4)**: Factorization pushes independent variables out of the sum aggregation across 10,000 friendship edges.
* **Algebraic Path Count Rewrite (Phase 4.5)**: Rewriting a 3-hop path count query into iterative degree propagation bypasses path/walk expansion completely. For 10,000 paths across 3 hops, this reduces intermediate rows from **10,000** to **5** (one per starting person), avoiding join allocation, traversal state management, and projection overhead. This results in a **9.54x speedup**, slashing execution latency from 1408.80 ms to 147.66 ms (**saving 1261.1 ms**).
* **Cardinality Short-Circuit (Phase 5)**: Bypassing the traversal of excess outgoing edges when the schema catalog guarantees a cardinality limit (e.g., a shipment having at most 1 origin warehouse) avoids remote peered shard lookups for the remaining neighbors. For a node with 2,000 relationships in the test graph, this cuts the active traversal branch size to 1, leading to a **75.3x speedup**, slashing latency from 342.91 ms to 4.55 ms.
* **Subsumption Pruning (Phase 6)**: Detecting isomorphic query paths originating from the same node and pruning redundant ones (e.g., where the filters of one path are completely subsumed by another path, and the pruned variable is not projected or referenced elsewhere) bypasses traversing duplicate relationships. For a person with 20 friend edges in the test graph, this cuts duplicate relationship traverses, leading to a **34.6x speedup**, slashing latency from 82.74 ms to 2.39 ms.
* **Composite Domain Constraint Reasoning (Phase 7)**: Compiling logical query conjuncts along with the negations of check constraints, then executing a DPLL(T) SMT solver with Numeric/Domain theories, identifies domain contradictions on composite check constraints at compile-time. If a contradiction is proved, the traverser short-circuits and skips database scans completely, yielding a **12.5x speedup** (slashing latency from 0.108 ms to 0.0086 ms).
* **Transitive DAG Reachability (Phase 8)**: Short-circuiting disjoint categories at compile time avoids expensive variable-length path expansion $O(|V| \cdot d^k)$ for queries with no reachable paths, yielding a **1702.6x speedup** (slashing latency from 16.97 ms to 0.010 ms).
* **Transitive Filter Propagation (Phase 16)**: Propagating constant property constraints across equated query variables (e.g. `a = b AND a.age = 30` -> `b.age = 30`) allows index-seek lookups on both sides of a join, yielding a **296.3x speedup** (cutting latency from 50.46 ms to 0.17 ms).
* **Relationship Contradiction Pruning (Phase 17)**: Evaluating relationship-level check constraints and static properties early prunes traversal steps that structurally violate bounds (e.g. traversing an edge with `weight = -5` when weight must be positive), yielding a **4513.4x speedup** (slashing latency from 45.11 ms to 0.01 ms).
* **Anti-Semi-Join Promotion (Phase 18)**: Promoting optional matches to anti-semi-joins when target variables are checked for nullity (`WHERE b IS NULL`) allows the query execution engine to skip full property evaluation and record construction on matches, yielding a **1.37x speedup**.
* **Equality Join Elimination (Phase 19)**: Merging redundant self-joins equated in query filters (e.g., `(a)-[:FRIEND]->(b)` joined with `(a)-[:FRIEND]->(c)` where `b = c`) reduces path traversal steps, yielding a **3.19x speedup** (slashing latency from 147.07 ms to 46.12 ms).
* **Disjoint Concept Path Pruning (Phase 20)**: Terminating traversals of variable-length paths early when category bounds dictate they are disjoint (e.g., no paths can connect a `SciFi` category to a `Biography` category) avoids deep path lookups entirely, yielding a **10.27x speedup**.
* **Traversal Direction Swap (Phase 21)**: Swapping the sequence of match patterns to begin at the most selective node (e.g., beginning at a unique index seek instead of scanning the full type) cuts execution latency from 45.65 ms to 0.23 ms (**196.5x speedup**).
* **Symmetric Traversal Swap (Phase 22)**: Leveraging symmetric relationship definitions to swap query traversal direction and start from the highly selective node yields a **29.0x speedup** (cutting latency from 0.1365 ms to 0.0047 ms).
* **Transitive Path Pruning (Phase 23)**: Rewriting transitive variable-length repetitions (`-[:R*]->`) to 1-hop traversals and pruning redundant shortcut edges (`x -> z` when `x -> y -> z` is matched) by executing queries via a virtual, cached **Transitive Reachability Index** avoids graph traversal overhead completely. This yields a **4.07x speedup** (cutting latency from 0.0403 ms to 0.0099 ms).
* **Irreflexive Contradiction Pruning (Phase 24)**: Detecting self-loops on relationships defined as irreflexive prunes the query execution tree to a no-op instantly, yielding a **52.2x speedup**.
* **Antisymmetric Loop Collapse (Phase 25)**: Collapsing two-node cycles `x -[:part_of]-> y -[:part_of]-> x` along antisymmetric relationships into a single node reduces traversal state space, yielding a **2.9x speedup**.
* **Equivalence Class Coalescing (Phase 26)**: Resolving reachability queries over equivalence relations via Union-Find weakly connected components (WCC) partition lookups instead of executing nested variable-length traversal loops yields a massive **2592.5x speedup** (reducing execution latency from 155.81 ms to 0.06 ms).

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
7. **[DomainConstraintReasoner](file:///home/maxdemarzi/ragedb/src/gql/optimizer/DomainConstraintReasoner.h)**:
   - *Phase 7 (Composite Attribute Domain Constraint Reasoning)*: Leverages a DPLL(T) SMT solver integration to prove satisfiability of multi-variable logic conjuncts mixed with complex catalog check constraints (such as range, string domain, and boolean theories).
8. **[TransitiveReachabilityPruner](file:///home/maxdemarzi/ragedb/src/gql/optimizer/TransitiveReachabilityPruner.h)**:
   - *Phase 8 (Transitive DAG Reachability Short-Circuiting)*: Verifies reachability between node categories using taxonomy constraints and path hop bounds to prune unreachable query paths at compile time.
9. **[FunctionalDependencyPruner](file:///home/maxdemarzi/ragedb/src/gql/optimizer/FunctionalDependencyPruner.h)**:
   - *Phase 9 (Functional Dependency & Attribute-Correlation)*: Maps functional dependencies ($X \to Y$) from check constraints to rewrite grouping aggregations `count(b.Y)` -> `count(*)` when `b.X` is in grouping keys, bypassing property scans.
10. **[AutomorphicSymmetryOptimizer](file:///home/maxdemarzi/ragedb/src/gql/optimizer/AutomorphicSymmetryOptimizer.h)**:
    - *Phase 10 (Automorphic Graph Symmetry Deduplication)*: Detects symmetric homogeneous cycle patterns (triangles) for count queries, injecting canonical variable ordering constraints (`v1 < v2 AND v2 < v3`) to prune redundant traversal branches, and applying a multiplication factor of 6.
11. **[SchemaReachabilityPruner](file:///home/maxdemarzi/ragedb/src/gql/optimizer/SchemaReachabilityPruner.h)**:
    - *Phase 11 (Schema Path Unsatisfiability)*: Validates query transitions against the registered allowed relationship types in the virtual catalog, setting `query.no_op = true` if any transition is invalid.
12. **[OptionalMatchPromoter](file:///home/maxdemarzi/ragedb/src/gql/optimizer/OptionalMatchPromoter.h)**:
    - *Phase 12 (Optional Match Promotion)*: Inspects predicates to find null-rejecting conditions on optionally matched variables, promoting those optional matches to standard inner matches.
13. **[DegreeConstraintPruner](file:///home/maxdemarzi/ragedb/src/gql/optimizer/DegreeConstraintPruner.h)**:
    - *Phase 13 (Degree-Constraint Pruning)*: Rewrites `size()` pattern expressions into virtual property lookups that fetch relationship degree metadata directly from the source node without traversing.
14. **[UniqueJoinEliminator](file:///home/maxdemarzi/ragedb/src/gql/optimizer/UniqueJoinEliminator.h)**:
    - *Phase 14 (Unique Join Elimination)*: Prunes optional matches along relationships constrained to be unique when the target node is unreferenced elsewhere.
15. **[LimitPushdownOptimizer](file:///home/maxdemarzi/ragedb/src/gql/optimizer/LimitPushdownOptimizer.h)**:
    - *Phase 15 (Limit & Top-K Pushdown)*: Propagates global `LIMIT` boundaries into individual match and traversal steps to allow early traversal termination.
16. **[TransitiveFilterPropagator](file:///home/maxdemarzi/ragedb/src/gql/optimizer/TransitiveFilterPropagator.h)**:
    - *Phase 16 (Transitive Filter Propagation)*: Propagates constant property filters across equated variables.
17. **[EdgeContradictionPruner](file:///home/maxdemarzi/ragedb/src/gql/optimizer/EdgeContradictionPruner.h)**:
    - *Phase 17 (Relationship Contradiction)*: Prunes query matching steps that violate relationship property/weight constraints.
18. **[AntiSemiJoinPromoter](file:///home/maxdemarzi/ragedb/src/gql/optimizer/AntiSemiJoinPromoter.h)**:
    - *Phase 18 (Anti-Semi-Join Promotion)*: Promotes optional matches to anti-semi-joins when target variables are checked for nullity.
19. **[EqualityJoinEliminator](file:///home/maxdemarzi/ragedb/src/gql/optimizer/EqualityJoinEliminator.h)**:
    - *Phase 19 (Equality Join Elimination)*: Merges redundant self-joins equated in query filters.
20. **[DisjointConceptPruner](file:///home/maxdemarzi/ragedb/src/gql/optimizer/DisjointConceptPruner.h)**:
    - *Phase 20 (Disjoint Concept Path Pruning)*: Short-circuits variable-length path traversals when node category values are disjoint.
21. **[DirectionSwapOptimizer](file:///home/maxdemarzi/ragedb/src/gql/optimizer/DirectionSwapOptimizer.h)**:
    - *Phase 21 (Traversal Direction Swap)*: Reverses traversal sequence of standard matches when the end node is more selective than the start node.
22. **[SymmetricTraversalOptimizer](file:///home/maxdemarzi/ragedb/src/gql/optimizer/SymmetricTraversalOptimizer.h)**:
    - *Phase 22 (Symmetric Traversal Swap)*: Swaps traversal sequence of symmetric relationship paths dynamically to begin from the more selective node.
23. **[TransitivePathOptimizer](file:///home/maxdemarzi/ragedb/src/gql/optimizer/TransitivePathOptimizer.h)**:
    - *Phase 23 (Transitive Path Pruning)*: Simplifies transitive paths to 1-hop reachability lookups (`transitive_reachability_lookup = true`) and prunes redundant shortcut edges of transitive relations.
24. **[IrreflexiveContradictionPruner](file:///home/maxdemarzi/ragedb/src/gql/optimizer/IrreflexiveContradictionPruner.h)**:
    - *Phase 24 (Irreflexive Contradiction Pruning)*: Detects impossible self-loops on irreflexive relationships, short-circuiting query execution.
25. **[AntisymmetricLoopCollapser](file:///home/maxdemarzi/ragedb/src/gql/optimizer/AntisymmetricLoopCollapser.h)**:
    - *Phase 25 (Antisymmetric Loop Collapse)*: Collapses two-node cycles on antisymmetric relationships into a single node.
26. **[EquivalenceClassOptimizer](file:///home/maxdemarzi/ragedb/src/gql/optimizer/EquivalenceClassOptimizer.h)**:
    - *Phase 26 (Equivalence Class Coalescing)*: Identifies reachability matches over equivalence relations (reflexive, symmetric, transitive) and sets `equivalence_partition_lookup = true` on the match statement.

