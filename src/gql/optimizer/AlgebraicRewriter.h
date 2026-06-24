/*
 * Copyright RageDB Contributors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef RAGEDB_ALGEBRAICREWRITER_H
#define RAGEDB_ALGEBRAICREWRITER_H

/**
 * @file AlgebraicRewriter.h
 * @brief Performs Phase 4 & Phase 4.5: Algebraic Query Rewrites.
 * 
 * Applies commutative semiring axioms to factor out independent terms and optimize multi-hop traversals.
 * 
 * 1. Phase 4: Commutative Semiring Distributivity sum(A * B) -> A * sum(B)
 *    Factors out terms that depend solely on grouping keys/variables from summation aggregations.
 *    Example:
 *      Query: MATCH (p:Person)-[:FRIEND]->(f:FriendNode) RETURN p.name, sum(p.age * f.age)
 *      Result: Rewritten to p.name, p.age * sum(f.age), reducing property access and multiplication overhead.
 * 
 * 2. Phase 4.5: Algebraic Path Count Rewrite
 *    Transforms exponential BFS expansion traversals for path/walk counts of length k (e.g. k-hop)
 *    into linear-time iterative degree-sum calculations.
 *    Example:
 *      Query: MATCH (p:Person)-[:FRIEND]->(a)-[:FRIEND]->(b)-[:FRIEND]->(f) RETURN p.name, count(f)
 *      Result: Truncates MATCH to (p:Person) and plans a step-wise Seastar-peered iterative degree propagation
 *              on the executor: v_m[u] = sum(v_{m-1}[v]), avoiding generating intermediate path rows.
 */

#include "../GqlAst.h"

namespace ragedb::gql {

class AlgebraicRewriter {
public:
    static void algebraic_rewriter_pass(GqlQuery& query);
};

} // namespace ragedb::gql

#endif // RAGEDB_ALGEBRAICREWRITER_H
