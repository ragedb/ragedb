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

#ifndef RAGEDB_CONTRADICTIONPRUNER_H
#define RAGEDB_CONTRADICTIONPRUNER_H

/**
 * @file ContradictionPruner.h
 * @brief Prunes queries that contain mathematical contradictions.
 * 
 * This optimizer pass is split into two phases:
 * 
 * 1. Phase 1 (semantic_pruning_pass): Single-variable constraint contradiction pruning.
 *    Validates if query property filters intersect impossible value regions derived from schema constraints.
 *    Example:
 *      Query: MATCH (p:Person) WHERE p.age = -5 RETURN p.name
 *      Schema Constraint: CREATE CONSTRAINT FOR (p:Person) REQUIRE p.age >= 0
 *      Result: The query interval [ -5, -5 ] is outside the valid range [ 0, inf ), proving contradiction.
 *              Sets query.no_op = true.
 * 
 * 2. Phase 3 (relational_pruning_pass): Multi-variable relational poset cycle contradiction pruning.
 *    Extracts inequalities between query variables and uses a cycle detection algorithm to identify unsatisfiable structures.
 *    Example:
 *      Query: MATCH (a:Node), (b:Node), (c:Node) WHERE a.val < b.val AND b.val <= c.val AND c.val < a.val
 *      Result: Constructs inequality graph, detects cycle with strict inequalities (a < b <= c < a -> a < a),
 *              proving contradiction. Sets query.no_op = true.
 */

#include "../GqlAst.h"

namespace ragedb::gql {

class ContradictionPruner {
public:
    static void semantic_pruning_pass(GqlQuery& query);
    static void relational_pruning_pass(GqlQuery& query);
};

} // namespace ragedb::gql

#endif // RAGEDB_CONTRADICTIONPRUNER_H
