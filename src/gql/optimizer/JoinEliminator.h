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

#ifndef RAGEDB_JOINELIMINATOR_H
#define RAGEDB_JOINELIMINATOR_H

/**
 * @file JoinEliminator.h
 * @brief Performs Phase 2: Join Elimination / Pruning.
 * 
 * Inspects mandatory relationship constraints defined in the virtual catalog. If a schema constraint
 * guarantees that a relationship exists for all instances of a source label, and the target node
 * and relationship variables are not filtered, projected, or referenced elsewhere in the query,
 * the join to the target node is pruned to avoid redundant network/hop overhead.
 * 
 * Example:
 *   Schema Constraint: Every (s:Shipment) must have an outgoing SHIPPED_FROM edge to a (:Location) node.
 *                      (Declared via a constraint where checking Shipment nodes without SHIPPED_FROM
 *                      edges returns empty).
 *   Query: MATCH (s:Shipment)-[:SHIPPED_FROM]->(l:Location) RETURN s.name
 *   Result: Since 'l' and the edge are neither projected, filtered, nor used in WHERE/ORDER BY,
 *           the query execution plan is simplified to match only the single node:
 *           MATCH (s:Shipment) RETURN s.name
 */

#include "../GqlAst.h"

namespace ragedb::gql {

class JoinEliminator {
public:
    static void semantic_join_elimination_pass(GqlQuery& query);
};

} // namespace ragedb::gql

#endif // RAGEDB_JOINELIMINATOR_H
