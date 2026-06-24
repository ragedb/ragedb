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

#ifndef RAGEDB_CARDINALITYSHORTCIRCUITER_H
#define RAGEDB_CARDINALITYSHORTCIRCUITER_H

/**
 * @file CardinalityShortCircuiter.h
 * @brief Performs Phase 5: Cardinality-Constrained Traversal Short-Circuiting.
 * 
 * Inspects cardinality constraints in the virtual catalog (e.g., verifying that a node has at most N
 * outgoing or incoming relationships of a given type, such as 1-to-1 or N-to-1 relations). It annotates
 * the matched edge patterns in the AST with a `max_cardinality_limit`. During traversal execution,
 * the neighbor list scan is truncated once the limit is reached, short-circuiting traversal and avoiding
 * redundant remote peered shard communication.
 * 
 * Example:
 *   Schema Constraint: A Shipment has at most 1 outgoing SHIPPED_FROM relationship.
 *                      (Declared via a constraint where checking a Shipment node with multiple
 *                      outgoing SHIPPED_FROM targets returns empty).
 *   Query: MATCH (s:Shipment)-[:SHIPPED_FROM]->(w:Warehouse) RETURN s.name, w.name
 *   Result: Sets max_cardinality_limit = 1 on the SHIPPED_FROM edge, allowing the traverser
 *           to terminate traversal steps early once the first relationship target is resolved.
 */

#include "../GqlAst.h"

namespace ragedb::gql {

class CardinalityShortCircuiter {
public:
    static void semantic_cardinality_limit_pass(GqlQuery& query);
};

} // namespace ragedb::gql

#endif // RAGEDB_CARDINALITYSHORTCIRCUITER_H
