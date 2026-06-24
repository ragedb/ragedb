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

#ifndef RAGEDB_SUBSUMPTIONPRUNER_H
#define RAGEDB_SUBSUMPTIONPRUNER_H

/**
 * @file SubsumptionPruner.h
 * @brief Performs Phase 6: Subsumption / Query Containment Pruning.
 * 
 * Identifies duplicate or redundant isomorphic query traversal paths originating from the same starting node
 * variable, and prunes paths whose range constraints are completely subsumed by another path.
 * 
 * Subsumption requires:
 *   - The patterns must be isomorphic (same node/edge structures).
 *   - They must originate from the same starting variable.
 *   - Subsequent node/edge labels and variables are subsumed (i.e., range filters in the pruned path are
 *     equal to or less strict than the corresponding variables in the keeping path).
 *   - Pruned variables must be dead-ends (neither projected, sorted, nor referenced in updates or other matches).
 * 
 * Example:
 *   Query: 
 *     MATCH (p:Person)-[:FRIEND]->(f1:Person) WHERE f1.age > 30
 *     MATCH (p)-[:FRIEND]->(f2:Person) WHERE f2.age > 20
 *     RETURN p.name
 *   Result: Since f1.age > 30 is a subset of/stricter than f2.age > 20, any person 'p' that has a friend
 *           older than 30 is guaranteed to have a friend older than 20. Because f2 is a dead-end variable,
 *           the second MATCH is pruned entirely.
 */

#include "../GqlAst.h"

namespace ragedb::gql {

class SubsumptionPruner {
public:
    static void semantic_subsumption_pass(GqlQuery& query);
};

} // namespace ragedb::gql

#endif // RAGEDB_SUBSUMPTIONPRUNER_H
