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

#ifndef RAGEDB_DISJOINTCONCEPTPRUNER_H
#define RAGEDB_DISJOINTCONCEPTPRUNER_H

#include "../GqlAst.h"

namespace ragedb::gql {

class DisjointConceptPruner {
public:
    /**
     * @brief Phase 20: Disjoint Concept Path Pruning pass.
     * 
     * Prunes queries that attempt to traverse variable-length paths between
     * endpoints with disjoint concepts/labels or disjoint taxonomy values.
     * 
     * Example:
     *   Catalog Disjointness: disjoint labels 'Person' and 'Company'
     *   Input Query:         MATCH (a:Person)-[:WORKS_FOR*]->(b:Company) RETURN a
     *   Output Query:        Empty query (marked as no_op = true)
     */
    static void disjoint_concept_pruning_pass(GqlQuery& query);
};

} // namespace ragedb::gql

#endif // RAGEDB_DISJOINTCONCEPTPRUNER_H
