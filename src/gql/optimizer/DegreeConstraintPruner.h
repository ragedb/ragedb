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

#ifndef RAGEDB_DEGREECONSTRAINTPRUNER_H
#define RAGEDB_DEGREECONSTRAINTPRUNER_H

#include "../GqlAst.h"

namespace ragedb::gql {

class DegreeConstraintPruner {
public:
    /**
     * @brief Phase 13: Degree-Constraint Pruning pass.
     * 
     * Identifies filters of the form size((v)-[:REL]->()) > N or size((v)-[:REL]->()) = N.
     * Rewrites them to virtual property lookup filters (e.g. v._deg_REL_OUT > N) and
     * adds degree population instructions (DegreePopulateInfo) to the start node pattern
     * so that degrees are fetched shard-locally instead of traversing.
     */
    static void degree_constraint_pruning_pass(GqlQuery& query);
};

} // namespace ragedb::gql

#endif // RAGEDB_DEGREECONSTRAINTPRUNER_H
