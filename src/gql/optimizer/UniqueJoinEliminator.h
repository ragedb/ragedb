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

#ifndef RAGEDB_UNIQUEJOINELIMINATOR_H
#define RAGEDB_UNIQUEJOINELIMINATOR_H

#include "../GqlAst.h"

namespace ragedb::gql {

class UniqueJoinEliminator {
public:
    /**
     * @brief Phase 14: Unique Constraint Join Elimination pass.
     * 
     * Identifies joins (a)-[r:REL]->(b:LabelB) where REL is a unique relationship
     * (max_cardinality = 1). If the join is optional, or both unique and mandatory,
     * and b is not referenced or only its ID is needed, eliminates the join to b.
     */
    static void unique_join_elimination_pass(GqlQuery& query);
};

} // namespace ragedb::gql

#endif // RAGEDB_UNIQUEJOINELIMINATOR_H
