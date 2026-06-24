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

#ifndef RAGEDB_IRREFLEXIVECONTRADICTIONPRUNER_H
#define RAGEDB_IRREFLEXIVECONTRADICTIONPRUNER_H

#include "../GqlAst.h"

namespace ragedb::gql {

class IrreflexiveContradictionPruner {
public:
    /**
     * @brief Phase 24: Irreflexive Contradiction Pruning pass.
     * 
     * Identifies self-loops on irreflexive relationships, or patterns
     * connecting variables with equality filters, and short-circuits the query.
     */
    static void irreflexive_contradiction_pass(GqlQuery& query);
};

} // namespace ragedb::gql

#endif // RAGEDB_IRREFLEXIVECONTRADICTIONPRUNER_H
