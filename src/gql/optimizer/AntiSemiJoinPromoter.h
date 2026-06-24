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

#ifndef RAGEDB_ANTISEMIJOINPROMOTER_H
#define RAGEDB_ANTISEMIJOINPROMOTER_H

#include "../GqlAst.h"

namespace ragedb::gql {

class AntiSemiJoinPromoter {
public:
    /**
     * @brief Phase 18: Anti-Semi-Join Promotion pass.
     * 
     * Promotes OPTIONAL MATCH statements to anti-semi-joins (NOT EXISTS subqueries)
     * if their variables are subsequently filtered by a null-checking predicate.
     * 
     * Example:
     *   Input:  OPTIONAL MATCH (a)-[r:FRIEND]->(b) WHERE b IS NULL RETURN a
     *   Output: MATCH (a) WHERE NOT EXISTS { MATCH (a)-[r:FRIEND]->(b) } RETURN a
     */
    static void optional_match_to_antisemijoin_pass(GqlQuery& query);
};

} // namespace ragedb::gql

#endif // RAGEDB_ANTISEMIJOINPROMOTER_H
