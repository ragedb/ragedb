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

#ifndef RAGEDB_DIRECTIONSAPOPTIMIZER_H
#define RAGEDB_DIRECTIONSAPOPTIMIZER_H

#include "../GqlAst.h"

namespace ragedb::gql {

class DirectionSwapOptimizer {
public:
    /**
     * @brief Phase 21: Traversal Direction Swap pass.
     * 
     * Swaps match traversal direction (node/edge reversal) if the target end node
     * is estimated to be more selective (e.g. unique ID lookup vs a scan).
     * 
     * Example:
     *   Input:  MATCH (a:Person)-[:FRIEND]->(b:Person) WHERE b.id == 1 RETURN a
     *   Output: MATCH (b:Person)<-[:FRIEND]-(a:Person) WHERE b.id == 1 RETURN a
     */
    static void direction_swap_pass(GqlQuery& query);
};

} // namespace ragedb::gql

#endif // RAGEDB_DIRECTIONSAPOPTIMIZER_H
