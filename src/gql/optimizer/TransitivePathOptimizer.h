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

#ifndef RAGEDB_TRANSITIVEPATHOPTIMIZER_H
#define RAGEDB_TRANSITIVEPATHOPTIMIZER_H

#include "../GqlAst.h"

namespace ragedb::gql {

class TransitivePathOptimizer {
public:
    /**
     * @brief Phase 23: Transitive Path Pruning pass.
     * 
     * Rewrites variable-length paths of transitive relations to single hops
     * and prunes redundant shortcut edges from the MATCH join trees.
     */
    static void transitive_path_pass(GqlQuery& query);
};

} // namespace ragedb::gql

#endif // RAGEDB_TRANSITIVEPATHOPTIMIZER_H
