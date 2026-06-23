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

#ifndef RAGEDB_LFTJ_EXECUTOR_H
#define RAGEDB_LFTJ_EXECUTOR_H

#include <vector>
#include <string>
#include <seastar/core/future.hh>
#include "../GqlValue.h"
#include "../GqlAst.h"
#include "FactorNode.h"

namespace ragedb::gql {

/**
 * @brief Reactor-local, synchronous Leapfrog Triejoin (WCOJ) query executor.
 * 
 * This executor processes cyclic queries directly on the calling Seastar reactor thread
 * without offloading to background worker threads. It builds Compressed Column Layout 
 * Trie indexes on relation arrays and performs multi-way leapfrog joins sequentially.
 * This is highly optimal for smaller relation sizes where offloading overhead dominates.
 */
class LftjExecutor {
public:
    /**
     * @brief Executes the cyclic query pattern synchronously on the reactor thread.
     * 
     * @param graph Reference to the RageDB Graph object.
     * @param matches List of GQL match statements representing the query pattern.
     * @param limit Maximum number of rows to return (0 for unlimited).
     * @return seastar::future<IntermediateResult> Future resolving to the query results.
     */
    static seastar::future<IntermediateResult> execute(
        ragedb::Graph& graph,
        const std::vector<MatchStatement>& matches,
        size_t limit
    );
};

} // namespace ragedb::gql

#endif // RAGEDB_LFTJ_EXECUTOR_H
