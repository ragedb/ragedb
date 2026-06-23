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

#ifndef RAGEDB_HONEYCOMB_EXECUTOR_H
#define RAGEDB_HONEYCOMB_EXECUTOR_H

#include <vector>
#include <string>
#include <set>
#include <map>
#include <unordered_map>
#include <memory>
#include <seastar/core/future.hh>
#include "../GqlValue.h"
#include "../GqlAst.h"
#include "FactorNode.h"


namespace ragedb::gql {

struct CoCoIndexLevel {
    std::vector<uint64_t> values;
    std::vector<size_t> offsets;
};

struct CoCoIndex {
    std::vector<CoCoIndexLevel> levels;
};

struct HoneycombRelation {
    std::vector<std::string> variables;
    std::vector<std::vector<uint64_t>> columns; // columns[i] holds IDs for variables[i]
};

class HoneycombExecutor {
public:
    static seastar::future<IntermediateResult> execute(
        ragedb::Graph& graph,
        const std::vector<MatchStatement>& matches,
        size_t limit
    );
};

} // namespace ragedb::gql

#endif // RAGEDB_HONEYCOMB_EXECUTOR_H
