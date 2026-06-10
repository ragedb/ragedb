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

#ifndef RAGEDB_PATH_TRAVERSER_H
#define RAGEDB_PATH_TRAVERSER_H

#include <vector>
#include <string>
#include <set>
#include <map>
#include <optional>
#include <seastar/core/future.hh>
#include "../graph/Graph.h"
#include "GqlAst.h"
#include "FactorNode.h"

namespace ragedb::gql {

struct ProjectionPruner {
    std::map<std::string, std::set<std::string>> accessed_props;
    std::set<std::string> whole_objects;

    bool should_prune(const std::string& var) const {
        if (var.empty()) return false;
        if (whole_objects.count(var)) return false;
        return true;
    }

    const std::set<std::string>& get_keys(const std::string& var) const {
        static const std::set<std::string> empty_keys;
        auto it = accessed_props.find(var);
        if (it != accessed_props.end()) {
            return it->second;
        }
        return empty_keys;
    }
};

void collect_accessed_properties(
    const Expression* expr,
    std::map<std::string, std::set<std::string>>& accessed_props,
    std::set<std::string>& whole_objects
);

seastar::future<IntermediateResult> execute_match_chain_factorized(
    ragedb::Graph& graph,
    std::vector<MatchStatement> matches,
    size_t match_idx,
    IntermediateResult incoming,
    size_t limit = 0,
    const ProjectionPruner& pruner = {},
    std::string sort_property = "",
    bool sort_ascending = true,
    bool sort_by_id = false
);

} // namespace ragedb::gql

#endif // RAGEDB_PATH_TRAVERSER_H
