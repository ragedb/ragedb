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

#ifndef RAGEDB_STAR_JOIN_REWRITER_H
#define RAGEDB_STAR_JOIN_REWRITER_H

#include <vector>
#include <string>
#include <set>
#include <optional>
#include <seastar/core/future.hh>
#include "../graph/Graph.h"
#include "GqlAst.h"
#include "FactorNode.h"
#include "ProjectionPruner.h"

namespace ragedb::gql {

struct StarJoinCandidate {
    std::string central_var;
    std::vector<size_t> match_indices;
};

/**
 * @brief Identifies if remaining MATCH statements form a star-join pattern around a central variable.
 */
std::optional<StarJoinCandidate> find_star_join_candidate(
    const std::vector<MatchStatement>& matches,
    size_t match_idx,
    const std::set<std::string>& incoming_vars
);

/**
 * @brief Recursively executes a chain of MATCH statements using factorized sub-trees.
 */
seastar::future<IntermediateResult> execute_match_chain_factorized(
    ragedb::Graph& graph,
    std::vector<MatchStatement> matches,
    size_t match_idx,
    IntermediateResult incoming,
    size_t limit = 0,
    const ProjectionPruner& pruner = {},
    std::string sort_property = "",
    bool sort_ascending = true,
    bool sort_by_id = false,
    std::shared_ptr<GqlQuery> query_ptr = nullptr
);

} // namespace ragedb::gql

#endif // RAGEDB_STAR_JOIN_REWRITER_H
