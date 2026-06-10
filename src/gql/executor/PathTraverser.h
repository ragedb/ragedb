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
#include "ProjectionPruner.h"

/**
 * @brief Handles vertex and edge traversals, start node index lookups, and factorized match execution.
 * 
 * Example Queries utilizing PathTraverser:
 * 
 * 1. Path Traversals and Variable Length patterns:
 *    MATCH (a:Person {name: 'Alice'})-[:FRIEND*1..3]->(b)
 *    RETURN b
 *    PathTraverser looks up start nodes (Alice) via indices, and performs
 *    multi-hop variable-length traversals matching constraints along the way.
 */
namespace ragedb::gql {

seastar::future<std::vector<Node>> get_start_nodes(
    ragedb::Graph& graph,
    const PatternNode& node,
    size_t limit = 0,
    const ProjectionPruner& pruner = {},
    std::string sort_property = "",
    bool sort_ascending = true,
    bool sort_by_id = false
);

seastar::future<std::vector<GqlRow>> traverse_path_pattern(
    ragedb::Graph& graph,
    const PathPattern& pattern,
    const GqlRow& base_row,
    size_t limit = 0,
    const ProjectionPruner& pruner = {},
    std::string sort_property = "",
    bool sort_ascending = true,
    bool sort_by_id = false
);

seastar::future<std::vector<GqlRow>> traverse_match_statement(
    ragedb::Graph& graph,
    const MatchStatement& stmt,
    const GqlRow& row,
    size_t limit = 0,
    const ProjectionPruner& pruner = {},
    std::string sort_property = "",
    bool sort_ascending = true,
    bool sort_by_id = false
);

} // namespace ragedb::gql

#endif // RAGEDB_PATH_TRAVERSER_H
