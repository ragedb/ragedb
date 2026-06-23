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

#ifndef RAGEDB_PLAN_BUILDER_H
#define RAGEDB_PLAN_BUILDER_H

#include <memory>
#include <map>
#include <vector>
#include <string>
#include "../../graph/Graph.h"
#include "../GqlAst.h"
#include "../GqlValue.h"

namespace ragedb::gql {

/**
 * @brief Builds the execution plan tree for a given GQL query.
 * 
 * Takes a logical query structure and generates a tree of PlanNodes
 * reflecting the scans, seeks, joins, filters, aggregates, sorting, and writes.
 * Used by EXPLAIN and PROFILE queries.
 * 
 * @param graph Reference to the sharded RageDB graph instance.
 * @param query The GqlQuery AST structure.
 * @return std::shared_ptr<PlanNode> The root node of the generated plan.
 */
std::shared_ptr<PlanNode> build_query_plan(ragedb::Graph& graph, const GqlQuery& query);

/**
 * @brief Traverses the plan tree recursively to build an index mapping node keys to their PlanNode shared pointers.
 * 
 * This index is used during PROFILE execution to dynamically look up plan nodes by their identifier
 * and update their actual metrics (such as execution time and actual row count).
 * 
 * @param node The current plan node in the recursion.
 * @param plan_nodes Map containing the indexed plan nodes.
 */
void index_plan_nodes(
    const std::shared_ptr<PlanNode>& node,
    std::map<std::string, std::shared_ptr<PlanNode>>& plan_nodes
);

/**
 * @brief Flattens the execution plan tree into a tabular format.
 * 
 * Recursively traverses the execution plan tree in pre-order, prefixing children
 * with standard branch-drawing characters (e.g. "├─ ", "└─ ", "│  "). Each node
 * is added as a row of GqlValues ready to be serialized and formatted.
 * 
 * @param node The current plan node to format.
 * @param rows Tabular row vector to append output fields to.
 * @param indent The string prefix indicating hierarchical depth.
 * @param is_last True if the current node is the last sibling at this level.
 */
void flatten_plan_tree(
    const std::shared_ptr<PlanNode>& node,
    std::vector<std::vector<GqlValue>>& rows,
    std::string indent = "",
    bool is_last = true
);

} // namespace ragedb::gql

#endif // RAGEDB_PLAN_BUILDER_H
