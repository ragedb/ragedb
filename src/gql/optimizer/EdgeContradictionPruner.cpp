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

#include "EdgeContradictionPruner.h"
#include "OptimizerUtils.h"
#include "../GqlVirtualCatalog.h"
#include "../GqlParser.h"
#include <set>

namespace ragedb::gql {

namespace {

/**
 * @brief Collects all edge variable names introduced in the query patterns.
 * @param query The GQL query.
 * @return A set containing all declared edge variables.
 */
std::set<std::string> get_edge_variables(const GqlQuery& query) {
    std::set<std::string> edge_vars;
    for (const auto& match : query.matches) {
        for (const auto& edge : match.pattern.edges) {
            if (!edge.variable.empty()) {
                edge_vars.insert(edge.variable);
            }
        }
    }
    return edge_vars;
}

} // namespace

/**
 * @brief Phase 17: Relationship-Attribute Contradiction Pruning.
 *        Checks for two types of contradictions on relationship (edge) attributes:
 *        1. Internal contradictions within the query filters themselves (e.g. weight > 10 AND weight < 5).
 *        2. Contradictions against virtual catalog check constraints (e.g. if the catalog defines a
 *           constraint that weight < 0 is invalid/impossible, any query matching negative weight is pruned).
 *        If any contradiction is detected, the query is marked as no_op to short-circuit execution.
 */
void EdgeContradictionPruner::edge_contradiction_pruning_pass(GqlQuery& query) {
    if (query.skip_semantic || query.kind != QueryKind::SINGLE) return;
    
    auto edge_vars = get_edge_variables(query);
    if (edge_vars.empty()) return;
    
    auto q_vars = collect_all_query_vars(query);
    std::vector<VarInfo> q_edge_vars;
    for (const auto& vi : q_vars) {
        if (edge_vars.count(vi.variable)) {
            q_edge_vars.push_back(vi);
        }
    }
    
    // 1. Check internal contradictions on edge properties (e.g. weight > 5 AND weight < 2)
    for (const auto& vi : q_edge_vars) {
        for (const auto& [prop, q_interval] : vi.intervals) {
            if (q_interval.is_empty()) {
                query.no_op = true;
                return;
            }
        }
    }
    
    // 2. Check contradictions against catalog relationship constraints
    const auto& constraints = GqlVirtualCatalog::local().get_constraints();
    for (const auto& [c_name, c_query_str] : constraints) {
        try {
            auto c_query = GqlParser::parse(c_query_str);
            auto c_edge_vars = get_edge_variables(c_query);
            if (c_edge_vars.empty()) continue; // Not an edge constraint
            
            auto c_vars = collect_all_query_vars(c_query);
            for (const auto& c_vi : c_vars) {
                if (c_vi.label.empty() || !c_edge_vars.count(c_vi.variable)) continue;
                
                for (const auto& q_vi : q_edge_vars) {
                    if (q_vi.label == c_vi.label) {
                        for (const auto& [prop, c_interval] : c_vi.intervals) {
                            auto it = q_vi.intervals.find(prop);
                            if (it != q_vi.intervals.end()) {
                                // If the query interval is entirely contained within the forbidden constraint interval, it's a contradiction.
                                if (c_interval.contains(it->second)) {
                                    query.no_op = true;
                                    return;
                                }
                            }
                        }
                    }
                }
            }
        } catch (...) {
            // Ignore parsing errors for other/invalid constraints
        }
    }
}

} // namespace ragedb::gql
