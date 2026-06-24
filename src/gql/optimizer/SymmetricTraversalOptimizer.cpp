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

#include "SymmetricTraversalOptimizer.h"
#include "../GqlVirtualCatalog.h"
#include "OptimizerUtils.h"
#include <algorithm>

namespace ragedb::gql {

namespace {

enum class SelectivityClass {
    UNIQUE = 1,
    INDEXED = 2,
    SCAN = 3
};

SelectivityClass estimate_selectivity(const std::string& var_name, const std::vector<VarInfo>& q_vars) {
    if (var_name.empty()) return SelectivityClass::SCAN;
    for (const auto& vi : q_vars) {
        if (vi.variable == var_name) {
            auto it = vi.intervals.find("id");
            if (it != vi.intervals.end()) {
                const auto& interval = it->second;
                if (interval.has_lower && interval.has_upper && interval.lower_val == interval.upper_val) {
                    return SelectivityClass::UNIQUE;
                }
            }
            return SelectivityClass::INDEXED;
        }
    }
    return SelectivityClass::SCAN;
}

void reverse_match_pattern(MatchStatement& match) {
    std::reverse(match.pattern.nodes.begin(), match.pattern.nodes.end());
    std::reverse(match.pattern.edges.begin(), match.pattern.edges.end());
    for (auto& edge : match.pattern.edges) {
        if (edge.direction == EdgeDirection::RIGHT) {
            edge.direction = EdgeDirection::LEFT;
        } else if (edge.direction == EdgeDirection::LEFT) {
            edge.direction = EdgeDirection::RIGHT;
        }
    }
}

} // namespace

void SymmetricTraversalOptimizer::symmetric_traversal_pass(GqlQuery& query) {
    if (query.skip_semantic || query.kind != QueryKind::SINGLE) return;
    
    auto q_vars = collect_query_vars(query);

    for (auto& match : query.matches) {
        if (match.is_optional || match.is_search || match.is_khop) continue;
        if (match.pattern.nodes.size() < 2 || match.pattern.edges.empty()) continue;

        // Check if all edges in the match pattern are symmetric
        bool all_symmetric = true;
        for (const auto& edge : match.pattern.edges) {
            if (edge.label_expr && edge.label_expr->kind == LabelExprKind::LITERAL) {
                std::string rel_type = edge.label_expr->name;
                if (!GqlVirtualCatalog::local().has_relationship_algebraic_property(rel_type, "symmetric")) {
                    all_symmetric = false;
                    break;
                }
            } else {
                all_symmetric = false;
                break;
            }
        }

        if (!all_symmetric) continue;

        // Since it's symmetric, we can swap direction to start with the more selective side
        std::string start_var = match.pattern.nodes.front().variable;
        std::string end_var = match.pattern.nodes.back().variable;

        auto start_sel = estimate_selectivity(start_var, q_vars);
        auto end_sel = estimate_selectivity(end_var, q_vars);

        if (static_cast<int>(end_sel) < static_cast<int>(start_sel)) {
            reverse_match_pattern(match);
        }
    }
}

} // namespace ragedb::gql
