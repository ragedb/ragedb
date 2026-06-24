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

#include "DirectionSwapOptimizer.h"
#include "OptimizerUtils.h"
#include <algorithm>

namespace ragedb::gql {

namespace {

/**
 * @brief Selectivity estimation classes based on query predicates.
 *        Lower value = higher selectivity (fewer rows expected).
 */
enum class SelectivityClass {
    UNIQUE = 1,   // Filtered by a unique identifier equality (e.g. id == 1)
    INDEXED = 2,  // Filtered by other property values (which can use a secondary index)
    SCAN = 3      // No filter bounds, requires scanning the whole node label
};

/**
 * @brief Simple rule-based estimator to assign a selectivity class to a query variable.
 * @param var_name Variable to estimate.
 * @param q_vars Structured query variable details including inferred constraint intervals.
 * @return The assigned SelectivityClass.
 */
SelectivityClass estimate_selectivity(const std::string& var_name, const std::vector<VarInfo>& q_vars) {
    if (var_name.empty()) return SelectivityClass::SCAN;
    
    const VarInfo* target_vi = nullptr;
    for (const auto& vi : q_vars) {
        if (vi.variable == var_name) {
            target_vi = &vi;
            break;
        }
    }
    
    if (!target_vi || target_vi->intervals.empty()) {
        return SelectivityClass::SCAN;
    }
    
    // Check if there is an equality restriction on "id"
    auto it = target_vi->intervals.find("id");
    if (it != target_vi->intervals.end()) {
        const auto& interval = it->second;
        if (interval.has_lower && interval.has_upper && interval.lower_val == interval.upper_val) {
            return SelectivityClass::UNIQUE;
        }
    }
    
    // Any other constraint interval qualifies as INDEXED
    return SelectivityClass::INDEXED;
}

/**
 * @brief Reverses the traversal sequence of a path pattern (nodes and edges),
 *        swapping edge directions accordingly.
 * @param match The MATCH statement containing the pattern.
 */
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

/**
 * @brief Phase 21: Cardinality-Aware Traversal Direction Swap.
 *        Examines standard inner MATCH patterns and compares the estimated selectivity
 *        of their start and end nodes. If the end node is more selective (has a lower
 *        SelectivityClass value), the match traversal direction is swapped (reversed)
 *        so that execution begins with the highly selective lookup instead of a scan.
 */
void DirectionSwapOptimizer::direction_swap_pass(GqlQuery& query) {
    if (query.skip_semantic || query.kind != QueryKind::SINGLE) return;
    
    auto q_vars = collect_query_vars(query);
    
    for (auto& match : query.matches) {
        if (match.is_optional || match.is_search || match.is_khop) continue;
        if (match.pattern.nodes.size() < 2 || match.pattern.edges.empty()) continue;
        
        std::string start_var = match.pattern.nodes.front().variable;
        std::string end_var = match.pattern.nodes.back().variable;
        
        auto start_sel = estimate_selectivity(start_var, q_vars);
        auto end_sel = estimate_selectivity(end_var, q_vars);
        
        // Swap if the end node is more selective than the start node
        if (static_cast<int>(end_sel) < static_cast<int>(start_sel)) {
            reverse_match_pattern(match);
        }
    }
}

} // namespace ragedb::gql
