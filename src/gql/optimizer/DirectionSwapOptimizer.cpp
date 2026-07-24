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
        if (match.is_optional || match.is_search || match.is_propagate || match.is_khop) continue;
        if (match.pattern.nodes.size() < 2 || match.pattern.edges.empty()) continue;
        
        std::string start_var = match.pattern.nodes.front().variable;
        std::string end_var = match.pattern.nodes.back().variable;
        
        auto start_sel = estimate_selectivity(start_var, q_vars);
        auto end_sel = estimate_selectivity(end_var, q_vars);
        
        // Swap if the end node is more selective than the start node. The reversal declines
        // matches whose path variable / shortest-path selector observes the node order.
        if (static_cast<int>(end_sel) < static_cast<int>(start_sel)) {
            reverse_match_pattern_if_safe(match);
        }
    }
}

} // namespace ragedb::gql
