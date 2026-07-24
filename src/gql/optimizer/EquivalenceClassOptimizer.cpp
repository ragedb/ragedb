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

#include "EquivalenceClassOptimizer.h"
#include "OptimizerUtils.h"
#include "../GqlVirtualCatalog.h"
#include <limits>

namespace ragedb::gql {

void EquivalenceClassOptimizer::equivalence_class_pass(GqlQuery& query) {
    if (query.skip_semantic || query.kind != QueryKind::SINGLE) return;
    // Every rewrite here is keyed on registered algebraic traits; with none registered
    // the pass cannot fire, so skip the per-match scanning entirely.
    if (GqlVirtualCatalog::local().get_relationship_algebraic_properties().empty()) return;

    for (auto& match : query.matches) {
        if (match.is_optional || match.is_search || match.is_propagate || match.is_khop) continue;
        if (match.pattern.nodes.size() == 2 && match.pattern.edges.size() == 1) {
            auto& edge = match.pattern.edges[0];
            if (edge.is_variable_length && edge.label_expr && edge.label_expr->kind == LabelExprKind::LITERAL) {
                std::string rel_type = edge.label_expr->name;
                
                // An equivalence relation must be reflexive, symmetric, and transitive
                bool is_equivalence = 
                    GqlVirtualCatalog::local().has_relationship_algebraic_property(rel_type, "reflexive") &&
                    GqlVirtualCatalog::local().has_relationship_algebraic_property(rel_type, "symmetric") &&
                    GqlVirtualCatalog::local().has_relationship_algebraic_property(rel_type, "transitive");

                // The equivalence-partition fast path (Case 0.5) only reproduces the original
                // traversal for the one safe shape encoded in is_simple_unbounded_right_hop.
                bool safe_to_rewrite = is_simple_unbounded_right_hop(match, edge);

                if (is_equivalence && safe_to_rewrite) {
                    match.equivalence_partition_lookup = true;
                    edge.is_variable_length = false;
                    edge.min_hops = 1;
                    edge.max_hops = 1;
                }
            }
        }
    }
}

} // namespace ragedb::gql
