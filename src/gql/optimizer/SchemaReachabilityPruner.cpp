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

#include "SchemaReachabilityPruner.h"
#include "../GqlVirtualCatalog.h"
#include <unordered_map>
#include <vector>
#include <algorithm>

namespace ragedb::gql {

void SchemaReachabilityPruner::schema_reachability_pass(GqlQuery& query) {
    if (query.kind != QueryKind::SINGLE) return;

    // Build variable-to-label mapping from all query patterns
    std::unordered_map<std::string, std::string> var_to_label;
    for (const auto& match : query.matches) {
        for (const auto& node : match.pattern.nodes) {
            if (!node.variable.empty() && node.label_expr && node.label_expr->kind == LabelExprKind::LITERAL) {
                var_to_label[node.variable] = node.label_expr->name;
            }
        }
    }

    const auto& allowed = GqlVirtualCatalog::local().get_allowed_relationships();
    if (allowed.empty()) return; // No schema rules registered, skip pruning

    for (const auto& match : query.matches) {
        if (match.is_search || match.is_khop) continue;
        const auto& pattern = match.pattern;
        
        for (size_t i = 0; i < pattern.edges.size(); ++i) {
            const auto& edge = pattern.edges[i];
            
            std::string src_var = pattern.nodes[i].variable;
            std::string tgt_var = pattern.nodes[i+1].variable;
            
            if (edge.direction == EdgeDirection::LEFT) {
                std::swap(src_var, tgt_var);
            } else if (edge.direction == EdgeDirection::ANY) {
                // For undirected edges, check both directions. If both are invalid, then it's unsatisfiable.
                std::string src_label = var_to_label[src_var];
                std::string tgt_label = var_to_label[tgt_var];
                if (edge.label_expr && edge.label_expr->kind == LabelExprKind::LITERAL && !src_label.empty() && !tgt_label.empty()) {
                    std::string rel_type = edge.label_expr->name;
                    bool forward_allowed = false;
                    bool backward_allowed = false;
                    for (const auto& [a_src, a_rel, a_tgt] : allowed) {
                        if (a_src == src_label && a_rel == rel_type && a_tgt == tgt_label) forward_allowed = true;
                        if (a_src == tgt_label && a_rel == rel_type && a_tgt == src_label) backward_allowed = true;
                    }
                    if (!forward_allowed && !backward_allowed) {
                        query.no_op = true;
                        return;
                    }
                }
                continue;
            }
            
            std::string src_label = var_to_label[src_var];
            std::string tgt_label = var_to_label[tgt_var];
            
            if (edge.label_expr && edge.label_expr->kind == LabelExprKind::LITERAL) {
                std::string rel_type = edge.label_expr->name;
                
                if (!src_label.empty() && !tgt_label.empty()) {
                    bool found = false;
                    for (const auto& [a_src, a_rel, a_tgt] : allowed) {
                        if (a_src == src_label && a_rel == rel_type && a_tgt == tgt_label) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        query.no_op = true;
                        return;
                    }
                }
            }
        }
    }
}

} // namespace ragedb::gql
