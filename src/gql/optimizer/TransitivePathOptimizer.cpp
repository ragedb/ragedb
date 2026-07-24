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

#include "TransitivePathOptimizer.h"
#include "OptimizerUtils.h"
#include "../GqlVirtualCatalog.h"
#include <unordered_map>
#include <unordered_set>
#include <limits>

namespace ragedb::gql {

namespace {

bool is_reachable_min_depth_2(const std::string& start, const std::string& target,
                              const std::unordered_map<std::string, std::unordered_set<std::string>>& adj,
                              std::unordered_set<std::string>& visited, int depth) {
    if (depth >= 2 && start == target) return true;
    visited.insert(start);
    auto it = adj.find(start);
    if (it != adj.end()) {
        for (const auto& next : it->second) {
            if (visited.count(next) == 0) {
                if (is_reachable_min_depth_2(next, target, adj, visited, depth + 1)) {
                    return true;
                }
            }
        }
    }
    visited.erase(start);
    return false;
}

} // namespace

void TransitivePathOptimizer::transitive_path_pass(GqlQuery& query) {
    if (query.skip_semantic || query.kind != QueryKind::SINGLE) return;
    // Every rewrite here is keyed on registered algebraic traits; with none registered
    // the pass cannot fire, so skip the per-match scanning entirely.
    if (GqlVirtualCatalog::local().get_relationship_algebraic_properties().empty()) return;

    // 1. Simplify variable-length paths of transitive relations to single hops
    for (auto& match : query.matches) {
        if (match.is_optional || match.is_search || match.is_propagate || match.is_khop) continue;
        if (match.pattern.nodes.size() == 2 && match.pattern.edges.size() == 1) {
            auto& edge = match.pattern.edges[0];
            if (edge.is_variable_length && edge.label_expr && edge.label_expr->kind == LabelExprKind::LITERAL) {
                std::string rel_type = edge.label_expr->name;
                bool is_transitive = GqlVirtualCatalog::local().has_relationship_algebraic_property(rel_type, "transitive");
                bool is_equivalence = 
                    GqlVirtualCatalog::local().has_relationship_algebraic_property(rel_type, "reflexive") &&
                    GqlVirtualCatalog::local().has_relationship_algebraic_property(rel_type, "symmetric") &&
                    GqlVirtualCatalog::local().has_relationship_algebraic_property(rel_type, "transitive");

                // The transitive-reachability fast path (Case 0.6) only reproduces the original
                // traversal for the one safe shape encoded in is_simple_unbounded_right_hop.
                bool safe_to_rewrite = is_simple_unbounded_right_hop(match, edge);

                if (is_transitive && !is_equivalence && safe_to_rewrite) {
                    match.transitive_reachability_lookup = true;
                    edge.is_variable_length = false;
                    edge.min_hops = 1;
                    edge.max_hops = 1;
                }
            }
        }
    }

    // 2. Prune redundant shortcut edges in joins
    // Map of rel_type -> adjacency map of variables (src_var -> set of dst_vars)
    std::unordered_map<std::string, std::unordered_map<std::string, std::unordered_set<std::string>>> rel_adj;

    // First collect all 1-hop matches on transitive relationship types
    for (const auto& match : query.matches) {
        if (match.is_optional || match.is_search || match.is_propagate || match.is_khop) continue;
        if (match.pattern.nodes.size() == 2 && match.pattern.edges.size() == 1) {
            const auto& edge = match.pattern.edges[0];
            if (!edge.is_variable_length && edge.label_expr && edge.label_expr->kind == LabelExprKind::LITERAL) {
                std::string rel_type = edge.label_expr->name;
                bool is_transitive = GqlVirtualCatalog::local().has_relationship_algebraic_property(rel_type, "transitive");
                if (is_transitive) {
                    std::string src = "";
                    std::string dst = "";
                    if (edge.direction == EdgeDirection::RIGHT) {
                        src = match.pattern.nodes[0].variable;
                        dst = match.pattern.nodes[1].variable;
                    } else if (edge.direction == EdgeDirection::LEFT) {
                        src = match.pattern.nodes[1].variable;
                        dst = match.pattern.nodes[0].variable;
                    }
                    if (!src.empty() && !dst.empty()) {
                        rel_adj[rel_type][src].insert(dst);
                    }
                }
            }
        }
    }

    // Now identify matches that are redundant shortcut edges
    std::vector<MatchStatement> filtered_matches;
    for (auto& match : query.matches) {
        bool prune = false;
        if (!match.is_optional && !match.is_search && !match.is_propagate && !match.is_khop &&
            match.pattern.nodes.size() == 2 && match.pattern.edges.size() == 1) {
            const auto& edge = match.pattern.edges[0];
            if (!edge.is_variable_length && edge.label_expr && edge.label_expr->kind == LabelExprKind::LITERAL) {
                std::string rel_type = edge.label_expr->name;
                bool is_transitive = GqlVirtualCatalog::local().has_relationship_algebraic_property(rel_type, "transitive");
                if (is_transitive) {
                    std::string src = "";
                    std::string dst = "";
                    if (edge.direction == EdgeDirection::RIGHT) {
                        src = match.pattern.nodes[0].variable;
                        dst = match.pattern.nodes[1].variable;
                    } else if (edge.direction == EdgeDirection::LEFT) {
                        src = match.pattern.nodes[1].variable;
                        dst = match.pattern.nodes[0].variable;
                    }
                    if (!src.empty() && !dst.empty()) {
                        // Only prune a shortcut edge that carries no constraints and binds nothing --
                        // otherwise pruning would silently drop labels/properties/filters or leave an
                        // edge/path variable unbound.
                        const auto& n0 = match.pattern.nodes[0];
                        const auto& n1 = match.pattern.nodes[1];
                        bool unconstrained =
                            !n0.label_expr && n0.properties.empty() && n0.property_filters.empty() && !n0.where_expr &&
                            !n1.label_expr && n1.properties.empty() && n1.property_filters.empty() && !n1.where_expr &&
                            edge.properties.empty() && edge.property_filters.empty() && !edge.where_expr &&
                            edge.variable.empty() && match.path_variable.empty();
                        std::unordered_set<std::string> visited;
                        if (unconstrained && is_reachable_min_depth_2(src, dst, rel_adj[rel_type], visited, 0)) {
                            prune = true;
                        }
                    }
                }
            }
        }
        if (!prune) {
            filtered_matches.push_back(std::move(match));
        }
    }
    query.matches = std::move(filtered_matches);
}

} // namespace ragedb::gql
