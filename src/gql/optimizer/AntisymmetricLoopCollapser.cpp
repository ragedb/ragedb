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

#include "AntisymmetricLoopCollapser.h"
#include "../GqlVirtualCatalog.h"
#include "OptimizerUtils.h"
#include <algorithm>
#include <map>
#include <set>
#include <unordered_set>
#include <vector>

namespace ragedb::gql {

namespace {

void merge_pattern_nodes(PatternNode& dest, PatternNode& src) {
    // Preserve src's label constraint: the merged node must satisfy BOTH labels, so combine them
    // with AND rather than dropping src's label (which silently widened the result).
    if (src.label_expr) {
        if (dest.label_expr) {
            auto both = std::make_shared<LabelExpression>();
            both->kind = LabelExprKind::AND;
            both->left = dest.label_expr;
            both->right = src.label_expr;
            dest.label_expr = both;
        } else {
            dest.label_expr = src.label_expr;
        }
    }
    for (const auto& [k, v] : src.properties) {
        dest.properties[k] = v;
    }
    dest.property_filters.insert(dest.property_filters.end(), src.property_filters.begin(), src.property_filters.end());
    dest.degree_opt_info.insert(dest.degree_opt_info.end(), src.degree_opt_info.begin(), src.degree_opt_info.end());
    if (src.where_expr) {
        if (dest.where_expr) {
            dest.where_expr = std::make_shared<BinaryOpExpr>(
                BinaryOpKind::AND,
                dest.where_expr->clone(),
                src.where_expr->clone()
            );
        } else {
            dest.where_expr = src.where_expr;
        }
    }
}

void rename_variables_in_matches(std::vector<MatchStatement>& matches, const std::map<std::string, std::string>& var_map) {
    for (auto& match : matches) {
        for (auto& node : match.pattern.nodes) {
            auto it = var_map.find(node.variable);
            if (it != var_map.end()) {
                node.variable = it->second;
            }
            rewrite_expr_vars(node.where_expr, var_map);
        }
        for (auto& edge : match.pattern.edges) {
            auto it = var_map.find(edge.variable);
            if (it != var_map.end()) {
                edge.variable = it->second;
            }
            rewrite_expr_vars(edge.where_expr, var_map);
            rewrite_expr_vars(edge.cost_expr, var_map);
        }
    }
}

void delete_edge_at(GqlQuery& query, size_t m_idx, size_t e_idx) {
    auto& match = query.matches[m_idx];
    if (e_idx >= match.pattern.edges.size()) return;

    auto& edge = match.pattern.edges[e_idx];
    if (edge.where_expr) {
        if (query.where_expr) {
            query.where_expr = std::make_unique<BinaryOpExpr>(
                BinaryOpKind::AND,
                std::move(query.where_expr),
                edge.where_expr->clone()
            );
        } else {
            query.where_expr = edge.where_expr->clone();
        }
    }

    merge_pattern_nodes(match.pattern.nodes[e_idx], match.pattern.nodes[e_idx + 1]);

    match.pattern.edges.erase(match.pattern.edges.begin() + static_cast<std::ptrdiff_t>(e_idx));
    match.pattern.nodes.erase(match.pattern.nodes.begin() + static_cast<std::ptrdiff_t>(e_idx) + 1);
}

void prune_redundant_single_node_matches(GqlQuery& query) {
    std::unordered_set<std::string> bound_vars;
    for (const auto& match : query.matches) {
        if (!match.is_optional && (match.pattern.edges.size() > 0 || match.is_search || match.is_propagate || match.is_khop)) {
            for (const auto& node : match.pattern.nodes) {
                if (!node.variable.empty()) {
                    bound_vars.insert(node.variable);
                }
            }
        }
    }

    auto it = query.matches.begin();
    while (it != query.matches.end()) {
        if (!it->is_optional && !it->is_search && !it->is_propagate && !it->is_khop &&
            it->pattern.edges.empty() && it->pattern.nodes.size() == 1) {
            std::string var = it->pattern.nodes[0].variable;
            if (bound_vars.count(var) > 0 &&
                !it->pattern.nodes[0].label_expr &&
                it->pattern.nodes[0].properties.empty() &&
                it->pattern.nodes[0].property_filters.empty() &&
                !it->pattern.nodes[0].where_expr) {
                it = query.matches.erase(it);
                continue;
            }
        }
        ++it;
    }
}

struct EdgeRef {
    size_t match_idx;
    size_t edge_idx;
    std::string src;
    std::string tgt;
    std::string rel_type;
};

} // namespace

void AntisymmetricLoopCollapser::antisymmetric_loop_pass(GqlQuery& query) {
    if (query.skip_semantic || query.kind != QueryKind::SINGLE) return;
    // Every rewrite here is keyed on registered algebraic traits; with none registered
    // the pass cannot fire, so skip the per-match scanning entirely.
    if (GqlVirtualCatalog::local().get_relationship_algebraic_properties().empty()) return;

    bool changed = false;
    do {
        changed = false;
        std::vector<EdgeRef> refs;

        for (size_t m_idx = 0; m_idx < query.matches.size(); ++m_idx) {
            auto& match = query.matches[m_idx];
            if (match.is_optional || match.is_search || match.is_propagate || match.is_khop) continue;
            for (size_t e_idx = 0; e_idx < match.pattern.edges.size(); ++e_idx) {
                const auto& edge = match.pattern.edges[e_idx];
                // Only collapse a totally-unconstrained edge: collapse deletes one edge, which would
                // otherwise silently drop its predicates or leave its bound variable unbound.
                bool edge_unconstrained = edge.variable.empty() && edge.properties.empty() &&
                    edge.property_filters.empty() && !edge.where_expr;
                if (!edge.is_variable_length && edge_unconstrained &&
                    edge.label_expr && edge.label_expr->kind == LabelExprKind::LITERAL) {
                    std::string rel_type = edge.label_expr->name;
                    if (GqlVirtualCatalog::local().has_relationship_algebraic_property(rel_type, "antisymmetric")) {
                        std::string src = match.pattern.nodes[e_idx].variable;
                        std::string tgt = match.pattern.nodes[e_idx + 1].variable;
                        if (!src.empty() && !tgt.empty() && src != tgt) {
                            if (edge.direction == EdgeDirection::RIGHT) {
                                refs.push_back({m_idx, e_idx, src, tgt, rel_type});
                            } else if (edge.direction == EdgeDirection::LEFT) {
                                refs.push_back({m_idx, e_idx, tgt, src, rel_type});
                            }
                        }
                    }
                }
            }
        }

        for (size_t i = 0; i < refs.size(); ++i) {
            for (size_t j = i + 1; j < refs.size(); ++j) {
                const auto& e1 = refs[i];
                const auto& e2 = refs[j];
                if (e1.rel_type == e2.rel_type && e1.src == e2.tgt && e1.tgt == e2.src) {
                    // An antisymmetric 2-cycle forces src == tgt (a self-loop). If the relation is
                    // also irreflexive, that self-loop is impossible, so the query matches nothing.
                    // (The irreflexive pruner runs before this pass and cannot see the folded loop.)
                    bool rel_reflexive = GqlVirtualCatalog::local().has_relationship_algebraic_property(e1.rel_type, "reflexive");
                    bool rel_irreflexive = GqlVirtualCatalog::local().has_relationship_algebraic_property(e1.rel_type, "irreflexive");
                    if (rel_irreflexive && !rel_reflexive) {
                        query.no_op = true;
                        return;
                    }

                    std::string keep_var = e1.src;
                    std::string prune_var = e1.tgt;

                    std::map<std::string, std::string> var_map;
                    var_map[prune_var] = keep_var;

                    rename_variables_in_matches(query.matches, var_map);

                    for (auto& item : query.returns) {
                        rewrite_expr_vars(item.expr, var_map);
                    }
                    for (auto& spec : query.order_by) {
                        rewrite_expr_vars(spec.expr, var_map);
                    }
                    for (auto& write : query.writes) {
                        auto write_it1 = var_map.find(write.set_var);
                        if (write_it1 != var_map.end()) write.set_var = write_it1->second;
                        auto write_it2 = var_map.find(write.remove_var);
                        if (write_it2 != var_map.end()) write.remove_var = write_it2->second;
                        auto write_it3 = var_map.find(write.delete_var);
                        if (write_it3 != var_map.end()) write.delete_var = write_it3->second;
                        rewrite_expr_vars(write.set_expr, var_map);
                    }

                    rewrite_expr_vars(query.where_expr, var_map);

                    // Delete edges.
                    bool is_reflexive = GqlVirtualCatalog::local().has_relationship_algebraic_property(e1.rel_type, "reflexive");
                    if (is_reflexive) {
                        // Delete both edges.
                        if (e1.match_idx == e2.match_idx) {
                            size_t first_idx = std::min(e1.edge_idx, e2.edge_idx);
                            size_t second_idx = std::max(e1.edge_idx, e2.edge_idx);
                            delete_edge_at(query, e1.match_idx, second_idx);
                            delete_edge_at(query, e1.match_idx, first_idx);
                        } else {
                            delete_edge_at(query, e1.match_idx, e1.edge_idx);
                            delete_edge_at(query, e2.match_idx, e2.edge_idx);
                        }
                    } else {
                        // Delete only one edge, leaving a self-loop.
                        if (e1.match_idx == e2.match_idx) {
                            size_t second_idx = std::max(e1.edge_idx, e2.edge_idx);
                            delete_edge_at(query, e1.match_idx, second_idx);
                        } else {
                            delete_edge_at(query, e2.match_idx, e2.edge_idx);
                        }
                    }

                    changed = true;
                    break;
                }
            }
            if (changed) break;
        }

    } while (changed);

    prune_redundant_single_node_matches(query);
}

} // namespace ragedb::gql
