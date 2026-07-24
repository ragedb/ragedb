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

#include "IrreflexiveContradictionPruner.h"
#include "../GqlVirtualCatalog.h"
#include <map>
#include <set>
#include <queue>

namespace ragedb::gql {

namespace {

void collect_equalities(const Expression* expr, std::map<std::string, std::set<std::string>>& node_eq) {
    if (!expr) return;
    if (expr->kind == ExpressionKind::BINARY_OP) {
        const auto* bin = static_cast<const BinaryOpExpr*>(expr);
        if (bin->op == BinaryOpKind::AND) {
            collect_equalities(bin->left.get(), node_eq);
            collect_equalities(bin->right.get(), node_eq);
        } else if (bin->op == BinaryOpKind::EQ) {
            const Expression* left = bin->left.get();
            const Expression* right = bin->right.get();
            
            if (left->kind == ExpressionKind::VARIABLE && right->kind == ExpressionKind::VARIABLE) {
                std::string v1 = static_cast<const VariableExpr*>(left)->name;
                std::string v2 = static_cast<const VariableExpr*>(right)->name;
                node_eq[v1].insert(v2);
                node_eq[v2].insert(v1);
            } else if (left->kind == ExpressionKind::PROPERTY_LOOKUP && right->kind == ExpressionKind::PROPERTY_LOOKUP) {
                const auto* pl1 = static_cast<const PropertyLookupExpr*>(left);
                const auto* pl2 = static_cast<const PropertyLookupExpr*>(right);
                if (pl1->property == "id" && pl2->property == "id") {
                    node_eq[pl1->variable].insert(pl2->variable);
                    node_eq[pl2->variable].insert(pl1->variable);
                }
            }
        }
    }
}

bool are_equal_vars(const std::string& v1, const std::string& v2, const std::map<std::string, std::set<std::string>>& node_eq) {
    if (v1 == v2) return true;
    auto it = node_eq.find(v1);
    if (it == node_eq.end()) return false;
    
    std::unordered_set<std::string> visited;
    std::queue<std::string> q;
    q.push(v1);
    visited.insert(v1);
    
    while (!q.empty()) {
        std::string curr = q.front();
        q.pop();
        if (curr == v2) return true;
        
        auto n_it = node_eq.find(curr);
        if (n_it != node_eq.end()) {
            for (const auto& neighbor : n_it->second) {
                if (visited.count(neighbor) == 0) {
                    visited.insert(neighbor);
                    q.push(neighbor);
                }
            }
        }
    }
    return false;
}

} // namespace

void IrreflexiveContradictionPruner::irreflexive_contradiction_pass(GqlQuery& query) {
    if (query.skip_semantic || query.kind != QueryKind::SINGLE) return;
    // Every rewrite here is keyed on registered algebraic traits; with none registered
    // the pass cannot fire, so skip the per-match scanning entirely.
    if (GqlVirtualCatalog::local().get_relationship_algebraic_properties().empty()) return;

    // Collect variable equality class map
    std::map<std::string, std::set<std::string>> node_eq;
    if (query.where_expr) {
        collect_equalities(query.where_expr.get(), node_eq);
    }
    for (const auto& match : query.matches) {
        // An equality inside an OPTIONAL MATCH only filters that optional binding; it must not
        // make the whole query a contradiction.
        if (match.is_optional) continue;
        for (const auto& node : match.pattern.nodes) {
            if (node.where_expr) {
                collect_equalities(node.where_expr.get(), node_eq);
            }
        }
        for (const auto& edge : match.pattern.edges) {
            if (edge.where_expr) {
                collect_equalities(edge.where_expr.get(), node_eq);
            }
        }
    }

    // Scan for self-loops on irreflexive relationships
    for (const auto& match : query.matches) {
        if (match.is_optional || match.is_search || match.is_propagate || match.is_khop) continue;
        
        for (size_t i = 0; i < match.pattern.edges.size(); ++i) {
            const auto& edge = match.pattern.edges[i];
            // Irreflexivity forbids only a DIRECT (1-hop) self-loop. A variable-length edge can
            // reach the same node via a longer path (e.g. *2..2 = a->b->a), which is satisfiable.
            if (edge.is_variable_length) continue;
            if (edge.label_expr && edge.label_expr->kind == LabelExprKind::LITERAL) {
                std::string rel_type = edge.label_expr->name;

                if (GqlVirtualCatalog::local().has_relationship_algebraic_property(rel_type, "irreflexive")) {
                    std::string src = match.pattern.nodes[i].variable;
                    std::string tgt = match.pattern.nodes[i + 1].variable;

                    // Anonymous endpoints are DISTINCT nodes, not a self-loop; only prune when the
                    // two endpoints are the same named variable (or provably equal via WHERE).
                    if (src.empty() || tgt.empty()) continue;

                    if (are_equal_vars(src, tgt, node_eq)) {
                        // Impossible self-loop on an irreflexive relationship!
                        query.no_op = true;
                        return;
                    }
                }
            }
        }
    }
}

} // namespace ragedb::gql
