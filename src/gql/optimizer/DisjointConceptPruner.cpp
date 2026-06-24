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

#include "DisjointConceptPruner.h"
#include "../GqlVirtualCatalog.h"
#include "../GqlParser.h"
#include "OptimizerUtils.h"
#include <queue>
#include <unordered_map>
#include <tuple>
#include <set>

namespace ragedb::gql {

namespace {

/**
 * @brief Key representing a unique parent-child taxonomy relationship on a specific label and property.
 */
struct HierarchyKey {
    std::string rel_type;
    std::string child_label;
    std::string parent_label;
    std::string prop_name;

    bool operator<(const HierarchyKey& other) const {
        return std::tie(rel_type, child_label, parent_label, prop_name) <
               std::tie(other.rel_type, other.child_label, other.parent_label, other.prop_name);
    }
};

/**
 * @brief Helper to convert a property value variant to a string representation.
 */
std::string to_string_val(const property_type_t& val) {
    if (std::holds_alternative<std::string>(val)) {
        return std::get<std::string>(val);
    } else if (std::holds_alternative<int64_t>(val)) {
        return std::to_string(std::get<int64_t>(val));
    }
    return "";
}

/**
 * @brief Scans an expression for equality comparisons to literals and adds them to the map.
 */
void extract_equalities(const Expression* expr, std::map<std::string, std::map<std::string, property_type_t>>& equalities) {
    if (!expr) return;
    if (expr->kind == ExpressionKind::BINARY_OP) {
        const auto* bin = static_cast<const BinaryOpExpr*>(expr);
        if (bin->op == BinaryOpKind::AND) {
            extract_equalities(bin->left.get(), equalities);
            extract_equalities(bin->right.get(), equalities);
        } else if (bin->op == BinaryOpKind::EQ) {
            if (bin->left->kind == ExpressionKind::PROPERTY_LOOKUP && bin->right->kind == ExpressionKind::LITERAL) {
                const auto* pl = static_cast<const PropertyLookupExpr*>(bin->left.get());
                const auto* lit = static_cast<const LiteralExpr*>(bin->right.get());
                equalities[pl->variable][pl->property] = lit->value;
            } else if (bin->right->kind == ExpressionKind::PROPERTY_LOOKUP && bin->left->kind == ExpressionKind::LITERAL) {
                const auto* pl = static_cast<const PropertyLookupExpr*>(bin->right.get());
                const auto* lit = static_cast<const LiteralExpr*>(bin->left.get());
                equalities[pl->variable][pl->property] = lit->value;
            }
        }
    }
}

/**
 * @brief Collects all query variable equality constraints mapping to property literals.
 */
std::map<std::string, std::map<std::string, property_type_t>> collect_query_equalities(const GqlQuery& query) {
    std::map<std::string, std::map<std::string, property_type_t>> equalities;
    for (const auto& match : query.matches) {
        if (match.is_search) continue;
        for (const auto& node : match.pattern.nodes) {
            if (!node.variable.empty()) {
                for (const auto& [prop, val] : node.properties) {
                    equalities[node.variable][prop] = val;
                }
                extract_equalities(node.where_expr.get(), equalities);
            }
        }
        for (const auto& edge : match.pattern.edges) {
            if (!edge.variable.empty()) {
                for (const auto& [prop, val] : edge.properties) {
                    equalities[edge.variable][prop] = val;
                }
                extract_equalities(edge.where_expr.get(), equalities);
            }
        }
    }
    extract_equalities(query.where_expr.get(), equalities);
    return equalities;
}

/**
 * @brief Parses virtual catalog check constraints to reconstruct taxonomy (parent-child category) hierarchies.
 * @return A map of hierarchy keys to child-to-parents adjacency lists.
 */
std::map<HierarchyKey, std::map<std::string, std::vector<std::string>>> build_taxonomy_graphs() {
    std::map<HierarchyKey, std::map<std::string, std::vector<std::string>>> graphs;
    const auto& constraints = GqlVirtualCatalog::local().get_constraints();
    for (const auto& [name, query_str] : constraints) {
        try {
            auto c_query = GqlParser::parse(query_str);
            if (c_query.kind != QueryKind::SINGLE) continue;
            if (c_query.matches.size() != 1) continue;
            
            const auto& match = c_query.matches[0];
            if (match.pattern.nodes.size() != 2 || match.pattern.edges.size() != 1) continue;
            
            const auto& node0 = match.pattern.nodes[0];
            const auto& edge = match.pattern.edges[0];
            const auto& node1 = match.pattern.nodes[1];
            
            if (!node0.label_expr || node0.label_expr->kind != LabelExprKind::LITERAL) continue;
            if (!node1.label_expr || node1.label_expr->kind != LabelExprKind::LITERAL) continue;
            if (!edge.label_expr || edge.label_expr->kind != LabelExprKind::LITERAL) continue;
            
            std::string rel_type = edge.label_expr->name;
            const PatternNode* child_ptr = nullptr;
            const PatternNode* parent_ptr = nullptr;
            
            // Map the child/parent roles based on relationship direction
            if (edge.direction == EdgeDirection::RIGHT) {
                child_ptr = &node0;
                parent_ptr = &node1;
            } else if (edge.direction == EdgeDirection::LEFT) {
                child_ptr = &node1;
                parent_ptr = &node0;
            } else {
                continue;
            }
            
            auto c_eqs = collect_query_equalities(c_query);
            const auto& child_var = child_ptr->variable;
            const auto& parent_var = parent_ptr->variable;
            
            if (child_var.empty() || parent_var.empty()) continue;
            
            auto child_it = c_eqs.find(child_var);
            auto parent_it = c_eqs.find(parent_var);
            if (child_it == c_eqs.end() || parent_it == c_eqs.end()) continue;
            
            // Find properties present in both endpoints (representing sub-category values)
            for (const auto& [prop, child_val_variant] : child_it->second) {
                auto parent_prop_it = parent_it->second.find(prop);
                if (parent_prop_it != parent_it->second.end()) {
                    std::string child_val = to_string_val(child_val_variant);
                    std::string parent_val = to_string_val(parent_prop_it->second);
                    if (!child_val.empty() && !parent_val.empty() && child_val != parent_val) {
                        HierarchyKey key{rel_type, child_ptr->label_expr->name, parent_ptr->label_expr->name, prop};
                        graphs[key][child_val].push_back(parent_val);
                    }
                }
            }
        } catch (...) {
            // Ignore parsing errors
        }
    }
    return graphs;
}

/**
 * @brief Performs a BFS to find all ancestor categories reachable from a given category value.
 */
std::set<std::string> get_all_ancestors(const std::map<std::string, std::vector<std::string>>& adj, const std::string& start) {
    std::set<std::string> visited;
    std::queue<std::string> q;
    q.push(start);
    visited.insert(start);
    
    while (!q.empty()) {
        std::string curr = q.front();
        q.pop();
        
        auto it = adj.find(curr);
        if (it != adj.end()) {
            for (const auto& parent : it->second) {
                if (visited.insert(parent).second) {
                    q.push(parent);
                }
            }
        }
    }
    return visited;
}

} // namespace

/**
 * @brief Phase 20: Disjoint Concept Path Pruning.
 *        Checks for impossibility/unsatisfiability on variable-length path traversal endpoints:
 *        1. Direct disjoint label check: If the start and end node labels of a variable-length edge
 *           are declared disjoint, the traversal is impossible.
 *        2. Taxonomy value check: Traverses taxonomy graphs to find all ancestor categories.
 *           If any ancestor of the start category is disjoint with any ancestor of the end category,
 *           the path is unsatisfiable.
 *        If either condition is met, the query is marked as no_op to short-circuit execution.
 */
void DisjointConceptPruner::disjoint_concept_pruning_pass(GqlQuery& query) {
    if (query.skip_semantic || query.kind != QueryKind::SINGLE) return;
    
    const auto& catalog = GqlVirtualCatalog::local();
    const auto& disjoint_labels = catalog.get_disjoint_labels();
    const auto& disjoint_values = catalog.get_disjoint_values();
    
    if (disjoint_labels.empty() && disjoint_values.empty()) return;
    
    auto graphs = build_taxonomy_graphs();
    auto equalities = collect_query_equalities(query);
    
    for (const auto& match : query.matches) {
        if (match.is_search) continue;
        
        size_t num_nodes = match.pattern.nodes.size();
        size_t num_edges = match.pattern.edges.size();
        if (num_nodes < 2 || num_edges < 1) continue;
        
        for (size_t i = 0; i < num_edges; ++i) {
            const auto& edge = match.pattern.edges[i];
            if (!edge.is_variable_length || !edge.label_expr || edge.label_expr->kind != LabelExprKind::LITERAL) continue;
            
            const auto& start_node = match.pattern.nodes[i];
            const auto& end_node = match.pattern.nodes[i + 1];
            
            // 1. Check direct label disjointness if both start and end have labels
            if (start_node.label_expr && start_node.label_expr->kind == LabelExprKind::LITERAL &&
                end_node.label_expr && end_node.label_expr->kind == LabelExprKind::LITERAL) {
                std::string l1 = start_node.label_expr->name;
                std::string l2 = end_node.label_expr->name;
                
                auto it = disjoint_labels.find(l1);
                if (it != disjoint_labels.end() && it->second.count(l2)) {
                    query.no_op = true;
                    return;
                }
            }
            
            // 2. Check value disjointness using the taxonomy graphs
            std::string rel_type = edge.label_expr->name;
            for (const auto& [key, adj] : graphs) {
                if (key.rel_type != rel_type) continue;
                
                std::string prop = key.prop_name;
                
                auto start_eq_it = equalities.find(start_node.variable);
                auto end_eq_it = equalities.find(end_node.variable);
                if (start_eq_it == equalities.end() || end_eq_it == equalities.end()) continue;
                
                auto start_prop_it = start_eq_it->second.find(prop);
                auto end_prop_it = end_eq_it->second.find(prop);
                if (start_prop_it == start_eq_it->second.end() || end_prop_it == end_eq_it->second.end()) continue;
                
                std::string val1 = to_string_val(start_prop_it->second);
                std::string val2 = to_string_val(end_prop_it->second);
                
                if (val1.empty() || val2.empty()) continue;
                
                // Get all ancestors (reachable nodes) for both values
                auto ancestors1 = get_all_ancestors(adj, val1);
                auto ancestors2 = get_all_ancestors(adj, val2);
                
                // If any ancestor of val1 is disjoint with any ancestor of val2, the path is impossible
                for (const auto& a1 : ancestors1) {
                    auto dj_it = disjoint_values.find(a1);
                    if (dj_it != disjoint_values.end()) {
                        for (const auto& a2 : ancestors2) {
                            if (dj_it->second.count(a2)) {
                                query.no_op = true;
                                return;
                            }
                        }
                    }
                }
            }
        }
    }
}

} // namespace ragedb::gql
