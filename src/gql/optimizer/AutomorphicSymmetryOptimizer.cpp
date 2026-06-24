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

#include "AutomorphicSymmetryOptimizer.h"
#include <unordered_map>
#include <set>
#include <vector>
#include <algorithm>

namespace ragedb::gql {

void AutomorphicSymmetryOptimizer::automorphic_symmetry_pass(GqlQuery& query) {
    if (query.kind != QueryKind::SINGLE) return;
    
    // Check if it is a pure counting query
    if (query.returns.size() != 1) return;
    const auto& ret = query.returns[0];
    if (!ret.expr || ret.expr->kind != ExpressionKind::AGGREGATION) return;
    auto* agg = static_cast<const AggregateExpr*>(ret.expr.get());
    if (agg->fn_kind != AggregateKind::COUNT) return;
    
    // Check that there are no global where filters or other complex expressions
    if (query.where_expr != nullptr) return;
    
    // Collect variables, labels, and connections
    std::set<std::string> node_vars;
    std::unordered_map<std::string, std::string> var_to_label;
    std::set<std::string> node_labels;
    std::set<std::string> edge_labels;
    
    struct TempEdge {
        std::string src;
        std::string tgt;
    };
    std::vector<TempEdge> edges;
    
    for (const auto& match : query.matches) {
        if (match.is_search || match.is_khop || match.algebraic_path_count) return;
        
        // Ensure no inline where filters on nodes or edges
        for (const auto& node : match.pattern.nodes) {
            if (!node.properties.empty() || !node.property_filters.empty() || node.where_expr != nullptr) return;
            if (node.variable.empty()) return;
            
            if (node.label_expr) {
                if (node.label_expr->kind == LabelExprKind::LITERAL) {
                    std::string label = node.label_expr->name;
                    auto it = var_to_label.find(node.variable);
                    if (it != var_to_label.end() && it->second != label) {
                        return; // Conflicting labels for the same variable
                    }
                    var_to_label[node.variable] = label;
                } else {
                    return; // Must be literal label if specified
                }
            }
            node_vars.insert(node.variable);
        }
        
        for (const auto& edge : match.pattern.edges) {
            if (!edge.properties.empty() || !edge.property_filters.empty() || edge.where_expr != nullptr) return;
            if (edge.is_variable_length) return;
            if (edge.label_expr && edge.label_expr->kind == LabelExprKind::LITERAL) {
                edge_labels.insert(edge.label_expr->name);
            } else {
                return; // Must have literal label
            }
        }
        
        for (size_t i = 0; i < match.pattern.edges.size(); ++i) {
            std::string src = match.pattern.nodes[i].variable;
            std::string tgt = match.pattern.nodes[i+1].variable;
            if (match.pattern.edges[i].direction == EdgeDirection::LEFT) {
                std::swap(src, tgt);
            } else if (match.pattern.edges[i].direction == EdgeDirection::ANY) {
                return; // Only support directed cycles for now
            }
            edges.push_back({src, tgt});
        }
    }
    
    // Resolve labels for all node variables
    for (const auto& var : node_vars) {
        auto it = var_to_label.find(var);
        if (it == var_to_label.end() || it->second.empty()) {
            return; // Must have label specified somewhere
        }
        node_labels.insert(it->second);
    }
    
    // Must be homogeneous nodes and edges
    if (node_labels.size() != 1 || edge_labels.size() != 1) return;
    
    // Check if it is a 3-cycle (triangle)
    if (node_vars.size() != 3 || edges.size() != 3) return;
    
    // Verify that the edges form a cycle of length 3
    std::vector<std::string> vars(node_vars.begin(), node_vars.end());
    std::sort(vars.begin(), vars.end());
    std::string v1 = vars[0];
    std::string v2 = vars[1];
    std::string v3 = vars[2];
    
    bool has_12 = false, has_23 = false, has_31 = false;
    for (const auto& edge : edges) {
        if ((edge.src == v1 && edge.tgt == v2) || (edge.src == v2 && edge.tgt == v1)) has_12 = true;
        if ((edge.src == v2 && edge.tgt == v3) || (edge.src == v3 && edge.tgt == v2)) has_23 = true;
        if ((edge.src == v3 && edge.tgt == v1) || (edge.src == v1 && edge.tgt == v3)) has_31 = true;
    }
    
    if (has_12 && has_23 && has_31) {
        // Enforce v1 < v2 AND v2 < v3 canonical ordering
        auto lt_12 = std::make_unique<BinaryOpExpr>(
            BinaryOpKind::LT,
            std::make_unique<VariableExpr>(v1),
            std::make_unique<VariableExpr>(v2)
        );
        auto lt_23 = std::make_unique<BinaryOpExpr>(
            BinaryOpKind::LT,
            std::make_unique<VariableExpr>(v2),
            std::make_unique<VariableExpr>(v3)
        );
        query.where_expr = std::make_unique<BinaryOpExpr>(
            BinaryOpKind::AND,
            std::move(lt_12),
            std::move(lt_23)
        );
        query.count_multiplication_factor = 6;
    }
}

} // namespace ragedb::gql
