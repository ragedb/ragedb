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

#include "AlgebraicRewriter.h"
#include "OptimizerUtils.h"

namespace ragedb::gql {

void AlgebraicRewriter::algebraic_rewriter_pass(GqlQuery& query) {
    if (query.kind != QueryKind::SINGLE) return;

    // Check for k-hop path count algebraic rewrite candidate
    if (query.matches.size() == 1 && query.writes.empty()) {
        auto& match = query.matches[0];
        if (!match.is_optional && match.pattern.nodes.size() >= 2) {
            bool match_candidate = false;
            uint16_t path_count_hops = 0;
            std::string start_var;
            std::string end_var;
            std::vector<std::string> rel_types;
            EdgeDirection edge_dir = EdgeDirection::RIGHT;

            // Case 1: Variable-length pattern (a)-[:EDGE]->{k}(d)
            if (match.pattern.nodes.size() == 2 && match.pattern.edges.size() == 1) {
                const auto& start_node = match.pattern.nodes[0];
                const auto& end_node = match.pattern.nodes[1];
                const auto& edge = match.pattern.edges[0];
                
                if (!start_node.variable.empty() && !end_node.variable.empty() &&
                    edge.is_variable_length && edge.min_hops == edge.max_hops && edge.min_hops > 1 &&
                    !end_node.label_expr && end_node.properties.empty() && end_node.property_filters.empty() &&
                    edge.properties.empty() && edge.property_filters.empty()) {
                    
                    path_count_hops = static_cast<uint16_t>(edge.min_hops);
                    start_var = start_node.variable;
                    end_var = end_node.variable;
                    
                    extract_rel_types(edge.label_expr.get(), rel_types);
                    edge_dir = edge.direction;
                    match_candidate = true;
                }
            }
            // Case 2: Hop chain (a)-[:EDGE]->(b)-[:EDGE]->(c)-[:EDGE]->(d)
            else if (match.pattern.nodes.size() > 2) {
                size_t num_edges = match.pattern.edges.size();
                size_t num_nodes = match.pattern.nodes.size();
                if (num_edges == num_nodes - 1) {
                    const auto& start_node = match.pattern.nodes[0];
                    const auto& end_node = match.pattern.nodes[num_nodes - 1];

                    // A cyclic chain (endpoints share one variable) carries a start==end join the
                    // from-scratch path count would drop.
                    if (!start_node.variable.empty() && !end_node.variable.empty() &&
                        start_node.variable != end_node.variable) {
                        // Check intermediate nodes
                        bool valid = true;
                        for (size_t i = 1; i < num_nodes - 1; ++i) {
                            const auto& node = match.pattern.nodes[i];
                            if (node.label_expr || !node.properties.empty() || !node.property_filters.empty()) {
                                valid = false;
                                break;
                            }
                        }
                        
                        // Check all edges consistency
                        if (valid) {
                            const auto& first_edge = match.pattern.edges[0];
                            if (first_edge.is_variable_length || !first_edge.properties.empty() || !first_edge.property_filters.empty()) {
                                valid = false;
                            } else {
                                std::vector<std::string> first_types;
                                extract_rel_types(first_edge.label_expr.get(), first_types);
                                EdgeDirection first_dir = first_edge.direction;
                                
                                for (size_t i = 1; i < num_edges; ++i) {
                                    const auto& edge = match.pattern.edges[i];
                                    if (edge.is_variable_length || !edge.properties.empty() || !edge.property_filters.empty()) {
                                        valid = false;
                                        break;
                                    }
                                    std::vector<std::string> types;
                                    extract_rel_types(edge.label_expr.get(), types);
                                    if (types != first_types) {
                                        valid = false;
                                        break;
                                    }
                                    if (edge.direction != first_dir) {
                                        valid = false;
                                        break;
                                    }
                                }
                                
                                if (valid) {
                                    path_count_hops = static_cast<uint16_t>(num_edges);
                                    start_var = start_node.variable;
                                    end_var = end_node.variable;
                                    rel_types = first_types;
                                    edge_dir = first_dir;
                                    match_candidate = true;
                                }
                            }
                        }
                    }
                }
            }

            if (match_candidate) {
                std::vector<std::string> vars_to_check;
                vars_to_check.push_back(end_var);
                
                // Add edge variables
                for (const auto& edge : match.pattern.edges) {
                    if (!edge.variable.empty()) {
                        vars_to_check.push_back(edge.variable);
                    }
                }
                
                // Add intermediate node variables
                for (size_t i = 1; i < match.pattern.nodes.size() - 1; ++i) {
                    const auto& node = match.pattern.nodes[i];
                    if (!node.variable.empty()) {
                        vars_to_check.push_back(node.variable);
                    }
                }
                
                bool referenced_outside = false;
                for (const auto& var : vars_to_check) {
                    if (query.where_expr && is_variable_referenced_outside_count(query.where_expr.get(), var)) {
                        referenced_outside = true;
                        break;
                    }
                    for (const auto& item : query.returns) {
                        if (is_variable_referenced_outside_count(item.expr.get(), var)) {
                            referenced_outside = true;
                            break;
                        }
                    }
                    for (const auto& spec : query.order_by) {
                        if (is_variable_referenced_outside_count(spec.expr.get(), var)) {
                            referenced_outside = true;
                            break;
                        }
                    }
                    if (referenced_outside) break;
                }

                // DISTINCT aggregates count distinct values, not paths; the algebraic path-count
                // rewrite (and the pattern truncation it triggers) would corrupt them. Piped-row
                // parts must count relative to their input rows, not from-scratch path counts.
                if (!referenced_outside && !query_has_distinct_aggregate(query) && !query.consumes_piped_rows) {
                    // Rewrite aggregate COUNT functions to the target variable directly.
                    for (auto& item : query.returns) {
                        if (!item.alias.has_value() && item.expr) {
                            if (item.expr->kind == ExpressionKind::AGGREGATION) {
                                auto* agg = static_cast<const AggregateExpr*>(item.expr.get());
                                if (agg->fn_kind == AggregateKind::COUNT) {
                                    if (!agg->expr) {
                                        item.alias = "count(*)";
                                    } else if (agg->expr->kind == ExpressionKind::VARIABLE) {
                                        item.alias = "count(" + static_cast<const VariableExpr*>(agg->expr.get())->name + ")";
                                    }
                                }
                            }
                        }
                    }

                    for (auto& item : query.returns) {
                        rewrite_khop_count_to_var(item.expr, end_var);
                    }
                    for (auto& spec : query.order_by) {
                        rewrite_khop_count_to_var(spec.expr, end_var);
                    }

                    match.algebraic_path_count = true;
                    match.path_count_hops = path_count_hops;
                    match.path_count_target_var = end_var;
                    match.path_count_rel_types = std::move(rel_types);
                    match.path_count_dir = edge_dir;

                    // Truncate match pattern to only start node
                    match.pattern.edges.clear();
                    match.pattern.nodes.resize(1);
                }
            }
        }
    }

    std::set<std::string> grouping_vars;
    for (const auto& item : query.returns) {
        if (item.expr && item.expr->kind != ExpressionKind::AGGREGATION) {
            if (item.expr->kind == ExpressionKind::VARIABLE) {
                grouping_vars.insert(static_cast<VariableExpr*>(item.expr.get())->name);
            } else if (item.expr->kind == ExpressionKind::PROPERTY_LOOKUP) {
                grouping_vars.insert(static_cast<PropertyLookupExpr*>(item.expr.get())->variable);
            }
        }
    }
    
    auto only_references_grouping = [&](const Expression* expr) -> bool {
        if (!expr) return true;
        std::vector<const Expression*> stack = { expr };
        while (!stack.empty()) {
            const auto* curr = stack.back();
            stack.pop_back();
            if (curr->kind == ExpressionKind::VARIABLE) {
                if (grouping_vars.find(static_cast<const VariableExpr*>(curr)->name) == grouping_vars.end()) {
                    return false;
                }
            } else if (curr->kind == ExpressionKind::PROPERTY_LOOKUP) {
                if (grouping_vars.find(static_cast<const PropertyLookupExpr*>(curr)->variable) == grouping_vars.end()) {
                    return false;
                }
            } else if (curr->kind == ExpressionKind::UNARY_OP) {
                stack.push_back(static_cast<const UnaryOpExpr*>(curr)->expr.get());
            } else if (curr->kind == ExpressionKind::BINARY_OP) {
                const auto* bin = static_cast<const BinaryOpExpr*>(curr);
                stack.push_back(bin->left.get());
                stack.push_back(bin->right.get());
            }
        }
        return true;
    };
    
    for (auto& item : query.returns) {
        if (item.expr && item.expr->kind == ExpressionKind::AGGREGATION) {
            auto* agg = static_cast<AggregateExpr*>(item.expr.get());
            // sum(DISTINCT a*b) dedups the products; factoring one side out changes what is deduped.
            if (agg->fn_kind == AggregateKind::SUM && !agg->distinct && agg->expr && agg->expr->kind == ExpressionKind::BINARY_OP) {
                auto* bin = static_cast<BinaryOpExpr*>(agg->expr.get());
                if (bin->op == BinaryOpKind::MUL) {
                    if (only_references_grouping(bin->left.get())) {
                        auto left_clone = bin->left->clone();
                        auto right_clone = bin->right->clone();
                        auto new_sum = std::make_unique<AggregateExpr>(AggregateKind::SUM, std::move(right_clone));
                        item.expr = std::make_unique<BinaryOpExpr>(BinaryOpKind::MUL, std::move(left_clone), std::move(new_sum));
                    }
                    else if (only_references_grouping(bin->right.get())) {
                        auto left_clone = bin->left->clone();
                        auto right_clone = bin->right->clone();
                        auto new_sum = std::make_unique<AggregateExpr>(AggregateKind::SUM, std::move(left_clone));
                        item.expr = std::make_unique<BinaryOpExpr>(BinaryOpKind::MUL, std::move(right_clone), std::move(new_sum));
                    }
                }
            }
        }
    }
}

} // namespace ragedb::gql
