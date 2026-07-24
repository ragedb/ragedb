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

#include "DegreeConstraintPruner.h"
#include <vector>

namespace ragedb::gql {

namespace {

template <typename T>
void rewrite_size_expressions_recursive(T& expr, GqlQuery& query) {
    if (!expr) return;

    if (expr->kind == ExpressionKind::SIZE_OP) {
        auto* size_expr = static_cast<SizeExpr*>(expr.get());
        if (size_expr->matches.size() == 1) {
            auto& match = size_expr->matches[0];
            if (match.pattern.nodes.size() == 2 && match.pattern.edges.size() == 1) {
                const auto& start_node = match.pattern.nodes[0];
                const auto& edge = match.pattern.edges[0];
                
                std::string v = start_node.variable;
                if (!v.empty() && !edge.is_variable_length) {
                    // The degree count is a pure edge count by rel-type + direction; it cannot see the far
                    // node or filter the edge. If either carries a constraint, leave the SizeExpr in place so
                    // the correlated precompute counts actual matching pattern rows instead (a bare degree
                    // here would silently ignore the far-node label/property and overcount).
                    const auto& far_node = match.pattern.nodes[1];
                    if (far_node.label_expr || !far_node.properties.empty() ||
                        !far_node.property_filters.empty() || far_node.where_expr ||
                        !edge.properties.empty() || !edge.property_filters.empty() || edge.where_expr) {
                        return;
                    }
                    std::string rel_type;
                    if (edge.label_expr && edge.label_expr->kind == LabelExprKind::LITERAL) {
                        rel_type = edge.label_expr->name;
                    }
                    
                    Direction dir = Direction::BOTH;
                    std::string dir_str = "BOTH";
                    if (edge.direction == EdgeDirection::RIGHT) {
                        dir = Direction::OUT;
                        dir_str = "OUT";
                    } else if (edge.direction == EdgeDirection::LEFT) {
                        dir = Direction::IN;
                        dir_str = "IN";
                    }
                    
                    std::string property_name = "_deg_" + v + "_" + (rel_type.empty() ? "ANY" : rel_type) + "_" + dir_str;
                    
                    // Add degree population info to the matching PatternNode in main query match pattern
                    bool found_node = false;
                    for (auto& q_match : query.matches) {
                        for (auto& q_node : q_match.pattern.nodes) {
                            if (q_node.variable == v) {
                                DegreePopulateInfo info;
                                info.property_name = property_name;
                                if (!rel_type.empty()) {
                                    info.rel_types.push_back(rel_type);
                                }
                                info.direction = dir;
                                q_node.degree_opt_info.push_back(std::move(info));
                                found_node = true;
                                break;
                            }
                        }
                        if (found_node) break;
                    }
                    
                    if (found_node) {
                        // Replace the SizeExpr with a property lookup of the virtual property
                        expr = std::make_unique<PropertyLookupExpr>(v, property_name);
                    }
                }
            }
        }
        return;
    }

    // Recurse into nested structures
    switch (expr->kind) {
        case ExpressionKind::UNARY_OP: {
            auto* un = static_cast<UnaryOpExpr*>(expr.get());
            rewrite_size_expressions_recursive(un->expr, query);
            break;
        }
        case ExpressionKind::IS_NULL_CHECK: {
            auto* is_null = static_cast<IsNullExpr*>(expr.get());
            rewrite_size_expressions_recursive(is_null->expr, query);
            break;
        }
        case ExpressionKind::BINARY_OP: {
            auto* bin = static_cast<BinaryOpExpr*>(expr.get());
            rewrite_size_expressions_recursive(bin->left, query);
            rewrite_size_expressions_recursive(bin->right, query);
            break;
        }
        case ExpressionKind::AGGREGATION: {
            auto* agg = static_cast<AggregateExpr*>(expr.get());
            rewrite_size_expressions_recursive(agg->expr, query);
            break;
        }
        default:
            break;
    }
}

} // namespace

void DegreeConstraintPruner::degree_constraint_pruning_pass(GqlQuery& query) {
    if (query.kind != QueryKind::SINGLE) return;

    // Rewrite SizeExprs in query where_expr
    rewrite_size_expressions_recursive(query.where_expr, query);

    // Rewrite SizeExprs in node pattern where_expr
    for (auto& match : query.matches) {
        for (auto& node : match.pattern.nodes) {
            rewrite_size_expressions_recursive(node.where_expr, query);
        }
        for (auto& edge : match.pattern.edges) {
            rewrite_size_expressions_recursive(edge.where_expr, query);
        }
    }

    // Rewrite SizeExprs in projected return expressions
    for (auto& item : query.returns) {
        rewrite_size_expressions_recursive(item.expr, query);
    }
}

} // namespace ragedb::gql
