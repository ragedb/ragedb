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

#include "GqlOptimizer.h"

namespace ragedb::gql {

/**
 * @brief Recursively traverses the expression tree to extract property filters for variables.
 *
 * Traverses binary expression trees, focusing on AND operators, to identify comparison operations
 * between a property lookup (e.g. `var.prop`) and a literal value. These filters are collected
 * and grouped by the variable name they apply to.
 *
 * Example:
 *   If the expression is: `p.age >= 30 AND e.since == 2020`
 *   Then `pushdowns` will be populated as:
 *     - "p" -> { property: "age", op: GTE, value: 30 }
 *     - "e" -> { property: "since", op: EQ, value: 2020 }
 *
 * @param expr The expression AST node to process.
 * @param pushdowns A map to accumulate property filters indexed by their variable names.
 */
void GqlOptimizer::extract_filters(Expression* expr, std::map<std::string, std::vector<PropertyFilter>>& pushdowns) {
    if (!expr) return;

    if (expr->kind == ExpressionKind::BINARY_OP) {
        auto* bin = static_cast<BinaryOpExpr*>(expr);
        if (bin->op == BinaryOpKind::AND) {
            extract_filters(bin->left.get(), pushdowns);
            extract_filters(bin->right.get(), pushdowns);
        } else {
            Operation op = Operation::UNKNOWN;
            switch (bin->op) {
                case BinaryOpKind::EQ: op = Operation::EQ; break;
                case BinaryOpKind::NE: op = Operation::NEQ; break;
                case BinaryOpKind::LT: op = Operation::LT; break;
                case BinaryOpKind::LE: op = Operation::LTE; break;
                case BinaryOpKind::GT: op = Operation::GT; break;
                case BinaryOpKind::GE: op = Operation::GTE; break;
                default: break;
            }

            if (op != Operation::UNKNOWN) {
                if (bin->left->kind == ExpressionKind::PROPERTY_LOOKUP && bin->right->kind == ExpressionKind::LITERAL) {
                    auto* prop_lookup = static_cast<PropertyLookupExpr*>(bin->left.get());
                    auto* lit = static_cast<LiteralExpr*>(bin->right.get());
                    pushdowns[prop_lookup->variable].push_back({prop_lookup->property, op, lit->value});
                } else if (bin->right->kind == ExpressionKind::PROPERTY_LOOKUP && bin->left->kind == ExpressionKind::LITERAL) {
                    auto* prop_lookup = static_cast<PropertyLookupExpr*>(bin->right.get());
                    auto* lit = static_cast<LiteralExpr*>(bin->left.get());
                    Operation inverted_op = op;
                    if (op == Operation::LT) inverted_op = Operation::GT;
                    else if (op == Operation::LTE) inverted_op = Operation::GTE;
                    else if (op == Operation::GT) inverted_op = Operation::LT;
                    else if (op == Operation::GTE) inverted_op = Operation::LTE;
                    pushdowns[prop_lookup->variable].push_back({prop_lookup->property, inverted_op, lit->value});
                }
            }
        }
    }
}

std::unique_ptr<Expression> GqlOptimizer::rebuild_expression_without_pushed_predicates(std::unique_ptr<Expression> expr, const std::map<std::string, std::vector<PropertyFilter>>& pushdowns) {
    if (!expr) return nullptr;

    if (expr->kind == ExpressionKind::BINARY_OP) {
        auto* bin = static_cast<BinaryOpExpr*>(expr.get());
        if (bin->op == BinaryOpKind::AND) {
            auto new_left = rebuild_expression_without_pushed_predicates(std::move(bin->left), pushdowns);
            auto new_right = rebuild_expression_without_pushed_predicates(std::move(bin->right), pushdowns);
            if (new_left && new_right) {
                return std::make_unique<BinaryOpExpr>(BinaryOpKind::AND, std::move(new_left), std::move(new_right));
            } else if (new_left) {
                return new_left;
            } else {
                return new_right;
            }
        } else {
            Operation op = Operation::UNKNOWN;
            switch (bin->op) {
                case BinaryOpKind::EQ: op = Operation::EQ; break;
                case BinaryOpKind::NE: op = Operation::NEQ; break;
                case BinaryOpKind::LT: op = Operation::LT; break;
                case BinaryOpKind::LE: op = Operation::LTE; break;
                case BinaryOpKind::GT: op = Operation::GT; break;
                case BinaryOpKind::GE: op = Operation::GTE; break;
                default: break;
            }
            if (op != Operation::UNKNOWN) {
                if (bin->left->kind == ExpressionKind::PROPERTY_LOOKUP && bin->right->kind == ExpressionKind::LITERAL) {
                    auto* prop_lookup = static_cast<PropertyLookupExpr*>(bin->left.get());
                    auto it = pushdowns.find(prop_lookup->variable);
                    if (it != pushdowns.end()) {
                        for (const auto& filter : it->second) {
                            if (filter.property == prop_lookup->property && filter.op == op) {
                                return nullptr;
                            }
                        }
                    }
                } else if (bin->right->kind == ExpressionKind::PROPERTY_LOOKUP && bin->left->kind == ExpressionKind::LITERAL) {
                    auto* prop_lookup = static_cast<PropertyLookupExpr*>(bin->right.get());
                    auto it = pushdowns.find(prop_lookup->variable);
                    if (it != pushdowns.end()) {
                        Operation inverted_op = op;
                        if (op == Operation::LT) inverted_op = Operation::GT;
                        else if (op == Operation::LTE) inverted_op = Operation::GTE;
                        else if (op == Operation::GT) inverted_op = Operation::LT;
                        else if (op == Operation::GTE) inverted_op = Operation::LTE;
                        for (const auto& filter : it->second) {
                            if (filter.property == prop_lookup->property && filter.op == inverted_op) {
                                return nullptr;
                            }
                        }
                    }
                }
            }
        }
    }

    return expr;
}

namespace {

/**
 * @brief Recursively checks if a variable name is referenced anywhere in an expression 
 * except inside a COUNT aggregate function.
 */
bool is_variable_referenced_outside_count(const Expression* expr, const std::string& var_name) {
    if (!expr) return false;
    
    switch (expr->kind) {
        case ExpressionKind::VARIABLE: {
            auto* ve = static_cast<const VariableExpr*>(expr);
            return ve->name == var_name;
        }
        case ExpressionKind::PROPERTY_LOOKUP: {
            auto* pl = static_cast<const PropertyLookupExpr*>(expr);
            return pl->variable == var_name;
        }
        case ExpressionKind::UNARY_OP: {
            auto* un = static_cast<const UnaryOpExpr*>(expr);
            return is_variable_referenced_outside_count(un->expr.get(), var_name);
        }
        case ExpressionKind::BINARY_OP: {
            auto* bin = static_cast<const BinaryOpExpr*>(expr);
            return is_variable_referenced_outside_count(bin->left.get(), var_name) ||
                   is_variable_referenced_outside_count(bin->right.get(), var_name);
        }
        case ExpressionKind::AGGREGATION: {
            auto* agg = static_cast<const AggregateExpr*>(expr);
            // COUNT aggregates are the allowed target, so we don't treat them as "outside count"
            if (agg->fn_kind == AggregateKind::COUNT) {
                return false;
            }
            return is_variable_referenced_outside_count(agg->expr.get(), var_name);
        }
        default:
            return false;
    }
}

/**
 * @brief Recursively traverses the expression tree to find COUNT(target) or COUNT(*)
 * and rewrites them to SUM(start_node._degree_prop).
 */
void rewrite_count_to_sum_degree(std::unique_ptr<Expression>& expr, 
                                 const std::string& start_var, 
                                 const std::string& end_var, 
                                 const std::string& edge_var, 
                                 const std::string& degree_prop, 
                                 bool& rewritten) {
    if (!expr) return;
    
    if (expr->kind == ExpressionKind::AGGREGATION) {
        auto* agg = static_cast<AggregateExpr*>(expr.get());
        if (agg->fn_kind == AggregateKind::COUNT) {
            bool target_matches = false;
            if (!agg->expr) {
                // COUNT(*)
                target_matches = true;
            } else if (agg->expr->kind == ExpressionKind::VARIABLE) {
                auto* ve = static_cast<const VariableExpr*>(agg->expr.get());
                if (ve->name == end_var || ve->name == edge_var) {
                    target_matches = true;
                }
            }
            if (target_matches) {
                // Rewrite: COUNT(var) -> SUM(start_var.degree_prop)
                agg->fn_kind = AggregateKind::SUM;
                agg->expr = std::make_unique<PropertyLookupExpr>(start_var, degree_prop);
                rewritten = true;
            }
        }
    } else if (expr->kind == ExpressionKind::UNARY_OP) {
        auto* un = static_cast<UnaryOpExpr*>(expr.get());
        rewrite_count_to_sum_degree(un->expr, start_var, end_var, edge_var, degree_prop, rewritten);
    } else if (expr->kind == ExpressionKind::BINARY_OP) {
        auto* bin = static_cast<BinaryOpExpr*>(expr.get());
        rewrite_count_to_sum_degree(bin->left, start_var, end_var, edge_var, degree_prop, rewritten);
        rewrite_count_to_sum_degree(bin->right, start_var, end_var, edge_var, degree_prop, rewritten);
    }
}

/**
 * @brief Recursively extracts literal relationship type strings from OR-union labels.
 */
void extract_rel_types(const LabelExpression* expr, std::vector<std::string>& rel_types) {
    if (!expr) return;
    if (expr->kind == LabelExprKind::LITERAL) {
        rel_types.push_back(expr->name);
    } else if (expr->kind == LabelExprKind::OR) {
        extract_rel_types(expr->left.get(), rel_types);
        extract_rel_types(expr->right.get(), rel_types);
    }
}

} // namespace

/**
 * @brief Performs AST-level optimization passes on a GQL query.
 *
 * Current optimization passes:
 * 
 * 1. Predicate Pushdown Optimization:
 *    Identifies node/edge property filters in the global WHERE clause and pushes them
 *    directly down to the corresponding PatternNode/PatternEdge in the MATCH pattern.
 *    This allows the scan and traversal stages to filter elements as early as possible
 *    instead of filtering them downstream in a filter step.
 *    
 *    Example:
 *      Original GQL: 
 *        MATCH (p:Person)-[e:KNOWS]->(f:Person) 
 *        WHERE p.age >= 30 AND e.since == 2020 
 *        RETURN p.name, f.name;
 *      
 *      Optimized AST:
 *        The predicate "p.age >= 30" is pushed to the PatternNode for 'p'.
 *        The predicate "e.since == 2020" is pushed to the PatternEdge for 'e'.
 *        The global WHERE expression is cleared/simplified to avoid redundant execution.
 * 
 * 2. Relationship Count / Node Degree Optimization (Phase 5):
 *    Optimizes single-hop queries that count neighboring nodes or edges (e.g., node degree)
 *    by bypassing edge traversal entirely. It retrieves the pre-calculated node degree
 *    metadata from the shard and rewrites the query to sum the degrees.
 *    
 *    Example:
 *      Original GQL:
 *        MATCH (p:Person)-[:FRIEND]->(f)
 *        RETURN p.name, count(f);
 *      
 *      Optimized AST:
 *        - Column name preservation: Adds an explicit alias "count(f)" to the return item.
 *        - Pattern truncation: The pattern is truncated to a single node MATCH (p:Person).
 *        - Traversal bypass: Stores metadata on node 'p' to load its OUT degree for rel type "FRIEND"
 *          into a temporary property "_degree_p_opt".
 *        - Aggregation rewrite: Rewrites the return expression count(f) to sum(p._degree_p_opt).
 *
 * @param query The GQL query AST to optimize.
 */
void GqlOptimizer::optimize(GqlQuery& query) {
    if (query.schema_op.has_value()) {
        return;
    }

    if (query.kind != QueryKind::SINGLE) {
        if (query.left) optimize(*query.left);
        if (query.right) optimize(*query.right);
        return;
    }

    // --- Relationship Count / Degree Optimization ---
    // Perform optimization only for single-hop match statements in READ queries.
    if (query.kind == QueryKind::SINGLE && query.matches.size() == 1 && query.writes.empty()) {
        auto& match = query.matches[0];
        if (!match.is_optional && match.pattern.nodes.size() == 2 && match.pattern.edges.size() == 1) {
            const auto& start_node = match.pattern.nodes[0];
            const auto& end_node = match.pattern.nodes[1];
            const auto& edge = match.pattern.edges[0];
            
            // Optimization requires that:
            // 1. The start node has a non-empty variable name.
            // 2. The edge has no variable-length repetition.
            // 3. The end node has no labels or property filters.
            // 4. The edge has no property filters.
            if (!start_node.variable.empty() && !edge.is_variable_length &&
                !end_node.label_expr && end_node.properties.empty() && end_node.property_filters.empty() &&
                edge.properties.empty() && edge.property_filters.empty()) {
                
                std::string start_var = start_node.variable;
                std::string end_var = end_node.variable;
                std::string edge_var = edge.variable;
                
                // Verify that end_var (and edge_var if present) are not referenced outside COUNT aggregations.
                bool referenced_outside = false;
                if (!end_var.empty()) {
                    if (is_variable_referenced_outside_count(query.where_expr.get(), end_var)) referenced_outside = true;
                    for (const auto& spec : query.order_by) {
                        if (is_variable_referenced_outside_count(spec.expr.get(), end_var)) referenced_outside = true;
                    }
                    for (const auto& item : query.returns) {
                        if (is_variable_referenced_outside_count(item.expr.get(), end_var)) referenced_outside = true;
                    }
                }
                if (!edge_var.empty()) {
                    if (is_variable_referenced_outside_count(query.where_expr.get(), edge_var)) referenced_outside = true;
                    for (const auto& spec : query.order_by) {
                        if (is_variable_referenced_outside_count(spec.expr.get(), edge_var)) referenced_outside = true;
                    }
                    for (const auto& item : query.returns) {
                        if (is_variable_referenced_outside_count(item.expr.get(), edge_var)) referenced_outside = true;
                    }
                }
                
                if (!referenced_outside) {
                    // Unique degree property name to populate on the start node.
                    std::string degree_prop = "_degree_" + start_var + "_opt";
                    
                    // Rewrite aggregate COUNT functions to SUM of the degree property.
                    bool rewritten = false;
                    
                    // Pre-assign aliases for returns to preserve column output format transparently.
                    for (auto& item : query.returns) {
                        if (!item.alias.has_value() && item.expr) {
                            if (item.expr->kind == ExpressionKind::AGGREGATION) {
                                auto* agg = static_cast<const AggregateExpr*>(item.expr.get());
                                if (agg->fn_kind == AggregateKind::COUNT) {
                                    if (!agg->expr) {
                                        item.alias = "count(*)";
                                    } else if (agg->expr->kind == ExpressionKind::VARIABLE) {
                                        item.alias = "count(" + static_cast<const VariableExpr*>(agg->expr.get())->name + ")";
                                    } else if (agg->expr->kind == ExpressionKind::PROPERTY_LOOKUP) {
                                        auto* pl = static_cast<const PropertyLookupExpr*>(agg->expr.get());
                                        item.alias = "count(" + pl->variable + "." + pl->property + ")";
                                    }
                                }
                            }
                        }
                    }
                    
                    // Execute the query AST rewrites.
                    for (auto& item : query.returns) {
                        rewrite_count_to_sum_degree(item.expr, start_var, end_var, edge_var, degree_prop, rewritten);
                    }
                    for (auto& spec : query.order_by) {
                        rewrite_count_to_sum_degree(spec.expr, start_var, end_var, edge_var, degree_prop, rewritten);
                    }
                    
                    if (rewritten) {
                        // Extract relationship types from label OR-union.
                        std::vector<std::string> rel_types;
                        extract_rel_types(edge.label_expr.get(), rel_types);
                        
                        // Map edge direction to Shard Direction.
                        Direction dir = Direction::BOTH;
                        if (edge.direction == EdgeDirection::RIGHT) {
                            dir = Direction::OUT;
                        } else if (edge.direction == EdgeDirection::LEFT) {
                            dir = Direction::IN;
                        } else if (edge.direction == EdgeDirection::ANY) {
                            dir = Direction::BOTH;
                        }
                        
                        // Populate optimization directives on the start node pattern.
                        DegreePopulateInfo info;
                        info.property_name = degree_prop;
                        info.direction = dir;
                        info.rel_types = std::move(rel_types);
                        match.pattern.nodes[0].degree_opt_info.push_back(std::move(info));
                        
                        // Rewrite the MATCH pattern to remove the edge traversal and end node.
                        match.pattern.edges.clear();
                        match.pattern.nodes.resize(1);
                    }
                }
            }
        }
    }

    if (!query.where_expr) return;

    std::map<std::string, std::vector<PropertyFilter>> pushdowns;
    extract_filters(query.where_expr.get(), pushdowns);

    if (pushdowns.empty()) return;

    for (auto& match : query.matches) {
        for (auto& node : match.pattern.nodes) {
            if (!node.variable.empty()) {
                auto it = pushdowns.find(node.variable);
                if (it != pushdowns.end()) {
                    for (const auto& filter : it->second) {
                        node.property_filters.push_back(filter);
                    }
                }
            }
        }
        for (auto& edge : match.pattern.edges) {
            if (!edge.variable.empty()) {
                auto it = pushdowns.find(edge.variable);
                if (it != pushdowns.end()) {
                    for (const auto& filter : it->second) {
                        edge.property_filters.push_back(filter);
                    }
                }
            }
        }
    }

    query.where_expr = rebuild_expression_without_pushed_predicates(std::move(query.where_expr), pushdowns);
}

} // namespace ragedb::gql
