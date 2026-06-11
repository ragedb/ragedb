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
#include "GqlValue.h"
#include "../graph/Graph.h"

namespace ragedb::gql {

namespace {

// Helper functions to compare label expressions, property maps, and path patterns
// to detect redundant joins for Phase 8 optimization.

bool is_equivalent_label_expr(const std::shared_ptr<LabelExpression>& e1, const std::shared_ptr<LabelExpression>& e2) {
    if (!e1 && !e2) return true;
    if (!e1 || !e2) return false;
    if (e1->kind != e2->kind) return false;
    if (e1->kind == LabelExprKind::LITERAL) {
        return e1->name == e2->name;
    }
    if (e1->kind == LabelExprKind::NOT) {
        return is_equivalent_label_expr(e1->expr, e2->expr);
    }
    return is_equivalent_label_expr(e1->left, e2->left) && is_equivalent_label_expr(e1->right, e2->right);
}

bool is_equivalent_properties(const std::map<std::string, property_type_t>& p1, const std::map<std::string, property_type_t>& p2) {
    if (p1.size() != p2.size()) return false;
    for (const auto& [k, v] : p1) {
        auto it = p2.find(k);
        if (it == p2.end()) return false;
        if (compare_properties(v, it->second) != 0) return false;
    }
    return true;
}

bool is_equivalent_pattern(const PathPattern& p1, const PathPattern& p2) {
    if (p1.nodes.size() != p2.nodes.size() || p1.edges.size() != p2.edges.size()) {
        return false;
    }
    for (size_t i = 0; i < p1.nodes.size(); ++i) {
        const auto& n1 = p1.nodes[i];
        const auto& n2 = p2.nodes[i];
        if (n1.variable != n2.variable) return false;
        if (!is_equivalent_label_expr(n1.label_expr, n2.label_expr)) return false;
        if (!is_equivalent_properties(n1.properties, n2.properties)) return false;
    }
    for (size_t i = 0; i < p1.edges.size(); ++i) {
        const auto& e1 = p1.edges[i];
        const auto& e2 = p2.edges[i];
        if (e1.variable != e2.variable) return false;
        if (e1.direction != e2.direction) return false;
        if (e1.is_variable_length != e2.is_variable_length) return false;
        if (e1.min_hops != e2.min_hops || e1.max_hops != e2.max_hops) return false;
        if (!is_equivalent_label_expr(e1.label_expr, e2.label_expr)) return false;
        if (!is_equivalent_properties(e1.properties, e2.properties)) return false;
    }
    return true;
}

} // namespace

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

void collect_variables_from_matches(const std::vector<MatchStatement>& matches, std::set<std::string>& vars) {
    for (const auto& match : matches) {
        for (const auto& node : match.pattern.nodes) {
            if (!node.variable.empty()) vars.insert(node.variable);
        }
        for (const auto& edge : match.pattern.edges) {
            if (!edge.variable.empty()) vars.insert(edge.variable);
        }
    }
}

} // namespace

void GqlOptimizer::unnest_subqueries_in_expr(
    std::unique_ptr<Expression>& expr,
    std::vector<MatchStatement>& query_matches,
    const std::set<std::string>& outer_vars,
    bool& has_unnested,
    int& var_counter
) {
    if (!expr) return;

    if (expr->kind == ExpressionKind::EXISTS) {
        auto* exists = static_cast<ExistsExpr*>(expr.get());
        if (exists->target_variable.empty()) {
            std::set<std::string> sub_vars;
            collect_variables_from_matches(exists->matches, sub_vars);
            
            std::vector<std::string> new_vars;
            for (const auto& var : sub_vars) {
                if (!outer_vars.count(var)) {
                    new_vars.push_back(var);
                }
            }

            if (new_vars.empty()) {
                std::string new_temp_var = "_exists_var_" + std::to_string(var_counter++);
                bool assigned = false;
                for (auto& match : exists->matches) {
                    for (auto& edge : match.pattern.edges) {
                        if (edge.variable.empty()) {
                            edge.variable = new_temp_var;
                            assigned = true;
                            break;
                        }
                    }
                    if (assigned) break;
                }
                if (!assigned) {
                    for (auto& match : exists->matches) {
                        for (auto& node : match.pattern.nodes) {
                            if (node.variable.empty()) {
                                node.variable = new_temp_var;
                                assigned = true;
                                break;
                            }
                        }
                        if (assigned) break;
                    }
                }
                if (assigned) {
                    new_vars.push_back(new_temp_var);
                }
            }

            if (!new_vars.empty()) {
                exists->target_variable = new_vars[0];
                has_unnested = true;
                
                if (exists->where_expr) {
                    // Push down filters within the EXISTS subquery itself.
                    std::map<std::string, std::vector<PropertyFilter>> sub_pushdowns;
                    extract_filters(exists->where_expr.get(), sub_pushdowns);
                    if (!sub_pushdowns.empty()) {
                        for (auto& match : exists->matches) {
                            for (auto& node : match.pattern.nodes) {
                                if (!node.variable.empty()) {
                                    auto it = sub_pushdowns.find(node.variable);
                                    if (it != sub_pushdowns.end()) {
                                        for (const auto& filter : it->second) {
                                            node.property_filters.push_back(filter);
                                        }
                                    }
                                }
                            }
                            for (auto& edge : match.pattern.edges) {
                                if (!edge.variable.empty()) {
                                    auto it = sub_pushdowns.find(edge.variable);
                                    if (it != sub_pushdowns.end()) {
                                        for (const auto& filter : it->second) {
                                            edge.property_filters.push_back(filter);
                                        }
                                    }
                                }
                            }
                        }
                        // Rebuild exists->where_expr without the pushed predicates.
                        exists->where_expr = rebuild_expression_without_pushed_predicates(std::move(exists->where_expr), sub_pushdowns);
                    }
                    
                    if (exists->where_expr) {
                        unnest_subqueries_in_expr(exists->where_expr, query_matches, outer_vars, has_unnested, var_counter);
                    }
                }

                // Append the subquery matches to the outer matches as optional matches
                for (const auto& match : exists->matches) {
                    MatchStatement opt_match = match;
                    opt_match.is_optional = true;
                    query_matches.push_back(std::move(opt_match));
                }
            }
        }
    }

    switch (expr->kind) {
        case ExpressionKind::UNARY_OP: {
            auto* un = static_cast<UnaryOpExpr*>(expr.get());
            unnest_subqueries_in_expr(un->expr, query_matches, outer_vars, has_unnested, var_counter);
            break;
        }
        case ExpressionKind::BINARY_OP: {
            auto* bin = static_cast<BinaryOpExpr*>(expr.get());
            unnest_subqueries_in_expr(bin->left, query_matches, outer_vars, has_unnested, var_counter);
            unnest_subqueries_in_expr(bin->right, query_matches, outer_vars, has_unnested, var_counter);
            break;
        }
        case ExpressionKind::AGGREGATION: {
            auto* agg = static_cast<AggregateExpr*>(expr.get());
            if (agg->expr) {
                unnest_subqueries_in_expr(agg->expr, query_matches, outer_vars, has_unnested, var_counter);
            }
            break;
        }
        default:
            break;
    }
}

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

    // --- Phase 10: Correlated Subquery Unnesting ---
    // Extract EXISTS subqueries, rewrite them, and append them as OPTIONAL MATCHes.
    std::set<std::string> outer_vars;
    collect_variables_from_matches(query.matches, outer_vars);
    int var_counter = 0;
    unnest_subqueries_in_expr(query.where_expr, query.matches, outer_vars, query.has_unnested_subquery, var_counter);
    for (auto& item : query.returns) {
        unnest_subqueries_in_expr(item.expr, query.matches, outer_vars, query.has_unnested_subquery, var_counter);
    }
    for (auto& spec : query.order_by) {
        unnest_subqueries_in_expr(spec.expr, query.matches, outer_vars, query.has_unnested_subquery, var_counter);
    }
    if (query.has_unnested_subquery) {
        query.outer_vars = std::move(outer_vars);
    }

    // --- Phase 8: Unnecessary Join Removal ---
    // Identify and remove duplicate/redundant MATCH patterns.
    for (auto it = query.matches.begin(); it != query.matches.end(); ) {
        bool duplicate_found = false;
        for (auto prev_it = query.matches.begin(); prev_it != it; ++prev_it) {
            if (is_equivalent_pattern(it->pattern, prev_it->pattern)) {
                // Promote first match to non-optional if the duplicate was non-optional
                if (!it->is_optional && prev_it->is_optional) {
                    prev_it->is_optional = false;
                }
                duplicate_found = true;
                break;
            }
        }
        if (duplicate_found) {
            it = query.matches.erase(it);
        } else {
            ++it;
        }
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

void GqlOptimizer::reverse_path_pattern(PathPattern& pattern) {
    std::reverse(pattern.nodes.begin(), pattern.nodes.end());
    std::reverse(pattern.edges.begin(), pattern.edges.end());
    for (auto& edge : pattern.edges) {
        if (edge.direction == EdgeDirection::RIGHT) {
            edge.direction = EdgeDirection::LEFT;
        } else if (edge.direction == EdgeDirection::LEFT) {
            edge.direction = EdgeDirection::RIGHT;
        }
    }
}

bool GqlOptimizer::has_node_index_seek(ragedb::Graph& graph, const PatternNode& node) {
    if (!node.label_expr || node.label_expr->kind != LabelExprKind::LITERAL) {
        return false;
    }
    std::string label = node.label_expr->name;
    for (const auto& [prop, val] : node.properties) {
        if (graph.shard.local().NodeIndexExists(label, prop)) {
            return true;
        }
    }
    for (const auto& filter : node.property_filters) {
        if (filter.op == Operation::EQ && graph.shard.local().NodeIndexExists(label, filter.property)) {
            return true;
        }
    }
    return false;
}

bool GqlOptimizer::has_relationship_index_seek(ragedb::Graph& graph, const PatternEdge& edge) {
    if (!edge.label_expr || edge.label_expr->kind != LabelExprKind::LITERAL) {
        return false;
    }
    std::string label = edge.label_expr->name;
    for (const auto& [prop, val] : edge.properties) {
        if (graph.shard.local().RelationshipIndexExists(label, prop)) {
            return true;
        }
    }
    for (const auto& filter : edge.property_filters) {
        if (filter.op == Operation::EQ && graph.shard.local().RelationshipIndexExists(label, filter.property)) {
            return true;
        }
    }
    return false;
}

void GqlOptimizer::optimize(ragedb::Graph& graph, GqlQuery& query) {
    optimize(query);

    if (query.kind == QueryKind::SINGLE) {
        for (auto& match : query.matches) {
            auto& pattern = match.pattern;
            if (pattern.nodes.size() >= 2) {
                bool start_node_idx = has_node_index_seek(graph, pattern.nodes.front());
                bool end_node_idx = has_node_index_seek(graph, pattern.nodes.back());
                
                if (end_node_idx && !start_node_idx) {
                    reverse_path_pattern(pattern);
                } else if (!start_node_idx && !end_node_idx && !pattern.edges.empty()) {
                    bool first_edge_idx = has_relationship_index_seek(graph, pattern.edges.front());
                    bool last_edge_idx = has_relationship_index_seek(graph, pattern.edges.back());
                    if (last_edge_idx && !first_edge_idx) {
                        reverse_path_pattern(pattern);
                    }
                }
            }
        }
    } else {
        if (query.left) optimize(graph, *query.left);
        if (query.right) optimize(graph, *query.right);
    }
}

} // namespace ragedb::gql
