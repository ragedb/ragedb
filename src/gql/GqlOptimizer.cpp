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
#include "GqlVirtualCatalog.h"
#include "GqlParser.h"
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

template <typename SmartPtr>
void rewrite_expr_vars(SmartPtr& expr, const std::map<std::string, std::string>& var_map) {
    if (!expr) return;
    if (expr->kind == ExpressionKind::VARIABLE) {
        auto* ve = static_cast<VariableExpr*>(expr.get());
        auto it = var_map.find(ve->name);
        if (it != var_map.end()) {
            ve->name = it->second;
        }
    } else if (expr->kind == ExpressionKind::PROPERTY_LOOKUP) {
        auto* pl = static_cast<PropertyLookupExpr*>(expr.get());
        auto it = var_map.find(pl->variable);
        if (it != var_map.end()) {
            pl->variable = it->second;
        }
    } else if (expr->kind == ExpressionKind::UNARY_OP) {
        auto* un = static_cast<UnaryOpExpr*>(expr.get());
        rewrite_expr_vars(un->expr, var_map);
    } else if (expr->kind == ExpressionKind::BINARY_OP) {
        auto* bin = static_cast<BinaryOpExpr*>(expr.get());
        rewrite_expr_vars(bin->left, var_map);
        rewrite_expr_vars(bin->right, var_map);
    } else if (expr->kind == ExpressionKind::AGGREGATION) {
        auto* agg = static_cast<AggregateExpr*>(expr.get());
        rewrite_expr_vars(agg->expr, var_map);
    } else if (expr->kind == ExpressionKind::EXISTS) {
        auto* exists = static_cast<ExistsExpr*>(expr.get());
        rewrite_expr_vars(exists->where_expr, var_map);
        for (auto& match : exists->matches) {
            for (auto& node : match.pattern.nodes) {
                auto it = var_map.find(node.variable);
                if (it != var_map.end()) node.variable = it->second;
                rewrite_expr_vars(node.where_expr, var_map);
            }
            for (auto& edge : match.pattern.edges) {
                auto it = var_map.find(edge.variable);
                if (it != var_map.end()) edge.variable = it->second;
                rewrite_expr_vars(edge.where_expr, var_map);
                rewrite_expr_vars(edge.cost_expr, var_map);
            }
        }
    }
}

bool expand_virtual_views(GqlQuery& query) {
    bool expanded_any = false;
    // Iterate over all match clauses in the GQL query
    for (auto& match : query.matches) {
        if (match.is_search) continue; // Skip full-text search matches
        
        std::vector<PatternNode> new_nodes;
        std::vector<PatternEdge> new_edges;
        
        // Inspect each node pattern to check if its label matches a registered virtual view name
        for (size_t i = 0; i < match.pattern.nodes.size(); ++i) {
            auto& node = match.pattern.nodes[i];
            std::string view_name;
            if (node.label_expr && node.label_expr->kind == LabelExprKind::LITERAL) {
                view_name = node.label_expr->name;
            }
            
            // Look up the view in the thread-local virtual catalog
            auto view_opt = GqlVirtualCatalog::local().get_view(view_name);
            if (view_opt.has_value()) {
                expanded_any = true;
                // Parse the view's query string into a GqlQuery AST structure
                auto view_q = GqlParser::parse(view_opt.value());
                
                // Determine the variable name that the view query returns
                std::string view_ret_var;
                if (!view_q.returns.empty() && view_q.returns[0].expr && view_q.returns[0].expr->kind == ExpressionKind::VARIABLE) {
                    view_ret_var = static_cast<VariableExpr*>(view_q.returns[0].expr.get())->name;
                }
                
                // Determine the target variable in the outer query
                std::string target_var = node.variable;
                if (target_var.empty()) {
                    target_var = "_v_node_" + view_name;
                }
                
                // Create a variable renaming map to avoid variable name collisions between the outer query and the view
                std::map<std::string, std::string> var_map;
                if (!view_ret_var.empty()) {
                    var_map[view_ret_var] = target_var;
                }
                
                // Collect all variables defined within the view query
                std::set<std::string> view_vars;
                for (const auto& vm : view_q.matches) {
                    if (vm.is_search) {
                        if (!vm.yield_var.empty()) view_vars.insert(vm.yield_var);
                        if (!vm.yield_score_var.empty()) view_vars.insert(vm.yield_score_var);
                    } else {
                        for (const auto& n : vm.pattern.nodes) {
                            if (!n.variable.empty()) view_vars.insert(n.variable);
                        }
                        for (const auto& e : vm.pattern.edges) {
                            if (!e.variable.empty()) view_vars.insert(e.variable);
                        }
                    }
                }
                
                // Map local view variables to globally unique names in the outer query
                int name_id = 0;
                for (const auto& var : view_vars) {
                    if (var != view_ret_var) {
                        var_map[var] = "_v_" + view_name + "_" + var + "_" + std::to_string(name_id++);
                    }
                }
                
                // Merge view pattern nodes and edges into the query match path, applying variable renaming
                if (!view_q.matches.empty()) {
                    const auto& first_view_match = view_q.matches[0];
                    const auto& path = first_view_match.pattern;
                    
                    for (size_t n_idx = 0; n_idx < path.nodes.size(); ++n_idx) {
                        PatternNode v_node = path.nodes[n_idx];
                        if (!v_node.variable.empty()) {
                            v_node.variable = var_map[v_node.variable];
                        }
                        rewrite_expr_vars(v_node.where_expr, var_map);
                        
                        // For the head node of the view, merge any outer property filters and where expressions
                        if (n_idx == 0) {
                            for (const auto& [pk, pv] : node.properties) {
                                v_node.properties[pk] = pv;
                            }
                            for (const auto& filter : node.property_filters) {
                                v_node.property_filters.push_back(filter);
                            }
                            if (node.where_expr) {
                                if (v_node.where_expr) {
                                    v_node.where_expr = std::make_shared<BinaryOpExpr>(BinaryOpKind::AND, v_node.where_expr->clone(), node.where_expr->clone());
                                } else {
                                    v_node.where_expr = node.where_expr->clone();
                                }
                            }
                        }
                        new_nodes.push_back(std::move(v_node));
                    }
                    
                    for (size_t e_idx = 0; e_idx < path.edges.size(); ++e_idx) {
                        PatternEdge v_edge = path.edges[e_idx];
                        if (!v_edge.variable.empty()) {
                            v_edge.variable = var_map[v_edge.variable];
                        }
                        rewrite_expr_vars(v_edge.where_expr, var_map);
                        rewrite_expr_vars(v_edge.cost_expr, var_map);
                        new_edges.push_back(std::move(v_edge));
                    }
                }
                
                // Rewrite and combine view level where expressions with the outer query
                if (view_q.where_expr) {
                    auto mapped_where = view_q.where_expr->clone();
                    rewrite_expr_vars(mapped_where, var_map);
                    if (query.where_expr) {
                        query.where_expr = std::make_unique<BinaryOpExpr>(BinaryOpKind::AND, std::move(query.where_expr), std::move(mapped_where));
                    } else {
                        query.where_expr = std::move(mapped_where);
                    }
                }
            } else {
                // If it is not a virtual view, keep the pattern node as is
                new_nodes.push_back(node);
            }
            
            if (i < match.pattern.edges.size()) {
                new_edges.push_back(match.pattern.edges[i]);
            }
        }
        
        match.pattern.nodes = std::move(new_nodes);
        match.pattern.edges = std::move(new_edges);
    }
    return expanded_any;
}

/**
 * Recursively expands virtual views inside the GQL query.
 * Maximum recursion depth is limited to 5 to prevent infinite recursion on self-referential views.
 */
void expand_views_recursive(GqlQuery& query, int depth = 0) {
    if (depth > 5) return;
    bool expanded = expand_virtual_views(query);
    if (expanded) {
        expand_views_recursive(query, depth + 1);
    }
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

void rewrite_khop_count_to_var(std::unique_ptr<Expression>& expr, const std::string& var_name) {
    if (!expr) return;

    if (expr->kind == ExpressionKind::AGGREGATION) {
        auto* agg = static_cast<AggregateExpr*>(expr.get());
        if (agg->fn_kind == AggregateKind::COUNT) {
            bool target_matches = false;
            if (!agg->expr) {
                target_matches = true;
            } else if (agg->expr->kind == ExpressionKind::VARIABLE) {
                auto* ve = static_cast<const VariableExpr*>(agg->expr.get());
                if (ve->name == var_name) {
                    target_matches = true;
                }
            }
            if (target_matches) {
                expr = std::make_unique<VariableExpr>(var_name);
            }
        }
    } else if (expr->kind == ExpressionKind::UNARY_OP) {
        auto* un = static_cast<UnaryOpExpr*>(expr.get());
        rewrite_khop_count_to_var(un->expr, var_name);
    } else if (expr->kind == ExpressionKind::BINARY_OP) {
        auto* bin = static_cast<BinaryOpExpr*>(expr.get());
        rewrite_khop_count_to_var(bin->left, var_name);
        rewrite_khop_count_to_var(bin->right, var_name);
    }
}

void collect_variables_from_matches(const std::vector<MatchStatement>& matches, std::set<std::string>& vars) {
    for (const auto& match : matches) {
        if (match.is_search) {
            if (!match.yield_var.empty()) vars.insert(match.yield_var);
            if (!match.yield_score_var.empty()) vars.insert(match.yield_score_var);
        } else {
            for (const auto& node : match.pattern.nodes) {
                if (!node.variable.empty()) vars.insert(node.variable);
            }
            for (const auto& edge : match.pattern.edges) {
                if (!edge.variable.empty()) vars.insert(edge.variable);
            }
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

    if (query.skip_semantic) {
        return;
    }

    if (query.kind != QueryKind::SINGLE) {
        if (query.left) optimize(*query.left);
        if (query.right) optimize(*query.right);
        return;
    }

    // Expand virtual views recursively before applying other optimization passes
    expand_views_recursive(query);

    // Apply Semantic Query Optimization Passes (Phases 1-4)
    semantic_pruning_pass(query);
    if (query.no_op) return;

    semantic_join_elimination_pass(query);
    relational_pruning_pass(query);
    if (query.no_op) return;

    algebraic_rewriter_pass(query);

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
        if (it->is_search) {
            ++it;
            continue;
        }
        bool duplicate_found = false;
        for (auto prev_it = query.matches.begin(); prev_it != it; ++prev_it) {
            if (prev_it->is_search) continue;
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

    // Optimize inline WHERE expressions of nodes and edges
    for (auto& match : query.matches) {
        if (match.is_search) continue;
        for (auto& node : match.pattern.nodes) {
            if (node.where_expr) {
                std::map<std::string, std::vector<PropertyFilter>> node_pushdowns;
                extract_filters(node.where_expr.get(), node_pushdowns);
                if (!node_pushdowns.empty() && !node.variable.empty()) {
                    auto it = node_pushdowns.find(node.variable);
                    if (it != node_pushdowns.end()) {
                        for (const auto& filter : it->second) {
                            node.property_filters.push_back(filter);
                        }
                        node.where_expr = rebuild_expression_without_pushed_predicates(node.where_expr->clone(), node_pushdowns);
                    }
                }
            }
        }
        for (auto& edge : match.pattern.edges) {
            if (edge.where_expr) {
                std::map<std::string, std::vector<PropertyFilter>> edge_pushdowns;
                extract_filters(edge.where_expr.get(), edge_pushdowns);
                if (!edge_pushdowns.empty() && !edge.variable.empty()) {
                    auto it = edge_pushdowns.find(edge.variable);
                    if (it != edge_pushdowns.end()) {
                        for (const auto& filter : it->second) {
                            edge.property_filters.push_back(filter);
                        }
                        edge.where_expr = rebuild_expression_without_pushed_predicates(edge.where_expr->clone(), edge_pushdowns);
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
        if (match.is_search) continue;
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

    // --- KHOP Count-Only Optimization ---
    for (auto& match : query.matches) {
        if (match.is_khop && match.pattern.nodes.size() == 2 && !match.pattern.nodes[1].variable.empty()) {
            std::string end_var = match.pattern.nodes[1].variable;
            bool referenced_outside = false;
            if (query.where_expr && is_variable_referenced_outside_count(query.where_expr.get(), end_var)) {
                referenced_outside = true;
            }
            for (const auto& item : query.returns) {
                if (is_variable_referenced_outside_count(item.expr.get(), end_var)) {
                    referenced_outside = true;
                    break;
                }
            }
            for (const auto& spec : query.order_by) {
                if (is_variable_referenced_outside_count(spec.expr.get(), end_var)) {
                    referenced_outside = true;
                    break;
                }
            }
            
            if (!referenced_outside) {
                match.khop_count_only = true;
                for (auto& item : query.returns) {
                    if (!item.alias.has_value()) {
                        if (item.expr && item.expr->kind == ExpressionKind::AGGREGATION) {
                            auto* agg = static_cast<AggregateExpr*>(item.expr.get());
                            if (agg->fn_kind == AggregateKind::COUNT) {
                                if (!agg->expr) {
                                    item.alias = "count(*)";
                                } else if (agg->expr->kind == ExpressionKind::VARIABLE) {
                                    item.alias = "count(" + static_cast<VariableExpr*>(agg->expr.get())->name + ")";
                                }
                            }
                        }
                    }
                    rewrite_khop_count_to_var(item.expr, end_var);
                }
                for (auto& spec : query.order_by) {
                    rewrite_khop_count_to_var(spec.expr, end_var);
                }
            }
        }
    }
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
            if (match.is_search) continue;
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

/**
 * @struct Interval
 * @brief Represents a value range interval for query predicate properties.
 *
 * Mathematically, values are compared using the Total Ordering (Poset) axioms of natural/real numbers.
 * An interval represents a subset of the totally ordered set.
 */
struct Interval {
    bool has_lower = false;
    double lower_val = 0;
    bool lower_inclusive = false;

    bool has_upper = false;
    double upper_val = 0;
    bool upper_inclusive = false;

    bool is_empty() const {
        if (has_lower && has_upper) {
            if (lower_val > upper_val) return true;
            if (lower_val == upper_val && (!lower_inclusive || !upper_inclusive)) return true;
        }
        return false;
    }

    bool contains(const Interval& other) const {
        if (has_lower) {
            if (!other.has_lower) return false;
            if (other.lower_val < lower_val) return false;
            if (other.lower_val == lower_val && !lower_inclusive && other.lower_inclusive) return false;
        }
        if (has_upper) {
            if (!other.has_upper) return false;
            if (other.upper_val > upper_val) return false;
            if (other.upper_val == upper_val && !upper_inclusive && other.upper_inclusive) return false;
        }
        return true;
    }
    
    void intersect(const Interval& other) {
        if (other.has_lower) {
            if (!has_lower || other.lower_val > lower_val || (other.lower_val == lower_val && !other.lower_inclusive)) {
                has_lower = true;
                lower_val = other.lower_val;
                lower_inclusive = other.lower_inclusive;
            }
        }
        if (other.has_upper) {
            if (!has_upper || other.upper_val < upper_val || (other.upper_val == upper_val && !other.upper_inclusive)) {
                has_upper = true;
                upper_val = other.upper_val;
                upper_inclusive = other.upper_inclusive;
            }
        }
    }
};

struct VarInfo {
    std::string variable;
    std::string label;
    std::map<std::string, Interval> intervals;
};

struct MandatoryRelation {
    std::string source_label;
    std::string rel_type;
    std::string target_label;
};

struct InequalityEdge {
    std::string u;
    std::string v;
    bool strict;
};

static void add_comparison_to_intervals(BinaryOpKind op, const std::string& variable, const std::string& property, double val, std::map<std::string, Interval>& intervals) {
    Interval& interval = intervals[property];
    Interval filter_int;
    if (op == BinaryOpKind::LT) {
        filter_int.has_upper = true;
        filter_int.upper_val = val;
        filter_int.upper_inclusive = false;
    } else if (op == BinaryOpKind::LE) {
        filter_int.has_upper = true;
        filter_int.upper_val = val;
        filter_int.upper_inclusive = true;
    } else if (op == BinaryOpKind::GT) {
        filter_int.has_lower = true;
        filter_int.lower_val = val;
        filter_int.lower_inclusive = false;
    } else if (op == BinaryOpKind::GE) {
        filter_int.has_lower = true;
        filter_int.lower_val = val;
        filter_int.lower_inclusive = true;
    } else if (op == BinaryOpKind::EQ) {
        filter_int.has_lower = true;
        filter_int.lower_val = val;
        filter_int.lower_inclusive = true;
        filter_int.has_upper = true;
        filter_int.upper_val = val;
        filter_int.upper_inclusive = true;
    }
    interval.intersect(filter_int);
}

static bool get_numeric_value(const Expression* expr, double& val) {
    if (!expr) return false;
    if (expr->kind == ExpressionKind::LITERAL) {
        const auto* lit = static_cast<const LiteralExpr*>(expr);
        if (std::holds_alternative<int64_t>(lit->value)) {
            val = std::get<int64_t>(lit->value);
            return true;
        } else if (std::holds_alternative<double>(lit->value)) {
            val = std::get<double>(lit->value);
            return true;
        }
    } else if (expr->kind == ExpressionKind::UNARY_OP) {
        const auto* un = static_cast<const UnaryOpExpr*>(expr);
        if (un->op == UnaryOpKind::NEG) {
            double inner_val = 0;
            if (get_numeric_value(un->expr.get(), inner_val)) {
                val = -inner_val;
                return true;
            }
        }
    }
    return false;
}

static void extract_intervals_from_expr(const Expression* expr, const std::string& target_var, std::map<std::string, Interval>& intervals, bool negate = false) {
    if (!expr) return;
    if (expr->kind == ExpressionKind::UNARY_OP) {
        const auto* un = static_cast<const UnaryOpExpr*>(expr);
        if (un->op == UnaryOpKind::NOT) {
            extract_intervals_from_expr(un->expr.get(), target_var, intervals, !negate);
        }
    } else if (expr->kind == ExpressionKind::BINARY_OP) {
        const auto* bin = static_cast<const BinaryOpExpr*>(expr);
        if (bin->op == BinaryOpKind::AND) {
            if (!negate) {
                extract_intervals_from_expr(bin->left.get(), target_var, intervals, false);
                extract_intervals_from_expr(bin->right.get(), target_var, intervals, false);
            }
        } else if (bin->op == BinaryOpKind::OR) {
            if (negate) {
                extract_intervals_from_expr(bin->left.get(), target_var, intervals, true);
                extract_intervals_from_expr(bin->right.get(), target_var, intervals, true);
            }
        } else {
            const Expression* left = bin->left.get();
            const Expression* right = bin->right.get();
            bool reversed = false;
            double left_val = 0;
            double right_val = 0;
            bool is_left_numeric = get_numeric_value(left, left_val);
            bool is_right_numeric = get_numeric_value(right, right_val);

            if (is_left_numeric && right->kind == ExpressionKind::PROPERTY_LOOKUP) {
                std::swap(left, right);
                std::swap(left_val, right_val);
                std::swap(is_left_numeric, is_right_numeric);
                reversed = true;
            }
            if (left->kind == ExpressionKind::PROPERTY_LOOKUP && is_right_numeric) {
                const auto* pl = static_cast<const PropertyLookupExpr*>(left);
                if (pl->variable == target_var) {
                    auto op = bin->op;
                    if (reversed) {
                        if (op == BinaryOpKind::LT) op = BinaryOpKind::GT;
                        else if (op == BinaryOpKind::LE) op = BinaryOpKind::GE;
                        else if (op == BinaryOpKind::GT) op = BinaryOpKind::LT;
                        else if (op == BinaryOpKind::GE) op = BinaryOpKind::LE;
                    }
                    if (negate) {
                        if (op == BinaryOpKind::LT) op = BinaryOpKind::GE;
                        else if (op == BinaryOpKind::LE) op = BinaryOpKind::GT;
                        else if (op == BinaryOpKind::GT) op = BinaryOpKind::LE;
                        else if (op == BinaryOpKind::GE) op = BinaryOpKind::LT;
                        else if (op == BinaryOpKind::EQ) return;
                    }
                    add_comparison_to_intervals(op, pl->variable, pl->property, right_val, intervals);
                }
            }
        }
    }
}

static std::vector<VarInfo> collect_query_vars(const GqlQuery& query) {
    std::vector<VarInfo> vars;
    for (const auto& match : query.matches) {
        for (const auto& node : match.pattern.nodes) {
            if (node.variable.empty()) continue;
            std::string label;
            if (node.label_expr && node.label_expr->kind == LabelExprKind::LITERAL) {
                label = node.label_expr->name;
            }
            VarInfo vi;
            vi.variable = node.variable;
            vi.label = label;
            
            for (const auto& [prop, val_type] : node.properties) {
                double val = 0;
                bool is_numeric = false;
                if (std::holds_alternative<int64_t>(val_type)) {
                    val = std::get<int64_t>(val_type);
                    is_numeric = true;
                } else if (std::holds_alternative<double>(val_type)) {
                    val = std::get<double>(val_type);
                    is_numeric = true;
                }
                if (is_numeric) {
                    Interval& interval = vi.intervals[prop];
                    Interval eq_int;
                    eq_int.has_lower = true;
                    eq_int.lower_val = val;
                    eq_int.lower_inclusive = true;
                    eq_int.has_upper = true;
                    eq_int.upper_val = val;
                    eq_int.upper_inclusive = true;
                    interval.intersect(eq_int);
                }
            }
            
            for (const auto& filter : node.property_filters) {
                double val = 0;
                bool is_numeric = false;
                if (std::holds_alternative<int64_t>(filter.value)) {
                    val = std::get<int64_t>(filter.value);
                    is_numeric = true;
                } else if (std::holds_alternative<double>(filter.value)) {
                    val = std::get<double>(filter.value);
                    is_numeric = true;
                }
                if (is_numeric) {
                    Interval& interval = vi.intervals[filter.property];
                    Interval filter_int;
                    auto op = filter.op;
                    if (op == Operation::LT) {
                        filter_int.has_upper = true;
                        filter_int.upper_val = val;
                        filter_int.upper_inclusive = false;
                    } else if (op == Operation::LTE) {
                        filter_int.has_upper = true;
                        filter_int.upper_val = val;
                        filter_int.upper_inclusive = true;
                    } else if (op == Operation::GT) {
                        filter_int.has_lower = true;
                        filter_int.lower_val = val;
                        filter_int.lower_inclusive = false;
                    } else if (op == Operation::GTE) {
                        filter_int.has_lower = true;
                        filter_int.lower_val = val;
                        filter_int.lower_inclusive = true;
                    } else if (op == Operation::EQ) {
                        filter_int.has_lower = true;
                        filter_int.lower_val = val;
                        filter_int.lower_inclusive = true;
                        filter_int.has_upper = true;
                        filter_int.upper_val = val;
                        filter_int.upper_inclusive = true;
                    }
                    interval.intersect(filter_int);
                }
            }
            
            if (node.where_expr) {
                extract_intervals_from_expr(node.where_expr.get(), node.variable, vi.intervals);
            }
            
            if (query.where_expr) {
                extract_intervals_from_expr(query.where_expr.get(), node.variable, vi.intervals);
            }
            
            vars.push_back(vi);
        }
    }
    return vars;
}

static bool is_var_referenced_in_query_except_match(const GqlQuery& query, const std::string& var) {
    auto check_expr = [&](const Expression* expr) -> bool {
        if (!expr) return false;
        std::vector<const Expression*> stack = { expr };
        while (!stack.empty()) {
            const auto* curr = stack.back();
            stack.pop_back();
            if (curr->kind == ExpressionKind::VARIABLE) {
                if (static_cast<const VariableExpr*>(curr)->name == var) return true;
            } else if (curr->kind == ExpressionKind::PROPERTY_LOOKUP) {
                if (static_cast<const PropertyLookupExpr*>(curr)->variable == var) return true;
            } else if (curr->kind == ExpressionKind::UNARY_OP) {
                stack.push_back(static_cast<const UnaryOpExpr*>(curr)->expr.get());
            } else if (curr->kind == ExpressionKind::BINARY_OP) {
                const auto* bin = static_cast<const BinaryOpExpr*>(curr);
                stack.push_back(bin->left.get());
                stack.push_back(bin->right.get());
            } else if (curr->kind == ExpressionKind::AGGREGATION) {
                stack.push_back(static_cast<const AggregateExpr*>(curr)->expr.get());
            }
        }
        return false;
    };

    for (const auto& item : query.returns) {
        if (check_expr(item.expr.get())) return true;
    }
    if (check_expr(query.where_expr.get())) return true;
    for (const auto& spec : query.order_by) {
        if (check_expr(spec.expr.get())) return true;
    }
    for (const auto& w : query.writes) {
        if (w.set_var == var || w.remove_var == var || w.delete_var == var) return true;
        if (check_expr(w.set_expr.get())) return true;
    }
    
    int match_count = 0;
    for (const auto& m : query.matches) {
        for (const auto& n : m.pattern.nodes) {
            if (n.variable == var) match_count++;
        }
        for (const auto& e : m.pattern.edges) {
            if (e.variable == var) match_count++;
        }
    }
    if (match_count > 1) return true;
    
    return false;
}

static std::vector<MandatoryRelation> find_mandatory_relations() {
    std::vector<MandatoryRelation> relations;
    const auto& constraints = GqlVirtualCatalog::local().get_constraints();
    for (const auto& [name, query_str] : constraints) {
        try {
            auto c_query = GqlParser::parse(query_str);
            if (c_query.kind != QueryKind::SINGLE || c_query.matches.size() != 1) continue;
            const auto& c_match = c_query.matches[0];
            if (c_match.pattern.nodes.size() != 1) continue;
            const auto& source_node = c_match.pattern.nodes[0];
            if (source_node.variable.empty() || !source_node.label_expr || source_node.label_expr->kind != LabelExprKind::LITERAL) continue;
            
            std::string source_label = source_node.label_expr->name;
            std::string source_var = source_node.variable;
            
            if (!c_query.where_expr || c_query.where_expr->kind != ExpressionKind::UNARY_OP) continue;
            const auto* un = static_cast<const UnaryOpExpr*>(c_query.where_expr.get());
            if (un->op != UnaryOpKind::NOT || !un->expr || un->expr->kind != ExpressionKind::EXISTS) continue;
            
            const auto* exists = static_cast<const ExistsExpr*>(un->expr.get());
            if (exists->matches.size() != 1) continue;
            const auto& exists_match = exists->matches[0];
            const auto& exists_pattern = exists_match.pattern;
            if (exists_pattern.nodes.size() != 2 || exists_pattern.edges.size() != 1) continue;
            
            if (exists_pattern.nodes[0].variable != source_var) continue;
            
            const auto& edge = exists_pattern.edges[0];
            if (edge.direction != EdgeDirection::RIGHT || edge.is_variable_length || !edge.label_expr || edge.label_expr->kind != LabelExprKind::LITERAL) continue;
            std::string rel_type = edge.label_expr->name;
            
            const auto& target_node = exists_pattern.nodes[1];
            if (!target_node.label_expr || target_node.label_expr->kind != LabelExprKind::LITERAL) continue;
            std::string target_label = target_node.label_expr->name;
            
            relations.push_back({source_label, rel_type, target_label});
        } catch (...) {
        }
    }
    return relations;
}

static void extract_inequalities(const Expression* expr, std::vector<InequalityEdge>& edges) {
    if (!expr) return;
    if (expr->kind == ExpressionKind::BINARY_OP) {
        const auto* bin = static_cast<const BinaryOpExpr*>(expr);
        if (bin->op == BinaryOpKind::AND) {
            extract_inequalities(bin->left.get(), edges);
            extract_inequalities(bin->right.get(), edges);
        } else {
            const Expression* left = bin->left.get();
            const Expression* right = bin->right.get();
            if (left->kind == ExpressionKind::PROPERTY_LOOKUP && right->kind == ExpressionKind::PROPERTY_LOOKUP) {
                const auto* pl1 = static_cast<const PropertyLookupExpr*>(left);
                const auto* pl2 = static_cast<const PropertyLookupExpr*>(right);
                
                std::string u = pl1->variable + "." + pl1->property;
                std::string v = pl2->variable + "." + pl2->property;
                
                auto op = bin->op;
                if (op == BinaryOpKind::LT) {
                    edges.push_back({u, v, true});
                } else if (op == BinaryOpKind::LE) {
                    edges.push_back({u, v, false});
                } else if (op == BinaryOpKind::GT) {
                    edges.push_back({v, u, true});
                } else if (op == BinaryOpKind::GE) {
                    edges.push_back({v, u, false});
                } else if (op == BinaryOpKind::EQ) {
                    edges.push_back({u, v, false});
                    edges.push_back({v, u, false});
                }
            }
        }
    }
}

static bool has_inequality_contradiction(const std::vector<InequalityEdge>& edges) {
    std::set<std::string> vertices;
    for (const auto& edge : edges) {
        vertices.insert(edge.u);
        vertices.insert(edge.v);
    }
    if (vertices.empty()) return false;
    
    std::vector<std::string> v_list(vertices.begin(), vertices.end());
    size_t n = v_list.size();
    std::map<std::string, size_t> v_map;
    for (size_t i = 0; i < n; ++i) {
        v_map[v_list[i]] = i;
    }
    
    std::vector<std::vector<bool>> reaches(n, std::vector<bool>(n, false));
    std::vector<std::vector<bool>> strict(n, std::vector<bool>(n, false));
    
    for (const auto& edge : edges) {
        size_t u_idx = v_map[edge.u];
        size_t v_idx = v_map[edge.v];
        reaches[u_idx][v_idx] = true;
        if (edge.strict) {
            strict[u_idx][v_idx] = true;
        }
    }
    
    for (size_t k = 0; k < n; ++k) {
        for (size_t i = 0; i < n; ++i) {
            for (size_t j = 0; j < n; ++j) {
                if (reaches[i][k] && reaches[k][j]) {
                    reaches[i][j] = true;
                    if (strict[i][k] || strict[k][j]) {
                        strict[i][j] = true;
                    }
                }
            }
        }
    }
    
    for (size_t i = 0; i < n; ++i) {
        if (reaches[i][i] && strict[i][i]) {
            return true;
        }
    }
    return false;
}

/**
 * @brief Phase 1: Primitive Constraints & Contradiction Pruning.
 *
 * Mathematical Axioms Applied: Total Ordering (Poset) axioms of natural/real numbers (Totality, Transitivity).
 *
 * For each property filter in the query, we construct the query interval.
 * We also retrieve the active constraints on the matching labels and construct their impossible intervals.
 * If the intersection of the query interval with the valid region (complement of the impossible interval)
 * is empty, then the query condition is unsatisfiable, meaning the query is mathematically guaranteed to
 * return zero rows. In this case, we prune execution by setting query.no_op = true.
 */
void GqlOptimizer::semantic_pruning_pass(GqlQuery& query) {
    if (query.kind != QueryKind::SINGLE) return;
    
    auto q_vars = collect_query_vars(query);
    
    // Check self-contradiction in query (e.g. x.age > 10 AND x.age < 5)
    for (const auto& vi : q_vars) {
        for (const auto& [prop, q_interval] : vi.intervals) {
            if (q_interval.is_empty()) {
                query.no_op = true;
                return;
            }
        }
    }
    
    // Check contradictions against catalog constraints
    const auto& constraints = GqlVirtualCatalog::local().get_constraints();
    for (const auto& [c_name, c_query_str] : constraints) {
        try {
            auto c_query = GqlParser::parse(c_query_str);
            auto c_vars = collect_query_vars(c_query);
            
            for (const auto& c_vi : c_vars) {
                if (c_vi.label.empty()) continue;
                for (const auto& q_vi : q_vars) {
                    if (q_vi.label == c_vi.label) {
                        for (const auto& [prop, c_interval] : c_vi.intervals) {
                            auto it = q_vi.intervals.find(prop);
                            if (it != q_vi.intervals.end()) {
                                if (c_interval.contains(it->second)) {
                                    query.no_op = true;
                                    return;
                                }
                            }
                        }
                    }
                }
            }
        } catch (...) {
        }
    }
}

/**
 * @brief Phase 2: Constraint-Based Join Pruning & Elimination.
 *
 * Mathematical Axioms Applied: Referential schema integrity, mandatory mappings.
 *
 * If a constraint query guarantees that every node of source_label has a target relationship rel_type
 * of target_label, then a query matching (s:source_label)-[:rel_type]->(t:target_label) can eliminate
 * the join traversal step to 't' if 't' is neither filtered nor projected in the query.
 * This simplifies the execution plan to a single node match (s:source_label).
 */
void GqlOptimizer::semantic_join_elimination_pass(GqlQuery& query) {
    if (query.kind != QueryKind::SINGLE) return;
    
    auto mandatory = find_mandatory_relations();
    if (mandatory.empty()) return;
    
    for (auto& match : query.matches) {
        if (match.is_search || match.is_optional) continue;
        auto& pattern = match.pattern;
        
        if (pattern.nodes.size() == 2 && pattern.edges.size() == 1) {
            const auto& start_node = pattern.nodes[0];
            const auto& end_node = pattern.nodes[1];
            const auto& edge = pattern.edges[0];
            
            if (start_node.label_expr && start_node.label_expr->kind == LabelExprKind::LITERAL &&
                end_node.label_expr && end_node.label_expr->kind == LabelExprKind::LITERAL &&
                edge.label_expr && edge.label_expr->kind == LabelExprKind::LITERAL &&
                edge.direction == EdgeDirection::RIGHT) {
                
                std::string s_label = start_node.label_expr->name;
                std::string t_label = end_node.label_expr->name;
                std::string r_type = edge.label_expr->name;
                
                bool is_mandatory = false;
                for (const auto& rel : mandatory) {
                    if (rel.source_label == s_label && rel.rel_type == r_type && rel.target_label == t_label) {
                        is_mandatory = true;
                        break;
                    }
                }
                
                if (is_mandatory) {
                    bool target_referenced = false;
                    if (!end_node.variable.empty()) {
                        target_referenced = is_var_referenced_in_query_except_match(query, end_node.variable);
                    }
                    
                    bool edge_referenced = false;
                    if (!edge.variable.empty()) {
                        edge_referenced = is_var_referenced_in_query_except_match(query, edge.variable);
                    }
                    
                    bool target_has_filters = !end_node.properties.empty() || !end_node.property_filters.empty() || end_node.where_expr;
                    bool edge_has_filters = !edge.properties.empty() || !edge.property_filters.empty() || edge.where_expr;
                    
                    if (!target_referenced && !edge_referenced && !target_has_filters && !edge_has_filters) {
                        pattern.nodes.pop_back();
                        pattern.edges.clear();
                    }
                }
            }
        }
    }
}

/**
 * @brief Phase 3: Multi-Variable Relational Predicate Reasoning.
 *
 * Mathematical Axioms Applied: Poset Transitivity, Anti-symmetry, and Cycle Contradiction detection.
 *
 * We construct a directed inequality graph of all inequality predicates between query variables (e.g. a.x < b.x).
 * Using the Floyd-Warshall transitive closure algorithm, we compute the reachability matrix and strictness matrix.
 * If we detect any strict self-loop (e.g. a.x < a.x), we have identified a contradiction cycle in the Poset graph,
 * which is mathematically impossible. The query is pruned immediately by setting query.no_op = true.
 */
void GqlOptimizer::relational_pruning_pass(GqlQuery& query) {
    if (query.kind != QueryKind::SINGLE) return;
    
    std::vector<InequalityEdge> edges;
    extract_inequalities(query.where_expr.get(), edges);
    
    for (const auto& match : query.matches) {
        for (const auto& node : match.pattern.nodes) {
            extract_inequalities(node.where_expr.get(), edges);
        }
        for (const auto& edge : match.pattern.edges) {
            extract_inequalities(edge.where_expr.get(), edges);
        }
    }
    
    if (has_inequality_contradiction(edges)) {
        query.no_op = true;
    }
}

/**
 * @brief Phase 4: Algebraic Query Rewrites (Aggregation Pushdown).
 *
 * Mathematical Axioms Applied: Semiring Distributivity (a * (b + c) = a * b + a * c).
 *
 * If the query returns a sum aggregation of a product sum(A * B) where A only references grouping
 * variables (which are constant within each aggregate group) and B references the aggregated variables,
 * we apply Semiring Distributivity to factor out the A term: sum(A * B) -> A * sum(B).
 * This optimization avoids performing multiplication on every individual joined row, moving the operation
 * to happen after aggregation.
 */
void GqlOptimizer::algebraic_rewriter_pass(GqlQuery& query) {
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
                    
                    path_count_hops = edge.min_hops;
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
                    
                    if (!start_node.variable.empty() && !end_node.variable.empty()) {
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
                                    path_count_hops = num_edges;
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

                if (!referenced_outside) {
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
            if (agg->fn_kind == AggregateKind::SUM && agg->expr && agg->expr->kind == ExpressionKind::BINARY_OP) {
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
