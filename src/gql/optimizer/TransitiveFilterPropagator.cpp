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

#include "TransitiveFilterPropagator.h"
#include "OptimizerUtils.h"
#include <queue>
#include <set>
#include <map>

namespace ragedb::gql {

namespace {

/**
 * @brief Collects equated variables and properties from an expression (e.g. a == b or a.age == b.age).
 * @param expr Expression tree node to scan.
 * @param node_eq Output map of variable-to-variable equivalences (a == b).
 * @param prop_eq Output map of property-to-property equivalences (a.age == b.age).
 */
void collect_equalities_from_expr(const Expression* expr,
                                  std::map<std::string, std::set<std::string>>& node_eq,
                                  std::map<std::pair<std::string, std::string>, std::set<std::pair<std::string, std::string>>>& prop_eq) {
    if (!expr) return;
    if (expr->kind == ExpressionKind::BINARY_OP) {
        const auto* bin = static_cast<const BinaryOpExpr*>(expr);
        if (bin->op == BinaryOpKind::AND) {
            collect_equalities_from_expr(bin->left.get(), node_eq, prop_eq);
            collect_equalities_from_expr(bin->right.get(), node_eq, prop_eq);
        } else if (bin->op == BinaryOpKind::EQ) {
            const Expression* left = bin->left.get();
            const Expression* right = bin->right.get();
            
            // Check direct variable equalities: `v1 == v2`
            if (left->kind == ExpressionKind::VARIABLE && right->kind == ExpressionKind::VARIABLE) {
                std::string v1 = static_cast<const VariableExpr*>(left)->name;
                std::string v2 = static_cast<const VariableExpr*>(right)->name;
                node_eq[v1].insert(v2);
                node_eq[v2].insert(v1);
            }
            // Check property lookup equalities: `v1.prop == v2.prop`
            else if (left->kind == ExpressionKind::PROPERTY_LOOKUP && right->kind == ExpressionKind::PROPERTY_LOOKUP) {
                const auto* pl1 = static_cast<const PropertyLookupExpr*>(left);
                const auto* pl2 = static_cast<const PropertyLookupExpr*>(right);
                if (pl1->property == pl2->property) {
                    if (pl1->property == "id") {
                        node_eq[pl1->variable].insert(pl2->variable);
                        node_eq[pl2->variable].insert(pl1->variable);
                    } else {
                        prop_eq[{pl1->variable, pl1->property}].insert({pl2->variable, pl2->property});
                        prop_eq[{pl2->variable, pl2->property}].insert({pl1->variable, pl1->property});
                    }
                }
            }
        }
    }
}

struct ConstantFilter {
    std::string variable;
    std::string property;
    const Expression* expr;
};

/**
 * @brief Traverses the expression tree to collect comparisons between properties and literal constants
 *        (e.g., `variable.property == constant` or `constant > variable.property`).
 * @param expr The expression tree node to scan.
 * @param filters Output collection of constant property filters.
 */
void collect_constant_filters(const Expression* expr, std::vector<ConstantFilter>& filters) {
    if (!expr) return;
    if (expr->kind == ExpressionKind::BINARY_OP) {
        const auto* bin = static_cast<const BinaryOpExpr*>(expr);
        if (bin->op == BinaryOpKind::AND) {
            collect_constant_filters(bin->left.get(), filters);
            collect_constant_filters(bin->right.get(), filters);
        } else if (bin->op == BinaryOpKind::EQ || bin->op == BinaryOpKind::NE ||
                   bin->op == BinaryOpKind::LT || bin->op == BinaryOpKind::LE ||
                   bin->op == BinaryOpKind::GT || bin->op == BinaryOpKind::GE ||
                   bin->op == BinaryOpKind::STARTS_WITH || bin->op == BinaryOpKind::ENDS_WITH ||
                   bin->op == BinaryOpKind::CONTAINS) {
            const Expression* left = bin->left.get();
            const Expression* right = bin->right.get();
            if (left->kind == ExpressionKind::PROPERTY_LOOKUP && right->kind == ExpressionKind::LITERAL) {
                const auto* pl = static_cast<const PropertyLookupExpr*>(left);
                filters.push_back({pl->variable, pl->property, bin});
            } else if (right->kind == ExpressionKind::PROPERTY_LOOKUP && left->kind == ExpressionKind::LITERAL) {
                const auto* pl = static_cast<const PropertyLookupExpr*>(right);
                filters.push_back({pl->variable, pl->property, bin});
            }
        }
    }
}

/**
 * @brief BFS helper that finds all equivalent variables for a given starting variable and property.
 *        Traverses both node-level equalities and property-specific equality edges.
 * @param start_var Starting variable name.
 * @param prop Target property name.
 * @param node_eq Variable equality map.
 * @param prop_eq Property equality map.
 * @return A set of equivalent variable names.
 */
std::set<std::string> get_equivalent_vars_for_prop(
    const std::string& start_var,
    const std::string& prop,
    const std::map<std::string, std::set<std::string>>& node_eq,
    const std::map<std::pair<std::string, std::string>, std::set<std::pair<std::string, std::string>>>& prop_eq) {
    
    std::set<std::string> visited_vars;
    std::queue<std::string> q;
    
    q.push(start_var);
    visited_vars.insert(start_var);
    
    while (!q.empty()) {
        std::string curr = q.front();
        q.pop();
        
        // 1. Follow node-level equality edges
        auto it = node_eq.find(curr);
        if (it != node_eq.end()) {
            for (const auto& next_var : it->second) {
                if (visited_vars.insert(next_var).second) {
                    q.push(next_var);
                }
            }
        }
        
        // 2. Follow property-specific equality edges
        auto prop_it = prop_eq.find({curr, prop});
        if (prop_it != prop_eq.end()) {
            for (const auto& [next_var, next_prop] : prop_it->second) {
                if (next_prop == prop) {
                    if (visited_vars.insert(next_var).second) {
                        q.push(next_var);
                    }
                }
            }
        }
    }
    
    return visited_vars;
}

/**
 * @brief Structurally compares two expressions to determine if they are equivalent.
 */
bool are_expressions_equal(const Expression* e1, const Expression* e2) {
    if (!e1 && !e2) return true;
    if (!e1 || !e2) return false;
    if (e1->kind != e2->kind) return false;
    
    switch (e1->kind) {
        case ExpressionKind::LITERAL: {
            return static_cast<const LiteralExpr*>(e1)->value == static_cast<const LiteralExpr*>(e2)->value;
        }
        case ExpressionKind::VARIABLE: {
            return static_cast<const VariableExpr*>(e1)->name == static_cast<const VariableExpr*>(e2)->name;
        }
        case ExpressionKind::PROPERTY_LOOKUP: {
            const auto* pl1 = static_cast<const PropertyLookupExpr*>(e1);
            const auto* pl2 = static_cast<const PropertyLookupExpr*>(e2);
            return pl1->variable == pl2->variable && pl1->property == pl2->property;
        }
        case ExpressionKind::UNARY_OP: {
            const auto* u1 = static_cast<const UnaryOpExpr*>(e1);
            const auto* u2 = static_cast<const UnaryOpExpr*>(e2);
            return u1->op == u2->op && are_expressions_equal(u1->expr.get(), u2->expr.get());
        }
        case ExpressionKind::BINARY_OP: {
            const auto* b1 = static_cast<const BinaryOpExpr*>(e1);
            const auto* b2 = static_cast<const BinaryOpExpr*>(e2);
            return b1->op == b2->op &&
                   are_expressions_equal(b1->left.get(), b2->left.get()) &&
                   are_expressions_equal(b1->right.get(), b2->right.get());
        }
        case ExpressionKind::AGGREGATION: {
            const auto* a1 = static_cast<const AggregateExpr*>(e1);
            const auto* a2 = static_cast<const AggregateExpr*>(e2);
            return a1->fn_kind == a2->fn_kind && are_expressions_equal(a1->expr.get(), a2->expr.get());
        }
        case ExpressionKind::IS_NULL_CHECK: {
            const auto* n1 = static_cast<const IsNullExpr*>(e1);
            const auto* n2 = static_cast<const IsNullExpr*>(e2);
            return n1->is_not == n2->is_not && are_expressions_equal(n1->expr.get(), n2->expr.get());
        }
        default:
            return false;
    }
}

/**
 * @brief Checks if a target filter expression is structurally present inside another expression.
 */
bool is_filter_present_in_expr(const Expression* haystack, const Expression* target_expr) {
    if (!haystack) return false;
    if (are_expressions_equal(haystack, target_expr)) return true;
    
    if (haystack->kind == ExpressionKind::BINARY_OP) {
        const auto* bin = static_cast<const BinaryOpExpr*>(haystack);
        if (bin->op == BinaryOpKind::AND) {
            return is_filter_present_in_expr(bin->left.get(), target_expr) ||
                   is_filter_present_in_expr(bin->right.get(), target_expr);
        }
    }
    return false;
}

/**
 * @brief Checks if a target filter expression is already present anywhere in the query filters.
 */
bool is_filter_already_present(const GqlQuery& query, const Expression* target_expr) {
    if (is_filter_present_in_expr(query.where_expr.get(), target_expr)) return true;
    for (const auto& match : query.matches) {
        for (const auto& node : match.pattern.nodes) {
            if (is_filter_present_in_expr(node.where_expr.get(), target_expr)) return true;
        }
        for (const auto& edge : match.pattern.edges) {
            if (is_filter_present_in_expr(edge.where_expr.get(), target_expr)) return true;
        }
    }
    return false;
}

/**
 * @brief Recursively replaces references of a variable name with a new variable name in an expression.
 */
void replace_var_in_expr(Expression* expr, const std::string& old_var, const std::string& new_var) {
    if (!expr) return;
    if (expr->kind == ExpressionKind::PROPERTY_LOOKUP) {
        auto* pl = static_cast<PropertyLookupExpr*>(expr);
        if (pl->variable == old_var) {
            pl->variable = new_var;
        }
    } else if (expr->kind == ExpressionKind::VARIABLE) {
        auto* v = static_cast<VariableExpr*>(expr);
        if (v->name == old_var) {
            v->name = new_var;
        }
    } else if (expr->kind == ExpressionKind::UNARY_OP) {
        replace_var_in_expr(static_cast<UnaryOpExpr*>(expr)->expr.get(), old_var, new_var);
    } else if (expr->kind == ExpressionKind::BINARY_OP) {
        auto* bin = static_cast<BinaryOpExpr*>(expr);
        replace_var_in_expr(bin->left.get(), old_var, new_var);
        replace_var_in_expr(bin->right.get(), old_var, new_var);
    } else if (expr->kind == ExpressionKind::AGGREGATION) {
        replace_var_in_expr(static_cast<AggregateExpr*>(expr)->expr.get(), old_var, new_var);
    } else if (expr->kind == ExpressionKind::IS_NULL_CHECK) {
        replace_var_in_expr(static_cast<IsNullExpr*>(expr)->expr.get(), old_var, new_var);
    }
}

} // namespace

/**
 * @brief Phase 16: Transitive Filter Propagation.
 *        Analyzes variable and property equalities in the query (e.g. a == b or a.age == b.age).
 *        If a variable has a constant constraint (e.g. a.age == 30), it transitively propagates
 *        this constraint to all equated variables (e.g. adding b.age == 30).
 *        This exposes local filter pushdown opportunities to individual nodes, improving index utilization.
 */
void TransitiveFilterPropagator::transitive_filter_propagation_pass(GqlQuery& query) {
    if (query.skip_semantic || query.kind != QueryKind::SINGLE) return;
    
    std::map<std::string, std::set<std::string>> node_eq;
    std::map<std::pair<std::string, std::string>, std::set<std::pair<std::string, std::string>>> prop_eq;
    
    // 1. Collect all variable/property equality constraints in the query
    collect_equalities_from_expr(query.where_expr.get(), node_eq, prop_eq);
    for (const auto& match : query.matches) {
        for (const auto& node : match.pattern.nodes) {
            collect_equalities_from_expr(node.where_expr.get(), node_eq, prop_eq);
        }
        for (const auto& edge : match.pattern.edges) {
            collect_equalities_from_expr(edge.where_expr.get(), node_eq, prop_eq);
        }
    }
    
    if (node_eq.empty() && prop_eq.empty()) return;
    
    // 2. Collect all constant property filters
    std::vector<ConstantFilter> const_filters;
    collect_constant_filters(query.where_expr.get(), const_filters);
    for (const auto& match : query.matches) {
        for (const auto& node : match.pattern.nodes) {
            collect_constant_filters(node.where_expr.get(), const_filters);
        }
        for (const auto& edge : match.pattern.edges) {
            collect_constant_filters(edge.where_expr.get(), const_filters);
        }
    }
    
    if (const_filters.empty()) return;
    
    // 3. Propagate filters transitively
    bool changed = false;
    for (const auto& cf : const_filters) {
        auto target_vars = get_equivalent_vars_for_prop(cf.variable, cf.property, node_eq, prop_eq);
        for (const auto& target_var : target_vars) {
            if (target_var == cf.variable) continue;
            
            // Rewrite the filter expression for the target variable
            auto cloned_filter = cf.expr->clone();
            replace_var_in_expr(cloned_filter.get(), cf.variable, target_var);
            
            // Inject if not already present
            if (!is_filter_already_present(query, cloned_filter.get())) {
                if (query.where_expr) {
                    query.where_expr = std::make_unique<BinaryOpExpr>(
                        BinaryOpKind::AND,
                        std::move(query.where_expr),
                        std::move(cloned_filter)
                    );
                } else {
                    query.where_expr = std::move(cloned_filter);
                }
                changed = true;
            }
        }
    }
    
    // If new filters were added, recursively propagate any new transitive relations that might emerge
    if (changed) {
        transitive_filter_propagation_pass(query);
    }
}

} // namespace ragedb::gql
