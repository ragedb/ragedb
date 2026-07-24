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

#include "EqualityJoinEliminator.h"
#include "OptimizerUtils.h"
#include <map>
#include <set>
#include <vector>

namespace ragedb::gql {

namespace {

/**
 * @brief Traverses the query WHERE expression tree to collect pairs of variables/IDs
 *        that are explicitly equated (e.g. `v1 == v2` or `v1.id == v2.id`).
 * @param expr The expression tree node to scan.
 * @param equated Output set of equated variable name pairs.
 */
void collect_equated_variables(const Expression* expr, std::set<std::pair<std::string, std::string>>& equated) {
    if (!expr) return;
    if (expr->kind == ExpressionKind::BINARY_OP) {
        const auto* bin = static_cast<const BinaryOpExpr*>(expr);
        if (bin->op == BinaryOpKind::AND) {
            collect_equated_variables(bin->left.get(), equated);
            collect_equated_variables(bin->right.get(), equated);
        } else if (bin->op == BinaryOpKind::EQ) {
            const Expression* left = bin->left.get();
            const Expression* right = bin->right.get();
            // Variable equality: `v1 == v2`
            if (left->kind == ExpressionKind::VARIABLE && right->kind == ExpressionKind::VARIABLE) {
                std::string v1 = static_cast<const VariableExpr*>(left)->name;
                std::string v2 = static_cast<const VariableExpr*>(right)->name;
                if (v1 < v2) equated.insert({v1, v2});
                else equated.insert({v2, v1});
            }
            // ID property lookup equality: `v1.id == v2.id`
            else if (left->kind == ExpressionKind::PROPERTY_LOOKUP && right->kind == ExpressionKind::PROPERTY_LOOKUP) {
                const auto* pl1 = static_cast<const PropertyLookupExpr*>(left);
                const auto* pl2 = static_cast<const PropertyLookupExpr*>(right);
                if (pl1->property == "id" && pl2->property == "id") {
                    std::string v1 = pl1->variable;
                    std::string v2 = pl2->variable;
                    if (v1 < v2) equated.insert({v1, v2});
                    else equated.insert({v2, v1});
                }
            }
        }
    }
}

/**
 * @brief Recursively traverses the query WHERE expression tree to locate and remove a specific equality filter.
 * @param expr The expression to search.
 * @param v1 First variable in the equality filter.
 * @param v2 Second variable in the equality filter.
 * @param removed Output flag set to true if the filter was found and removed.
 * @return A new expression tree with the target equality filter removed.
 */
std::unique_ptr<Expression> remove_equality_recursive(std::unique_ptr<Expression> expr, const std::string& v1, const std::string& v2, bool& removed) {
    if (!expr) return nullptr;
    
    if (expr->kind == ExpressionKind::BINARY_OP) {
        auto* bin = static_cast<BinaryOpExpr*>(expr.get());
        if (bin->op == BinaryOpKind::EQ) {
            const Expression* left = bin->left.get();
            const Expression* right = bin->right.get();
            bool is_match = false;
            if (left->kind == ExpressionKind::VARIABLE && right->kind == ExpressionKind::VARIABLE) {
                std::string name1 = static_cast<const VariableExpr*>(left)->name;
                std::string name2 = static_cast<const VariableExpr*>(right)->name;
                if ((name1 == v1 && name2 == v2) || (name1 == v2 && name2 == v1)) {
                    is_match = true;
                }
            } else if (left->kind == ExpressionKind::PROPERTY_LOOKUP && right->kind == ExpressionKind::PROPERTY_LOOKUP) {
                const auto* pl1 = static_cast<const PropertyLookupExpr*>(left);
                const auto* pl2 = static_cast<const PropertyLookupExpr*>(right);
                if (pl1->property == "id" && pl2->property == "id") {
                    std::string name1 = pl1->variable;
                    std::string name2 = pl2->variable;
                    if ((name1 == v1 && name2 == v2) || (name1 == v2 && name2 == v1)) {
                        is_match = true;
                    }
                }
            }
            if (is_match) {
                removed = true;
                return nullptr; // Remove this equality filter node
            }
        } else if (bin->op == BinaryOpKind::AND) {
            auto left = remove_equality_recursive(std::move(bin->left), v1, v2, removed);
            auto right = remove_equality_recursive(std::move(bin->right), v1, v2, removed);
            if (!left) return right;
            if (!right) return left;
            bin->left = std::move(left);
            bin->right = std::move(right);
            return expr;
        }
    }
    
    return expr;
}

/**
 * @brief Checks if two match statements represent structurally isomorphic patterns
 *        sharing the same starting root, but ending at two different variable names.
 * @param m1 First match statement.
 * @param m2 Second match statement.
 * @param target1 Output variable name of the first match's target.
 * @param target2 Output variable name of the second match's target.
 * @return True if matches are isomorphic except for their last node variable.
 */
bool are_matches_isomorphic_except_target(const MatchStatement& m1, const MatchStatement& m2, std::string& target1, std::string& target2) {
    if (m1.is_optional != m2.is_optional || m1.is_search != m2.is_search || m1.is_propagate != m2.is_propagate) return false;
    if (m1.pattern.nodes.size() != m2.pattern.nodes.size() ||
        m1.pattern.edges.size() != m2.pattern.edges.size()) return false;
    if (m1.pattern.nodes.empty()) return false;
    
    // Both must start from the same root variable
    if (m1.pattern.nodes[0].variable != m2.pattern.nodes[0].variable || m1.pattern.nodes[0].variable.empty()) return false;
    
    size_t num_nodes = m1.pattern.nodes.size();
    size_t num_edges = m1.pattern.edges.size();
    
    // Intermediate nodes must match exactly
    for (size_t i = 1; i < num_nodes - 1; ++i) {
        if (m1.pattern.nodes[i].variable != m2.pattern.nodes[i].variable) return false;
        if (!is_equivalent_label_expr(m1.pattern.nodes[i].label_expr, m2.pattern.nodes[i].label_expr)) return false;
    }
    
    // Traversing edges must match exactly
    for (size_t i = 0; i < num_edges; ++i) {
        const auto& e1 = m1.pattern.edges[i];
        const auto& e2 = m2.pattern.edges[i];
        if (!is_equivalent_label_expr(e1.label_expr, e2.label_expr) ||
            e1.direction != e2.direction ||
            e1.is_variable_length != e2.is_variable_length ||
            e1.min_hops != e2.min_hops ||
            e1.max_hops != e2.max_hops) {
            return false;
        }
    }
    
    // Last nodes must share the same type/labels
    if (!is_equivalent_label_expr(m1.pattern.nodes.back().label_expr, m2.pattern.nodes.back().label_expr)) return false;
    
    target1 = m1.pattern.nodes.back().variable;
    target2 = m2.pattern.nodes.back().variable;
    
    return !target1.empty() && !target2.empty() && target1 != target2;
}

/**
 * @brief Updates variable names inside the pattern nodes, edges, and predicates of match statements.
 */
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

} // namespace

/**
 * @brief Phase 19: Equality Join Elimination.
 *        Looks for redundant self-joins equated in query filters (e.g. `MATCH (a)-[:FRIEND]->(b) MATCH (a)-[:FRIEND]->(c) WHERE b == c`).
 *        Since `b` and `c` are equated and their path patterns from `a` are identical, one match is redundant.
 *        This pass merges them into a single traversal statement, replaces all references of `c` with `b`
 *        across variables, writes, order-bys, returns and WHERE clauses, and removes the redundant equality filter.
 */
void EqualityJoinEliminator::equality_join_elimination_pass(GqlQuery& query) {
    if (query.skip_semantic || query.kind != QueryKind::SINGLE) return;
    
    bool changed = false;
    
    do {
        changed = false;
        
        // 1. Collect all equated variables
        std::set<std::pair<std::string, std::string>> equated;
        collect_equated_variables(query.where_expr.get(), equated);
        for (const auto& match : query.matches) {
            for (const auto& node : match.pattern.nodes) {
                collect_equated_variables(node.where_expr.get(), equated);
            }
            for (const auto& edge : match.pattern.edges) {
                collect_equated_variables(edge.where_expr.get(), equated);
            }
        }
        
        if (equated.empty()) break;
        
        // 2. Scan for isomorphic matches whose target variables are equated
        for (auto it2 = query.matches.begin(); it2 != query.matches.end(); ++it2) {
            bool inner_break = false;
            
            for (auto it1 = query.matches.begin(); it1 != query.matches.end(); ++it1) {
                if (it1 == it2) continue;
                
                std::string target1, target2;
                if (are_matches_isomorphic_except_target(*it1, *it2, target1, target2)) {
                    // Check if target1 and target2 are equated
                    std::pair<std::string, std::string> p = (target1 < target2) ?
                        std::make_pair(target1, target2) : std::make_pair(target2, target1);
                    
                    if (equated.count(p)) {
                        std::string keep_var = target1;
                        std::string prune_var = target2;
                        
                        // Setup the variable replacement mapping
                        std::map<std::string, std::string> var_map;
                        var_map[prune_var] = keep_var;
                        
                        // Rewrite variables in matches, projections, order clauses, writes, and filters
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
                        
                        // Remove the equality filter since it's now trivially true (e.g. keep_var == keep_var)
                        bool removed = false;
                        query.where_expr = remove_equality_recursive(std::move(query.where_expr), keep_var, prune_var, removed);

                        rewrite_expr_vars(query.where_expr, var_map);
                        
                        // Prune the redundant match statement from the query
                        query.matches.erase(it2);
                        
                        changed = true;
                        inner_break = true;
                        break;
                    }
                }
            }
            
            if (inner_break) break;
        }
        
    } while (changed);
}

} // namespace ragedb::gql
