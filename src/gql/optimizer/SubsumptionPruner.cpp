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

#include "SubsumptionPruner.h"
#include "OptimizerUtils.h"

namespace ragedb::gql {

namespace {

bool is_var_safe_in_where_expr(const Expression* expr, const std::string& var) {
    if (!expr) return true;
    if (expr->kind == ExpressionKind::BINARY_OP) {
        const auto* bin = static_cast<const BinaryOpExpr*>(expr);
        if (bin->op == BinaryOpKind::AND) {
            return is_var_safe_in_where_expr(bin->left.get(), var) &&
                   is_var_safe_in_where_expr(bin->right.get(), var);
        }
    }
    
    bool references_var = false;
    std::vector<const Expression*> stack = { expr };
    while (!stack.empty()) {
        const auto* curr = stack.back();
        stack.pop_back();
        if (curr->kind == ExpressionKind::VARIABLE) {
            if (static_cast<const VariableExpr*>(curr)->name == var) {
                references_var = true;
                break;
            }
        } else if (curr->kind == ExpressionKind::PROPERTY_LOOKUP) {
            if (static_cast<const PropertyLookupExpr*>(curr)->variable == var) {
                references_var = true;
                break;
            }
        } else if (curr->kind == ExpressionKind::UNARY_OP) {
            stack.push_back(static_cast<const UnaryOpExpr*>(curr)->expr.get());
        } else if (curr->kind == ExpressionKind::BINARY_OP) {
            const auto* b = static_cast<const BinaryOpExpr*>(curr);
            stack.push_back(b->left.get());
            stack.push_back(b->right.get());
        } else if (curr->kind == ExpressionKind::AGGREGATION) {
            stack.push_back(static_cast<const AggregateExpr*>(curr)->expr.get());
        }
    }
    
    if (!references_var) {
        return true;
    }
    
    if (expr->kind != ExpressionKind::BINARY_OP) return false;
    const auto* bin = static_cast<const BinaryOpExpr*>(expr);
    if (bin->op != BinaryOpKind::EQ && bin->op != BinaryOpKind::NE &&
        bin->op != BinaryOpKind::LT && bin->op != BinaryOpKind::LE &&
        bin->op != BinaryOpKind::GT && bin->op != BinaryOpKind::GE) {
        return false;
    }
    
    const Expression* left = bin->left.get();
    const Expression* right = bin->right.get();
    double temp_val;
    
    if (left->kind == ExpressionKind::PROPERTY_LOOKUP && static_cast<const PropertyLookupExpr*>(left)->variable == var) {
        return get_numeric_value(right, temp_val);
    }
    if (right->kind == ExpressionKind::PROPERTY_LOOKUP && static_cast<const PropertyLookupExpr*>(right)->variable == var) {
        return get_numeric_value(left, temp_val);
    }
    
    return false;
}

/// Whether an expression tree contains an aggregate anywhere. Local to this pass so the optimizer keeps
/// no dependency on the executor's evaluator, where the general has_aggregates lives.
static bool subsumption_expr_has_aggregate(const Expression* expr) {
    if (!expr) return false;
    std::vector<const Expression*> stack = { expr };
    while (!stack.empty()) {
        const auto* curr = stack.back();
        stack.pop_back();
        if (!curr) continue;
        if (curr->kind == ExpressionKind::AGGREGATION) return true;
        if (curr->kind == ExpressionKind::UNARY_OP) {
            stack.push_back(static_cast<const UnaryOpExpr*>(curr)->expr.get());
        } else if (curr->kind == ExpressionKind::BINARY_OP) {
            const auto* bin = static_cast<const BinaryOpExpr*>(curr);
            stack.push_back(bin->left.get());
            stack.push_back(bin->right.get());
        }
    }
    return false;
}

/// Set semantics: a DISTINCT projection with no aggregate. Under it, dropping a self-join arm's row
/// multiplication is invisible -- the DISTINCT dedups it and the arm's variables are dead (unprojected).
static bool subsumption_query_is_set_semantic(const GqlQuery& q) {
    if (!q.distinct) return false;
    for (const auto& item : q.returns) {
        if (subsumption_expr_has_aggregate(item.expr.get())) return false;
    }
    for (const auto& spec : q.order_by) {
        if (subsumption_expr_has_aggregate(spec.expr.get())) return false;
    }
    return true;
}

/// Whether the erasure candidate m2 binds exactly the same variables as m1 in every position. Such a
/// match is a TRUE DUPLICATE: it contributes multiplicity 1, so erasing it never changes the result and
/// is always safe. When any variable differs, m1 and m2 are a self-join, and erasing m2 drops the rows
/// it multiplies in -- sound only under set semantics.
static bool subsumption_binds_same_vars(const MatchStatement& m1, const MatchStatement& m2) {
    if (m1.pattern.nodes.size() != m2.pattern.nodes.size() ||
        m1.pattern.edges.size() != m2.pattern.edges.size()) {
        return false;
    }
    for (size_t i = 0; i < m1.pattern.nodes.size(); ++i) {
        if (m1.pattern.nodes[i].variable != m2.pattern.nodes[i].variable) return false;
    }
    for (size_t i = 0; i < m1.pattern.edges.size(); ++i) {
        if (m1.pattern.edges[i].variable != m2.pattern.edges[i].variable) return false;
    }
    return true;
}

bool is_var_dead_end_except_global_where(const GqlQuery& query, const std::string& var, int my_match_id) {
    auto check_expr = [&](const Expression* expr) -> bool {
        if (!expr) return false;
        std::vector<const Expression*> stack = { expr };
        while (!stack.empty()) {
            const auto* curr = stack.back();
            stack.pop_back();
            // A child pushed below can be null: count(*) is an AggregateExpr with no argument, and a
            // malformed binary/unary op can carry a null operand. Skip it rather than dereference kind.
            if (!curr) continue;
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
        if (check_expr(item.expr.get())) return false;
    }
    for (const auto& spec : query.order_by) {
        if (check_expr(spec.expr.get())) return false;
    }
    for (const auto& w : query.writes) {
        if (w.set_var == var || w.remove_var == var || w.delete_var == var) return false;
        if (check_expr(w.set_expr.get())) return false;
    }
    for (const auto& m : query.matches) {
        if (m.id == my_match_id) continue;
        for (const auto& n : m.pattern.nodes) {
            if (n.variable == var) return false;
        }
        for (const auto& e : m.pattern.edges) {
            if (e.variable == var) return false;
        }
    }
    return true;
}

bool has_only_simple_numeric_filters(const GqlQuery& query, const MatchStatement& m, const std::string& v) {
    if (v.empty()) return true;
    for (const auto& node : m.pattern.nodes) {
        if (node.variable == v) {
            for (const auto& [prop, val] : node.properties) {
                if (!std::holds_alternative<int64_t>(val) && !std::holds_alternative<double>(val)) {
                    return false;
                }
            }
            for (const auto& filter : node.property_filters) {
                if (!std::holds_alternative<int64_t>(filter.value) && !std::holds_alternative<double>(filter.value)) {
                    return false;
                }
            }
            if (node.where_expr && !is_var_safe_in_where_expr(node.where_expr.get(), v)) {
                return false;
            }
        }
    }
    for (const auto& edge : m.pattern.edges) {
        if (edge.variable == v) {
            for (const auto& [prop, val] : edge.properties) {
                if (!std::holds_alternative<int64_t>(val) && !std::holds_alternative<double>(val)) {
                    return false;
                }
            }
            for (const auto& filter : edge.property_filters) {
                if (!std::holds_alternative<int64_t>(filter.value) && !std::holds_alternative<double>(filter.value)) {
                    return false;
                }
            }
            if (edge.where_expr && !is_var_safe_in_where_expr(edge.where_expr.get(), v)) {
                return false;
            }
        }
    }
    if (query.where_expr && !is_var_safe_in_where_expr(query.where_expr.get(), v)) {
        return false;
    }
    return true;
}

bool are_filters_subsumed(const VarInfo& vi1, const VarInfo& vi2) {
    if (!vi2.label.empty() && vi1.label != vi2.label) return false;
    for (const auto& [prop, int2] : vi2.intervals) {
        auto it = vi1.intervals.find(prop);
        if (it == vi1.intervals.end()) {
            return false;
        }
        const auto& int1 = it->second;
        if (!int2.contains(int1)) {
            return false;
        }
    }
    return true;
}

std::unique_ptr<Expression> rebuild_expr_without_vars(std::unique_ptr<Expression> expr, const std::set<std::string>& vars) {
    if (!expr) return nullptr;
    if (expr->kind == ExpressionKind::BINARY_OP) {
        auto* bin = static_cast<BinaryOpExpr*>(expr.get());
        if (bin->op == BinaryOpKind::AND) {
            auto left = rebuild_expr_without_vars(std::move(bin->left), vars);
            auto right = rebuild_expr_without_vars(std::move(bin->right), vars);
            if (left && right) {
                return std::make_unique<BinaryOpExpr>(BinaryOpKind::AND, std::move(left), std::move(right));
            }
            return left ? std::move(left) : std::move(right);
        }
    }
    
    bool references_var = false;
    std::vector<const Expression*> stack = { expr.get() };
    while (!stack.empty()) {
        const auto* curr = stack.back();
        stack.pop_back();
        if (curr->kind == ExpressionKind::VARIABLE) {
            if (vars.count(static_cast<const VariableExpr*>(curr)->name)) {
                references_var = true;
                break;
            }
        } else if (curr->kind == ExpressionKind::PROPERTY_LOOKUP) {
            if (vars.count(static_cast<const PropertyLookupExpr*>(curr)->variable)) {
                references_var = true;
                break;
            }
        } else if (curr->kind == ExpressionKind::UNARY_OP) {
            stack.push_back(static_cast<const UnaryOpExpr*>(curr)->expr.get());
        } else if (curr->kind == ExpressionKind::BINARY_OP) {
            const auto* b = static_cast<const BinaryOpExpr*>(curr);
            stack.push_back(b->left.get());
            stack.push_back(b->right.get());
        } else if (curr->kind == ExpressionKind::AGGREGATION) {
            stack.push_back(static_cast<const AggregateExpr*>(curr)->expr.get());
        }
    }
    
    if (references_var) {
        return nullptr;
    }
    return expr;
}

} // namespace

void SubsumptionPruner::semantic_subsumption_pass(GqlQuery& query) {
    if (query.skip_semantic || query.kind != QueryKind::SINGLE) return;
    const bool set_semantic = subsumption_query_is_set_semantic(query);

    bool pruned_any = false;
    
    do {
        pruned_any = false;
        
        auto all_vars = collect_all_query_vars(query);
        std::map<std::string, VarInfo> var_info_map;
        for (const auto& vi : all_vars) {
            var_info_map[vi.variable] = vi;
        }
        
        for (auto it2 = query.matches.begin(); it2 != query.matches.end(); ++it2) {
            if (it2->is_optional || it2->is_search || it2->is_propagate) continue;
            
            for (auto it1 = query.matches.begin(); it1 != query.matches.end(); ++it1) {
                if (it1 == it2 || it1->is_optional || it1->is_search || it1->is_propagate) continue;
                
                const auto& m1 = *it1;
                const auto& m2 = *it2;
                
                if (m1.pattern.nodes.size() != m2.pattern.nodes.size() ||
                    m1.pattern.edges.size() != m2.pattern.edges.size()) {
                    continue;
                }
                if (m1.pattern.nodes.empty()) continue;
                
                if (m1.pattern.nodes[0].variable.empty() ||
                    m1.pattern.nodes[0].variable != m2.pattern.nodes[0].variable) {
                    continue;
                }
                
                bool isomorphic = true;
                for (size_t i = 1; i < m1.pattern.nodes.size(); ++i) {
                    if (!is_label_subsumed(m1.pattern.nodes[i].label_expr, m2.pattern.nodes[i].label_expr)) {
                        isomorphic = false;
                        break;
                    }
                }
                if (!isomorphic) continue;
                
                for (size_t i = 0; i < m1.pattern.edges.size(); ++i) {
                    const auto& e1 = m1.pattern.edges[i];
                    const auto& e2 = m2.pattern.edges[i];
                    if (!is_label_subsumed(e1.label_expr, e2.label_expr) ||
                        e1.direction != e2.direction ||
                        e1.is_variable_length != e2.is_variable_length ||
                        e1.min_hops != e2.min_hops ||
                        e1.max_hops != e2.max_hops) {
                        isomorphic = false;
                        break;
                    }
                }
                if (!isomorphic) continue;
                
                std::set<std::string> dead_vars;
                bool dead_ends_ok = true;
                for (size_t i = 1; i < m2.pattern.nodes.size(); ++i) {
                    const std::string& v = m2.pattern.nodes[i].variable;
                    if (!v.empty()) {
                        if (!is_var_dead_end_except_global_where(query, v, m2.id) ||
                            !has_only_simple_numeric_filters(query, m2, v)) {
                            dead_ends_ok = false;
                            break;
                        }
                        dead_vars.insert(v);
                    }
                }
                if (!dead_ends_ok) continue;
                
                for (const auto& edge : m2.pattern.edges) {
                    const std::string& v = edge.variable;
                    if (!v.empty()) {
                        if (!is_var_dead_end_except_global_where(query, v, m2.id) ||
                            !has_only_simple_numeric_filters(query, m2, v)) {
                            dead_ends_ok = false;
                            break;
                        }
                        dead_vars.insert(v);
                    }
                }
                if (!dead_ends_ok) continue;
                
                bool subsumed = true;
                for (size_t i = 1; i < m2.pattern.nodes.size(); ++i) {
                    const std::string& v2 = m2.pattern.nodes[i].variable;
                    const std::string& v1 = m1.pattern.nodes[i].variable;
                    if (!v2.empty() && !v1.empty()) {
                        const auto& vi2 = var_info_map[v2];
                        const auto& vi1 = var_info_map[v1];
                        if (!are_filters_subsumed(vi1, vi2)) {
                            subsumed = false;
                            break;
                        }
                    }
                }
                if (!subsumed) continue;
                
                for (size_t i = 0; i < m2.pattern.edges.size(); ++i) {
                    const std::string& v2 = m2.pattern.edges[i].variable;
                    const std::string& v1 = m1.pattern.edges[i].variable;
                    if (!v2.empty() && !v1.empty()) {
                        const auto& vi2 = var_info_map[v2];
                        const auto& vi1 = var_info_map[v1];
                        if (!are_filters_subsumed(vi1, vi2)) {
                            subsumed = false;
                            break;
                        }
                    }
                }
                if (!subsumed) continue;

                // Erasing m2 is sound when it is a true duplicate of m1 (same variables, multiplicity 1)
                // or when the query is set-semantic. Otherwise m2 is a self-join arm whose row
                // multiplication would be lost -- undercounting count(*) and dropping bag-semantic rows.
                if (!subsumption_binds_same_vars(m1, m2) && !set_semantic) continue;

                if (!dead_vars.empty() && query.where_expr) {
                    query.where_expr = rebuild_expr_without_vars(std::move(query.where_expr), dead_vars);
                }

                query.matches.erase(it2);
                pruned_any = true;
                break;
            }
            if (pruned_any) break;
        }
    } while (pruned_any);
}

} // namespace ragedb::gql
