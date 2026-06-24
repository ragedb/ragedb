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

#include "OptimizerUtils.h"
#include "../GqlValue.h"
#include <algorithm>

namespace ragedb::gql {

bool Interval::is_empty() const {
    if (has_lower && has_upper) {
        if (lower_val > upper_val) return true;
        if (lower_val == upper_val && (!lower_inclusive || !upper_inclusive)) return true;
    }
    return false;
}

bool Interval::contains(const Interval& other) const {
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

void Interval::intersect(const Interval& other) {
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

bool is_label_subsumed(const std::shared_ptr<LabelExpression>& e1, const std::shared_ptr<LabelExpression>& e2) {
    if (!e2) return true; // e2 is wildcard/empty, always subsumed
    if (!e1) return false; // e1 is wildcard but e2 is specific, not subsumed
    return is_equivalent_label_expr(e1, e2);
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

bool get_numeric_value(const Expression* expr, double& val) {
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

static void add_comparison_to_intervals(BinaryOpKind op, const std::string& property, double val, std::map<std::string, Interval>& intervals) {
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

void extract_intervals_from_expr(const Expression* expr, const std::string& target_var, std::map<std::string, Interval>& intervals, bool negate) {
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
                    add_comparison_to_intervals(op, pl->property, right_val, intervals);
                }
            }
        }
    }
}

std::vector<VarInfo> collect_query_vars(const GqlQuery& query) {
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

std::vector<VarInfo> collect_all_query_vars(const GqlQuery& query) {
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
        for (const auto& edge : match.pattern.edges) {
            if (edge.variable.empty()) continue;
            std::string label;
            if (edge.label_expr && edge.label_expr->kind == LabelExprKind::LITERAL) {
                label = edge.label_expr->name;
            }
            VarInfo vi;
            vi.variable = edge.variable;
            vi.label = label;
            
            for (const auto& [prop, val_type] : edge.properties) {
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
            
            for (const auto& filter : edge.property_filters) {
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
            
            if (edge.where_expr) {
                extract_intervals_from_expr(edge.where_expr.get(), edge.variable, vi.intervals);
            }
            if (query.where_expr) {
                extract_intervals_from_expr(query.where_expr.get(), edge.variable, vi.intervals);
            }
            vars.push_back(vi);
        }
    }
    return vars;
}

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
            if (agg->fn_kind == AggregateKind::COUNT) {
                return false;
            }
            return is_variable_referenced_outside_count(agg->expr.get(), var_name);
        }
        default:
            return false;
    }
}

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
                target_matches = true;
            } else if (agg->expr->kind == ExpressionKind::VARIABLE) {
                auto* ve = static_cast<const VariableExpr*>(agg->expr.get());
                if (ve->name == end_var || ve->name == edge_var) {
                    target_matches = true;
                }
            }
            if (target_matches) {
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

} // namespace ragedb::gql
