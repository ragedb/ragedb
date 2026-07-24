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

// The predicate/selectivity analysis and query-rewrite helpers the optimizer passes lean on: the
// reference and residual-predicate checks that gate limit pushdown, the selectivity estimator, safe
// pattern reversal, the count-shape rewrites, and filter extraction/reconstruction. Split out of
// OptimizerUtils.cpp (which kept the interval/equivalence and per-variable collection primitives).

#include "OptimizerUtils.h"
#include "../GqlValue.h"
#include <algorithm>
#include <limits>

namespace ragedb::gql {

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
        case ExpressionKind::FUNCTION_CALL: {
            auto* fc = static_cast<const FunctionCallExpr*>(expr);
            for (const auto& a : fc->args) {
                if (is_variable_referenced_outside_count(a.get(), var_name)) return true;
            }
            return false;
        }
        case ExpressionKind::CASE_WHEN: {
            auto* ce = static_cast<const CaseExpr*>(expr);
            for (const auto& b : ce->branches) {
                if (is_variable_referenced_outside_count(b.first.get(), var_name) ||
                    is_variable_referenced_outside_count(b.second.get(), var_name)) return true;
            }
            return ce->else_expr && is_variable_referenced_outside_count(ce->else_expr.get(), var_name);
        }
        case ExpressionKind::IN_LIST: {
            auto* in = static_cast<const InExpr*>(expr);
            return is_variable_referenced_outside_count(in->value.get(), var_name) ||
                   is_variable_referenced_outside_count(in->list.get(), var_name);
        }
        case ExpressionKind::CAST: {
            return is_variable_referenced_outside_count(static_cast<const CastExpr*>(expr)->value.get(), var_name);
        }
        case ExpressionKind::IS_LABELED: {
            return is_variable_referenced_outside_count(static_cast<const IsLabeledExpr*>(expr)->value.get(), var_name);
        }
        case ExpressionKind::IS_DIRECTED: {
            return is_variable_referenced_outside_count(static_cast<const IsDirectedExpr*>(expr)->value.get(), var_name);
        }
        case ExpressionKind::IS_SOURCE_DEST: {
            auto* s = static_cast<const IsSourceDestExpr*>(expr);
            return is_variable_referenced_outside_count(s->value.get(), var_name) ||
                   is_variable_referenced_outside_count(s->edge.get(), var_name);
        }
        case ExpressionKind::LIST_LITERAL: {
            for (const auto& element : static_cast<const ListExpr*>(expr)->elements) {
                if (is_variable_referenced_outside_count(element.get(), var_name)) return true;
            }
            return false;
        }
        case ExpressionKind::LIST_INDEX: {
            auto* ie = static_cast<const IndexExpr*>(expr);
            return is_variable_referenced_outside_count(ie->list.get(), var_name) ||
                   is_variable_referenced_outside_count(ie->index.get(), var_name);
        }
        case ExpressionKind::LIST_COMPREHENSION: {
            auto* lc = static_cast<const ListComprehensionExpr*>(expr);
            return is_variable_referenced_outside_count(lc->list.get(), var_name) ||
                   is_variable_referenced_outside_count(lc->filter.get(), var_name) ||
                   is_variable_referenced_outside_count(lc->projection.get(), var_name);
        }
        case ExpressionKind::QUANTIFIED_PREDICATE: {
            auto* qp = static_cast<const QuantifiedPredicateExpr*>(expr);
            return is_variable_referenced_outside_count(qp->list.get(), var_name) ||
                   is_variable_referenced_outside_count(qp->predicate.get(), var_name);
        }
        case ExpressionKind::TEMPORAL_FIELD:
            return is_variable_referenced_outside_count(static_cast<const TemporalFieldExpr*>(expr)->value.get(), var_name);
        default:
            return false;
    }
}

/**
 * @brief Whether any of the query's predicates are evaluated AFTER the physical scan, so that
 *        using the LIMIT as the scan bound would under-return (rows are filtered or collapsed
 *        after scanning). Shared by the executor's limit_val gate and LimitPushdownOptimizer so
 *        the two cannot drift. Residual conditions:
 *        - a query-level WHERE, node WHERE, or non-literal (multi-)label anywhere;
 *        - edge WHERE / non-literal edge label / edge properties or filters (post-filtered on the
 *          relationship-index path);
 *        - inline properties or property filters on any node other than the first match's scan
 *          anchor (applied after the anchor scan);
 *        - more than one filter on the scan anchor itself (per-filter lists are truncated to the
 *          limit BEFORE the intersection, which can under-return);
 *        - RETURN DISTINCT (dedup collapses rows after the scan).
 */
bool has_post_scan_residual_predicate(const GqlQuery& query) {
    if (query.where_expr) return true;
    if (query.distinct) return true;
    for (size_t mi = 0; mi < query.matches.size(); ++mi) {
        const auto& m = query.matches[mi];
        for (size_t ni = 0; ni < m.pattern.nodes.size(); ++ni) {
            const auto& node = m.pattern.nodes[ni];
            if (node.where_expr) return true;
            if (node.label_expr && node.label_expr->kind != LabelExprKind::LITERAL) return true;
            const size_t filter_count = node.properties.size() + node.property_filters.size();
            const bool is_scan_anchor = (mi == 0 && ni == 0);
            if (!is_scan_anchor && filter_count > 0) return true;
            // Anchor filters run inside the scan only for a labeled single-filter lookup; an
            // unlabeled anchor post-filters, and multi-filter scans truncate each per-filter
            // list to the bound before intersecting.
            const bool literal_label = node.label_expr && node.label_expr->kind == LabelExprKind::LITERAL;
            if (is_scan_anchor && filter_count > 0 && (!literal_label || filter_count > 1)) return true;
        }
        for (const auto& edge : m.pattern.edges) {
            if (edge.where_expr) return true;
            if (edge.label_expr && edge.label_expr->kind != LabelExprKind::LITERAL) return true;
            if (!edge.properties.empty() || !edge.property_filters.empty()) return true;
        }
    }
    return false;
}

/**
 * @brief Simple rule-based estimator to assign a selectivity class to a query variable: a unique
 *        id equality beats any other constraint interval, which beats an unconstrained scan.
 */
SelectivityClass estimate_selectivity(const std::string& var_name, const std::vector<VarInfo>& q_vars) {
    if (var_name.empty()) return SelectivityClass::SCAN;

    const VarInfo* target_vi = nullptr;
    for (const auto& vi : q_vars) {
        if (vi.variable == var_name) {
            target_vi = &vi;
            break;
        }
    }

    if (!target_vi || target_vi->intervals.empty()) {
        return SelectivityClass::SCAN;
    }

    auto it = target_vi->intervals.find("id");
    if (it != target_vi->intervals.end()) {
        const auto& interval = it->second;
        if (interval.has_lower && interval.has_upper && interval.lower_val == interval.upper_val) {
            return SelectivityClass::UNIQUE;
        }
    }

    return SelectivityClass::INDEXED;
}

/**
 * @brief Reverse a match's traversal sequence (nodes, edges, and edge directions) when doing so
 *        preserves semantics. A bound path variable or shortest-path selector observes the path's
 *        node order, so those matches are left untouched and false is returned.
 */
bool reverse_match_pattern_if_safe(MatchStatement& match) {
    if (!match.path_variable.empty() || match.shortest_path_kind != ShortestPathKind::NONE) {
        return false;
    }
    std::reverse(match.pattern.nodes.begin(), match.pattern.nodes.end());
    std::reverse(match.pattern.edges.begin(), match.pattern.edges.end());
    for (auto& edge : match.pattern.edges) {
        if (edge.direction == EdgeDirection::RIGHT) {
            edge.direction = EdgeDirection::LEFT;
        } else if (edge.direction == EdgeDirection::LEFT) {
            edge.direction = EdgeDirection::RIGHT;
        }
    }
    return true;
}

/**
 * @brief Whether a variable-length match is the one shape the algebraic fast paths (equivalence
 *        partition / transitive reachability, Cases 0.5/0.6) reproduce faithfully: a simple,
 *        unbounded, RIGHT-directed hop with no edge binding or predicates, no path variable, and
 *        no shortest-path selector. Both rewriting passes share this test so their firing
 *        conditions cannot drift apart.
 */
bool is_simple_unbounded_right_hop(const MatchStatement& match, const PatternEdge& edge) {
    return edge.direction == EdgeDirection::RIGHT &&
           edge.min_hops == 1 && edge.max_hops == std::numeric_limits<uint64_t>::max() &&
           edge.variable.empty() &&
           edge.properties.empty() && edge.property_filters.empty() && !edge.where_expr &&
           match.path_variable.empty() &&
           match.shortest_path_kind == ShortestPathKind::NONE;
}

/**
 * @brief Whether the expression tree contains an aggregate with the DISTINCT modifier.
 */
bool expression_has_distinct_aggregate(const Expression* expr) {
    if (!expr) return false;
    switch (expr->kind) {
        case ExpressionKind::AGGREGATION:
            return static_cast<const AggregateExpr*>(expr)->distinct;
        case ExpressionKind::UNARY_OP:
            return expression_has_distinct_aggregate(static_cast<const UnaryOpExpr*>(expr)->expr.get());
        case ExpressionKind::BINARY_OP: {
            const auto* bin = static_cast<const BinaryOpExpr*>(expr);
            return expression_has_distinct_aggregate(bin->left.get()) ||
                   expression_has_distinct_aggregate(bin->right.get());
        }
        default:
            return false;
    }
}

/**
 * @brief Whether any RETURN item or ORDER BY expression carries a DISTINCT aggregate. Count-shape
 *        rewrites (count -> degree sum / path count) change row multiplicity, which is exactly what
 *        DISTINCT aggregates observe, so passes using those rewrites must skip such queries.
 */
bool query_has_distinct_aggregate(const GqlQuery& query) {
    for (const auto& item : query.returns) {
        if (expression_has_distinct_aggregate(item.expr.get())) return true;
    }
    for (const auto& spec : query.order_by) {
        if (expression_has_distinct_aggregate(spec.expr.get())) return true;
    }
    return false;
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
        // count(DISTINCT x) observes distinct values, not row multiplicity; summing degrees
        // (and deduping by degree VALUE) would be doubly wrong, so leave it untouched.
        if (agg->fn_kind == AggregateKind::COUNT && !agg->distinct) {
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
                agg->count_to_sum = true;
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
        // count(DISTINCT x) counts distinct endpoints, not paths; the algebraic path count would
        // overcount whenever two paths share an endpoint, so leave it untouched.
        if (agg->fn_kind == AggregateKind::COUNT && !agg->distinct) {
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
        if (match.is_search || match.is_propagate) {
            if (!match.yield_var.empty()) vars.insert(match.yield_var);
            if (!match.yield_score_var.empty()) vars.insert(match.yield_score_var);
            if (!match.yield_depth_var.empty()) vars.insert(match.yield_depth_var);
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

void extract_filters(Expression* expr, std::map<std::string, std::vector<PropertyFilter>>& pushdowns) {
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

std::unique_ptr<Expression> rebuild_expression_without_pushed_predicates(std::unique_ptr<Expression> expr, const std::map<std::string, std::vector<PropertyFilter>>& pushdowns) {
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

} // namespace ragedb::gql
