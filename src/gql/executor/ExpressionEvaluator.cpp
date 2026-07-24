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

#include "ExpressionEvaluator.h"
#include <algorithm>

/**
 * @file ExpressionEvaluator.cpp
 * @brief Implementation of helper functions for identifying and evaluating AST aggregates.
 * 
 * Example Query context:
 *   MATCH (p:Person) RETURN p.city, count(*), avg(p.age)
 *   - `has_aggregates` determines if count(*) or avg(p.age) are present.
 *   - `find_aggregates` gathers them for execution.
 *   - `evaluate_group_expression` combines the calculated aggregates into the projected group row.
 */
namespace ragedb::gql {

/**
 * @brief Recursively checks if an expression node or any of its sub-expressions
 * contains an AGGREGATION node (e.g., COUNT, SUM, AVG, MIN, MAX).
 * 
 * Used to distinguish between plain projections and aggregate groupings.
 */
bool has_aggregates(const Expression* expr) {
    if (!expr) return false;
    if (expr->kind == ExpressionKind::AGGREGATION) return true;
    if (expr->kind == ExpressionKind::UNARY_OP) {
        auto* un = static_cast<const UnaryOpExpr*>(expr);
        return has_aggregates(un->expr.get());
    }
    if (expr->kind == ExpressionKind::BINARY_OP) {
        auto* bin = static_cast<const BinaryOpExpr*>(expr);
        return has_aggregates(bin->left.get()) || has_aggregates(bin->right.get());
    }
    if (expr->kind == ExpressionKind::IS_NULL_CHECK) {
        auto* is_null = static_cast<const IsNullExpr*>(expr);
        return has_aggregates(is_null->expr.get());
    }
    if (expr->kind == ExpressionKind::CASE_WHEN) {
        auto* ce = static_cast<const CaseExpr*>(expr);
        for (const auto& b : ce->branches) {
            if (has_aggregates(b.first.get()) || has_aggregates(b.second.get())) return true;
        }
        return ce->else_expr && has_aggregates(ce->else_expr.get());
    }
    if (expr->kind == ExpressionKind::FUNCTION_CALL) {
        auto* fc = static_cast<const FunctionCallExpr*>(expr);
        for (const auto& a : fc->args) {
            if (has_aggregates(a.get())) return true;
        }
        return false;
    }
    if (expr->kind == ExpressionKind::IN_LIST) {
        auto* in = static_cast<const InExpr*>(expr);
        return has_aggregates(in->value.get()) || has_aggregates(in->list.get());
    }
    if (expr->kind == ExpressionKind::CAST) {
        return has_aggregates(static_cast<const CastExpr*>(expr)->value.get());
    }
    if (expr->kind == ExpressionKind::IS_LABELED) {
        return has_aggregates(static_cast<const IsLabeledExpr*>(expr)->value.get());
    }
    if (expr->kind == ExpressionKind::IS_DIRECTED) {
        return has_aggregates(static_cast<const IsDirectedExpr*>(expr)->value.get());
    }
    if (expr->kind == ExpressionKind::IS_SOURCE_DEST) {
        auto* s = static_cast<const IsSourceDestExpr*>(expr);
        return has_aggregates(s->value.get()) || has_aggregates(s->edge.get());
    }
    if (expr->kind == ExpressionKind::LIST_LITERAL) {
        for (const auto& element : static_cast<const ListExpr*>(expr)->elements) {
            if (has_aggregates(element.get())) return true;
        }
        return false;
    }
    if (expr->kind == ExpressionKind::LIST_INDEX) {
        auto* ie = static_cast<const IndexExpr*>(expr);
        return has_aggregates(ie->list.get()) || has_aggregates(ie->index.get());
    }
    if (expr->kind == ExpressionKind::LIST_COMPREHENSION) {
        auto* lc = static_cast<const ListComprehensionExpr*>(expr);
        return has_aggregates(lc->list.get()) || has_aggregates(lc->filter.get()) || has_aggregates(lc->projection.get());
    }
    if (expr->kind == ExpressionKind::QUANTIFIED_PREDICATE) {
        auto* qp = static_cast<const QuantifiedPredicateExpr*>(expr);
        return has_aggregates(qp->list.get()) || has_aggregates(qp->predicate.get());
    }
    if (expr->kind == ExpressionKind::TEMPORAL_FIELD) {
        return has_aggregates(static_cast<const TemporalFieldExpr*>(expr)->value.get());
    }
    return false;
}

/**
 * @brief Recursively traverses the expression AST and collects all AggregateExpr nodes.
 * 
 * These extracted aggregates are evaluated before the final projection and stored
 * in the group's aggregate results map.
 */
void find_aggregates(const Expression* expr, std::vector<const AggregateExpr*>& aggregates) {
    if (!expr) return;
    if (expr->kind == ExpressionKind::AGGREGATION) {
        aggregates.push_back(static_cast<const AggregateExpr*>(expr));
        return;
    }
    if (expr->kind == ExpressionKind::UNARY_OP) {
        auto* un = static_cast<const UnaryOpExpr*>(expr);
        find_aggregates(un->expr.get(), aggregates);
    } else if (expr->kind == ExpressionKind::BINARY_OP) {
        auto* bin = static_cast<const BinaryOpExpr*>(expr);
        find_aggregates(bin->left.get(), aggregates);
        find_aggregates(bin->right.get(), aggregates);
    } else if (expr->kind == ExpressionKind::IS_NULL_CHECK) {
        auto* is_null = static_cast<const IsNullExpr*>(expr);
        find_aggregates(is_null->expr.get(), aggregates);
    } else if (expr->kind == ExpressionKind::CASE_WHEN) {
        auto* ce = static_cast<const CaseExpr*>(expr);
        for (const auto& b : ce->branches) {
            find_aggregates(b.first.get(), aggregates);
            find_aggregates(b.second.get(), aggregates);
        }
        if (ce->else_expr) find_aggregates(ce->else_expr.get(), aggregates);
    } else if (expr->kind == ExpressionKind::FUNCTION_CALL) {
        auto* fc = static_cast<const FunctionCallExpr*>(expr);
        for (const auto& a : fc->args) find_aggregates(a.get(), aggregates);
    } else if (expr->kind == ExpressionKind::IN_LIST) {
        auto* in = static_cast<const InExpr*>(expr);
        find_aggregates(in->value.get(), aggregates);
        find_aggregates(in->list.get(), aggregates);
    } else if (expr->kind == ExpressionKind::CAST) {
        find_aggregates(static_cast<const CastExpr*>(expr)->value.get(), aggregates);
    } else if (expr->kind == ExpressionKind::IS_LABELED) {
        find_aggregates(static_cast<const IsLabeledExpr*>(expr)->value.get(), aggregates);
    } else if (expr->kind == ExpressionKind::IS_DIRECTED) {
        find_aggregates(static_cast<const IsDirectedExpr*>(expr)->value.get(), aggregates);
    } else if (expr->kind == ExpressionKind::IS_SOURCE_DEST) {
        auto* s = static_cast<const IsSourceDestExpr*>(expr);
        find_aggregates(s->value.get(), aggregates);
        find_aggregates(s->edge.get(), aggregates);
    } else if (expr->kind == ExpressionKind::LIST_LITERAL) {
        for (const auto& element : static_cast<const ListExpr*>(expr)->elements) {
            find_aggregates(element.get(), aggregates);
        }
    } else if (expr->kind == ExpressionKind::LIST_INDEX) {
        auto* ie = static_cast<const IndexExpr*>(expr);
        find_aggregates(ie->list.get(), aggregates);
        find_aggregates(ie->index.get(), aggregates);
    } else if (expr->kind == ExpressionKind::LIST_COMPREHENSION) {
        auto* lc = static_cast<const ListComprehensionExpr*>(expr);
        find_aggregates(lc->list.get(), aggregates);
        find_aggregates(lc->filter.get(), aggregates);
        find_aggregates(lc->projection.get(), aggregates);
    } else if (expr->kind == ExpressionKind::QUANTIFIED_PREDICATE) {
        auto* qp = static_cast<const QuantifiedPredicateExpr*>(expr);
        find_aggregates(qp->list.get(), aggregates);
        find_aggregates(qp->predicate.get(), aggregates);
    } else if (expr->kind == ExpressionKind::TEMPORAL_FIELD) {
        find_aggregates(static_cast<const TemporalFieldExpr*>(expr)->value.get(), aggregates);
    }
}

/**
 * @brief Evaluates an expression for a specific group of rows using a representative row.
 * 
 * Resolves aggregate sub-expressions by looking them up in the precalculated
 * `aggregate_results` map, and evaluates other sub-expressions normally.
 */
GqlValue evaluate_group_expression(const GqlRow& representative, const std::map<const AggregateExpr*, GqlValue>& aggregate_results, const Expression* expr) {
    if (!expr) return GqlValue();

    switch (expr->kind) {
        case ExpressionKind::IS_NULL_CHECK: {
            auto* is_null_expr = static_cast<const IsNullExpr*>(expr);
            auto val = evaluate_group_expression(representative, aggregate_results, is_null_expr->expr.get());
            bool is_nil = (val.type == GqlValue::NIL);
            if (val.type == GqlValue::PROPERTY) {
                if (std::holds_alternative<std::monostate>(val.property)) {
                    is_nil = true;
                }
            }
            return GqlValue(is_null_expr->is_not ? !is_nil : is_nil);
        }
        case ExpressionKind::AGGREGATION: {
            // Aggregation: Look up precomputed results in the results map populated by the caller
            auto* agg = static_cast<const AggregateExpr*>(expr);
            auto it = aggregate_results.find(agg);
            if (it != aggregate_results.end()) {
                return it->second;
            }
            return GqlValue();
        }
        case ExpressionKind::CASE_WHEN: {
            auto* ce = static_cast<const CaseExpr*>(expr);
            for (const auto& branch : ce->branches) {
                if (evaluate_group_expression(representative, aggregate_results, branch.first.get()).is_truthy()) {
                    return evaluate_group_expression(representative, aggregate_results, branch.second.get());
                }
            }
            if (ce->else_expr) {
                return evaluate_group_expression(representative, aggregate_results, ce->else_expr.get());
            }
            return GqlValue();
        }
        case ExpressionKind::FUNCTION_CALL: {
            // Scalar functions evaluate over the representative row's grouping keys, but a nested aggregate
            // argument must resolve from the results map rather than re-evaluate per row -- so
            // cardinality(collect_list(x)) sees the whole list, not a per-row NULL.
            return evaluate_scalar_function_with(
                static_cast<const FunctionCallExpr*>(expr),
                [&representative, &aggregate_results](const Expression* e) {
                    return evaluate_group_expression(representative, aggregate_results, e);
                });
        }
        case ExpressionKind::LIST_LITERAL: {
            auto* le = static_cast<const ListExpr*>(expr);
            auto items = std::make_shared<std::vector<GqlValue>>();
            items->reserve(le->elements.size());
            for (const auto& element : le->elements) {
                items->push_back(evaluate_group_expression(representative, aggregate_results, element.get()));
            }
            GqlValue out;
            out.type = GqlValue::LIST;
            out.list = std::move(items);
            return out;
        }
        case ExpressionKind::LIST_INDEX: {
            auto* ie = static_cast<const IndexExpr*>(expr);
            GqlValue list = evaluate_group_expression(representative, aggregate_results, ie->list.get());
            GqlValue idx = evaluate_group_expression(representative, aggregate_results, ie->index.get());
            if (list.type != GqlValue::LIST || !list.list) return GqlValue();
            if (idx.type != GqlValue::PROPERTY || !std::holds_alternative<int64_t>(idx.property)) return GqlValue();
            int64_t i = std::get<int64_t>(idx.property);
            int64_t n = static_cast<int64_t>(list.list->size());
            if (i < 0) i += n;
            if (i < 0 || i >= n) return GqlValue();
            return (*list.list)[static_cast<size_t>(i)];
        }
        case ExpressionKind::LIST_COMPREHENSION: {
            auto* lc = static_cast<const ListComprehensionExpr*>(expr);
            GqlValue src = evaluate_group_expression(representative, aggregate_results, lc->list.get());
            if (src.type != GqlValue::LIST || !src.list) return GqlValue();
            auto items = std::make_shared<std::vector<GqlValue>>();
            for (const auto& elem : *src.list) {
                GqlRow scoped = representative;
                scoped.bindings[lc->variable] = elem;
                if (lc->filter && !evaluate_group_expression(scoped, aggregate_results, lc->filter.get()).is_truthy()) continue;
                items->push_back(lc->projection ? evaluate_group_expression(scoped, aggregate_results, lc->projection.get()) : elem);
            }
            GqlValue out;
            out.type = GqlValue::LIST;
            out.list = std::move(items);
            return out;
        }
        case ExpressionKind::QUANTIFIED_PREDICATE: {
            auto* qp = static_cast<const QuantifiedPredicateExpr*>(expr);
            GqlValue src = evaluate_group_expression(representative, aggregate_results, qp->list.get());
            if (src.type != GqlValue::LIST || !src.list) return GqlValue();
            int64_t matched = 0, total = 0;
            for (const auto& elem : *src.list) {
                GqlRow scoped = representative;
                scoped.bindings[qp->variable] = elem;
                ++total;
                if (!qp->predicate || evaluate_group_expression(scoped, aggregate_results, qp->predicate.get()).is_truthy()) ++matched;
            }
            switch (qp->quant) {
                case QuantifiedPredicateExpr::ALL:    return GqlValue(matched == total);
                case QuantifiedPredicateExpr::ANY:    return GqlValue(matched > 0);
                case QuantifiedPredicateExpr::NONE:   return GqlValue(matched == 0);
                case QuantifiedPredicateExpr::SINGLE: return GqlValue(matched == 1);
            }
            return GqlValue(false);
        }
        case ExpressionKind::TEMPORAL_FIELD: {
            auto* tf = static_cast<const TemporalFieldExpr*>(expr);
            GqlValue v = evaluate_group_expression(representative, aggregate_results, tf->value.get());
            if (v.type != GqlValue::PROPERTY || !std::holds_alternative<int64_t>(v.property)) return GqlValue();
            return gql_temporal_field(std::get<int64_t>(v.property), tf->field);
        }
        case ExpressionKind::CAST: {
            auto* c = static_cast<const CastExpr*>(expr);
            return apply_cast(evaluate_group_expression(representative, aggregate_results, c->value.get()), c->target);
        }
        case ExpressionKind::IS_LABELED: {
            auto* l = static_cast<const IsLabeledExpr*>(expr);
            return apply_is_labeled(evaluate_group_expression(representative, aggregate_results, l->value.get()),
                                    l->label_expr, l->negated);
        }
        case ExpressionKind::IS_DIRECTED: {
            auto* d = static_cast<const IsDirectedExpr*>(expr);
            GqlValue v = evaluate_group_expression(representative, aggregate_results, d->value.get());
            if (v.type != GqlValue::RELATIONSHIP) return GqlValue();
            return GqlValue(d->negated ? false : true);
        }
        case ExpressionKind::IS_SOURCE_DEST: {
            auto* s = static_cast<const IsSourceDestExpr*>(expr);
            GqlValue node = evaluate_group_expression(representative, aggregate_results, s->value.get());
            GqlValue edge = evaluate_group_expression(representative, aggregate_results, s->edge.get());
            if (node.type != GqlValue::NODE || edge.type != GqlValue::RELATIONSHIP) return GqlValue();
            uint64_t endpoint = s->is_source ? edge.relationship->getStartingNodeId()
                                             : edge.relationship->getEndingNodeId();
            bool result = (node.node->getId() == endpoint);
            return GqlValue(s->negated ? !result : result);
        }
        case ExpressionKind::IN_LIST: {
            auto* in = static_cast<const InExpr*>(expr);
            GqlValue needle = evaluate_group_expression(representative, aggregate_results, in->value.get());
            GqlValue hay = evaluate_group_expression(representative, aggregate_results, in->list.get());
            if (needle.type == GqlValue::NIL || hay.type != GqlValue::LIST) return GqlValue();
            for (const auto& item : *hay.list) {
                if (compare_gql_values(needle, item) == 0) return GqlValue(true);
            }
            return GqlValue(false);
        }
        case ExpressionKind::LITERAL: {
            // Literal: Convert literal value from AST to runtime GqlValue
            auto* lit = static_cast<const LiteralExpr*>(expr);
            return GqlValue(lit->value);
        }
        case ExpressionKind::VARIABLE: {
            // Variable: Retrieve bound vertex or relationship from the representative row
            auto* var = static_cast<const VariableExpr*>(expr);
            auto it = representative.bindings.find(var->name);
            if (it != representative.bindings.end()) {
                return it->second;
            }
            return GqlValue();
        }
        case ExpressionKind::PROPERTY_LOOKUP: {
            // Property Lookup: Extract a property from a bound node or relationship variable
            auto* prop_lookup = static_cast<const PropertyLookupExpr*>(expr);
            auto it = representative.bindings.find(prop_lookup->variable);
            if (it != representative.bindings.end()) {
                const auto& val = it->second;
                if (val.type == GqlValue::NODE) {
                    // `key` is a distinct Node field, not a properties-map entry (see GqlValue.cpp).
                    if (prop_lookup->property == "key") {
                        return GqlValue(property_type_t(val.node->getKey()));
                    }
                    return GqlValue(val.node->getProperty(prop_lookup->property));
                } else if (val.type == GqlValue::RELATIONSHIP) {
                    return GqlValue(val.relationship->getProperty(prop_lookup->property));
                }
            }
            return GqlValue();
        }
        case ExpressionKind::UNARY_OP: {
            // Unary Operation: Handle boolean negation (NOT) or arithmetic negation (NEG)
            auto* un = static_cast<const UnaryOpExpr*>(expr);
            auto val = evaluate_group_expression(representative, aggregate_results, un->expr.get());
            if (un->op == UnaryOpKind::NOT) {
                return GqlValue(!val.is_truthy());
            } else if (un->op == UnaryOpKind::NEG) {
                if (val.type == GqlValue::PROPERTY) {
                    if (std::holds_alternative<int64_t>(val.property)) {
                        return GqlValue(-std::get<int64_t>(val.property));
                    }
                    if (std::holds_alternative<double>(val.property)) {
                        return GqlValue(-std::get<double>(val.property));
                    }
                }
                return GqlValue();
            }
            return GqlValue();
        }
        case ExpressionKind::BINARY_OP: {
            // Binary Operation: Handle logical boolean, comparisons, and arithmetic operators
            auto* bin = static_cast<const BinaryOpExpr*>(expr);
            
            // Short-circuiting logical operations
            if (bin->op == BinaryOpKind::AND) {
                auto lhs = evaluate_group_expression(representative, aggregate_results, bin->left.get());
                if (!lhs.is_truthy()) return GqlValue(false);
                auto rhs = evaluate_group_expression(representative, aggregate_results, bin->right.get());
                return GqlValue(rhs.is_truthy());
            }
            if (bin->op == BinaryOpKind::OR) {
                auto lhs = evaluate_group_expression(representative, aggregate_results, bin->left.get());
                if (lhs.is_truthy()) return GqlValue(true);
                auto rhs = evaluate_group_expression(representative, aggregate_results, bin->right.get());
                return GqlValue(rhs.is_truthy());
            }

            auto lhs = evaluate_group_expression(representative, aggregate_results, bin->left.get());
            auto rhs = evaluate_group_expression(representative, aggregate_results, bin->right.get());

            if (lhs.type == GqlValue::NIL || rhs.type == GqlValue::NIL) {
                return GqlValue();
            }

            // Comparison operators (EQ, NE, LT, LE, GT, GE)
            if (bin->op == BinaryOpKind::EQ) {
                return GqlValue(compare_gql_values(lhs, rhs) == 0);
            }
            if (bin->op == BinaryOpKind::NE) {
                return GqlValue(compare_gql_values(lhs, rhs) != 0);
            }
            if (bin->op == BinaryOpKind::LT) {
                return GqlValue(compare_gql_values(lhs, rhs) < 0);
            }
            if (bin->op == BinaryOpKind::LE) {
                return GqlValue(compare_gql_values(lhs, rhs) <= 0);
            }
            if (bin->op == BinaryOpKind::GT) {
                return GqlValue(compare_gql_values(lhs, rhs) > 0);
            }
            if (bin->op == BinaryOpKind::GE) {
                return GqlValue(compare_gql_values(lhs, rhs) >= 0);
            }

            // Arithmetic operators (ADD, SUB, MUL, DIV)
            if (lhs.type == GqlValue::PROPERTY && rhs.type == GqlValue::PROPERTY) {
                // Integer arithmetic
                if (std::holds_alternative<int64_t>(lhs.property) && std::holds_alternative<int64_t>(rhs.property)) {
                    int64_t l = std::get<int64_t>(lhs.property);
                    int64_t r = std::get<int64_t>(rhs.property);
                    if (bin->op == BinaryOpKind::ADD) return GqlValue(l + r);
                    if (bin->op == BinaryOpKind::SUB) return GqlValue(l - r);
                    if (bin->op == BinaryOpKind::MUL) return GqlValue(l * r);
                    if (bin->op == BinaryOpKind::DIV) return r != 0 ? GqlValue(l / r) : GqlValue();
                }
                // Double precision floating point arithmetic
                if (std::holds_alternative<double>(lhs.property) || std::holds_alternative<double>(rhs.property)) {
                    double l = std::holds_alternative<double>(lhs.property) ? std::get<double>(lhs.property) : static_cast<double>(std::get<int64_t>(lhs.property));
                    double r = std::holds_alternative<double>(rhs.property) ? std::get<double>(rhs.property) : static_cast<double>(std::get<int64_t>(rhs.property));
                    if (bin->op == BinaryOpKind::ADD) return GqlValue(l + r);
                    if (bin->op == BinaryOpKind::SUB) return GqlValue(l - r);
                    if (bin->op == BinaryOpKind::MUL) return GqlValue(l * r);
                    if (bin->op == BinaryOpKind::DIV) return r != 0.0 ? GqlValue(l / r) : GqlValue();
                }
                // String concatenation
                if (std::holds_alternative<std::string>(lhs.property) && std::holds_alternative<std::string>(rhs.property)) {
                    if (bin->op == BinaryOpKind::ADD || bin->op == BinaryOpKind::CONCAT) {
                        return GqlValue(std::get<std::string>(lhs.property) + std::get<std::string>(rhs.property));
                    }
                }
                // String comparison operators
                if (std::holds_alternative<std::string>(lhs.property) && std::holds_alternative<std::string>(rhs.property)) {
                    const auto& l = std::get<std::string>(lhs.property);
                    const auto& r = std::get<std::string>(rhs.property);
                    if (bin->op == BinaryOpKind::STARTS_WITH) {
                        return GqlValue(l.rfind(r, 0) == 0);
                    }
                    if (bin->op == BinaryOpKind::ENDS_WITH) {
                        if (l.length() >= r.length()) {
                            return GqlValue(l.compare(l.length() - r.length(), r.length(), r) == 0);
                        }
                        return GqlValue(false);
                    }
                    if (bin->op == BinaryOpKind::CONTAINS) {
                        return GqlValue(l.find(r) != std::string::npos);
                    }
                }
            }
            return GqlValue();
        }
        default:
            return GqlValue();
    }
    return GqlValue();
}

} // namespace ragedb::gql
