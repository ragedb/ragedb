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

// The AST expression evaluator: evaluate_expression walks an Expression tree against a query row's bindings,
// dispatching to the value primitives (comparison, cast, temporal, list coercion) in GqlValue.cpp and the
// scalar-function library in GqlValueFunctions.cpp. Split out of GqlValue.cpp to keep it under the file-size
// target; the entry point is declared in GqlValue.h.

#include "GqlValue.h"
#include <memory>
#include <string>
#include <vector>

namespace ragedb::gql {

/**
 * @brief Evaluates an AST expression node against a query row's bindings.
 *
 * Evaluates variables, literals, properties, unary operations (NOT, NEG),
 * and binary operations (AND, OR, logical comparisons, arithmetic additions/subtractions).
 *
 * @param row The row representing the query context and bindings.
 * @param expr The AST expression node to evaluate.
 * @return GqlValue The result of the evaluation.
 */
GqlValue evaluate_expression(const GqlRow& row, const Expression* expr) {
    if (!expr) return GqlValue();

    switch (expr->kind) {
        case ExpressionKind::IS_NULL_CHECK: {
            auto* is_null_expr = static_cast<const IsNullExpr*>(expr);
            auto val = evaluate_expression(row, is_null_expr->expr.get());
            bool is_nil = (val.type == GqlValue::NIL);
            if (val.type == GqlValue::PROPERTY) {
                if (std::holds_alternative<std::monostate>(val.property)) {
                    is_nil = true;
                }
            }
            return GqlValue(is_null_expr->is_not ? !is_nil : is_nil);
        }
        case ExpressionKind::SIZE_OP: {
            // A pure `COUNT { (v)-[:R]->() }` is rewritten to a degree property by DegreeConstraintPruner and
            // never reaches here. A constrained/correlated COUNT{} is computed per row by the precompute pass
            // and bound under "_count_<subquery_id>"; read it back. Null if it was not precomputed (an as-yet
            // unsupported subquery shape) -- never a silent wrong count.
            auto* se = static_cast<const SizeExpr*>(expr);
            if (se->subquery_id >= 0) {
                auto it = row.bindings.find("_count_" + std::to_string(se->subquery_id));
                if (it != row.bindings.end()) {
                    return it->second;
                }
            }
            return GqlValue();
        }
        case ExpressionKind::AGGREGATION: {
            return GqlValue(); // Aggregations are not evaluated on single rows
        }
        case ExpressionKind::CASE_WHEN: {
            auto* ce = static_cast<const CaseExpr*>(expr);
            for (const auto& branch : ce->branches) {
                if (evaluate_expression(row, branch.first.get()).is_truthy()) {
                    return evaluate_expression(row, branch.second.get());
                }
            }
            if (ce->else_expr) {
                return evaluate_expression(row, ce->else_expr.get());
            }
            return GqlValue();
        }
        case ExpressionKind::FUNCTION_CALL: {
            return evaluate_scalar_function(row, static_cast<const FunctionCallExpr*>(expr));
        }
        case ExpressionKind::LIST_LITERAL: {
            auto* le = static_cast<const ListExpr*>(expr);
            auto items = std::make_shared<std::vector<GqlValue>>();
            items->reserve(le->elements.size());
            for (const auto& element : le->elements) {
                items->push_back(evaluate_expression(row, element.get()));
            }
            GqlValue out;
            out.type = GqlValue::LIST;
            out.list = std::move(items);
            return out;
        }
        case ExpressionKind::LIST_INDEX: {
            auto* ie = static_cast<const IndexExpr*>(expr);
            GqlValue list = evaluate_expression(row, ie->list.get());
            GqlValue idx = evaluate_expression(row, ie->index.get());
            if (list.type != GqlValue::LIST || !list.list) return GqlValue();
            if (idx.type != GqlValue::PROPERTY || !std::holds_alternative<int64_t>(idx.property)) return GqlValue();
            int64_t i = std::get<int64_t>(idx.property);
            int64_t n = static_cast<int64_t>(list.list->size());
            if (i < 0) i += n;                       // negative index counts from the end
            if (i < 0 || i >= n) return GqlValue();  // out of range -> null
            return (*list.list)[static_cast<size_t>(i)];
        }
        case ExpressionKind::LIST_COMPREHENSION: {
            auto* lc = static_cast<const ListComprehensionExpr*>(expr);
            GqlValue src = evaluate_expression(row, lc->list.get());
            if (src.type != GqlValue::LIST || !src.list) return GqlValue();
            auto items = std::make_shared<std::vector<GqlValue>>();
            for (const auto& elem : *src.list) {
                GqlRow scoped = row;                        // bind the iteration variable in a child scope
                scoped.bindings[lc->variable] = elem;
                if (lc->filter && !evaluate_expression(scoped, lc->filter.get()).is_truthy()) continue;
                items->push_back(lc->projection ? evaluate_expression(scoped, lc->projection.get()) : elem);
            }
            GqlValue out;
            out.type = GqlValue::LIST;
            out.list = std::move(items);
            return out;
        }
        case ExpressionKind::QUANTIFIED_PREDICATE: {
            auto* qp = static_cast<const QuantifiedPredicateExpr*>(expr);
            GqlValue src = evaluate_expression(row, qp->list.get());
            if (src.type != GqlValue::LIST || !src.list) return GqlValue();
            int64_t matched = 0;
            int64_t total = 0;
            for (const auto& elem : *src.list) {
                GqlRow scoped = row;
                scoped.bindings[qp->variable] = elem;
                ++total;
                if (!qp->predicate || evaluate_expression(scoped, qp->predicate.get()).is_truthy()) ++matched;
            }
            bool result = false;
            switch (qp->quant) {
                case QuantifiedPredicateExpr::ALL:    result = (matched == total); break;
                case QuantifiedPredicateExpr::ANY:    result = (matched > 0); break;
                case QuantifiedPredicateExpr::NONE:   result = (matched == 0); break;
                case QuantifiedPredicateExpr::SINGLE: result = (matched == 1); break;
            }
            return GqlValue(result);
        }
        case ExpressionKind::TEMPORAL_FIELD: {
            auto* tf = static_cast<const TemporalFieldExpr*>(expr);
            GqlValue v = evaluate_expression(row, tf->value.get());
            if (v.type != GqlValue::PROPERTY || !std::holds_alternative<int64_t>(v.property)) return GqlValue();
            return gql_temporal_field(std::get<int64_t>(v.property), tf->field);
        }
        case ExpressionKind::CAST: {
            auto* c = static_cast<const CastExpr*>(expr);
            return apply_cast(evaluate_expression(row, c->value.get()), c->target);
        }
        case ExpressionKind::IS_LABELED: {
            auto* l = static_cast<const IsLabeledExpr*>(expr);
            return apply_is_labeled(evaluate_expression(row, l->value.get()), l->label_expr, l->negated);
        }
        case ExpressionKind::IS_DIRECTED: {
            auto* d = static_cast<const IsDirectedExpr*>(expr);
            GqlValue v = evaluate_expression(row, d->value.get());
            if (v.type != GqlValue::RELATIONSHIP) return GqlValue();  // undefined for non-relationships
            // ragedb relationships are always directed.
            return GqlValue(d->negated ? false : true);
        }
        case ExpressionKind::IS_SOURCE_DEST: {
            auto* s = static_cast<const IsSourceDestExpr*>(expr);
            GqlValue node = evaluate_expression(row, s->value.get());
            GqlValue edge = evaluate_expression(row, s->edge.get());
            if (node.type != GqlValue::NODE || edge.type != GqlValue::RELATIONSHIP) return GqlValue();
            uint64_t endpoint = s->is_source ? edge.relationship->getStartingNodeId()
                                             : edge.relationship->getEndingNodeId();
            bool result = (node.node->getId() == endpoint);
            return GqlValue(s->negated ? !result : result);
        }
        case ExpressionKind::IN_LIST: {
            auto* in = static_cast<const InExpr*>(expr);
            GqlValue needle = evaluate_expression(row, in->value.get());
            GqlValue hay = evaluate_expression(row, in->list.get());
            if (needle.type == GqlValue::NIL || hay.type != GqlValue::LIST) {
                return GqlValue();
            }
            for (const auto& item : *hay.list) {
                if (compare_gql_values(needle, item) == 0) {
                    return GqlValue(true);
                }
            }
            return GqlValue(false);
        }
        case ExpressionKind::EXISTS: {
            auto* exists = static_cast<const ExistsExpr*>(expr);
            // Nested EXISTS (inside a subquery WHERE): the correlated precompute bound the answer per row
            // as "_exists_<id>" because the semi-join rewrite does not reach a subquery's own WHERE.
            if (exists->subquery_id >= 0) {
                auto it = row.bindings.find("_exists_" + std::to_string(exists->subquery_id));
                if (it != row.bindings.end()) {
                    return GqlValue(it->second.is_truthy());
                }
                return GqlValue(false);
            }
            if (!exists->target_variable.empty()) {
                auto it = row.bindings.find(exists->target_variable);
                if (it != row.bindings.end() && it->second.type != GqlValue::NIL) {
                    if (exists->where_expr) {
                        return evaluate_expression(row, exists->where_expr.get());
                    }
                    return GqlValue(true);
                }
                return GqlValue(false);
            }
            return GqlValue(false);
        }
        case ExpressionKind::LITERAL: {
            auto* lit = static_cast<const LiteralExpr*>(expr);
            return GqlValue(lit->value);
        }
        case ExpressionKind::VARIABLE: {
            auto* var = static_cast<const VariableExpr*>(expr);
            auto it = row.bindings.find(var->name);
            if (it != row.bindings.end()) {
                return it->second;
            }
            return GqlValue();
        }
        case ExpressionKind::PROPERTY_LOOKUP: {
            auto* prop_lookup = static_cast<const PropertyLookupExpr*>(expr);
            auto it = row.bindings.find(prop_lookup->variable);
            if (it != row.bindings.end()) {
                const auto& val = it->second;
                if (val.type == GqlValue::NODE) {
                    // `key` is a distinct Node field (the external/business key), not a properties-map
                    // entry, so it must resolve through getKey() rather than a properties lookup.
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
            auto* un = static_cast<const UnaryOpExpr*>(expr);
            auto val = evaluate_expression(row, un->expr.get());
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
            auto* bin = static_cast<const BinaryOpExpr*>(expr);
            if (bin->op == BinaryOpKind::AND) {
                auto lhs = evaluate_expression(row, bin->left.get());
                if (!lhs.is_truthy()) return GqlValue(false);
                auto rhs = evaluate_expression(row, bin->right.get());
                return GqlValue(rhs.is_truthy());
            }
            if (bin->op == BinaryOpKind::OR) {
                auto lhs = evaluate_expression(row, bin->left.get());
                if (lhs.is_truthy()) return GqlValue(true);
                auto rhs = evaluate_expression(row, bin->right.get());
                return GqlValue(rhs.is_truthy());
            }

            auto lhs = evaluate_expression(row, bin->left.get());
            auto rhs = evaluate_expression(row, bin->right.get());

            if (lhs.type == GqlValue::NIL || rhs.type == GqlValue::NIL) {
                return GqlValue();
            }

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

            if (lhs.type == GqlValue::PROPERTY && rhs.type == GqlValue::PROPERTY) {
                if (std::holds_alternative<int64_t>(lhs.property) && std::holds_alternative<int64_t>(rhs.property)) {
                    int64_t l = std::get<int64_t>(lhs.property);
                    int64_t r = std::get<int64_t>(rhs.property);
                    if (bin->op == BinaryOpKind::ADD) return GqlValue(l + r);
                    if (bin->op == BinaryOpKind::SUB) return GqlValue(l - r);
                    if (bin->op == BinaryOpKind::MUL) return GqlValue(l * r);
                    if (bin->op == BinaryOpKind::DIV) return r != 0 ? GqlValue(l / r) : GqlValue();
                }
                if (std::holds_alternative<double>(lhs.property) || std::holds_alternative<double>(rhs.property)) {
                    double l = std::holds_alternative<double>(lhs.property) ? std::get<double>(lhs.property) : static_cast<double>(std::get<int64_t>(lhs.property));
                    double r = std::holds_alternative<double>(rhs.property) ? std::get<double>(rhs.property) : static_cast<double>(std::get<int64_t>(rhs.property));
                    if (bin->op == BinaryOpKind::ADD) return GqlValue(l + r);
                    if (bin->op == BinaryOpKind::SUB) return GqlValue(l - r);
                    if (bin->op == BinaryOpKind::MUL) return GqlValue(l * r);
                    if (bin->op == BinaryOpKind::DIV) return r != 0.0 ? GqlValue(l / r) : GqlValue();
                }
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
    }
    return GqlValue();
}

} // namespace ragedb::gql
