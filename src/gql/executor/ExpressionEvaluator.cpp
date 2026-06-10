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
        case ExpressionKind::AGGREGATION: {
            // Aggregation: Look up precomputed results in the results map populated by the caller
            auto* agg = static_cast<const AggregateExpr*>(expr);
            auto it = aggregate_results.find(agg);
            if (it != aggregate_results.end()) {
                return it->second;
            }
            return GqlValue();
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
                    if (bin->op == BinaryOpKind::ADD) {
                        return GqlValue(std::get<std::string>(lhs.property) + std::get<std::string>(rhs.property));
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
