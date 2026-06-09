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

namespace ragedb::gql {

void GqlOptimizer::extract_eq_predicates(Expression* expr, std::map<std::string, std::map<std::string, property_type_t>>& pushdowns) {
    if (!expr) return;

    if (expr->kind == ExpressionKind::BINARY_OP) {
        auto* bin = static_cast<BinaryOpExpr*>(expr);
        if (bin->op == BinaryOpKind::AND) {
            extract_eq_predicates(bin->left.get(), pushdowns);
            extract_eq_predicates(bin->right.get(), pushdowns);
        } else if (bin->op == BinaryOpKind::EQ) {
            if (bin->left->kind == ExpressionKind::PROPERTY_LOOKUP && bin->right->kind == ExpressionKind::LITERAL) {
                auto* prop_lookup = static_cast<PropertyLookupExpr*>(bin->left.get());
                auto* lit = static_cast<LiteralExpr*>(bin->right.get());
                pushdowns[prop_lookup->variable][prop_lookup->property] = lit->value;
            } else if (bin->right->kind == ExpressionKind::PROPERTY_LOOKUP && bin->left->kind == ExpressionKind::LITERAL) {
                auto* prop_lookup = static_cast<PropertyLookupExpr*>(bin->right.get());
                auto* lit = static_cast<LiteralExpr*>(bin->left.get());
                pushdowns[prop_lookup->variable][prop_lookup->property] = lit->value;
            }
        }
    }
}

std::unique_ptr<Expression> GqlOptimizer::rebuild_expression_without_pushed_predicates(std::unique_ptr<Expression> expr, const std::map<std::string, std::map<std::string, property_type_t>>& pushdowns) {
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
        } else if (bin->op == BinaryOpKind::EQ) {
            if (bin->left->kind == ExpressionKind::PROPERTY_LOOKUP && bin->right->kind == ExpressionKind::LITERAL) {
                auto* prop_lookup = static_cast<PropertyLookupExpr*>(bin->left.get());
                auto it = pushdowns.find(prop_lookup->variable);
                if (it != pushdowns.end() && it->second.count(prop_lookup->property)) {
                    return nullptr;
                }
            } else if (bin->right->kind == ExpressionKind::PROPERTY_LOOKUP && bin->left->kind == ExpressionKind::LITERAL) {
                auto* prop_lookup = static_cast<PropertyLookupExpr*>(bin->right.get());
                auto it = pushdowns.find(prop_lookup->variable);
                if (it != pushdowns.end() && it->second.count(prop_lookup->property)) {
                    return nullptr;
                }
            }
        }
    }

    return expr;
}

void GqlOptimizer::optimize(GqlQuery& query) {
    if (query.kind != QueryKind::SINGLE) {
        if (query.left) optimize(*query.left);
        if (query.right) optimize(*query.right);
        return;
    }

    if (!query.where_expr) return;

    std::map<std::string, std::map<std::string, property_type_t>> pushdowns;
    extract_eq_predicates(query.where_expr.get(), pushdowns);

    if (pushdowns.empty()) return;

    for (auto& match : query.matches) {
        for (auto& node : match.pattern.nodes) {
            if (!node.variable.empty()) {
                auto it = pushdowns.find(node.variable);
                if (it != pushdowns.end()) {
                    for (const auto& [prop, val] : it->second) {
                        node.properties[prop] = val;
                    }
                }
            }
        }
        for (auto& edge : match.pattern.edges) {
            if (!edge.variable.empty()) {
                auto it = pushdowns.find(edge.variable);
                if (it != pushdowns.end()) {
                    for (const auto& [prop, val] : it->second) {
                        edge.properties[prop] = val;
                    }
                }
            }
        }
    }

    query.where_expr = rebuild_expression_without_pushed_predicates(std::move(query.where_expr), pushdowns);
}

} // namespace ragedb::gql
