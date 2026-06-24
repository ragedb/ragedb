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

#include "OptionalMatchPromoter.h"
#include "OptimizerUtils.h"
#include <vector>

namespace ragedb::gql {

namespace {

bool is_null_rejecting(const Expression* expr, const std::string& var) {
    if (!expr) return false;
    
    switch (expr->kind) {
        case ExpressionKind::PROPERTY_LOOKUP: {
            const auto* pl = static_cast<const PropertyLookupExpr*>(expr);
            return pl->variable == var;
        }
        case ExpressionKind::VARIABLE: {
            const auto* v = static_cast<const VariableExpr*>(expr);
            return v->name == var;
        }
        case ExpressionKind::IS_NULL_CHECK: {
            const auto* is_null = static_cast<const IsNullExpr*>(expr);
            if (is_null->is_not) {
                return is_null_rejecting(is_null->expr.get(), var);
            }
            return false;
        }
        case ExpressionKind::UNARY_OP: {
            const auto* un = static_cast<const UnaryOpExpr*>(expr);
            return is_null_rejecting(un->expr.get(), var);
        }
        case ExpressionKind::BINARY_OP: {
            const auto* bin = static_cast<const BinaryOpExpr*>(expr);
            if (bin->op == BinaryOpKind::AND) {
                return is_null_rejecting(bin->left.get(), var) || is_null_rejecting(bin->right.get(), var);
            }
            if (bin->op == BinaryOpKind::OR) {
                return is_null_rejecting(bin->left.get(), var) && is_null_rejecting(bin->right.get(), var);
            }
            return is_null_rejecting(bin->left.get(), var) || is_null_rejecting(bin->right.get(), var);
        }
        default:
            return false;
    }
}

} // namespace

void OptionalMatchPromoter::optional_match_promotion_pass(GqlQuery& query) {
    if (query.kind != QueryKind::SINGLE) return;
    if (!query.where_expr) return;

    for (auto& match : query.matches) {
        if (match.is_optional) {
            // Find all variables introduced in this OPTIONAL MATCH
            std::vector<std::string> match_vars;
            for (const auto& node : match.pattern.nodes) {
                if (!node.variable.empty()) match_vars.push_back(node.variable);
            }
            for (const auto& edge : match.pattern.edges) {
                if (!edge.variable.empty()) match_vars.push_back(edge.variable);
            }
            
            // Check if any variable (except the start/anchor node) is null-rejected in the WHERE clause
            bool promoted = false;
            std::string start_var = match.pattern.nodes.empty() ? "" : match.pattern.nodes.front().variable;
            for (const auto& var : match_vars) {
                if (var == start_var) continue; // Skip start/anchor node variable
                if (is_null_rejecting(query.where_expr.get(), var)) {
                    promoted = true;
                    break;
                }
            }
            
            if (promoted) {
                match.is_optional = false;
            }
        }
    }
}

} // namespace ragedb::gql
