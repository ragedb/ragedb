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

#include "AntiSemiJoinPromoter.h"
#include <set>
#include <vector>

namespace ragedb::gql {

namespace {

/**
 * @brief Recursively traverses the query WHERE expression tree to locate and remove IS NULL checks
 *        targeting any variables defined within the optional match.
 * @param expr The query WHERE expression to search.
 * @param vars The set of variable names bound by the OPTIONAL MATCH statement.
 * @param removed Output flag set to true if an IS NULL check was found and removed.
 * @return A new expression tree with target IS NULL check nodes removed (replaced by their siblings in AND operations).
 */
std::unique_ptr<Expression> remove_isnull_checks_recursive(std::unique_ptr<Expression> expr, const std::set<std::string>& vars, bool& removed) {
    if (!expr) return nullptr;
    
    // Check if the current node is an IS NULL check of the form `variable IS NULL`
    if (expr->kind == ExpressionKind::IS_NULL_CHECK) {
        auto* is_null = static_cast<IsNullExpr*>(expr.get());
        if (!is_null->is_not && is_null->expr->kind == ExpressionKind::VARIABLE) {
            std::string var_name = static_cast<VariableExpr*>(is_null->expr.get())->name;
            if (vars.count(var_name)) {
                removed = true;
                return nullptr; // Remove this check by returning null
            }
        }
    }
    
    // Recursively process AND clauses
    if (expr->kind == ExpressionKind::BINARY_OP) {
        auto* bin = static_cast<BinaryOpExpr*>(expr.get());
        if (bin->op == BinaryOpKind::AND) {
            auto left = remove_isnull_checks_recursive(std::move(bin->left), vars, removed);
            auto right = remove_isnull_checks_recursive(std::move(bin->right), vars, removed);
            if (!left) return right; // If left was removed, return right
            if (!right) return left;  // If right was removed, return left
            bin->left = std::move(left);
            bin->right = std::move(right);
            return expr;
        }
    }
    
    return expr;
}

} // namespace

/**
 * @brief Phase 18: Anti-Semi-Join Promotion.
 *        Identifies OPTIONAL MATCH patterns that are followed by an IS NULL check on their variables.
 *        Since the query expects these variables to be null, this represents an anti-semi-join
 *        (i.e., we only want rows where the optional pattern does NOT exist).
 *        The OPTIONAL MATCH is promoted to a NOT EXISTS (anti-semi-join) filter and removed
 *        from the main list of query matches to optimize execution paths.
 */
void AntiSemiJoinPromoter::optional_match_to_antisemijoin_pass(GqlQuery& query) {
    if (query.skip_semantic || query.kind != QueryKind::SINGLE) return;
    
    for (auto it = query.matches.begin(); it != query.matches.end(); ) {
        if (!it->is_optional) {
            ++it;
            continue;
        }
        
        // Collect all variable names introduced by this OPTIONAL MATCH pattern
        std::set<std::string> opt_vars;
        for (const auto& node : it->pattern.nodes) {
            if (!node.variable.empty()) opt_vars.insert(node.variable);
        }
        for (const auto& edge : it->pattern.edges) {
            if (!edge.variable.empty()) opt_vars.insert(edge.variable);
        }
        
        if (opt_vars.empty()) {
            ++it;
            continue;
        }
        
        // Look for and remove IS NULL checks on these variables in the WHERE expression
        bool removed = false;
        query.where_expr = remove_isnull_checks_recursive(std::move(query.where_expr), opt_vars, removed);
        
        if (removed) {
            // Convert the OPTIONAL MATCH into a NOT EXISTS subquery
            MatchStatement inner_match = *it;
            inner_match.is_optional = false;
            inner_match.optional_group_id = -1;
            
            std::vector<MatchStatement> nested_matches = { inner_match };
            auto exists_expr = std::make_unique<ExistsExpr>(std::move(nested_matches), nullptr);
            auto not_exists_expr = std::make_unique<UnaryOpExpr>(UnaryOpKind::NOT, std::move(exists_expr));
            
            // Append the NOT EXISTS expression to the WHERE clause
            if (query.where_expr) {
                query.where_expr = std::make_unique<BinaryOpExpr>(
                    BinaryOpKind::AND,
                    std::move(query.where_expr),
                    std::move(not_exists_expr)
                );
            } else {
                query.where_expr = std::move(not_exists_expr);
            }
            
            // Remove the OPTIONAL MATCH from the top-level matches list
            it = query.matches.erase(it);
        } else {
            ++it;
        }
    }
}

} // namespace ragedb::gql
