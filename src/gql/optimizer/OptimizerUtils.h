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

#ifndef RAGEDB_OPTIMIZERUTILS_H
#define RAGEDB_OPTIMIZERUTILS_H

/**
 * @file OptimizerUtils.h
 * @brief Common helper structures and functions for RageDB Semantic Query Optimizer passes.
 * 
 * Provides shared abstractions such as:
 *  - Interval: A bounding interval representation used for variable constraint satisfiability checks.
 *  - VarInfo: Information gathered per variable (its label, and intervals per property).
 *  - AST Rewriters and Traversers: Functions to rewrite variable names, evaluate numeric literals,
 *    and analyze GQL query patterns for equivalence or containment.
 */

#include "../GqlAst.h"
#include <map>
#include <vector>
#include <string>
#include <set>
#include <memory>

namespace ragedb::gql {

struct Interval {
    bool has_lower = false;
    double lower_val = 0;
    bool lower_inclusive = false;

    bool has_upper = false;
    double upper_val = 0;
    bool upper_inclusive = false;

    bool is_empty() const;
    bool contains(const Interval& other) const;
    void intersect(const Interval& other);
};

struct VarInfo {
    std::string variable;
    std::string label;
    std::map<std::string, Interval> intervals;
};

// Label & Pattern Equivalence helpers
bool is_equivalent_label_expr(const std::shared_ptr<LabelExpression>& e1, const std::shared_ptr<LabelExpression>& e2);
bool is_label_subsumed(const std::shared_ptr<LabelExpression>& e1, const std::shared_ptr<LabelExpression>& e2);
bool is_equivalent_properties(const std::map<std::string, property_type_t>& p1, const std::map<std::string, property_type_t>& p2);
bool is_equivalent_pattern(const PathPattern& p1, const PathPattern& p2);

template <typename SmartPtr>
void rewrite_expr_vars(SmartPtr& expr, const std::map<std::string, std::string>& var_map) {
    if (!expr) return;
    if (expr->kind == ExpressionKind::VARIABLE) {
        auto* ve = static_cast<VariableExpr*>(expr.get());
        auto it = var_map.find(ve->name);
        if (it != var_map.end()) {
            ve->name = it->second;
        }
    } else if (expr->kind == ExpressionKind::PROPERTY_LOOKUP) {
        auto* pl = static_cast<PropertyLookupExpr*>(expr.get());
        auto it = var_map.find(pl->variable);
        if (it != var_map.end()) {
            pl->variable = it->second;
        }
    } else if (expr->kind == ExpressionKind::UNARY_OP) {
        auto* un = static_cast<UnaryOpExpr*>(expr.get());
        rewrite_expr_vars(un->expr, var_map);
    } else if (expr->kind == ExpressionKind::BINARY_OP) {
        auto* bin = static_cast<BinaryOpExpr*>(expr.get());
        rewrite_expr_vars(bin->left, var_map);
        rewrite_expr_vars(bin->right, var_map);
    } else if (expr->kind == ExpressionKind::AGGREGATION) {
        auto* agg = static_cast<AggregateExpr*>(expr.get());
        rewrite_expr_vars(agg->expr, var_map);
    } else if (expr->kind == ExpressionKind::EXISTS) {
        auto* exists = static_cast<ExistsExpr*>(expr.get());
        rewrite_expr_vars(exists->where_expr, var_map);
        for (auto& match : exists->matches) {
            for (auto& node : match.pattern.nodes) {
                auto it = var_map.find(node.variable);
                if (it != var_map.end()) node.variable = it->second;
                rewrite_expr_vars(node.where_expr, var_map);
            }
            for (auto& edge : match.pattern.edges) {
                auto it = var_map.find(edge.variable);
                if (it != var_map.end()) edge.variable = it->second;
                rewrite_expr_vars(edge.where_expr, var_map);
                rewrite_expr_vars(edge.cost_expr, var_map);
            }
        }
    }
}

// Numeric & expression evaluation helpers
bool get_numeric_value(const Expression* expr, double& val);
void extract_intervals_from_expr(const Expression* expr, const std::string& target_var, std::map<std::string, Interval>& intervals, bool negate = false);
std::vector<VarInfo> collect_query_vars(const GqlQuery& query);
std::vector<VarInfo> collect_all_query_vars(const GqlQuery& query);

bool is_variable_referenced_outside_count(const Expression* expr, const std::string& var_name);
void rewrite_count_to_sum_degree(std::unique_ptr<Expression>& expr, const std::string& start_var, const std::string& end_var, const std::string& edge_var, const std::string& degree_prop, bool& rewritten);
void extract_rel_types(const LabelExpression* expr, std::vector<std::string>& rel_types);
void rewrite_khop_count_to_var(std::unique_ptr<Expression>& expr, const std::string& var_name);
void collect_variables_from_matches(const std::vector<MatchStatement>& matches, std::set<std::string>& vars);

} // namespace ragedb::gql

#endif // RAGEDB_OPTIMIZERUTILS_H
