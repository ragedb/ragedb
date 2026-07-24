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

#include "LimitPushdownOptimizer.h"
#include "OptimizerUtils.h"
#include "../GqlVirtualCatalog.h"
#include "../GqlParser.h"
#include "../executor/ExpressionEvaluator.h"
#include <vector>
#include <unordered_map>
#include <set>
#include <algorithm>
#include <limits>

namespace ragedb::gql {

namespace {

struct MandatoryRelation {
    std::string source_label;
    std::string rel_type;
    std::string target_label;
};

std::vector<MandatoryRelation> find_mandatory_relations() {
    std::vector<MandatoryRelation> relations;
    const auto& constraints = GqlVirtualCatalog::local().get_constraints();
    for (const auto& [name, query_str] : constraints) {
        try {
            auto c_query = GqlParser::parse(query_str);
            if (c_query.kind != QueryKind::SINGLE || c_query.matches.size() != 1) continue;
            const auto& c_match = c_query.matches[0];
            if (c_match.pattern.nodes.size() != 1) continue;
            const auto& source_node = c_match.pattern.nodes[0];
            if (source_node.variable.empty() || !source_node.label_expr || source_node.label_expr->kind != LabelExprKind::LITERAL) continue;
            
            std::string source_label = source_node.label_expr->name;
            std::string source_var = source_node.variable;
            
            if (!c_query.where_expr || c_query.where_expr->kind != ExpressionKind::UNARY_OP) continue;
            const auto* un = static_cast<const UnaryOpExpr*>(c_query.where_expr.get());
            if (un->op != UnaryOpKind::NOT || !un->expr || un->expr->kind != ExpressionKind::EXISTS) continue;
            
            const auto* exists = static_cast<const ExistsExpr*>(un->expr.get());
            if (exists->matches.size() != 1) continue;
            const auto& exists_match = exists->matches[0];
            const auto& exists_pattern = exists_match.pattern;
            if (exists_pattern.nodes.size() != 2 || exists_pattern.edges.size() != 1) continue;
            
            if (exists_pattern.nodes[0].variable != source_var) continue;
            
            const auto& edge = exists_pattern.edges[0];
            if (edge.direction != EdgeDirection::RIGHT || edge.is_variable_length || !edge.label_expr || edge.label_expr->kind != LabelExprKind::LITERAL) continue;
            std::string rel_type = edge.label_expr->name;
            
            const auto& target_node = exists_pattern.nodes[1];
            if (!target_node.label_expr || target_node.label_expr->kind != LabelExprKind::LITERAL) continue;
            std::string target_label = target_node.label_expr->name;
            
            relations.push_back({source_label, rel_type, target_label});
        } catch (...) {
        }
    }
    return relations;
}

} // namespace

void LimitPushdownOptimizer::limit_pushdown_pass(GqlQuery& query) {
    if (query.kind != QueryKind::SINGLE || !query.limit.has_value() || !query.order_by.empty()) return;
    // A piped-row part's LIMIT bounds its total output, not the per-input-row expansion a
    // per-match scan limit would bound.
    if (query.consumes_piped_rows) return;

    if (query.matches.empty()) return;

    // Do not push the LIMIT into the scan when any predicate is evaluated (or rows are collapsed)
    // AFTER the scan: scanning exactly `limit` rows would under-return. The residual conditions
    // live in has_post_scan_residual_predicate, shared with the executor's limit_val gate.
    if (has_post_scan_residual_predicate(query)) return;

    // An aggregate folds the matched rows into far fewer result rows, so a LIMIT bounds the RESULT, not the
    // scan. Pushing it down truncates the rows the aggregate folds over and silently answers with the limit
    // instead of the aggregate: `RETURN count(f) LIMIT 1` returned 1 rather than the count. (The executor's
    // own scan-limit gate already refuses this; the pushdown pass did not.)
    for (const auto& item : query.returns) {
        if (has_aggregates(item.expr.get())) return;
    }

    // Push the whole page window (offset + limit), not the bare limit: an OFFSET skips past the first rows
    // of the result, so a scan bounded at `limit` would stop before reaching the rows the page returns.
    const uint64_t skip = query.offset.value_or(0);
    if (skip > std::numeric_limits<uint64_t>::max() - *query.limit) return;
    const std::optional<uint64_t> window = skip + *query.limit;

    if (query.matches.size() == 1) {
        query.matches[0].limit = window;
        return;
    }

    // For multiple matches, verify if all joins from the first match onwards are mandatory.
    auto mandatory = find_mandatory_relations();
    std::unordered_map<std::string, std::string> var_to_label;
    for (const auto& match : query.matches) {
        for (const auto& node : match.pattern.nodes) {
            if (!node.variable.empty() && node.label_expr && node.label_expr->kind == LabelExprKind::LITERAL) {
                var_to_label[node.variable] = node.label_expr->name;
            }
        }
    }

    std::set<std::string> active_vars;
    for (const auto& node : query.matches[0].pattern.nodes) {
        if (!node.variable.empty()) active_vars.insert(node.variable);
    }

    bool all_mandatory = true;
    for (size_t i = 1; i < query.matches.size(); ++i) {
        const auto& match = query.matches[i];
        if (match.is_optional || match.is_search || match.is_propagate) {
            all_mandatory = false;
            break;
        }
        
        // Find if this match pattern represents a mandatory join branching out from an active variable
        const auto& pattern = match.pattern;
        if (pattern.nodes.size() == 2 && pattern.edges.size() == 1) {
            const auto& start_node = pattern.nodes[0];
            const auto& edge = pattern.edges[0];
            const auto& end_node = pattern.nodes[1];
            
            if (active_vars.count(start_node.variable) && edge.direction == EdgeDirection::RIGHT && !edge.is_variable_length &&
                end_node.label_expr && end_node.label_expr->kind == LabelExprKind::LITERAL &&
                edge.label_expr && edge.label_expr->kind == LabelExprKind::LITERAL) {

                // The start node anchors on an already-active variable. Its label is read off this
                // pattern when spelled, otherwise -- when an earlier match bound the variable with a
                // label and this join re-uses it bare -- recovered from the variable-to-label map.
                std::string s_label;
                if (start_node.label_expr && start_node.label_expr->kind == LabelExprKind::LITERAL) {
                    s_label = start_node.label_expr->name;
                } else {
                    auto it = var_to_label.find(start_node.variable);
                    if (it != var_to_label.end()) s_label = it->second;
                }

                std::string t_label = end_node.label_expr->name;
                std::string r_type = edge.label_expr->name;

                bool match_mandatory = false;
                if (!s_label.empty()) {
                    for (const auto& rel : mandatory) {
                        if (rel.source_label == s_label && rel.rel_type == r_type && rel.target_label == t_label) {
                            match_mandatory = true;
                            break;
                        }
                    }
                }

                if (match_mandatory) {
                    if (!end_node.variable.empty()) active_vars.insert(end_node.variable);
                    continue;
                }
            }
        }
        
        all_mandatory = false;
        break;
    }

    if (all_mandatory) {
        query.matches[0].limit = window;
    }
}

} // namespace ragedb::gql
