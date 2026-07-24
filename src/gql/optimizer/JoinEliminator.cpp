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

#include "JoinEliminator.h"
#include "../GqlVirtualCatalog.h"
#include "../GqlParser.h"
#include "OptimizerUtils.h"

namespace ragedb::gql {

namespace {

/// Whether an expression tree contains an aggregate anywhere. File-local so the optimizer keeps no
/// dependency on the executor's evaluator (SubsumptionPruner carries the same helper; a future cleanup
/// could hoist both into OptimizerUtils).
static bool je_expr_has_aggregate(const Expression* expr) {
    if (!expr) return false;
    std::vector<const Expression*> stack = { expr };
    while (!stack.empty()) {
        const auto* curr = stack.back();
        stack.pop_back();
        if (!curr) continue;
        if (curr->kind == ExpressionKind::AGGREGATION) return true;
        if (curr->kind == ExpressionKind::UNARY_OP) {
            stack.push_back(static_cast<const UnaryOpExpr*>(curr)->expr.get());
        } else if (curr->kind == ExpressionKind::BINARY_OP) {
            const auto* bin = static_cast<const BinaryOpExpr*>(curr);
            stack.push_back(bin->left.get());
            stack.push_back(bin->right.get());
        }
    }
    return false;
}

/// Set semantics: a DISTINCT projection with no aggregate. Only then is dropping a mandatory-but-not-
/// functional edge invisible -- the DISTINCT dedups the rows the edge multiplies in.
static bool je_query_is_set_semantic(const GqlQuery& q) {
    if (!q.distinct) return false;
    for (const auto& item : q.returns) {
        if (je_expr_has_aggregate(item.expr.get())) return false;
    }
    for (const auto& spec : q.order_by) {
        if (je_expr_has_aggregate(spec.expr.get())) return false;
    }
    return true;
}

struct MandatoryRelation {
    std::string source_label;
    std::string rel_type;
    std::string target_label;
};

bool is_var_referenced_in_query_except_match(const GqlQuery& query, const std::string& var) {
    auto check_expr = [&](const Expression* expr) -> bool {
        if (!expr) return false;
        std::vector<const Expression*> stack = { expr };
        while (!stack.empty()) {
            const auto* curr = stack.back();
            stack.pop_back();
            // A child pushed below can be null: count(*) is an AggregateExpr with no argument, and a
            // malformed binary/unary op can carry a null operand. Skip it rather than dereference kind.
            if (!curr) continue;
            if (curr->kind == ExpressionKind::VARIABLE) {
                if (static_cast<const VariableExpr*>(curr)->name == var) return true;
            } else if (curr->kind == ExpressionKind::PROPERTY_LOOKUP) {
                if (static_cast<const PropertyLookupExpr*>(curr)->variable == var) return true;
            } else if (curr->kind == ExpressionKind::UNARY_OP) {
                stack.push_back(static_cast<const UnaryOpExpr*>(curr)->expr.get());
            } else if (curr->kind == ExpressionKind::BINARY_OP) {
                const auto* bin = static_cast<const BinaryOpExpr*>(curr);
                stack.push_back(bin->left.get());
                stack.push_back(bin->right.get());
            } else if (curr->kind == ExpressionKind::AGGREGATION) {
                stack.push_back(static_cast<const AggregateExpr*>(curr)->expr.get());
            }
        }
        return false;
    };

    for (const auto& item : query.returns) {
        if (check_expr(item.expr.get())) return true;
    }
    if (check_expr(query.where_expr.get())) return true;
    for (const auto& spec : query.order_by) {
        if (check_expr(spec.expr.get())) return true;
    }
    for (const auto& w : query.writes) {
        if (w.set_var == var || w.remove_var == var || w.delete_var == var) return true;
        if (check_expr(w.set_expr.get())) return true;
    }
    
    return false;
}

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

void JoinEliminator::semantic_join_elimination_pass(GqlQuery& query) {
    if (query.kind != QueryKind::SINGLE) return;
    
    auto mandatory = find_mandatory_relations();
    if (mandatory.empty()) return;
    
    for (auto& match : query.matches) {
        if (match.is_search || match.is_propagate || match.is_optional) continue;
        auto& pattern = match.pattern;
        
        if (pattern.nodes.size() == 2 && pattern.edges.size() == 1) {
            const auto& start_node = pattern.nodes[0];
            const auto& end_node = pattern.nodes[1];
            const auto& edge = pattern.edges[0];
            
            if (start_node.label_expr && start_node.label_expr->kind == LabelExprKind::LITERAL &&
                end_node.label_expr && end_node.label_expr->kind == LabelExprKind::LITERAL &&
                edge.label_expr && edge.label_expr->kind == LabelExprKind::LITERAL &&
                edge.direction == EdgeDirection::RIGHT) {
                
                std::string s_label = start_node.label_expr->name;
                std::string t_label = end_node.label_expr->name;
                std::string r_type = edge.label_expr->name;
                
                bool is_mandatory = false;
                for (const auto& rel : mandatory) {
                    if (rel.source_label == s_label && rel.rel_type == r_type && rel.target_label == t_label) {
                        is_mandatory = true;
                        break;
                    }
                }
                
                if (is_mandatory) {
                    bool target_referenced = false;
                    if (!end_node.variable.empty()) {
                        target_referenced = is_var_referenced_in_query_except_match(query, end_node.variable);
                    }
                    
                    bool edge_referenced = false;
                    if (!edge.variable.empty()) {
                        edge_referenced = is_var_referenced_in_query_except_match(query, edge.variable);
                    }
                    
                    bool target_has_filters = !end_node.properties.empty() || !end_node.property_filters.empty() || end_node.where_expr;
                    bool edge_has_filters = !edge.properties.empty() || !edge.property_filters.empty() || edge.where_expr;
                    
                    // The mandatory constraint is at-least-one, not exactly-one: s may have several R
                    // edges. Stripping the edge drops the rows they multiply in, so it is sound only
                    // under set semantics (DISTINCT, no aggregate), where the multiplication is deduped.
                    // A genuinely 1:1 relation is stripped by UniqueJoinEliminator via its unique
                    // cardinality constraint instead.
                    if (!target_referenced && !edge_referenced && !target_has_filters && !edge_has_filters
                        && je_query_is_set_semantic(query)) {
                        pattern.nodes.pop_back();
                        pattern.edges.clear();
                    }
                }
            }
        }
    }
}

} // namespace ragedb::gql
