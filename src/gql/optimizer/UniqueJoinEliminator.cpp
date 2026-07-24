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

#include "UniqueJoinEliminator.h"
#include "../GqlVirtualCatalog.h"
#include "../GqlParser.h"
#include "OptimizerUtils.h"
#include <vector>
#include <set>
#include <algorithm>

namespace ragedb::gql {

namespace {

struct CardinalityConstraint {
    std::string source_label;
    std::string rel_type;
    Direction direction; // OUT or IN
    std::string target_label; // optional
    uint64_t max_cardinality;
};

std::vector<CardinalityConstraint> find_cardinality_constraints() {
    std::vector<CardinalityConstraint> constraints_list;
    const auto& constraints = GqlVirtualCatalog::local().get_constraints();
    for (const auto& [name, query_str] : constraints) {
        try {
            auto c_query = GqlParser::parse(query_str);
            if (c_query.kind != QueryKind::SINGLE) continue;
            
            size_t N = c_query.matches.size();
            if (N < 2) continue;
            
            std::string source_var = "";
            std::string source_label = "";
            std::string rel_type = "";
            std::string target_label = "";
            Direction dir = Direction::BOTH;
            bool ok = true;
            std::vector<std::string> target_vars;
            
            for (size_t i = 0; i < N; ++i) {
                const auto& match = c_query.matches[i];
                if (match.pattern.nodes.size() != 2 || match.pattern.edges.size() != 1) {
                    ok = false;
                    break;
                }
                const auto& src_node = match.pattern.nodes[0];
                const auto& edge = match.pattern.edges[0];
                const auto& tgt_node = match.pattern.nodes[1];
                
                if (src_node.variable.empty()) {
                    ok = false;
                    break;
                }
                if (source_var.empty()) {
                    source_var = src_node.variable;
                } else if (src_node.variable != source_var) {
                    ok = false;
                    break;
                }
                
                if (src_node.label_expr && src_node.label_expr->kind == LabelExprKind::LITERAL) {
                    if (source_label.empty()) {
                        source_label = src_node.label_expr->name;
                    } else if (src_node.label_expr->name != source_label) {
                        ok = false;
                        break;
                    }
                }
                
                Direction current_dir = Direction::BOTH;
                if (edge.direction == EdgeDirection::RIGHT) {
                    current_dir = Direction::OUT;
                } else if (edge.direction == EdgeDirection::LEFT) {
                    current_dir = Direction::IN;
                } else {
                    ok = false;
                    break;
                }
                
                if (i == 0) {
                    dir = current_dir;
                } else if (current_dir != dir) {
                    ok = false;
                    break;
                }
                
                if (!edge.label_expr || edge.label_expr->kind != LabelExprKind::LITERAL) {
                    ok = false;
                    break;
                }
                if (rel_type.empty()) {
                    rel_type = edge.label_expr->name;
                } else if (edge.label_expr->name != rel_type) {
                    ok = false;
                    break;
                }
                
                if (tgt_node.variable.empty()) {
                    ok = false;
                    break;
                }
                target_vars.push_back(tgt_node.variable);
                
                if (tgt_node.label_expr && tgt_node.label_expr->kind == LabelExprKind::LITERAL) {
                    if (target_label.empty()) {
                        target_label = tgt_node.label_expr->name;
                    } else if (tgt_node.label_expr->name != target_label) {
                        ok = false;
                        break;
                    }
                }
            }
            
            if (!ok) continue;
            
            std::set<std::pair<std::string, std::string>> ne_pairs;
            auto collect_ne = [](auto& self, const Expression* expr, std::set<std::pair<std::string, std::string>>& pairs) -> void {
                if (!expr) return;
                if (expr->kind == ExpressionKind::BINARY_OP) {
                    const auto* bin = static_cast<const BinaryOpExpr*>(expr);
                    if (bin->op == BinaryOpKind::AND) {
                        self(self, bin->left.get(), pairs);
                        self(self, bin->right.get(), pairs);
                    } else if (bin->op == BinaryOpKind::NE) {
                        if (bin->left->kind == ExpressionKind::VARIABLE && bin->right->kind == ExpressionKind::VARIABLE) {
                            std::string v1 = static_cast<const VariableExpr*>(bin->left.get())->name;
                            std::string v2 = static_cast<const VariableExpr*>(bin->right.get())->name;
                            if (v1 > v2) std::swap(v1, v2);
                            pairs.insert({v1, v2});
                        }
                    }
                }
            };
            
            collect_ne(collect_ne, c_query.where_expr.get(), ne_pairs);
            
            bool all_distinct = true;
            for (size_t i = 0; i < target_vars.size(); ++i) {
                for (size_t j = i + 1; j < target_vars.size(); ++j) {
                    std::string v1 = target_vars[i];
                    std::string v2 = target_vars[j];
                    if (v1 > v2) std::swap(v1, v2);
                    if (ne_pairs.find({v1, v2}) == ne_pairs.end()) {
                        all_distinct = false;
                        break;
                    }
                }
                if (!all_distinct) break;
            }
            
            if (all_distinct) {
                constraints_list.push_back({source_label, rel_type, dir, target_label, N - 1});
            }
        } catch (...) {
        }
    }
    return constraints_list;
}

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
            } else if (curr->kind == ExpressionKind::IS_NULL_CHECK) {
                stack.push_back(static_cast<const IsNullExpr*>(curr)->expr.get());
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

} // namespace

void UniqueJoinEliminator::unique_join_elimination_pass(GqlQuery& query) {
    if (query.kind != QueryKind::SINGLE) return;

    auto card_constraints = find_cardinality_constraints();
    auto mandatory = find_mandatory_relations();

    for (auto& match : query.matches) {
        if (match.is_search || match.is_propagate) continue;
        auto& pattern = match.pattern;
        
        if (pattern.nodes.size() == 2 && pattern.edges.size() == 1) {
            const auto& start_node = pattern.nodes[0];
            const auto& end_node = pattern.nodes[1];
            const auto& edge = pattern.edges[0];
            
            if (start_node.label_expr && start_node.label_expr->kind == LabelExprKind::LITERAL &&
                end_node.label_expr && end_node.label_expr->kind == LabelExprKind::LITERAL &&
                edge.label_expr && edge.label_expr->kind == LabelExprKind::LITERAL &&
                edge.direction == EdgeDirection::RIGHT && !edge.is_variable_length) {
                
                std::string s_label = start_node.label_expr->name;
                std::string t_label = end_node.label_expr->name;
                std::string r_type = edge.label_expr->name;
                
                // Check if relationship is unique (max_cardinality == 1)
                bool is_unique = false;
                for (const auto& card : card_constraints) {
                    if (card.source_label == s_label && card.rel_type == r_type && card.target_label == t_label && card.max_cardinality == 1 && card.direction == Direction::OUT) {
                        is_unique = true;
                        break;
                    }
                }
                
                if (is_unique) {
                    // Check if it is also mandatory
                    bool is_mandatory = false;
                    for (const auto& rel : mandatory) {
                        if (rel.source_label == s_label && rel.rel_type == r_type && rel.target_label == t_label) {
                            is_mandatory = true;
                            break;
                        }
                    }
                    
                    // If it is unique, we can eliminate the join if:
                    // 1. The match is OPTIONAL MATCH (so null references are naturally preserved).
                    // 2. OR it is regular MATCH but also mandatory.
                    if (match.is_optional || is_mandatory) {
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
                        
                        if (!target_referenced && !edge_referenced && !target_has_filters && !edge_has_filters) {
                            pattern.nodes.pop_back();
                            pattern.edges.clear();
                        }
                    }
                }
            }
        }
    }
}

} // namespace ragedb::gql
