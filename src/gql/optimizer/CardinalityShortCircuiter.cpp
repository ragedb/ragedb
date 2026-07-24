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

#include "CardinalityShortCircuiter.h"
#include "../GqlVirtualCatalog.h"
#include "../GqlParser.h"
#include "OptimizerUtils.h"
#include <limits>

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

} // namespace

void CardinalityShortCircuiter::semantic_cardinality_limit_pass(GqlQuery& query) {
    if (query.skip_semantic || query.kind != QueryKind::SINGLE) return;
    
    auto constraints_list = find_cardinality_constraints();
    if (constraints_list.empty()) return;
    
    for (auto& match : query.matches) {
        if (match.is_search || match.is_propagate) continue;
        auto& pattern = match.pattern;
        for (size_t i = 0; i < pattern.edges.size(); ++i) {
            auto& edge = pattern.edges[i];
            const auto& src_node = pattern.nodes[i];
            const auto& tgt_node = pattern.nodes[i + 1];
            
            if (!src_node.label_expr || src_node.label_expr->kind != LabelExprKind::LITERAL) continue;
            if (!edge.label_expr || edge.label_expr->kind != LabelExprKind::LITERAL) continue;
            
            std::string src_label = src_node.label_expr->name;
            std::string rel_type = edge.label_expr->name;
            
            Direction dir = Direction::BOTH;
            if (edge.direction == EdgeDirection::RIGHT) {
                dir = Direction::OUT;
            } else if (edge.direction == EdgeDirection::LEFT) {
                dir = Direction::IN;
            } else {
                continue;
            }
            
            std::string tgt_label = "";
            if (tgt_node.label_expr && tgt_node.label_expr->kind == LabelExprKind::LITERAL) {
                tgt_label = tgt_node.label_expr->name;
            }
            
            uint64_t min_limit = std::numeric_limits<uint64_t>::max();
            bool found_constraint = false;
            for (const auto& c : constraints_list) {
                if (c.source_label == src_label && c.rel_type == rel_type && c.direction == dir) {
                    if (c.target_label.empty() || c.target_label == tgt_label) {
                        if (c.max_cardinality < min_limit) {
                            min_limit = c.max_cardinality;
                            found_constraint = true;
                        }
                    }
                }
            }
            
            if (found_constraint) {
                edge.max_cardinality_limit = min_limit;
            }
        }
    }
}

} // namespace ragedb::gql
