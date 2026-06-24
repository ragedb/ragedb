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

#include "TransitiveReachabilityPruner.h"
#include "../GqlVirtualCatalog.h"
#include "../GqlParser.h"
#include "OptimizerUtils.h"
#include <queue>
#include <unordered_map>
#include <tuple>
#include <set>

namespace ragedb::gql {

namespace {

struct HierarchyKey {
    std::string rel_type;
    std::string child_label;
    std::string parent_label;
    std::string prop_name;

    bool operator<(const HierarchyKey& other) const {
        return std::tie(rel_type, child_label, parent_label, prop_name) <
               std::tie(other.rel_type, other.child_label, other.parent_label, other.prop_name);
    }
};

std::string to_string_val(const property_type_t& val) {
    if (std::holds_alternative<std::string>(val)) {
        return std::get<std::string>(val);
    } else if (std::holds_alternative<int64_t>(val)) {
        return std::to_string(std::get<int64_t>(val));
    }
    return "";
}

void extract_equalities(const Expression* expr, std::map<std::string, std::map<std::string, property_type_t>>& equalities) {
    if (!expr) return;
    if (expr->kind == ExpressionKind::BINARY_OP) {
        const auto* bin = static_cast<const BinaryOpExpr*>(expr);
        if (bin->op == BinaryOpKind::AND) {
            extract_equalities(bin->left.get(), equalities);
            extract_equalities(bin->right.get(), equalities);
        } else if (bin->op == BinaryOpKind::EQ) {
            if (bin->left->kind == ExpressionKind::PROPERTY_LOOKUP && bin->right->kind == ExpressionKind::LITERAL) {
                const auto* pl = static_cast<const PropertyLookupExpr*>(bin->left.get());
                const auto* lit = static_cast<const LiteralExpr*>(bin->right.get());
                equalities[pl->variable][pl->property] = lit->value;
            } else if (bin->right->kind == ExpressionKind::PROPERTY_LOOKUP && bin->left->kind == ExpressionKind::LITERAL) {
                const auto* pl = static_cast<const PropertyLookupExpr*>(bin->right.get());
                const auto* lit = static_cast<const LiteralExpr*>(bin->left.get());
                equalities[pl->variable][pl->property] = lit->value;
            }
        }
    }
}

std::map<std::string, std::map<std::string, property_type_t>> collect_query_equalities(const GqlQuery& query) {
    std::map<std::string, std::map<std::string, property_type_t>> equalities;
    
    for (const auto& match : query.matches) {
        if (match.is_search) continue;
        for (const auto& node : match.pattern.nodes) {
            if (!node.variable.empty()) {
                for (const auto& [prop, val] : node.properties) {
                    equalities[node.variable][prop] = val;
                }
                extract_equalities(node.where_expr.get(), equalities);
            }
        }
        for (const auto& edge : match.pattern.edges) {
            if (!edge.variable.empty()) {
                for (const auto& [prop, val] : edge.properties) {
                    equalities[edge.variable][prop] = val;
                }
                extract_equalities(edge.where_expr.get(), equalities);
            }
        }
    }
    
    extract_equalities(query.where_expr.get(), equalities);
    return equalities;
}

std::map<HierarchyKey, std::map<std::string, std::vector<std::string>>> build_taxonomy_graphs() {
    std::map<HierarchyKey, std::map<std::string, std::vector<std::string>>> graphs;
    const auto& constraints = GqlVirtualCatalog::local().get_constraints();
    for (const auto& [name, query_str] : constraints) {
        try {
            auto c_query = GqlParser::parse(query_str);
            if (c_query.kind != QueryKind::SINGLE) continue;
            if (c_query.matches.size() != 1) continue;
            
            const auto& match = c_query.matches[0];
            if (match.pattern.nodes.size() != 2 || match.pattern.edges.size() != 1) continue;
            
            const auto& node0 = match.pattern.nodes[0];
            const auto& edge = match.pattern.edges[0];
            const auto& node1 = match.pattern.nodes[1];
            
            if (!node0.label_expr || node0.label_expr->kind != LabelExprKind::LITERAL) continue;
            if (!node1.label_expr || node1.label_expr->kind != LabelExprKind::LITERAL) continue;
            if (!edge.label_expr || edge.label_expr->kind != LabelExprKind::LITERAL) continue;
            
            std::string rel_type = edge.label_expr->name;
            
            const PatternNode* child_ptr = nullptr;
            const PatternNode* parent_ptr = nullptr;
            if (edge.direction == EdgeDirection::RIGHT) {
                child_ptr = &node0;
                parent_ptr = &node1;
            } else if (edge.direction == EdgeDirection::LEFT) {
                child_ptr = &node1;
                parent_ptr = &node0;
            } else {
                continue;
            }
            
            auto c_eqs = collect_query_equalities(c_query);
            const auto& child_var = child_ptr->variable;
            const auto& parent_var = parent_ptr->variable;
            
            if (child_var.empty() || parent_var.empty()) continue;
            
            auto child_it = c_eqs.find(child_var);
            auto parent_it = c_eqs.find(parent_var);
            if (child_it == c_eqs.end() || parent_it == c_eqs.end()) continue;
            
            for (const auto& [prop, child_val_variant] : child_it->second) {
                auto parent_prop_it = parent_it->second.find(prop);
                if (parent_prop_it != parent_it->second.end()) {
                    std::string child_val = to_string_val(child_val_variant);
                    std::string parent_val = to_string_val(parent_prop_it->second);
                    if (!child_val.empty() && !parent_val.empty() && child_val != parent_val) {
                        HierarchyKey key{rel_type, child_ptr->label_expr->name, parent_ptr->label_expr->name, prop};
                        graphs[key][child_val].push_back(parent_val);
                    }
                }
            }
        } catch (...) {
        }
    }
    return graphs;
}

std::optional<uint64_t> find_shortest_path_hops(
    const std::map<std::string, std::vector<std::string>>& adj,
    const std::string& start,
    const std::string& target
) {
    if (start == target) return 0;
    
    std::unordered_map<std::string, uint64_t> dist;
    std::queue<std::string> q;
    
    dist[start] = 0;
    q.push(start);
    
    while (!q.empty()) {
        std::string curr = q.front();
        q.pop();
        
        uint64_t curr_dist = dist[curr];
        if (curr == target) return curr_dist;
        
        auto it = adj.find(curr);
        if (it != adj.end()) {
            for (const auto& next : it->second) {
                if (dist.find(next) == dist.end()) {
                    dist[next] = curr_dist + 1;
                    q.push(next);
                }
            }
        }
    }
    return std::nullopt;
}

} // namespace

void TransitiveReachabilityPruner::transitive_reachability_pruning_pass(GqlQuery& query) {
    if (query.skip_semantic || query.kind != QueryKind::SINGLE) return;
    
    auto graphs = build_taxonomy_graphs();
    if (graphs.empty()) return;
    
    auto q_eqs = collect_query_equalities(query);
    
    for (auto& match : query.matches) {
        if (match.is_search) continue;
        auto& pattern = match.pattern;
        
        for (size_t i = 0; i < pattern.edges.size(); ++i) {
            auto& edge = pattern.edges[i];
            const auto& src_node = pattern.nodes[i];
            const auto& tgt_node = pattern.nodes[i + 1];
            
            if (!src_node.label_expr || src_node.label_expr->kind != LabelExprKind::LITERAL) continue;
            if (!tgt_node.label_expr || tgt_node.label_expr->kind != LabelExprKind::LITERAL) continue;
            if (!edge.label_expr || edge.label_expr->kind != LabelExprKind::LITERAL) continue;
            
            std::string rel_type = edge.label_expr->name;
            std::string src_label = src_node.label_expr->name;
            std::string tgt_label = tgt_node.label_expr->name;
            
            std::string child_var = "";
            std::string parent_var = "";
            
            if (edge.direction == EdgeDirection::RIGHT) {
                child_var = src_node.variable;
                parent_var = tgt_node.variable;
            } else if (edge.direction == EdgeDirection::LEFT) {
                child_var = tgt_node.variable;
                parent_var = src_node.variable;
            } else {
                continue;
            }
            
            if (child_var.empty() || parent_var.empty()) continue;
            
            auto child_it = q_eqs.find(child_var);
            auto parent_it = q_eqs.find(parent_var);
            if (child_it == q_eqs.end() || parent_it == q_eqs.end()) continue;
            
            for (const auto& [prop, child_val_variant] : child_it->second) {
                auto parent_prop_it = parent_it->second.find(prop);
                if (parent_prop_it != parent_it->second.end()) {
                    std::string child_val = to_string_val(child_val_variant);
                    std::string parent_val = to_string_val(parent_prop_it->second);
                    
                    if (!child_val.empty() && !parent_val.empty() && child_val != parent_val) {
                        std::string child_label_expected = (edge.direction == EdgeDirection::RIGHT) ? src_label : tgt_label;
                        std::string parent_label_expected = (edge.direction == EdgeDirection::RIGHT) ? tgt_label : src_label;
                        
                        HierarchyKey key{rel_type, child_label_expected, parent_label_expected, prop};
                        auto graph_it = graphs.find(key);
                        if (graph_it != graphs.end()) {
                            auto path_len_opt = find_shortest_path_hops(graph_it->second, child_val, parent_val);
                            if (!path_len_opt.has_value()) {
                                query.no_op = true;
                                return;
                            }
                            
                            uint64_t hops = path_len_opt.value();
                            if (edge.is_variable_length) {
                                if (hops < edge.min_hops || hops > edge.max_hops) {
                                    query.no_op = true;
                                    return;
                                }
                            } else {
                                if (hops != 1) {
                                    query.no_op = true;
                                    return;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

} // namespace ragedb::gql
