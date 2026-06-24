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

#include "ContradictionPruner.h"
#include "OptimizerUtils.h"
#include "../GqlVirtualCatalog.h"
#include "../GqlParser.h"
#include <set>

namespace ragedb::gql {

namespace {

struct InequalityEdge {
    std::string u;
    std::string v;
    bool strict;
};

void extract_inequalities(const Expression* expr, std::vector<InequalityEdge>& edges) {
    if (!expr) return;
    if (expr->kind == ExpressionKind::BINARY_OP) {
        const auto* bin = static_cast<const BinaryOpExpr*>(expr);
        if (bin->op == BinaryOpKind::AND) {
            extract_inequalities(bin->left.get(), edges);
            extract_inequalities(bin->right.get(), edges);
        } else {
            const Expression* left = bin->left.get();
            const Expression* right = bin->right.get();
            if (left->kind == ExpressionKind::PROPERTY_LOOKUP && right->kind == ExpressionKind::PROPERTY_LOOKUP) {
                const auto* pl1 = static_cast<const PropertyLookupExpr*>(left);
                const auto* pl2 = static_cast<const PropertyLookupExpr*>(right);
                
                std::string u = pl1->variable + "." + pl1->property;
                std::string v = pl2->variable + "." + pl2->property;
                
                auto op = bin->op;
                if (op == BinaryOpKind::LT) {
                    edges.push_back({u, v, true});
                } else if (op == BinaryOpKind::LE) {
                    edges.push_back({u, v, false});
                } else if (op == BinaryOpKind::GT) {
                    edges.push_back({v, u, true});
                } else if (op == BinaryOpKind::GE) {
                    edges.push_back({v, u, false});
                } else if (op == BinaryOpKind::EQ) {
                    edges.push_back({u, v, false});
                    edges.push_back({v, u, false});
                }
            }
        }
    }
}

bool has_inequality_contradiction(const std::vector<InequalityEdge>& edges) {
    std::set<std::string> vertices;
    for (const auto& edge : edges) {
        vertices.insert(edge.u);
        vertices.insert(edge.v);
    }
    if (vertices.empty()) return false;
    
    std::vector<std::string> v_list(vertices.begin(), vertices.end());
    size_t n = v_list.size();
    std::map<std::string, size_t> v_map;
    for (size_t i = 0; i < n; ++i) {
        v_map[v_list[i]] = i;
    }
    
    std::vector<std::vector<bool>> reaches(n, std::vector<bool>(n, false));
    std::vector<std::vector<bool>> strict(n, std::vector<bool>(n, false));
    
    for (const auto& edge : edges) {
        size_t u_idx = v_map[edge.u];
        size_t v_idx = v_map[edge.v];
        reaches[u_idx][v_idx] = true;
        if (edge.strict) {
            strict[u_idx][v_idx] = true;
        }
    }
    
    for (size_t k = 0; k < n; ++k) {
        for (size_t i = 0; i < n; ++i) {
            for (size_t j = 0; j < n; ++j) {
                if (reaches[i][k] && reaches[k][j]) {
                    reaches[i][j] = true;
                    if (strict[i][k] || strict[k][j]) {
                        strict[i][j] = true;
                    }
                }
            }
        }
    }
    
    for (size_t i = 0; i < n; ++i) {
        if (reaches[i][i] && strict[i][i]) {
            return true;
        }
    }
    return false;
}

} // namespace

void ContradictionPruner::semantic_pruning_pass(GqlQuery& query) {
    if (query.kind != QueryKind::SINGLE) return;
    
    auto q_vars = collect_query_vars(query);
    
    // Check self-contradiction in query (e.g. x.age > 10 AND x.age < 5)
    for (const auto& vi : q_vars) {
        for (const auto& [prop, q_interval] : vi.intervals) {
            if (q_interval.is_empty()) {
                query.no_op = true;
                return;
            }
        }
    }
    
    // Check contradictions against catalog constraints
    const auto& constraints = GqlVirtualCatalog::local().get_constraints();
    for (const auto& [c_name, c_query_str] : constraints) {
        try {
            auto c_query = GqlParser::parse(c_query_str);
            auto c_vars = collect_query_vars(c_query);
            
            for (const auto& c_vi : c_vars) {
                if (c_vi.label.empty()) continue;
                for (const auto& q_vi : q_vars) {
                    if (q_vi.label == c_vi.label) {
                        for (const auto& [prop, c_interval] : c_vi.intervals) {
                            auto it = q_vi.intervals.find(prop);
                            if (it != q_vi.intervals.end()) {
                                if (c_interval.contains(it->second)) {
                                    query.no_op = true;
                                    return;
                                }
                            }
                        }
                    }
                }
            }
        } catch (...) {
        }
    }
}

void ContradictionPruner::relational_pruning_pass(GqlQuery& query) {
    if (query.kind != QueryKind::SINGLE) return;
    
    std::vector<InequalityEdge> edges;
    extract_inequalities(query.where_expr.get(), edges);
    
    for (const auto& match : query.matches) {
        for (const auto& node : match.pattern.nodes) {
            extract_inequalities(node.where_expr.get(), edges);
        }
        for (const auto& edge : match.pattern.edges) {
            extract_inequalities(edge.where_expr.get(), edges);
        }
    }
    
    if (has_inequality_contradiction(edges)) {
        query.no_op = true;
    }
}

} // namespace ragedb::gql
