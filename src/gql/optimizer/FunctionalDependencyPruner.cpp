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

#include "FunctionalDependencyPruner.h"
#include "../GqlVirtualCatalog.h"
#include "../GqlParser.h"
#include "OptimizerUtils.h"
#include <unordered_map>
#include <set>
#include <vector>

namespace ragedb::gql {

struct FunctionalDependency {
    std::string label;
    std::string x;
    std::string y;
};

static std::vector<FunctionalDependency> find_functional_dependencies() {
    std::vector<FunctionalDependency> fd_list;
    const auto& constraints = GqlVirtualCatalog::local().get_constraints();
    for (const auto& [name, query_str] : constraints) {
        try {
            auto c_query = GqlParser::parse(query_str);
            if (c_query.kind != QueryKind::SINGLE) continue;
            
            // Collect node variable labels
            std::unordered_map<std::string, std::string> var_labels;
            for (const auto& match : c_query.matches) {
                for (const auto& node : match.pattern.nodes) {
                    if (!node.variable.empty()) {
                        std::string label = "";
                        if (node.label_expr && node.label_expr->kind == LabelExprKind::LITERAL) {
                            label = node.label_expr->name;
                        }
                        var_labels[node.variable] = label;
                    }
                }
            }
            
            if (var_labels.size() < 2) continue;
            
            // Helper to collect conjuncts
            std::vector<const Expression*> conjuncts;
            auto collect_conjuncts = [&](auto& self, const Expression* expr) -> void {
                if (!expr) return;
                if (expr->kind == ExpressionKind::BINARY_OP) {
                    auto* bin = static_cast<const BinaryOpExpr*>(expr);
                    if (bin->op == BinaryOpKind::AND) {
                        self(self, bin->left.get());
                        self(self, bin->right.get());
                        return;
                    }
                }
                conjuncts.push_back(expr);
            };
            collect_conjuncts(collect_conjuncts, c_query.where_expr.get());
            
            // Find equalities of form: u.X = v.X and inequalities of form: u.Y != v.Y
            std::unordered_map<std::string, std::string> eq_props;
            std::unordered_map<std::string, std::string> ne_props;
            
            auto make_key = [](const std::string& v1, const std::string& v2) {
                return v1 < v2 ? (v1 + "," + v2) : (v2 + "," + v1);
            };
            
            for (const auto* conj : conjuncts) {
                if (conj->kind != ExpressionKind::BINARY_OP) continue;
                auto* bin = static_cast<const BinaryOpExpr*>(conj);
                if (bin->op == BinaryOpKind::EQ) {
                    if (bin->left->kind == ExpressionKind::PROPERTY_LOOKUP && bin->right->kind == ExpressionKind::PROPERTY_LOOKUP) {
                        auto* pl_l = static_cast<const PropertyLookupExpr*>(bin->left.get());
                        auto* pl_r = static_cast<const PropertyLookupExpr*>(bin->right.get());
                        if (pl_l->property == pl_r->property && pl_l->variable != pl_r->variable) {
                            eq_props[make_key(pl_l->variable, pl_r->variable)] = pl_l->property;
                        }
                    }
                } else if (bin->op == BinaryOpKind::NE) {
                    if (bin->left->kind == ExpressionKind::PROPERTY_LOOKUP && bin->right->kind == ExpressionKind::PROPERTY_LOOKUP) {
                        auto* pl_l = static_cast<const PropertyLookupExpr*>(bin->left.get());
                        auto* pl_r = static_cast<const PropertyLookupExpr*>(bin->right.get());
                        if (pl_l->property == pl_r->property && pl_l->variable != pl_r->variable) {
                            ne_props[make_key(pl_l->variable, pl_r->variable)] = pl_l->property;
                        }
                    }
                }
            }
            
            for (const auto& [pair_key, x] : eq_props) {
                auto it = ne_props.find(pair_key);
                if (it != ne_props.end()) {
                    std::string y = it->second;
                    size_t comma = pair_key.find(',');
                    std::string u = pair_key.substr(0, comma);
                    std::string v = pair_key.substr(comma + 1);
                    
                    std::string label_u = var_labels[u];
                    std::string label_v = var_labels[v];
                    
                    std::string common_label = "";
                    if (!label_u.empty()) common_label = label_u;
                    else if (!label_v.empty()) common_label = label_v;
                    
                    if (!common_label.empty()) {
                        fd_list.push_back({common_label, x, y});
                    }
                }
            }
        } catch (...) {
            // Ignore
        }
    }
    return fd_list;
}

void FunctionalDependencyPruner::functional_dependency_pass(GqlQuery& query) {
    auto fds = find_functional_dependencies();
    if (fds.empty()) return;
    
    // Map variable to its literal label in matches
    std::unordered_map<std::string, std::string> query_var_labels;
    for (const auto& match : query.matches) {
        for (const auto& node : match.pattern.nodes) {
            if (!node.variable.empty()) {
                if (node.label_expr && node.label_expr->kind == LabelExprKind::LITERAL) {
                    query_var_labels[node.variable] = node.label_expr->name;
                }
            }
        }
    }
    
    // Map variable to grouping property keys
    std::unordered_map<std::string, std::set<std::string>> grouping_props;
    for (const auto& item : query.returns) {
        if (item.expr && item.expr->kind != ExpressionKind::AGGREGATION) {
            if (item.expr->kind == ExpressionKind::PROPERTY_LOOKUP) {
                auto* pl = static_cast<const PropertyLookupExpr*>(item.expr.get());
                grouping_props[pl->variable].insert(pl->property);
            }
        }
    }
    
    // Helper to rewrite expressions recursively
    auto rewrite_aggregates = [&](auto& self, std::unique_ptr<Expression>& expr) -> void {
        if (!expr) return;
        if (expr->kind == ExpressionKind::AGGREGATION) {
            auto* agg = static_cast<AggregateExpr*>(expr.get());
            if (agg->fn_kind == AggregateKind::COUNT && agg->expr && agg->expr->kind == ExpressionKind::PROPERTY_LOOKUP) {
                auto* pl = static_cast<PropertyLookupExpr*>(agg->expr.get());
                std::string var = pl->variable;
                std::string y = pl->property;
                std::string label = query_var_labels[var];
                if (!label.empty()) {
                    const auto& g_keys = grouping_props[var];
                    for (const auto& x : g_keys) {
                        bool found = false;
                        for (const auto& fd : fds) {
                            if (fd.label == label && fd.x == x && fd.y == y) {
                                found = true;
                                break;
                            }
                        }
                        if (found) {
                            agg->expr = nullptr; // Rewrite count(b.y) to count(*)
                            break;
                        }
                    }
                }
            }
        } else if (expr->kind == ExpressionKind::UNARY_OP) {
            auto* un = static_cast<UnaryOpExpr*>(expr.get());
            self(self, un->expr);
        } else if (expr->kind == ExpressionKind::BINARY_OP) {
            auto* bin = static_cast<BinaryOpExpr*>(expr.get());
            self(self, bin->left);
            self(self, bin->right);
        }
    };
    
    // Run rewriter on query expressions
    for (auto& item : query.returns) {
        rewrite_aggregates(rewrite_aggregates, item.expr);
    }
    for (auto& spec : query.order_by) {
        rewrite_aggregates(rewrite_aggregates, spec.expr);
    }
    rewrite_aggregates(rewrite_aggregates, query.where_expr);
}

} // namespace ragedb::gql
