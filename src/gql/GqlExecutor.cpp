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

#include "GqlExecutor.h"
#include "GqlValue.h"
#include "GqlWriter.h"
#include "GqlCatalog.h"
#include "GqlVirtualCatalog.h"
#include "GqlTypechecker.h"
#include "Join.h"
#include "GqlParser.h"
#include "GqlOptimizer.h"
#include "GqlQueryCache.h"
#include "executor/FactorNode.h"
#include "executor/JoinHelpers.h"
#include "executor/ExpressionEvaluator.h"
#include "executor/PathTraverser.h"
#include "executor/ProjectionPruner.h"
#include "executor/StarJoinRewriter.h"
#include "executor/PlanBuilder.h"
#include "executor/HoneycombExecutor.h"
#include "executor/LftjExecutor.h"

#include <sstream>
#include <unordered_set>
#include <unordered_map>
#include <set>
#include <map>
#include <algorithm>
#include <cstdlib>
#include <chrono>
#include <seastar/core/when_all.hh>

namespace ragedb::gql {

/**
 * @brief Represents a grouped set of rows.
 */
struct GqlGroup {
    GqlRow representative;
    std::vector<GqlRow> rows;
};

/**
 * @brief Custom vector comparator for GqlValues to group mapping.
 */
struct GqlValueVectorLess {
    bool operator()(const std::vector<GqlValue>& a, const std::vector<GqlValue>& b) const {
        if (a.size() != b.size()) return a.size() < b.size();
        for (size_t i = 0; i < a.size(); ++i) {
            int cmp = compare_gql_values(a[i], b[i]);
            if (cmp != 0) return cmp < 0;
        }
        return false;
    }
};

/**
 * @brief Helper sorting structure representing a row mapped to its sort keys.
 */
struct RowSortKey {
    std::vector<GqlValue> keys;
    GqlRow row;
};

/**
 * @brief Main execution entry point for the GQL engine.
 */
struct QueryResult {
    std::vector<std::string> column_names;
    std::vector<std::vector<GqlValue>> rows;
    bool is_sorted = false;
};

/**
 * @brief Sorts the combined query results.
 */
static void sort_combined_result(QueryResult& res, const std::vector<SortSpec>& order_by, std::optional<size_t> limit = std::nullopt) {
    if (order_by.empty()) return;
    
    auto comp = [&res, &order_by](const std::vector<GqlValue>& a, const std::vector<GqlValue>& b) {
        for (const auto& spec : order_by) {
            size_t col_idx = res.column_names.size();
            if (spec.expr->kind == ExpressionKind::VARIABLE) {
                auto* ve = static_cast<const VariableExpr*>(spec.expr.get());
                for (size_t i = 0; i < res.column_names.size(); ++i) {
                    if (res.column_names[i] == ve->name) {
                        col_idx = i;
                        break;
                    }
                }
            } else if (spec.expr->kind == ExpressionKind::PROPERTY_LOOKUP) {
                auto* pl = static_cast<const PropertyLookupExpr*>(spec.expr.get());
                std::string col_name = pl->variable + "." + pl->property;
                for (size_t i = 0; i < res.column_names.size(); ++i) {
                    if (res.column_names[i] == col_name) {
                        col_idx = i;
                        break;
                    }
                }
            }
            
            if (col_idx < res.column_names.size()) {
                int cmp = compare_gql_values(a[col_idx], b[col_idx]);
                if (cmp != 0) {
                    return spec.ascending ? (cmp < 0) : (cmp > 0);
                }
            }
        }
        return false;
    };

    if (limit && *limit < res.rows.size()) {
        std::partial_sort(res.rows.begin(), res.rows.begin() + *limit, res.rows.end(), comp);
        res.rows.resize(*limit);
    } else {
        std::stable_sort(res.rows.begin(), res.rows.end(), comp);
    }
}

struct GqlRowOuterVarsLess {
    std::set<std::string> outer_vars;
    bool operator()(const GqlRow& lhs, const GqlRow& rhs) const {
        for (const auto& var : outer_vars) {
            auto it_l = lhs.bindings.find(var);
            auto it_r = rhs.bindings.find(var);
            GqlValue val_l = (it_l != lhs.bindings.end()) ? it_l->second : GqlValue();
            GqlValue val_r = (it_r != rhs.bindings.end()) ? it_r->second : GqlValue();
            int cmp = compare_gql_values(val_l, val_r);
            if (cmp < 0) return true;
            if (cmp > 0) return false;
        }
        return false;
    }
};

static seastar::future<QueryResult> execute_query_internal(ragedb::Graph& graph, std::shared_ptr<GqlQuery> query_ptr) {
    if (query_ptr->no_op) {
        QueryResult query_res;
        for (size_t i = 0; i < query_ptr->returns.size(); ++i) {
            const auto& item = query_ptr->returns[i];
            std::string key;
            if (item.alias) {
                key = *item.alias;
            } else {
                if (item.expr->kind == ExpressionKind::PROPERTY_LOOKUP) {
                    auto* pl = static_cast<const PropertyLookupExpr*>(item.expr.get());
                    key = pl->variable + "." + pl->property;
                } else if (item.expr->kind == ExpressionKind::VARIABLE) {
                    auto* ve = static_cast<const VariableExpr*>(item.expr.get());
                    key = ve->name;
                } else if (item.expr->kind == ExpressionKind::AGGREGATION) {
                    auto* ae = static_cast<const AggregateExpr*>(item.expr.get());
                    std::string fn_name;
                    if (ae->fn_kind == AggregateKind::COUNT) fn_name = "count";
                    else if (ae->fn_kind == AggregateKind::SUM) fn_name = "sum";
                    else if (ae->fn_kind == AggregateKind::AVG) fn_name = "avg";
                    else if (ae->fn_kind == AggregateKind::MIN) fn_name = "min";
                    else fn_name = "max";

                    if (!ae->expr) {
                        key = fn_name + "(*)";
                    } else if (ae->expr->kind == ExpressionKind::PROPERTY_LOOKUP) {
                        auto* pl = static_cast<const PropertyLookupExpr*>(ae->expr.get());
                        key = fn_name + "(" + pl->variable + "." + pl->property + ")";
                    } else if (ae->expr->kind == ExpressionKind::VARIABLE) {
                        auto* ve = static_cast<const VariableExpr*>(ae->expr.get());
                        key = fn_name + "(" + ve->name + ")";
                    } else {
                        key = fn_name + "(expr)";
                    }
                } else {
                    key = "column_" + std::to_string(i);
                }
            }
            query_res.column_names.push_back(key);
        }
        return seastar::make_ready_future<QueryResult>(std::move(query_res));
    }

    // 1. Handle Set Operations (UNION, UNION ALL, INTERSECT, INTERSECT ALL)
    if (query_ptr->kind != QueryKind::SINGLE) {
        // Release ownership of subqueries to execute them separately
        auto left_ptr = std::shared_ptr<GqlQuery>(query_ptr->left.release());
        auto right_ptr = std::shared_ptr<GqlQuery>(query_ptr->right.release());

        // Extract left and right plan nodes if profiling is enabled
        std::shared_ptr<PlanNode> left_node = nullptr;
        std::shared_ptr<PlanNode> right_node = nullptr;
        if (query_ptr->profile && query_ptr->plan_root) {
            if (query_ptr->plan_root->children.size() > 0) {
                left_node = query_ptr->plan_root->children[0];
            }
            if (query_ptr->plan_root->children.size() > 1) {
                right_node = query_ptr->plan_root->children[1];
            }
        }

        // Set up profiling contexts for subqueries
        if (query_ptr->profile) {
            left_ptr->profile = true;
            left_ptr->plan_root = left_node;
            index_plan_nodes(left_node, left_ptr->plan_nodes);

            right_ptr->profile = true;
            right_ptr->plan_root = right_node;
            index_plan_nodes(right_node, right_ptr->plan_nodes);
        }

        // Start timing the set operation
        auto start = std::chrono::steady_clock::now();
        
        // Execute the left subquery first
        return execute_query_internal(graph, left_ptr)
        .then([&graph, right_ptr, query_ptr, start](QueryResult left_res) {
            // Then execute the right subquery
            return execute_query_internal(graph, right_ptr)
            .then([left_res = std::move(left_res), query_ptr, start](QueryResult right_res) {
                // Ensure column structures match
                if (left_res.column_names.size() != right_res.column_names.size()) {
                    throw std::runtime_error("All subqueries in a GQL Set operation must return the same number of columns");
                }

                QueryResult combined_res;
                combined_res.column_names = left_res.column_names;

                // Merge results based on the set operation type
                if (query_ptr->kind == QueryKind::UNION_ALL) {
                    // UNION ALL: Concatenate all rows
                    combined_res.rows = std::move(left_res.rows);
                    for (auto& r : right_res.rows) {
                        combined_res.rows.push_back(std::move(r));
                    }
                } else if (query_ptr->kind == QueryKind::UNION) {
                    // UNION: Concatenate and dedup rows using a set
                    std::set<std::vector<GqlValue>, GqlValueVectorLess> seen;
                    for (auto& r : left_res.rows) {
                        if (seen.insert(r).second) {
                            combined_res.rows.push_back(std::move(r));
                        }
                    }
                    for (auto& r : right_res.rows) {
                        if (seen.insert(r).second) {
                            combined_res.rows.push_back(std::move(r));
                        }
                    }
                } else if (query_ptr->kind == QueryKind::INTERSECT) {
                    // INTERSECT: Retain unique rows present in both left and right results
                    std::set<std::vector<GqlValue>, GqlValueVectorLess> left_set;
                    for (const auto& r : left_res.rows) {
                        left_set.insert(r);
                    }
                    std::set<std::vector<GqlValue>, GqlValueVectorLess> seen_intersection;
                    for (auto& r : right_res.rows) {
                        if (left_set.count(r)) {
                            if (seen_intersection.insert(r).second) {
                                combined_res.rows.push_back(std::move(r));
                            }
                        }
                    }
                } else if (query_ptr->kind == QueryKind::INTERSECT_ALL) {
                    // INTERSECT ALL: Retain rows present in both, respecting multi-set count limits
                    std::map<std::vector<GqlValue>, int64_t, GqlValueVectorLess> left_counts;
                    for (const auto& r : left_res.rows) {
                        left_counts[r]++;
                    }
                    for (auto& r : right_res.rows) {
                        auto it = left_counts.find(r);
                        if (it != left_counts.end() && it->second > 0) {
                            combined_res.rows.push_back(std::move(r));
                            it->second--;
                        }
                    }
                }

                // Record actual row count and duration for set operation plan node
                if (query_ptr->profile && query_ptr->plan_root) {
                    auto end = std::chrono::steady_clock::now();
                    query_ptr->plan_root->actual_rows = combined_res.rows.size();
                    query_ptr->plan_root->time_ms = std::chrono::duration<double, std::milli>(end - start).count();
                }

                return combined_res;
            });
        });
    }

    // 2. Detect if the query contains aggregate functions (COUNT, SUM, AVG, MIN, MAX)
    bool query_contains_aggregates = false;
    for (const auto& item : query_ptr->returns) {
        if (has_aggregates(item.expr.get())) {
            query_contains_aggregates = true;
            break;
        }
    }
    if (!query_contains_aggregates) {
        for (const auto& spec : query_ptr->order_by) {
            if (has_aggregates(spec.expr.get())) {
                query_contains_aggregates = true;
                break;
            }
        }
    }

    // Determine if we can push limit evaluation directly into the traversal
    size_t limit_val = 0;
    if (query_ptr->limit.has_value() && query_ptr->order_by.empty() && !query_contains_aggregates) {
        limit_val = *query_ptr->limit;
    }

    // 3. Build ProjectionPruner to avoid reading properties that are never accessed
    ProjectionPruner pruner;

    // Collect properties accessed in RETURN expressions
    for (const auto& item : query_ptr->returns) {
        collect_accessed_properties(item.expr.get(), pruner.accessed_props, pruner.whole_objects);
    }

    // Collect properties accessed in ORDER BY specs
    for (const auto& spec : query_ptr->order_by) {
        collect_accessed_properties(spec.expr.get(), pruner.accessed_props, pruner.whole_objects);
    }

    // Collect properties accessed in WHERE filter
    if (query_ptr->where_expr) {
        collect_accessed_properties(query_ptr->where_expr.get(), pruner.accessed_props, pruner.whole_objects);
    }

    // Collect properties accessed/modified in WRITE operations
    for (const auto& w : query_ptr->writes) {
        if (w.type == WriteOp::Type::INSERT) {
            for (const auto& n : w.insert_pattern.nodes) {
                if (!n.variable.empty()) {
                    pruner.whole_objects.insert(n.variable);
                }
            }
            for (const auto& e : w.insert_pattern.edges) {
                if (!e.variable.empty()) {
                    pruner.whole_objects.insert(e.variable);
                }
            }
        } else if (w.type == WriteOp::Type::SET) {
            if (!w.set_var.empty()) {
                pruner.whole_objects.insert(w.set_var);
            }
            collect_accessed_properties(w.set_expr.get(), pruner.accessed_props, pruner.whole_objects);
        } else if (w.type == WriteOp::Type::REMOVE) {
            if (!w.remove_var.empty()) {
                pruner.whole_objects.insert(w.remove_var);
            }
        } else if (w.type == WriteOp::Type::DELETE_OP) {
            if (!w.delete_var.empty()) {
                pruner.whole_objects.insert(w.delete_var);
            }
        }
    }

    // Collect properties referenced in matching patterns (filters/indexed properties)
    for (const auto& match : query_ptr->matches) {
        for (const auto& n : match.pattern.nodes) {
            if (!n.variable.empty()) {
                for (const auto& [prop, val] : n.properties) {
                    pruner.accessed_props[n.variable].insert(prop);
                }
                for (const auto& filter : n.property_filters) {
                    pruner.accessed_props[n.variable].insert(filter.property);
                }
            }
        }
        for (const auto& e : match.pattern.edges) {
            if (!e.variable.empty()) {
                for (const auto& [prop, val] : e.properties) {
                    pruner.accessed_props[e.variable].insert(prop);
                }
                for (const auto& filter : e.property_filters) {
                    pruner.accessed_props[e.variable].insert(filter.property);
                }
            }
        }
    }

    // 4. Optimize sorting if the order matches the start node traversal index order
    std::string sort_property = "";
    bool sort_ascending = true;
    bool sort_by_id = false;

    if (query_ptr->order_by.size() == 1 && !query_ptr->matches.empty()) {
        const auto& spec = query_ptr->order_by[0];
        std::string var_name = "";
        std::string prop_name = "";
        bool ok = false;
        
        if (spec.expr->kind == ExpressionKind::PROPERTY_LOOKUP) {
            auto* pl = static_cast<const PropertyLookupExpr*>(spec.expr.get());
            var_name = pl->variable;
            prop_name = pl->property;
            ok = true;
        } else if (spec.expr->kind == ExpressionKind::VARIABLE) {
            auto* ve = static_cast<const VariableExpr*>(spec.expr.get());
            var_name = ve->name;
            ok = true;
            sort_by_id = true;
        }
        
        if (ok) {
            const auto& first_match = query_ptr->matches[0];
            if (!first_match.pattern.nodes.empty()) {
                std::string start_var = first_match.pattern.nodes[0].variable;
                if (start_var == var_name || (start_var.empty() && var_name == "_n_0_user_empty")) {
                    sort_property = prop_name;
                    sort_ascending = spec.ascending;
                }
            }
        }
    }

    // 5. Execute matching patterns recursively using sharded traversal
    IntermediateResult initial_res(std::vector<GqlRow>{ GqlRow{} });
    auto is_sorted_shared = std::make_shared<bool>(false);

    auto is_query_cyclic = [](const std::vector<MatchStatement>& matches) -> bool {
        std::unordered_map<std::string, std::vector<std::pair<std::string, std::string>>> adj_list;
        std::set<std::string> node_vars;
        
        for (size_t m_idx = 0; m_idx < matches.size(); ++m_idx) {
            const auto& match = matches[m_idx];
            if (match.is_search) continue;
            const auto& pattern = match.pattern;
            for (size_t i = 0; i < pattern.edges.size(); ++i) {
                std::string u = pattern.nodes[i].variable;
                std::string v = pattern.nodes[i+1].variable;
                std::string e = pattern.edges[i].variable;
                if (u.empty()) u = "_n_anon_" + std::to_string(m_idx) + "_" + std::to_string(i);
                if (v.empty()) v = "_n_anon_" + std::to_string(m_idx) + "_" + std::to_string(i+1);
                if (e.empty()) e = "_e_anon_" + std::to_string(m_idx) + "_" + std::to_string(i);
                
                node_vars.insert(u);
                node_vars.insert(v);
                adj_list[u].push_back({v, e});
                adj_list[v].push_back({u, e});
            }
        }
        
        std::unordered_set<std::string> visited;
        std::function<bool(const std::string&, const std::string&)> dfs = [&](const std::string& curr, const std::string& parent_edge) -> bool {
            visited.insert(curr);
            for (const auto& neighbor_info : adj_list[curr]) {
                std::string neighbor = neighbor_info.first;
                std::string edge_var = neighbor_info.second;
                if (edge_var == parent_edge) continue;
                if (visited.count(neighbor)) {
                    return true;
                }
                if (dfs(neighbor, edge_var)) {
                    return true;
                }
            }
            return false;
        };
        
        for (const auto& node : node_vars) {
            if (!visited.count(node)) {
                if (dfs(node, "")) return true;
            }
        }
        return false;
    };

    bool use_honeycomb = false;
    bool use_lftj = false;
    if (is_query_cyclic(query_ptr->matches)) {
        uint64_t total_rels = 0;
        for (const auto& match : query_ptr->matches) {
            if (match.is_search) continue;
            for (const auto& edge : match.pattern.edges) {
                std::string rel_type = "";
                if (edge.label_expr && edge.label_expr->kind == LabelExprKind::LITERAL) {
                    rel_type = edge.label_expr->name;
                }
                if (!rel_type.empty()) {
                    total_rels += graph.shard.local().RelationshipCount(rel_type);
                } else {
                    total_rels += graph.shard.local().AllRelationshipsCount();
                }
            }
        }
        if (GqlExecutor::force_enable_honeycomb) {
            use_honeycomb = true;
        } else if (GqlExecutor::force_enable_lftj) {
            use_lftj = true;
        } else {
            if (total_rels > 10000) {
                if (!GqlExecutor::force_disable_honeycomb) {
                    use_honeycomb = true;
                } else if (!GqlExecutor::force_disable_lftj) {
                    use_lftj = true;
                }
            } else {
                if (!GqlExecutor::force_disable_lftj) {
                    use_lftj = true;
                } else if (!GqlExecutor::force_disable_honeycomb) {
                    use_honeycomb = true;
                }
            }
        }
    }

    auto hc_start = std::chrono::steady_clock::now();
    auto matched_fut = [&]() {
        if (use_honeycomb) {
            return HoneycombExecutor::execute(graph, query_ptr->matches, limit_val)
            .then([query_ptr, hc_start](IntermediateResult res) {
                if (query_ptr && query_ptr->profile) {
                    auto end = std::chrono::steady_clock::now();
                    auto node = query_ptr->plan_nodes["honeycomb_join"];
                    if (node) {
                        node->actual_rows = res.rows.size();
                        node->time_ms = std::chrono::duration<double, std::milli>(end - hc_start).count();
                    }
                }
                return res;
            });
        } else if (use_lftj) {
            return LftjExecutor::execute(graph, query_ptr->matches, limit_val)
            .then([query_ptr, hc_start](IntermediateResult res) {
                if (query_ptr && query_ptr->profile) {
                    auto end = std::chrono::steady_clock::now();
                    auto node = query_ptr->plan_nodes["lftj_join"];
                    if (node) {
                        node->actual_rows = res.rows.size();
                        node->time_ms = std::chrono::duration<double, std::milli>(end - hc_start).count();
                    }
                }
                return res;
            });
        } else {
            return execute_match_chain_factorized(graph, query_ptr->matches, 0, std::move(initial_res), limit_val, pruner, sort_property, sort_ascending, sort_by_id, query_ptr);
        }
    }();

    return matched_fut

    .then([&graph, query_ptr, is_sorted_shared](IntermediateResult matched_res) {
        *is_sorted_shared = matched_res.is_sorted;
        matched_res.ensure_flat();
        std::vector<GqlRow> matched_rows = std::move(matched_res.rows);
        const auto& query = *query_ptr;

        // 6. Enforce DIFFERENT EDGES match mode globally across joined rows (GQL semantics)
        std::set<std::string> diff_edge_vars;
        for (const auto& stmt : query.matches) {
            if (stmt.match_mode == MatchMode::DIFFERENT_EDGES) {
                for (const auto& edge : stmt.pattern.edges) {
                    if (!edge.variable.empty() && edge.variable[0] != '_') {
                        diff_edge_vars.insert(edge.variable);
                    }
                }
            }
        }

        if (!diff_edge_vars.empty()) {
            std::vector<GqlRow> edge_filtered_rows;
            for (auto& row : matched_rows) {
                std::vector<uint64_t> rel_ids;
                for (const auto& var : diff_edge_vars) {
                    auto it = row.bindings.find(var);
                    if (it != row.bindings.end()) {
                        const auto& val = it->second;
                        if (val.type == GqlValue::RELATIONSHIP) {
                            rel_ids.push_back(val.relationship->getId());
                        } else if (val.type == GqlValue::RELATIONSHIP_LIST) {
                            for (const auto& r : *val.relationship_list) {
                                rel_ids.push_back(r.getId());
                                }
                        } else if (val.type == GqlValue::PATH) {
                            for (const auto& r : val.path->GetRelationships()) {
                                rel_ids.push_back(r.getId());
                            }
                        }
                    }
                }
                std::set<uint64_t> unique_rels(rel_ids.begin(), rel_ids.end());
                if (unique_rels.size() == rel_ids.size()) {
                    edge_filtered_rows.push_back(std::move(row));
                }
            }
            matched_rows = std::move(edge_filtered_rows);
        }

        // 7. Evaluate GQL WHERE clause filter expression on match results
        std::vector<GqlRow> filtered_rows;
        auto filter_start = std::chrono::steady_clock::now();
        for (auto& row : matched_rows) {
            if (!query.where_expr || evaluate_expression(row, query.where_expr.get()).is_truthy()) {
                filtered_rows.push_back(std::move(row));
            }
        }

        // Record profile timing for filter operator
        if (query_ptr->profile && query.where_expr) {
            auto filter_end = std::chrono::steady_clock::now();
            auto node = query_ptr->plan_nodes["filter"];
            if (node) {
                node->actual_rows = filtered_rows.size();
                node->time_ms = std::chrono::duration<double, std::milli>(filter_end - filter_start).count();
            }
        }

        // Deduplicate rows if querying with unnested subqueries
        if (query.has_unnested_subquery && !query.outer_vars.empty()) {
            std::vector<GqlRow> deduped_rows;
            std::set<GqlRow, GqlRowOuterVarsLess> seen((GqlRowOuterVarsLess{query.outer_vars}));
            for (auto& row : filtered_rows) {
                if (seen.insert(row).second) {
                    deduped_rows.push_back(std::move(row));
                }
            }
            filtered_rows = std::move(deduped_rows);
        }


        if (query.writes.empty()) {
            return seastar::make_ready_future<std::vector<GqlRow>>(std::move(filtered_rows));
        }

        auto write_start = std::chrono::steady_clock::now();
        std::vector<seastar::future<GqlRow>> futs;
        for (auto& row : filtered_rows) {
            futs.push_back(execute_writes_for_row(graph, query_ptr, 0, std::move(row)));
        }
        return seastar::when_all_succeed(futs.begin(), futs.end())
        .then([query_ptr, write_start](std::vector<GqlRow> written_rows) {
            if (query_ptr->profile) {
                auto write_end = std::chrono::steady_clock::now();
                auto node = query_ptr->plan_nodes["write"];
                if (node) {
                    node->actual_rows = written_rows.size();
                    node->time_ms = std::chrono::duration<double, std::milli>(write_end - write_start).count();
                }
            }
            return written_rows;
        });
    })
    .then([query_ptr, is_sorted_shared](std::vector<GqlRow> written_rows) {
        const auto& query = *query_ptr;
        std::vector<GqlRow> filtered_rows = std::move(written_rows);

        bool contains_aggregates = false;
        for (const auto& item : query.returns) {
            if (has_aggregates(item.expr.get())) {
                contains_aggregates = true;
                break;
            }
        }
        if (!contains_aggregates) {
            for (const auto& spec : query.order_by) {
                if (has_aggregates(spec.expr.get())) {
                    contains_aggregates = true;
                    break;
                }
            }
        }

        QueryResult query_res;
        query_res.is_sorted = *is_sorted_shared;

        for (size_t i = 0; i < query.returns.size(); ++i) {
            const auto& item = query.returns[i];
            std::string key;
            if (item.alias) {
                key = *item.alias;
            } else {
                if (item.expr->kind == ExpressionKind::PROPERTY_LOOKUP) {
                    auto* pl = static_cast<const PropertyLookupExpr*>(item.expr.get());
                    key = pl->variable + "." + pl->property;
                } else if (item.expr->kind == ExpressionKind::VARIABLE) {
                    auto* ve = static_cast<const VariableExpr*>(item.expr.get());
                    key = ve->name;
                } else if (item.expr->kind == ExpressionKind::AGGREGATION) {
                    auto* ae = static_cast<const AggregateExpr*>(item.expr.get());
                    std::string fn_name;
                    if (ae->fn_kind == AggregateKind::COUNT) fn_name = "count";
                    else if (ae->fn_kind == AggregateKind::SUM) fn_name = "sum";
                    else if (ae->fn_kind == AggregateKind::AVG) fn_name = "avg";
                    else if (ae->fn_kind == AggregateKind::MIN) fn_name = "min";
                    else fn_name = "max";

                    if (!ae->expr) {
                        key = fn_name + "(*)";
                    } else if (ae->expr->kind == ExpressionKind::PROPERTY_LOOKUP) {
                        auto* pl = static_cast<const PropertyLookupExpr*>(ae->expr.get());
                        key = fn_name + "(" + pl->variable + "." + pl->property + ")";
                    } else if (ae->expr->kind == ExpressionKind::VARIABLE) {
                        auto* ve = static_cast<const VariableExpr*>(ae->expr.get());
                        key = fn_name + "(" + ve->name + ")";
                    } else {
                        key = fn_name + "(expr)";
                    }
                } else {
                    key = "column_" + std::to_string(i);
                }
            }
            query_res.column_names.push_back(key);
        }

        std::unordered_set<std::string> seen_distinct;

        if (contains_aggregates) {
            auto agg_start = std::chrono::steady_clock::now();
            std::vector<const AggregateExpr*> aggregate_exprs;
            for (const auto& item : query.returns) {
                find_aggregates(item.expr.get(), aggregate_exprs);
            }
            for (const auto& spec : query.order_by) {
                find_aggregates(spec.expr.get(), aggregate_exprs);
            }

            std::set<std::string> group_variables;
            for (const auto& item : query.returns) {
                if (item.expr && !has_aggregates(item.expr.get())) {
                    if (item.expr->kind == ExpressionKind::VARIABLE) {
                        group_variables.insert(static_cast<const VariableExpr*>(item.expr.get())->name);
                    }
                }
            }

            std::vector<const Expression*> grouping_keys;
            for (const auto& item : query.returns) {
                if (item.expr && !has_aggregates(item.expr.get())) {
                    if (item.expr->kind == ExpressionKind::PROPERTY_LOOKUP) {
                        auto* pl = static_cast<const PropertyLookupExpr*>(item.expr.get());
                        if (group_variables.count(pl->variable)) {
                            continue;
                        }
                    }
                    grouping_keys.push_back(item.expr.get());
                }
            }

            std::map<std::vector<GqlValue>, GqlGroup, GqlValueVectorLess> groups;
            for (auto& row : filtered_rows) {
                std::vector<GqlValue> key;
                for (const auto* gk : grouping_keys) {
                    key.push_back(evaluate_expression(row, gk));
                }
                auto& group = groups[key];
                if (group.rows.empty()) {
                    group.representative = row;
                }
                group.rows.push_back(std::move(row));
            }

            if (groups.empty() && grouping_keys.empty()) {
                GqlGroup default_group;
                default_group.representative = GqlRow{};
                groups[{}] = default_group;
            }

            struct GroupSortKey {
                std::vector<GqlValue> sort_keys;
                std::vector<GqlValue> projected_row;
            };
            std::vector<GroupSortKey> sorted_groups;

            for (const auto& [key, group] : groups) {
                std::map<const AggregateExpr*, GqlValue> aggregate_results;
                for (const auto* agg : aggregate_exprs) {
                    if (agg->fn_kind == AggregateKind::COUNT) {
                        int64_t count = 0;
                        if (!agg->expr) {
                            count = static_cast<int64_t>(group.rows.size());
                        } else {
                            for (const auto& r : group.rows) {
                                GqlValue v = evaluate_expression(r, agg->expr.get());
                                if (v.type != GqlValue::NIL) {
                                    count++;
                                }
                            }
                        }
                        aggregate_results[agg] = GqlValue(count);
                    } else if (agg->fn_kind == AggregateKind::SUM || agg->fn_kind == AggregateKind::AVG) {
                        int64_t sum_int = 0;
                        double sum_double = 0.0;
                        bool has_double = false;
                        int64_t count = 0;

                        for (const auto& r : group.rows) {
                            GqlValue v = evaluate_expression(r, agg->expr.get());
                            if (v.type == GqlValue::PROPERTY) {
                                if (std::holds_alternative<int64_t>(v.property)) {
                                    if (has_double) sum_double += static_cast<double>(std::get<int64_t>(v.property));
                                    else sum_int += std::get<int64_t>(v.property);
                                    count++;
                                } else if (std::holds_alternative<double>(v.property)) {
                                    if (!has_double) {
                                        sum_double = static_cast<double>(sum_int);
                                        has_double = true;
                                    }
                                    sum_double += std::get<double>(v.property);
                                    count++;
                                }
                            }
                        }

                        if (agg->fn_kind == AggregateKind::SUM) {
                            if (count == 0) {
                                aggregate_results[agg] = GqlValue();
                            } else if (has_double) {
                                aggregate_results[agg] = GqlValue(sum_double);
                            } else {
                                aggregate_results[agg] = GqlValue(sum_int);
                            }
                        } else {
                            if (count == 0) {
                                aggregate_results[agg] = GqlValue();
                            } else if (has_double) {
                                aggregate_results[agg] = GqlValue(sum_double / static_cast<double>(count));
                            } else {
                                aggregate_results[agg] = GqlValue(static_cast<double>(sum_int) / static_cast<double>(count));
                            }
                        }
                    } else if (agg->fn_kind == AggregateKind::MIN) {
                        GqlValue min_val;
                        for (const auto& r : group.rows) {
                            GqlValue v = evaluate_expression(r, agg->expr.get());
                            if (v.type != GqlValue::NIL) {
                                if (min_val.type == GqlValue::NIL || compare_gql_values(v, min_val) < 0) {
                                    min_val = v;
                                }
                            }
                        }
                        aggregate_results[agg] = min_val;
                    } else if (agg->fn_kind == AggregateKind::MAX) {
                        GqlValue max_val;
                        for (const auto& r : group.rows) {
                            GqlValue v = evaluate_expression(r, agg->expr.get());
                            if (v.type != GqlValue::NIL) {
                                if (max_val.type == GqlValue::NIL || compare_gql_values(v, max_val) > 0) {
                                    max_val = v;
                                }
                            }
                        }
                        aggregate_results[agg] = max_val;
                    }
                }

                std::vector<GqlValue> projected;
                for (size_t i = 0; i < query.returns.size(); ++i) {
                    const auto& item = query.returns[i];
                    projected.push_back(evaluate_group_expression(group.representative, aggregate_results, item.expr.get()));
                }

                GroupSortKey gsk;
                gsk.projected_row = std::move(projected);
                for (const auto& spec : query.order_by) {
                    gsk.sort_keys.push_back(evaluate_group_expression(group.representative, aggregate_results, spec.expr.get()));
                }
                sorted_groups.push_back(std::move(gsk));
            }

            if (!query.order_by.empty()) {
                auto comp = [&query](const GroupSortKey& a, const GroupSortKey& b) {
                    for (size_t i = 0; i < query.order_by.size(); ++i) {
                        int cmp = compare_gql_values(a.sort_keys[i], b.sort_keys[i]);
                        if (cmp != 0) {
                            return query.order_by[i].ascending ? (cmp < 0) : (cmp > 0);
                        }
                    }
                    return false;
                };
                
                if (query.limit && *query.limit < sorted_groups.size()) {
                    std::partial_sort(sorted_groups.begin(), sorted_groups.begin() + *query.limit, sorted_groups.end(), comp);
                    sorted_groups.resize(*query.limit);
                } else {
                    std::stable_sort(sorted_groups.begin(), sorted_groups.end(), comp);
                }
            }

            if (query_ptr->profile) {
                auto agg_end = std::chrono::steady_clock::now();
                auto node = query_ptr->plan_nodes["aggregate"];
                if (node) {
                    node->actual_rows = sorted_groups.size();
                    node->time_ms = std::chrono::duration<double, std::milli>(agg_end - agg_start).count();
                }
            }

            for (const auto& gsk : sorted_groups) {
                if (query.distinct) {
                    std::string serialized_distinct;
                    for (const auto& v : gsk.projected_row) {
                        serialized_distinct += serialize_gql_value(v) + ",";
                    }
                    if (seen_distinct.insert(serialized_distinct).second) {
                        query_res.rows.push_back(gsk.projected_row);
                    }
                } else {
                    query_res.rows.push_back(gsk.projected_row);
                }
            }
        } else {
            if (!query.order_by.empty() && !query_res.is_sorted) {
                auto sort_start = std::chrono::steady_clock::now();
                std::vector<RowSortKey> sorted_keys;
                for (auto& row : filtered_rows) {
                    RowSortKey rk;
                    rk.row = row;
                    for (const auto& spec : query.order_by) {
                        rk.keys.push_back(evaluate_expression(row, spec.expr.get()));
                    }
                    sorted_keys.push_back(std::move(rk));
                }

                auto comp = [&query](const RowSortKey& a, const RowSortKey& b) {
                    for (size_t i = 0; i < query.order_by.size(); ++i) {
                        int cmp = compare_gql_values(a.keys[i], b.keys[i]);
                        if (cmp != 0) {
                            return query.order_by[i].ascending ? (cmp < 0) : (cmp > 0);
                        }
                    }
                    return false;
                };

                if (query.limit && *query.limit < sorted_keys.size()) {
                    std::partial_sort(sorted_keys.begin(), sorted_keys.begin() + *query.limit, sorted_keys.end(), comp);
                    sorted_keys.resize(*query.limit);
                } else {
                    std::stable_sort(sorted_keys.begin(), sorted_keys.end(), comp);
                }

                filtered_rows.clear();
                for (auto& rk : sorted_keys) {
                    filtered_rows.push_back(std::move(rk.row));
                }

                if (query_ptr->profile) {
                    auto sort_end = std::chrono::steady_clock::now();
                    auto node = query_ptr->plan_nodes["sort"];
                    if (node) {
                        node->actual_rows = filtered_rows.size();
                        node->time_ms = std::chrono::duration<double, std::milli>(sort_end - sort_start).count();
                    }
                }
            } else if (query.limit && *query.limit < filtered_rows.size()) {
                auto limit_start = std::chrono::steady_clock::now();
                filtered_rows.resize(*query.limit);
                if (query_ptr->profile) {
                    auto limit_end = std::chrono::steady_clock::now();
                    auto node = query_ptr->plan_nodes["limit"];
                    if (node) {
                        node->actual_rows = filtered_rows.size();
                        node->time_ms = std::chrono::duration<double, std::milli>(limit_end - limit_start).count();
                    }
                }
            }

            auto produce_start = std::chrono::steady_clock::now();
            for (const auto& row : filtered_rows) {
                std::vector<GqlValue> projected;
                for (size_t i = 0; i < query.returns.size(); ++i) {
                    const auto& item = query.returns[i];
                    projected.push_back(evaluate_expression(row, item.expr.get()));
                }

                if (query.distinct) {
                    std::string serialized_distinct;
                    for (const auto& v : projected) {
                        serialized_distinct += serialize_gql_value(v) + ",";
                    }
                    if (seen_distinct.insert(serialized_distinct).second) {
                        query_res.rows.push_back(std::move(projected));
                    }
                } else {
                    query_res.rows.push_back(std::move(projected));
                }
            }

            if (query_ptr->profile) {
                auto produce_end = std::chrono::steady_clock::now();
                auto node = query_ptr->plan_nodes["produce_results"];
                if (node) {
                    node->actual_rows = query_res.rows.size();
                    node->time_ms = std::chrono::duration<double, std::milli>(produce_end - produce_start).count();
                }
                if (query.distinct) {
                    auto dist_node = query_ptr->plan_nodes["distinct"];
                    if (dist_node) {
                        dist_node->actual_rows = query_res.rows.size();
                        dist_node->time_ms = std::chrono::duration<double, std::milli>(produce_end - produce_start).count();
                    }
                }
            }
        }

        if (query.limit && query_res.rows.size() > *query.limit) {
            query_res.rows.resize(*query.limit);
        }

        return seastar::make_ready_future<QueryResult>(std::move(query_res));
    });
}

static seastar::future<> validate_constraints(ragedb::Graph& graph) {
    const auto& constraints = GqlVirtualCatalog::local().get_constraints();
    if (constraints.empty()) {
        return seastar::make_ready_future<>();
    }
    
    seastar::future<> f = seastar::make_ready_future<>();
    for (const auto& [name, query_str] : constraints) {
        f = f.then([&graph, name = name, query_str = query_str] {
            try {
                auto constraint_query = GqlParser::parse(query_str);
                constraint_query.limit = 1;
                auto query_ptr = std::make_shared<GqlQuery>(std::move(constraint_query));
                return execute_query_internal(graph, query_ptr)
                .then([name](QueryResult res) {
                    if (!res.rows.empty()) {
                        throw std::runtime_error("Constraint violation: '" + name + "' failed");
                    }
                });
            } catch (...) {
                return seastar::make_exception_future<>(std::current_exception());
            }
        });
    }
    return f;
}


seastar::future<std::string> GqlExecutor::execute(ragedb::Graph& graph, GqlQuery query_val) {
    GqlTypechecker::typecheck(graph, query_val);

    if (query_val.explain) {
        auto plan = build_query_plan(graph, query_val);
        std::vector<std::vector<GqlValue>> plan_rows;
        flatten_plan_tree(plan, plan_rows, "", true);

        std::vector<std::string> column_names = { "Operator", "Details", "Outputs", "Cache" };

        std::string json_res = "[";
        bool first_row = true;
        for (const auto& row : plan_rows) {
            if (!first_row) json_res += ", ";
            json_res += "{";
            bool first_col = true;
            for (size_t i = 0; i < column_names.size() - 1; ++i) {
                if (!first_col) json_res += ", ";
                json_res += "\"" + column_names[i] + "\": " + serialize_gql_value(row[i]);
                first_col = false;
            }
            std::string cache_status = query_val.plan_cache_hit ? "\"HIT\"" : "\"MISS\"";
            json_res += ", \"Cache\": " + cache_status;
            json_res += "}";
            first_row = false;
        }
        json_res += "]";

        return seastar::make_ready_future<std::string>(json_res);
    }

    if (query_val.clear_cache) {
        return graph.shard.invoke_on_all([](Shard&) {
            GqlQueryCache::local().clear();
        }).then([] {
            return seastar::make_ready_future<std::string>("{\"status\": \"cache cleared\"}");
        });
    }

    if (query_val.schema_op.has_value()) {
        return GqlCatalog::execute_schema_op(graph, *query_val.schema_op);
    }

    auto query_ptr = std::make_shared<GqlQuery>(std::move(query_val));

    if (query_ptr->profile) {
        query_ptr->plan_root = build_query_plan(graph, *query_ptr);
        index_plan_nodes(query_ptr->plan_root, query_ptr->plan_nodes);
    }

    return execute_query_internal(graph, query_ptr)
    .then([&graph, query_ptr](QueryResult result) {
        if (!query_ptr->writes.empty()) {
            return validate_constraints(graph)
            .then([result = std::move(result)]() mutable {
                return result;
            });
        }
        return seastar::make_ready_future<QueryResult>(std::move(result));
    })
    .then([query_ptr](QueryResult result) {
        if (query_ptr->profile) {
            std::vector<std::vector<GqlValue>> plan_rows;
            flatten_plan_tree(query_ptr->plan_root, plan_rows, "", true);

            std::vector<std::string> column_names = { "Operator", "Details", "Outputs", "Actual Rows", "Time (ms)", "Cache" };

            std::string json_res = "[";
            bool first_row = true;
            for (const auto& row : plan_rows) {
                if (!first_row) json_res += ", ";
                json_res += "{";
                bool first_col = true;
                for (size_t i = 0; i < column_names.size() - 1; ++i) {
                    if (!first_col) json_res += ", ";
                    json_res += "\"" + column_names[i] + "\": " + serialize_gql_value(row[i]);
                    first_col = false;
                }
                std::string cache_status = query_ptr->plan_cache_hit ? "\"HIT\"" : "\"MISS\"";
                json_res += ", \"Cache\": " + cache_status;
                json_res += "}";
                first_row = false;
            }
            json_res += "]";

            return seastar::make_ready_future<std::string>(json_res);
        }

        const auto& query = *query_ptr;

        if (!query.order_by.empty() && !result.is_sorted) {
            sort_combined_result(result, query.order_by, query.limit);
        }

        if (query.limit && result.rows.size() > *query.limit) {
            result.rows.resize(*query.limit);
        }

        std::string json_res = "[";
        bool first_row = true;
        for (const auto& row : result.rows) {
            if (!first_row) json_res += ", ";
            json_res += "{";
            bool first_col = true;
            for (size_t i = 0; i < result.column_names.size(); ++i) {
                if (!first_col) json_res += ", ";
                json_res += "\"" + result.column_names[i] + "\": " + serialize_gql_value(row[i]);
                first_col = false;
            }
            json_res += "}";
            first_row = false;
        }
        json_res += "]";

        return seastar::make_ready_future<std::string>(json_res);
    });
}

static std::string trim(const std::string& str) {
    size_t first = str.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) return "";
    size_t last = str.find_last_not_of(" \t\r\n");
    return str.substr(first, (last - first + 1));
}

seastar::future<std::string> GqlExecutor::execute(ragedb::Graph& graph, const std::string& query_str) {
    std::string key = trim(query_str);
    if (key.empty()) {
        return seastar::make_exception_future<std::string>(std::runtime_error("Empty query"));
    }

    if (auto cached_opt = GqlQueryCache::local().get(key)) {
        GqlQuery cloned = std::move(*cached_opt);
        cloned.plan_cache_hit = true;
        return execute(graph, std::move(cloned));
    }

    try {
        auto query = GqlParser::parse(key);
        GqlOptimizer::optimize(graph, query);

        if (query.schema_op.has_value()) {
            return execute(graph, std::move(query))
            .then([&graph](std::string res) {
                return graph.shard.invoke_on_all([](Shard&) {
                    GqlQueryCache::local().clear();
                }).then([res = std::move(res)] {
                    return res;
                });
            });
        }

        if (query.clear_cache) {
            return execute(graph, std::move(query));
        }

        query.plan_cache_hit = false;
        GqlQueryCache::local().put(key, query);
        return execute(graph, std::move(query));
    } catch (...) {
        return seastar::make_exception_future<std::string>(std::current_exception());
    }
}

} // namespace ragedb::gql
