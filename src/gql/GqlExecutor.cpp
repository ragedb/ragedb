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
#include "GqlTypechecker.h"
#include "Join.h"
#include "executor/FactorNode.h"
#include "executor/JoinHelpers.h"
#include "executor/ExpressionEvaluator.h"
#include "executor/PathTraverser.h"
#include "executor/ProjectionPruner.h"
#include "executor/StarJoinRewriter.h"
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
    if (query_ptr->kind != QueryKind::SINGLE) {
        auto left_ptr = std::shared_ptr<GqlQuery>(query_ptr->left.release());
        auto right_ptr = std::shared_ptr<GqlQuery>(query_ptr->right.release());

        return execute_query_internal(graph, left_ptr)
        .then([&graph, right_ptr, query_ptr](QueryResult left_res) {
            return execute_query_internal(graph, right_ptr)
            .then([left_res = std::move(left_res), query_ptr](QueryResult right_res) {
                if (left_res.column_names.size() != right_res.column_names.size()) {
                    throw std::runtime_error("All subqueries in a GQL Set operation must return the same number of columns");
                }

                QueryResult combined_res;
                combined_res.column_names = left_res.column_names;

                if (query_ptr->kind == QueryKind::UNION_ALL) {
                    combined_res.rows = std::move(left_res.rows);
                    for (auto& r : right_res.rows) {
                        combined_res.rows.push_back(std::move(r));
                    }
                } else if (query_ptr->kind == QueryKind::UNION) {
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

                return combined_res;
            });
        });
    }

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

    size_t limit_val = 0;
    if (query_ptr->limit.has_value() && query_ptr->order_by.empty() && !query_contains_aggregates) {
        limit_val = *query_ptr->limit;
    }

    ProjectionPruner pruner;

    for (const auto& item : query_ptr->returns) {
        collect_accessed_properties(item.expr.get(), pruner.accessed_props, pruner.whole_objects);
    }

    for (const auto& spec : query_ptr->order_by) {
        collect_accessed_properties(spec.expr.get(), pruner.accessed_props, pruner.whole_objects);
    }

    if (query_ptr->where_expr) {
        collect_accessed_properties(query_ptr->where_expr.get(), pruner.accessed_props, pruner.whole_objects);
    }

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

    IntermediateResult initial_res(std::vector<GqlRow>{ GqlRow{} });
    auto is_sorted_shared = std::make_shared<bool>(false);

    return execute_match_chain_factorized(graph, query_ptr->matches, 0, std::move(initial_res), limit_val, pruner, sort_property, sort_ascending, sort_by_id)
    .then([&graph, query_ptr, is_sorted_shared](IntermediateResult matched_res) {
        *is_sorted_shared = matched_res.is_sorted;
        matched_res.ensure_flat();
        std::vector<GqlRow> matched_rows = std::move(matched_res.rows);
        const auto& query = *query_ptr;
        std::vector<GqlRow> filtered_rows;
        for (auto& row : matched_rows) {
            if (!query.where_expr || evaluate_expression(row, query.where_expr.get()).is_truthy()) {
                filtered_rows.push_back(std::move(row));
            }
        }

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

        std::vector<seastar::future<GqlRow>> futs;
        for (auto& row : filtered_rows) {
            futs.push_back(execute_writes_for_row(graph, query_ptr, 0, std::move(row)));
        }
        return seastar::when_all_succeed(futs.begin(), futs.end());
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
            } else if (query.limit && *query.limit < filtered_rows.size()) {
                filtered_rows.resize(*query.limit);
            }

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
        }

        if (query.limit && query_res.rows.size() > *query.limit) {
            query_res.rows.resize(*query.limit);
        }

        return seastar::make_ready_future<QueryResult>(std::move(query_res));
    });
}

seastar::future<std::string> GqlExecutor::execute(ragedb::Graph& graph, GqlQuery query_val) {
    GqlTypechecker::typecheck(graph, query_val);

    if (query_val.schema_op.has_value()) {
        return GqlCatalog::execute_schema_op(graph, *query_val.schema_op);
    }

    auto query_ptr = std::make_shared<GqlQuery>(std::move(query_val));

    return execute_query_internal(graph, query_ptr)
    .then([query_ptr](QueryResult result) {
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

} // namespace ragedb::gql
