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
#include "optimizer/OptimizerUtils.h"
#include "executor/FactorNode.h"
#include "executor/JoinHelpers.h"
#include "executor/ExpressionEvaluator.h"
#include "executor/PathTraverser.h"
#include "../graph/cache/WccCache.h"
#include "../graph/cache/TransitiveReachabilityCache.h"
#include "executor/ProjectionPruner.h"
#include "executor/StarJoinRewriter.h"
#include "executor/PlanBuilder.h"
#include "executor/HoneycombExecutor.h"
#include "executor/LftjExecutor.h"
#include "executor/ExecutorInternal.h"

#include <sstream>
#include <cmath>
#include <optional>
#include <unordered_set>
#include <unordered_map>
#include <set>
#include <map>
#include <algorithm>
#include <limits>
#include <cstdlib>
#include <chrono>
#include <seastar/core/when_all.hh>
#include <seastar/core/loop.hh>

namespace ragedb::gql {

/**
 * @brief Represents a grouped set of rows.
 */
struct GqlGroup {
    GqlRow representative;
    std::vector<GqlRow> rows;
};

/**
 * @brief Helper sorting structure representing a row mapped to its sort keys.
 */
struct RowSortKey {
    std::vector<GqlValue> keys;
    GqlRow row;
};

/**
 * @brief Sorts the combined query results according to GQL ORDER BY specification.
 * @param res The QueryResult containing column names and rows.
 * @param order_by The sorting specifications.
 * @param limit Optional limit constraint to restrict result size. Pass the page window (offset + limit),
 *        not the bare limit, when an OFFSET follows -- the sort prunes, and the skipped rows are needed.
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
        std::partial_sort(res.rows.begin(), res.rows.begin() + static_cast<std::ptrdiff_t>(*limit), res.rows.end(), comp);
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
/**
 * @brief Internally executes a parsed GQL query against the database graph,
 * managing joins, projections, filtering, and aggregation.
 * @param graph The database graph instance.
 * @param query_ptr Shared pointer to the GQL query.
 * @return A future resolving to a QueryResult containing columns and values.
 */
/**
 * @brief Derive the output column name for a RETURN item. The single source of truth for column
 *        naming: query_result_to_rows turns column names into next-segment bindings in WITH
 *        pipelines, so every path that names columns must agree.
 */
std::string return_item_column_name(const ReturnItem& item, size_t index) {
    if (item.alias) return *item.alias;
    if (item.expr->kind == ExpressionKind::PROPERTY_LOOKUP) {
        auto* pl = static_cast<const PropertyLookupExpr*>(item.expr.get());
        return pl->variable + "." + pl->property;
    }
    if (item.expr->kind == ExpressionKind::VARIABLE) {
        return static_cast<const VariableExpr*>(item.expr.get())->name;
    }
    if (item.expr->kind == ExpressionKind::AGGREGATION) {
        auto* ae = static_cast<const AggregateExpr*>(item.expr.get());
        std::string fn = ae->fn_kind == AggregateKind::COUNT ? "count"
            : ae->fn_kind == AggregateKind::SUM ? "sum"
            : ae->fn_kind == AggregateKind::AVG ? "avg"
            : ae->fn_kind == AggregateKind::MIN ? "min"
            : ae->fn_kind == AggregateKind::COLLECT ? "collect_list"
            : ae->fn_kind == AggregateKind::STDDEV_POP ? "stddev_pop"
            : ae->fn_kind == AggregateKind::STDDEV_SAMP ? "stddev_samp"
            : ae->fn_kind == AggregateKind::PERCENTILE_CONT ? "percentile_cont"
            : ae->fn_kind == AggregateKind::PERCENTILE_DISC ? "percentile_disc" : "max";
        if (!ae->expr) return fn + "(*)";
        if (ae->expr->kind == ExpressionKind::PROPERTY_LOOKUP) {
            auto* pl = static_cast<const PropertyLookupExpr*>(ae->expr.get());
            return fn + "(" + pl->variable + "." + pl->property + ")";
        }
        if (ae->expr->kind == ExpressionKind::VARIABLE) {
            return fn + "(" + static_cast<const VariableExpr*>(ae->expr.get())->name + ")";
        }
        return fn + "(expr)";
    }
    return "column_" + std::to_string(index);
}

// Streaming fast-path aggregate machinery lives in executor/StreamingAggregates.cpp (see ExecutorInternal.h).

static seastar::future<QueryResult> execute_query_internal(ragedb::Graph& graph, std::shared_ptr<GqlQuery> query_ptr, std::optional<std::vector<GqlRow>> incoming = std::nullopt);

/**
 * @brief Convert a completed segment's QueryResult into input rows for the next WITH-pipeline
 *        segment: each projected output column becomes a binding under its column name. Takes the
 *        result by value and moves every cell, so the pipe holds one copy of the data, not two.
 */
static std::vector<GqlRow> query_result_to_rows(QueryResult res) {
    std::vector<GqlRow> rows;
    rows.reserve(res.rows.size());
    for (auto& r : res.rows) {
        GqlRow gr;
        for (size_t i = 0; i < res.column_names.size() && i < r.size(); ++i) {
            gr.bindings[res.column_names[i]] = std::move(r[i]);
        }
        rows.push_back(std::move(gr));
    }
    return rows;
}

/**
 * @brief Execute WITH-pipeline segments [idx..], piping each one's projected rows into the next, and
 *        return the rows to feed into the final (RETURN) segment. An empty segment still pipes its
 *        (empty) rows onward: a later ungrouped aggregate over zero rows must yield one row (count 0).
 */
static seastar::future<std::vector<GqlRow>> execute_with_segments(
        ragedb::Graph& graph, std::shared_ptr<GqlQuery> query_ptr, size_t idx,
        std::optional<std::vector<GqlRow>> incoming) {
    if (idx >= query_ptr->with_segments.size()) {
        return seastar::make_ready_future<std::vector<GqlRow>>(
            incoming.has_value() ? std::move(*incoming) : std::vector<GqlRow>{});
    }
    auto seg = query_ptr->with_segments[idx];
    return execute_query_internal(graph, seg, std::move(incoming)).then(
        [&graph, query_ptr, idx](QueryResult res) {
            std::vector<GqlRow> rows = query_result_to_rows(std::move(res));
            return execute_with_segments(graph, query_ptr, idx + 1,
                std::optional<std::vector<GqlRow>>(std::move(rows)));
        });
}

/**
 * @brief Execute a scoped subquery `CALL (imports) { ... }` once per incoming row, importing only the
 *        named variables into the subquery's scope, and return all its result rows unioned together.
 *        Runs sequentially over incoming rows (idx..) accumulating into `acc`; each invocation clones the
 *        subquery because execute_query_internal takes ownership of and mutates its query.
 */
static seastar::future<std::vector<GqlRow>> execute_call_subquery(
        ragedb::Graph& graph, std::shared_ptr<GqlQuery> subquery, std::vector<std::string> imports,
        std::shared_ptr<std::vector<GqlRow>> in_rows, size_t idx, std::shared_ptr<std::vector<GqlRow>> acc) {
    if (idx >= in_rows->size()) {
        return seastar::make_ready_future<std::vector<GqlRow>>(std::move(*acc));
    }
    // Import isolation: the subquery sees only the named outer variables, bound from this incoming row.
    GqlRow imported;
    for (const auto& v : imports) {
        auto it = (*in_rows)[idx].bindings.find(v);
        if (it != (*in_rows)[idx].bindings.end()) {
            imported.bindings[v] = it->second;
        }
    }
    auto sub_clone = std::make_shared<GqlQuery>(subquery->clone());
    std::vector<GqlRow> single{std::move(imported)};
    return execute_query_internal(graph, sub_clone, std::optional<std::vector<GqlRow>>(std::move(single)))
        .then([&graph, subquery, imports = std::move(imports), in_rows, idx, acc](QueryResult res) mutable {
            for (auto& r : query_result_to_rows(std::move(res))) {
                acc->push_back(std::move(r));
            }
            return execute_call_subquery(graph, subquery, std::move(imports), in_rows, idx + 1, acc);
        });
}

/**
 * @brief Derive the grouping-key expressions for an aggregate query, and collect the group variables.
 *        With an explicit GROUP BY the keys are its variables; otherwise a bare non-aggregate returned
 *        variable groups by the whole entity and a property lookup on such a variable is not a separate
 *        key. Both the streaming and materialising aggregate paths use this so they cannot diverge.
 */
static std::vector<const Expression*> derive_grouping_keys(const GqlQuery& q, std::set<std::string>& group_variables) {
    std::vector<const Expression*> grouping_keys;
    if (!q.group_by.empty()) {
        for (const auto& g : q.group_by) {
            if (g && g->kind == ExpressionKind::VARIABLE) {
                group_variables.insert(static_cast<const VariableExpr*>(g.get())->name);
            }
            if (g) grouping_keys.push_back(g.get());
        }
        return grouping_keys;
    }
    for (const auto& item : q.returns) {
        if (item.expr && !has_aggregates(item.expr.get()) && item.expr->kind == ExpressionKind::VARIABLE) {
            group_variables.insert(static_cast<const VariableExpr*>(item.expr.get())->name);
        }
    }
    for (const auto& item : q.returns) {
        if (!item.expr || has_aggregates(item.expr.get())) continue;
        if (item.expr->kind == ExpressionKind::PROPERTY_LOOKUP &&
            group_variables.count(static_cast<const PropertyLookupExpr*>(item.expr.get())->variable)) {
            continue;
        }
        grouping_keys.push_back(item.expr.get());
    }
    return grouping_keys;
}

/// True if the expression contains a nested braced subquery (COUNT{} or EXISTS{}). Such a subquery WHERE
/// needs the full correlated executor and is not yet handled by the pattern-count precompute below.
static bool expr_contains_subquery(const Expression* expr) {
    if (!expr) return false;
    if (expr->kind == ExpressionKind::SIZE_OP || expr->kind == ExpressionKind::EXISTS) return true;
    switch (expr->kind) {
        case ExpressionKind::UNARY_OP: return expr_contains_subquery(static_cast<const UnaryOpExpr*>(expr)->expr.get());
        case ExpressionKind::IS_NULL_CHECK: return expr_contains_subquery(static_cast<const IsNullExpr*>(expr)->expr.get());
        case ExpressionKind::BINARY_OP: {
            auto* b = static_cast<const BinaryOpExpr*>(expr);
            return expr_contains_subquery(b->left.get()) || expr_contains_subquery(b->right.get());
        }
        case ExpressionKind::AGGREGATION: return expr_contains_subquery(static_cast<const AggregateExpr*>(expr)->expr.get());
        case ExpressionKind::CASE_WHEN: {
            auto* ce = static_cast<const CaseExpr*>(expr);
            for (const auto& br : ce->branches) {
                if (expr_contains_subquery(br.first.get()) || expr_contains_subquery(br.second.get())) return true;
            }
            return expr_contains_subquery(ce->else_expr.get());
        }
        case ExpressionKind::IN_LIST: {
            auto* in = static_cast<const InExpr*>(expr);
            return expr_contains_subquery(in->value.get()) || expr_contains_subquery(in->list.get());
        }
        case ExpressionKind::CAST: return expr_contains_subquery(static_cast<const CastExpr*>(expr)->value.get());
        case ExpressionKind::LIST_LITERAL: {
            auto* le = static_cast<const ListExpr*>(expr);
            for (const auto& e : le->elements) if (expr_contains_subquery(e.get())) return true;
            return false;
        }
        case ExpressionKind::LIST_INDEX: {
            auto* ie = static_cast<const IndexExpr*>(expr);
            return expr_contains_subquery(ie->list.get()) || expr_contains_subquery(ie->index.get());
        }
        case ExpressionKind::LIST_COMPREHENSION: {
            auto* lc = static_cast<const ListComprehensionExpr*>(expr);
            return expr_contains_subquery(lc->list.get()) || expr_contains_subquery(lc->filter.get()) ||
                   expr_contains_subquery(lc->projection.get());
        }
        case ExpressionKind::QUANTIFIED_PREDICATE: {
            auto* qp = static_cast<const QuantifiedPredicateExpr*>(expr);
            return expr_contains_subquery(qp->list.get()) || expr_contains_subquery(qp->predicate.get());
        }
        case ExpressionKind::TEMPORAL_FIELD:
            return expr_contains_subquery(static_cast<const TemporalFieldExpr*>(expr)->value.get());
        default: return false;
    }
}

/// Walks an expression collecting the subqueries the precompute will evaluate, assigning each a stable id.
/// Degree-rewritten counts are already PropertyLookupExprs, so only the constrained/correlated ones remain
/// as SizeExprs here. When include_exists is set (a subquery's own WHERE, which the semi-join rewrite does
/// not reach) EXISTS subqueries are collected too. Does not recurse into a collected subquery's body -- the
/// precompute recurses into that per row, over the sub-rows the outer traversal produced.
static void collect_subqueries(Expression* expr, std::vector<Expression*>& out, int64_t& next_id,
                               bool include_exists) {
    if (!expr) return;
    if (expr->kind == ExpressionKind::SIZE_OP) {
        static_cast<SizeExpr*>(expr)->subquery_id = next_id++;   // deterministic by walk order; reassigned each execution
        out.push_back(expr);
        return;
    }
    if (include_exists && expr->kind == ExpressionKind::EXISTS) {
        auto* ee = static_cast<ExistsExpr*>(expr);
        // An EXISTS the optimizer already semi-join-rewrote carries a target_variable and is answered by
        // that binding; only precompute the ones the rewrite did not reach (e.g. an EXISTS inside a CASE
        // or aggregate argument in a projection, which is a value, not a row filter).
        if (ee->target_variable.empty()) {
            ee->subquery_id = next_id++;
            out.push_back(expr);
        } else {
            ee->subquery_id = -1;  // the semi-join binding answers it; clear any id from a prior execution
        }
        return;
    }
    switch (expr->kind) {
        case ExpressionKind::UNARY_OP: collect_subqueries(static_cast<UnaryOpExpr*>(expr)->expr.get(), out, next_id, include_exists); break;
        case ExpressionKind::IS_NULL_CHECK: collect_subqueries(static_cast<IsNullExpr*>(expr)->expr.get(), out, next_id, include_exists); break;
        case ExpressionKind::BINARY_OP: {
            auto* b = static_cast<BinaryOpExpr*>(expr);
            collect_subqueries(b->left.get(), out, next_id, include_exists);
            collect_subqueries(b->right.get(), out, next_id, include_exists);
            break;
        }
        case ExpressionKind::AGGREGATION: collect_subqueries(static_cast<AggregateExpr*>(expr)->expr.get(), out, next_id, include_exists); break;
        case ExpressionKind::CASE_WHEN: {
            auto* ce = static_cast<CaseExpr*>(expr);
            for (auto& br : ce->branches) {
                collect_subqueries(br.first.get(), out, next_id, include_exists);
                collect_subqueries(br.second.get(), out, next_id, include_exists);
            }
            collect_subqueries(ce->else_expr.get(), out, next_id, include_exists);
            break;
        }
        case ExpressionKind::IN_LIST: {
            auto* in = static_cast<InExpr*>(expr);
            collect_subqueries(in->value.get(), out, next_id, include_exists);
            collect_subqueries(in->list.get(), out, next_id, include_exists);
            break;
        }
        case ExpressionKind::CAST: collect_subqueries(static_cast<CastExpr*>(expr)->value.get(), out, next_id, include_exists); break;
        case ExpressionKind::LIST_LITERAL: {
            auto* le = static_cast<ListExpr*>(expr);
            for (auto& e : le->elements) collect_subqueries(e.get(), out, next_id, include_exists);
            break;
        }
        case ExpressionKind::LIST_INDEX: {
            auto* ie = static_cast<IndexExpr*>(expr);
            collect_subqueries(ie->list.get(), out, next_id, include_exists);
            collect_subqueries(ie->index.get(), out, next_id, include_exists);
            break;
        }
        case ExpressionKind::LIST_COMPREHENSION: {
            auto* lc = static_cast<ListComprehensionExpr*>(expr);
            collect_subqueries(lc->list.get(), out, next_id, include_exists);
            collect_subqueries(lc->filter.get(), out, next_id, include_exists);
            collect_subqueries(lc->projection.get(), out, next_id, include_exists);
            break;
        }
        case ExpressionKind::QUANTIFIED_PREDICATE: {
            auto* qp = static_cast<QuantifiedPredicateExpr*>(expr);
            collect_subqueries(qp->list.get(), out, next_id, include_exists);
            collect_subqueries(qp->predicate.get(), out, next_id, include_exists);
            break;
        }
        case ExpressionKind::TEMPORAL_FIELD:
            collect_subqueries(static_cast<TemporalFieldExpr*>(expr)->value.get(), out, next_id, include_exists);
            break;
        default: break;
    }
}

/// Reduces one subquery's sub-rows (already filtered/bound) to its scalar answer and stores it on the outer
/// row: a match count under "_count_<id>" for COUNT{}, or a boolean under "_exists_<id>" for EXISTS{}.
static void reduce_and_bind(std::shared_ptr<std::vector<GqlRow>> outer, size_t ri, int64_t sid,
                            Expression* where, bool is_exists, std::vector<GqlRow>& sub_rows) {
    if (is_exists) {
        bool found = false;
        for (auto& sr : sub_rows) {
            if (!where || evaluate_expression(sr, where).is_truthy()) { found = true; break; }
        }
        (*outer)[ri].bindings["_exists_" + std::to_string(sid)] = GqlValue(found);
    } else {
        int64_t cnt = 0;
        for (auto& sr : sub_rows) {
            if (!where || evaluate_expression(sr, where).is_truthy()) cnt++;
        }
        (*outer)[ri].bindings["_count_" + std::to_string(sid)] = GqlValue(property_type_t(cnt));
    }
}

/// Serialize a bound value into a stable grouping key for correlated-subquery decorrelation. Nodes and
/// relationships key on their id; scalars on their typed value. Returns false for values that cannot be a
/// clean grouping key (lists, paths), which makes the caller fall back to the per-row precompute.
static bool gql_value_group_key(const GqlValue& v, std::string& out) {
    switch (v.type) {
        case GqlValue::NODE:
            if (!v.node) return false;
            out = "N" + std::to_string(v.node->getId());
            return true;
        case GqlValue::RELATIONSHIP:
            if (!v.relationship) return false;
            out = "R" + std::to_string(v.relationship->getId());
            return true;
        case GqlValue::NIL:
            out = "z";
            return true;
        case GqlValue::PROPERTY: {
            const auto& p = v.property;
            if (std::holds_alternative<std::monostate>(p)) { out = "z"; return true; }
            if (std::holds_alternative<bool>(p)) { out = std::get<bool>(p) ? "b1" : "b0"; return true; }
            if (std::holds_alternative<int64_t>(p)) { out = "i" + std::to_string(std::get<int64_t>(p)); return true; }
            if (std::holds_alternative<double>(p)) { out = "d" + std::to_string(std::get<double>(p)); return true; }
            if (std::holds_alternative<std::string>(p)) { out = "s" + std::get<std::string>(p); return true; }
            return false;
        }
        default:
            return false;
    }
}

/// Correlated subquery precompute. For each row, runs each supported subquery's pattern anchored on the
/// row's bindings; when the subquery's own WHERE nests a further subquery (e.g. IC10's COUNT{} whose WHERE
/// holds an EXISTS) it recursively precomputes that over the sub-rows before filtering, so the synchronous
/// evaluator reads a fully-resolved WHERE. Binds "_count_<id>"/"_exists_<id>" per row. Handles far-node
/// label/property filters, LET, and piped nodes -- everything the far-node-blind degree rewrite cannot.
static seastar::future<std::vector<GqlRow>> precompute_subqueries(
    ragedb::Graph& graph, std::shared_ptr<GqlQuery> query_ptr, std::vector<GqlRow> rows,
    std::vector<Expression*> subqueries) {
    if (subqueries.empty() || rows.empty()) {
        return seastar::make_ready_future<std::vector<GqlRow>>(std::move(rows));
    }

    auto shared_rows = std::make_shared<std::vector<GqlRow>>(std::move(rows));
    std::vector<seastar::future<>> futs;

    // Outer variables available on the incoming rows (same schema across rows) -- the set a subquery can
    // correlate on.
    std::set<std::string> outer_vars;
    for (const auto& kv : (*shared_rows)[0].bindings) outer_vars.insert(kv.first);

    for (auto* sq : subqueries) {
        std::vector<MatchStatement>* matches;
        Expression* where;
        int64_t sid;
        bool is_exists;
        if (sq->kind == ExpressionKind::SIZE_OP) {
            auto* se = static_cast<SizeExpr*>(sq);
            matches = &se->matches; where = se->where_expr.get(); sid = se->subquery_id; is_exists = false;
        } else {
            auto* ee = static_cast<ExistsExpr*>(sq);
            matches = &ee->matches; where = ee->where_expr.get(); sid = ee->subquery_id; is_exists = true;
        }
        if (matches->size() != 1) continue;                          // multi-match not yet supported
        const PathPattern& pattern = (*matches)[0].pattern;
        ProjectionPruner pruner;   // keep every subquery variable so the WHERE can read properties
        for (const auto& n : pattern.nodes) if (!n.variable.empty()) pruner.whole_objects.insert(n.variable);
        for (const auto& e : pattern.edges) if (!e.variable.empty()) pruner.whole_objects.insert(e.variable);
        const bool nested = where && expr_contains_subquery(where);

        // Decorrelate: the subquery's result depends only on the OUTER variables it reads -- the pattern's
        // pre-bound anchor variables plus any outer variables its WHERE references. Group the rows by those
        // and traverse the pattern ONCE per distinct key instead of once per row (O(rows x subquery) ->
        // O(distinct-keys x subquery)), then bind the shared count/exists to every row in the group. Fall
        // back to the per-row precompute when the subquery nests a further subquery or a key value is not
        // groupable, so correctness is never at risk.
        std::set<std::string> key_vars;
        for (const auto& n : pattern.nodes) if (!n.variable.empty() && outer_vars.count(n.variable)) key_vars.insert(n.variable);
        for (const auto& e : pattern.edges) if (!e.variable.empty() && outer_vars.count(e.variable)) key_vars.insert(e.variable);
        if (where) {
            std::map<std::string, std::set<std::string>> wprops;
            std::set<std::string> wwhole;
            collect_accessed_properties(where, wprops, wwhole);
            for (const auto& kv : wprops) if (outer_vars.count(kv.first)) key_vars.insert(kv.first);
            for (const auto& v : wwhole) if (outer_vars.count(v)) key_vars.insert(v);
        }

        std::unordered_map<std::string, std::vector<size_t>> groups;
        bool groupable = !nested;
        for (size_t ri = 0; groupable && ri < shared_rows->size(); ++ri) {
            std::string ks;
            for (const auto& kv : key_vars) {
                auto it = (*shared_rows)[ri].bindings.find(kv);
                std::string part;
                if (it == (*shared_rows)[ri].bindings.end() || !gql_value_group_key(it->second, part)) {
                    groupable = false;
                    break;
                }
                ks += part;
                ks += '\x1f';
            }
            if (groupable) groups[ks].push_back(ri);
        }

        if (groupable) {
            for (auto& g : groups) {
                auto idx = std::make_shared<std::vector<size_t>>(g.second);
                size_t rep = (*idx)[0];
                futs.push_back(
                    traverse_path_pattern(graph, pattern, (*shared_rows)[rep], 0, pruner)
                    .then([shared_rows, idx, sid, where, is_exists](std::vector<GqlRow> sub_rows) {
                        // The pattern anchors and every WHERE-referenced outer variable are constant across
                        // the group (they are the key), so the sub-rows and the reduced value are shared.
                        if (is_exists) {
                            bool found = false;
                            for (auto& sr : sub_rows) if (!where || evaluate_expression(sr, where).is_truthy()) { found = true; break; }
                            for (size_t ri : *idx) (*shared_rows)[ri].bindings["_exists_" + std::to_string(sid)] = GqlValue(found);
                        } else {
                            int64_t cnt = 0;
                            for (auto& sr : sub_rows) if (!where || evaluate_expression(sr, where).is_truthy()) cnt++;
                            for (size_t ri : *idx) (*shared_rows)[ri].bindings["_count_" + std::to_string(sid)] = GqlValue(property_type_t(cnt));
                        }
                    })
                );
            }
        } else {
            // Per-row fallback: a nested subquery (its own WHERE is never semi-join-rewritten, so it needs
            // the recursive precompute) or a non-groupable key value.
            for (size_t ri = 0; ri < shared_rows->size(); ++ri) {
                futs.push_back(
                    traverse_path_pattern(graph, pattern, (*shared_rows)[ri], 0, pruner)
                    .then([&graph, shared_rows, ri, sid, where, is_exists, nested, query_ptr](std::vector<GqlRow> sub_rows) -> seastar::future<> {
                        if (nested) {
                            std::vector<Expression*> inner;
                            int64_t inner_id = 0;
                            collect_subqueries(where, inner, inner_id, /*include_exists=*/true);
                            return precompute_subqueries(graph, query_ptr, std::move(sub_rows), std::move(inner))
                                .then([shared_rows, ri, sid, where, is_exists](std::vector<GqlRow> resolved) {
                                    reduce_and_bind(shared_rows, ri, sid, where, is_exists, resolved);
                                });
                        }
                        reduce_and_bind(shared_rows, ri, sid, where, is_exists, sub_rows);
                        return seastar::make_ready_future<>();
                    })
                );
            }
        }
    }
    return seastar::when_all_succeed(futs.begin(), futs.end())
        .then([shared_rows]() { return std::move(*shared_rows); });
}

/// Top-level entry: precompute the correlated subqueries reachable from a query's LET/RETURN/WHERE/ORDER BY.
/// A top-level EXISTS is already handled by the semi-join rewrite, so only COUNT{} is collected here; nested
/// EXISTS inside a collected COUNT{}'s WHERE is picked up by the recursion in precompute_subqueries.
static seastar::future<std::vector<GqlRow>> precompute_count_subqueries(
    ragedb::Graph& graph, std::shared_ptr<GqlQuery> query_ptr, std::vector<GqlRow> rows) {
    std::vector<Expression*> subqueries;
    int64_t next_id = 0;
    // An EXISTS in LET/RETURN/ORDER BY is a value (e.g. inside CASE or an aggregate argument), never
    // reached by the WHERE-only semi-join rewrite, so collect it here for precompute (include_exists).
    // A top-level WHERE EXISTS *is* semi-join-rewritten, so collect only COUNT{} from the WHERE to avoid
    // double-handling. COUNT{} (SizeExpr) is collected in either case.
    for (auto& let : query_ptr->let_bindings) collect_subqueries(let.expr.get(), subqueries, next_id, true);
    for (auto& item : query_ptr->returns) collect_subqueries(item.expr.get(), subqueries, next_id, true);
    if (query_ptr->where_expr) collect_subqueries(query_ptr->where_expr.get(), subqueries, next_id, false);
    for (auto& spec : query_ptr->order_by) collect_subqueries(spec.expr.get(), subqueries, next_id, true);
    return precompute_subqueries(graph, query_ptr, std::move(rows), std::move(subqueries));
}

static seastar::future<QueryResult> execute_query_internal(ragedb::Graph& graph, std::shared_ptr<GqlQuery> query_ptr, std::optional<std::vector<GqlRow>> incoming) {
    // WITH continuation segments carry piped rows; planners that answer from indexes or run the
    // pattern from scratch cannot consume them and must be skipped (they would ignore the pipe).
    const bool has_incoming = incoming.has_value();
    if (query_ptr->no_op) {
        QueryResult query_res;
        for (size_t i = 0; i < query_ptr->returns.size(); ++i) {
            query_res.column_names.push_back(return_item_column_name(query_ptr->returns[i], i));
        }
        return seastar::make_ready_future<QueryResult>(std::move(query_res));
    }

    // WITH pipeline: execute the prefix segments (each projecting rows forward), then run the final
    // RETURN segment on the piped rows. The final segment is this query with its with_segments removed.
    if (!query_ptr->with_segments.empty()) {
        return execute_with_segments(graph, query_ptr, 0, std::move(incoming))
        .then([&graph, query_ptr](std::vector<GqlRow> piped) {
            auto final_seg = std::make_shared<GqlQuery>(query_ptr->clone());
            final_seg->with_segments.clear();
            return execute_query_internal(graph, final_seg, std::optional<std::vector<GqlRow>>(std::move(piped)));
        });
    }

    // Scoped subquery `CALL (imports) { <subquery> }`: this segment is sourced by running the nested
    // subquery once per incoming row (importing only the named variables), unioning its result rows, then
    // projecting them through this segment's own RETURN/GROUP BY/ORDER BY/LIMIT. The projection is a
    // matchless re-entry (call_subquery cleared) -- the same pure-projection path a WITH final segment uses.
    if (query_ptr->call_subquery) {
        auto in_rows = std::make_shared<std::vector<GqlRow>>(
            incoming.has_value() ? std::move(*incoming) : std::vector<GqlRow>{GqlRow{}});
        auto sub = query_ptr->call_subquery;
        auto imports = query_ptr->call_import_vars;
        auto acc = std::make_shared<std::vector<GqlRow>>();
        return execute_call_subquery(graph, sub, std::move(imports), in_rows, 0, acc)
            .then([&graph, query_ptr](std::vector<GqlRow> sub_rows) {
                auto proj = std::make_shared<GqlQuery>(query_ptr->clone());
                proj->call_subquery = nullptr;
                proj->call_import_vars.clear();
                return execute_query_internal(graph, proj, std::optional<std::vector<GqlRow>>(std::move(sub_rows)));
            });
    }

    // 1. Handle Set Operations (UNION, INTERSECT, EXCEPT, each with an optional ALL)
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

        // A set operation used as a scoped-subquery body receives the imported rows; run each branch over
        // them. A top-level set operation has no incoming, so both branches see nullopt exactly as before.
        std::optional<std::vector<GqlRow>> inc_right = incoming;

        // Execute the left subquery first
        return execute_query_internal(graph, left_ptr, std::move(incoming))
        .then([&graph, right_ptr, query_ptr, start, inc_right = std::move(inc_right)](QueryResult left_res) mutable {
            // Then execute the right subquery
            return execute_query_internal(graph, right_ptr, std::move(inc_right))
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
                } else if (query_ptr->kind == QueryKind::EXCEPT) {
                    // EXCEPT: distinct rows of the left that do not appear in the right.
                    std::set<std::vector<GqlValue>, GqlValueVectorLess> right_set;
                    for (const auto& r : right_res.rows) {
                        right_set.insert(r);
                    }
                    std::set<std::vector<GqlValue>, GqlValueVectorLess> emitted;
                    for (auto& r : left_res.rows) {
                        if (right_set.count(r)) continue;
                        if (emitted.insert(r).second) {
                            combined_res.rows.push_back(std::move(r));
                        }
                    }
                } else if (query_ptr->kind == QueryKind::EXCEPT_ALL) {
                    // EXCEPT ALL: multiset difference -- each right occurrence cancels one left occurrence.
                    std::map<std::vector<GqlValue>, int64_t, GqlValueVectorLess> right_counts;
                    for (const auto& r : right_res.rows) {
                        right_counts[r]++;
                    }
                    for (auto& r : left_res.rows) {
                        auto it = right_counts.find(r);
                        if (it != right_counts.end() && it->second > 0) {
                            it->second--;   // this left row is cancelled by a right occurrence
                            continue;
                        }
                        combined_res.rows.push_back(std::move(r));
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

    // Fast path: a pure COUNT over a single node pattern is answered directly from the shard count
    // indexes, without materializing Node objects--otherwise counting a large label (e.g.
    // count(Comment) over millions of rows) exhausts memory.
    {
        SimpleNodeCountPlan cplan;
        if (!has_incoming && query_ptr->let_bindings.empty() && query_ptr->for_bindings.empty() &&
            try_plan_simple_node_count(*query_ptr, cplan)) {
            seastar::future<uint64_t> count_fut = cplan.has_filter
                ? graph.shard.local().FindNodeCountPeered(cplan.label, cplan.filter_property, cplan.filter_op, cplan.filter_value)
                : (cplan.has_label ? graph.shard.local().AllNodesCountPeered(cplan.label)
                                   : graph.shard.local().AllNodesCountPeered());
            return count_fut.then([cplan](uint64_t c) {
                int64_t value = static_cast<int64_t>(c);
                if (cplan.multiplier > 1) {
                    value *= static_cast<int64_t>(cplan.multiplier);
                }
                QueryResult result;
                result.column_names.push_back(cplan.column_name);
                std::vector<GqlValue> row;
                row.push_back(GqlValue(value));
                result.rows.push_back(std::move(row));
                return result;
            });
        }
    }

    // Fast path: a grouped/ungrouped aggregate over a single-edge expansion is streamed group by
    // group (see EdgeAggPlan) instead of materialising the whole (a,b) expansion--otherwise a
    // large expansion (e.g. count(comment) per person over millions of comments) exhausts memory.
    {
        EdgeAggPlan eplan;
        if (!has_incoming && query_ptr->let_bindings.empty() && query_ptr->for_bindings.empty() &&
            plan_streaming_edge_aggregate(*query_ptr, eplan)) {
            return run_streaming_edge_aggregate(graph, query_ptr, std::move(eplan));
        }
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

    // Determine if we can push limit evaluation directly into the traversal. Pushing the LIMIT as
    // the physical scan bound is only sound when every remaining predicate is applied INSIDE the
    // scan; if any predicate is evaluated (or rows are collapsed) after the scan, scanning exactly
    // `limit` rows under-returns. In those cases scan with the default bound and truncate. The
    // residual conditions live in has_post_scan_residual_predicate, shared with the
    // LimitPushdownOptimizer so the two gates cannot drift.
    size_t limit_val = 0;
    if (query_ptr->limit.has_value() && query_ptr->order_by.empty() &&
        !query_contains_aggregates && !has_post_scan_residual_predicate(*query_ptr)) {
        // Scan the whole page window: with an OFFSET, scanning only `limit` rows would stop short of the
        // rows the page actually returns.
        const auto window = page_window(*query_ptr);
        if (window) {
            limit_val = static_cast<size_t>(*window);
        }
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

    // Collect properties accessed in ISO GQL LET binding expressions
    for (const auto& let : query_ptr->let_bindings) {
        collect_accessed_properties(let.expr.get(), pruner.accessed_props, pruner.whole_objects);
    }

    // Collect properties accessed in ISO GQL FOR list expressions
    for (const auto& binding : query_ptr->for_bindings) {
        collect_accessed_properties(binding.list_expr.get(), pruner.accessed_props, pruner.whole_objects);
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
                for (const auto& [prop, expr] : n.property_exprs) {
                    pruner.accessed_props[n.variable].insert(prop);
                    collect_accessed_properties(expr.get(), pruner.accessed_props, pruner.whole_objects);
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
                for (const auto& [prop, expr] : e.property_exprs) {
                    pruner.accessed_props[e.variable].insert(prop);
                    collect_accessed_properties(expr.get(), pruner.accessed_props, pruner.whole_objects);
                }
                for (const auto& filter : e.property_filters) {
                    pruner.accessed_props[e.variable].insert(filter.property);
                }
            }
        }
    }

    // A variable used ONLY as a bare count(...) / count(DISTINCT ...) argument contributes just
    // its identity (node comparison and dedup are by id), so its properties can be pruned: a
    // count(DISTINCT post) over a large expansion otherwise carries every post's full property
    // map through traversal and into the distinct set. Skipped when a path variable is bound
    // (paths embed the nodes wholesale) or when writes are present.
    if (query_ptr->writes.empty() &&
        std::none_of(query_ptr->matches.begin(), query_ptr->matches.end(),
                     [](const MatchStatement& m) { return !m.path_variable.empty(); })) {
        std::vector<const AggregateExpr*> pruning_aggs;
        for (const auto& item : query_ptr->returns) find_aggregates(item.expr.get(), pruning_aggs);
        for (const auto& spec : query_ptr->order_by) find_aggregates(spec.expr.get(), pruning_aggs);
        for (const auto* agg : pruning_aggs) {
            if (agg->fn_kind != AggregateKind::COUNT || !agg->expr ||
                agg->expr->kind != ExpressionKind::VARIABLE) {
                continue;
            }
            const std::string& v = static_cast<const VariableExpr*>(agg->expr.get())->name;
            bool outside = query_ptr->where_expr &&
                is_variable_referenced_outside_count(query_ptr->where_expr.get(), v);
            for (const auto& item : query_ptr->returns) {
                if (outside) break;
                outside = is_variable_referenced_outside_count(item.expr.get(), v);
            }
            for (const auto& spec : query_ptr->order_by) {
                if (outside) break;
                outside = is_variable_referenced_outside_count(spec.expr.get(), v);
            }
            if (!outside) {
                pruner.whole_objects.erase(v);
                pruner.accessed_props.try_emplace(v);
            }
        }
    }

    // Streaming execution (tasks 018/020): expansion-heavy shapes fold matched rows as they are
    // produced instead of materialising the whole expansion, bounding peak memory.
    // Streaming top-K: a single-match ORDER BY + LIMIT without aggregates folds into a bounded heap.
    if (stream_eligible(*query_ptr) && !query_ptr->distinct &&
        !query_ptr->order_by.empty() && query_ptr->limit.has_value() && !query_contains_aggregates) {
        return run_streaming_topk(graph, query_ptr, std::move(incoming), pruner);
    }
    // Streaming grouped aggregate: one or more streamable matches, bounding the FoF -> expand ->
    // count(DISTINCT) crash class by folding each joined row into per-group accumulators.
    if (stream_group_eligible(*query_ptr) && !query_ptr->distinct && query_contains_aggregates) {
        std::set<std::string> group_variables;
        std::vector<const Expression*> grouping_keys = derive_grouping_keys(*query_ptr, group_variables);
        std::vector<const AggregateExpr*> aggs;
        for (const auto& item : query_ptr->returns) find_aggregates(item.expr.get(), aggs);
        for (const auto& spec : query_ptr->order_by) find_aggregates(spec.expr.get(), aggs);
        // collect_list gathers a list, and stddev needs a sum-of-squares, neither of which the streaming
        // accumulator builds; use the materialising group path for them.
        bool needs_materialising = false;
        for (const auto* a : aggs) {
            if (a->fn_kind == AggregateKind::COLLECT ||
                a->fn_kind == AggregateKind::STDDEV_POP || a->fn_kind == AggregateKind::STDDEV_SAMP ||
                a->fn_kind == AggregateKind::PERCENTILE_CONT || a->fn_kind == AggregateKind::PERCENTILE_DISC) {
                needs_materialising = true;
                break;
            }
        }
        if (!aggs.empty() && !needs_materialising) {
            return run_streaming_group_fold(graph, query_ptr, std::move(incoming), pruner,
                                            std::move(grouping_keys), std::move(aggs));
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
        // A bare variable orders by element identity, which the start-node scan can satisfy by scanning
        // in node-id order -- but only when the variable IS the start node. Ordering by any other bare
        // variable (a LET column, a piped column, a FOR variable, a later pattern node) has nothing to do
        // with the traversal order, so the flags must stay off and the ordinary sort must run.
        bool orders_by_element_identity = false;

        if (spec.expr->kind == ExpressionKind::PROPERTY_LOOKUP) {
            auto* pl = static_cast<const PropertyLookupExpr*>(spec.expr.get());
            var_name = pl->variable;
            prop_name = pl->property;
            ok = true;
        } else if (spec.expr->kind == ExpressionKind::VARIABLE) {
            auto* ve = static_cast<const VariableExpr*>(spec.expr.get());
            var_name = ve->name;
            ok = true;
            orders_by_element_identity = true;
        }

        if (ok) {
            const auto& first_match = query_ptr->matches[0];
            if (!first_match.pattern.nodes.empty()) {
                std::string start_var = first_match.pattern.nodes[0].variable;
                if (start_var == var_name || (start_var.empty() && var_name == "_n_0_user_empty")) {
                    sort_property = prop_name;
                    sort_ascending = spec.ascending;
                    sort_by_id = orders_by_element_identity;
                }
            }
        }
    }

    // 5. Execute matching patterns recursively using sharded traversal. WITH-pipeline continuation
    // segments start from the rows piped in from the previous segment rather than a single empty row.
    std::vector<GqlRow> seed_rows = incoming.has_value()
        ? std::move(*incoming)
        : std::vector<GqlRow>{ GqlRow{} };

    // A FOR whose variable is a MATCH pattern anchor -- FOR x IN list MATCH (x)-[:R]->(y) -- must expand
    // BEFORE the match so each element seeds the traversal. Expanding it after the match (the default FOR
    // stage, correct for FOR over a matched value) would leave x free in the match and then overwrite it,
    // yielding a cross product of the whole match with the list. Pre-expand those onto the seed rows here
    // and record them so the post-match FOR stage skips them. Such a FOR's list cannot depend on a match
    // variable (that would be circular), so it is always evaluable against the incoming rows.
    std::set<std::string> prematch_for_vars;
    if (has_incoming && !query_ptr->for_bindings.empty() && !query_ptr->matches.empty()) {
        std::set<std::string> match_pattern_vars;
        for (const auto& m : query_ptr->matches) {
            for (const auto& n : m.pattern.nodes) {
                if (!n.variable.empty()) match_pattern_vars.insert(n.variable);
            }
            for (const auto& e : m.pattern.edges) {
                if (!e.variable.empty()) match_pattern_vars.insert(e.variable);
            }
        }
        for (const auto& binding : query_ptr->for_bindings) {
            if (match_pattern_vars.count(binding.variable) == 0) {
                continue;   // not a match anchor -- keep it in the post-match FOR stage
            }
            std::vector<GqlRow> expanded;
            for (const auto& row : seed_rows) {
                const GqlValue list_val = evaluate_expression(row, binding.list_expr.get());
                const auto elements = as_list_elements(list_val);
                if (!elements) {
                    continue;
                }
                for (const auto& element : *elements) {
                    GqlRow expanded_row = row;
                    expanded_row.bindings[binding.variable] = element;
                    expanded.push_back(std::move(expanded_row));
                }
            }
            seed_rows = std::move(expanded);
            prematch_for_vars.insert(binding.variable);
        }
    }

    IntermediateResult initial_res(std::move(seed_rows));
    auto is_sorted_shared = std::make_shared<bool>(false);

    auto is_query_cyclic = [](const std::vector<MatchStatement>& matches) -> bool {
        std::unordered_map<std::string, std::vector<std::pair<std::string, std::string>>> adj_list;
        std::set<std::string> node_vars;
        
        for (size_t m_idx = 0; m_idx < matches.size(); ++m_idx) {
            const auto& match = matches[m_idx];
            if (match.is_search || match.is_propagate) continue;
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
    if (!has_incoming && is_query_cyclic(query_ptr->matches) && query_ptr->count_multiplication_factor <= 1) {
        uint64_t total_rels = 0;
        for (const auto& match : query_ptr->matches) {
            if (match.is_search || match.is_propagate) continue;
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

    .then([&graph, query_ptr, is_sorted_shared, prematch_for_vars](IntermediateResult matched_res) {
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

        // Resolve correlated COUNT{} subqueries (async) before LET / WHERE / RETURN read their per-row
        // counts. No-op and cost-free when the segment contains none.
        return precompute_count_subqueries(graph, query_ptr, std::move(matched_rows))
        .then([&graph, query_ptr, prematch_for_vars](std::vector<GqlRow> matched_rows) {
        const auto& query = *query_ptr;

        // 6a. Expand ISO GQL FOR (the standard's UNWIND) before anything else reads the working table:
        //     unlike LET, which adds a column to each row, FOR multiplies the rows -- one per element of
        //     the list. A pattern-less query (FOR x IN [1, 2, 3] RETURN x) still has a row to expand,
        //     because the match stage yields a single empty row when there is nothing to match.
        if (!query.for_bindings.empty()) {
            if (matched_rows.empty() && query.matches.empty()) {
                matched_rows.push_back(GqlRow{});
            }
            for (const auto& binding : query.for_bindings) {
                if (prematch_for_vars.count(binding.variable)) {
                    continue;   // already expanded before the match as a pattern anchor
                }
                std::vector<GqlRow> expanded;
                for (const auto& row : matched_rows) {
                    const GqlValue list_val = evaluate_expression(row, binding.list_expr.get());
                    const auto elements = as_list_elements(list_val);
                    if (!elements) {
                        continue;   // a non-list (or null) expands to no rows, as an empty list does
                    }
                    for (const auto& element : *elements) {
                        GqlRow expanded_row = row;
                        expanded_row.bindings[binding.variable] = element;
                        expanded.push_back(std::move(expanded_row));
                    }
                }
                matched_rows = std::move(expanded);
            }
        }

        // 6b. Evaluate ISO GQL LET bindings against each matched row so the segment's WHERE/FILTER,
        //     ORDER BY, and RETURN can reference them (LET adds computed columns to the working table).
        if (!query.let_bindings.empty()) {
            for (auto& row : matched_rows) {
                for (const auto& let : query.let_bindings) {
                    if (let.alias) {
                        row.bindings[*let.alias] = evaluate_expression(row, let.expr.get());
                    }
                }
            }
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
            query_res.column_names.push_back(return_item_column_name(query.returns[i], i));
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
            std::vector<const Expression*> grouping_keys = derive_grouping_keys(query, group_variables);

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
                // One accumulator per aggregate, folded over the group's rows. EdgeAggAccumulator is
                // the single implementation of every set function (COUNT/SUM/AVG/MIN/MAX/COLLECT/
                // STDDEV, distinct and not); the streaming group fold uses the same struct, so the
                // materialising and streaming paths can no longer drift apart.
                std::map<const AggregateExpr*, GqlValue> aggregate_results;
                for (const auto* agg : aggregate_exprs) {
                    EdgeAggAccumulator acc = EdgeAggAccumulator::make(agg);
                    for (const auto& r : group.rows) acc.add(r);
                    aggregate_results[agg] = acc.finalize(query.count_multiplication_factor);
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
                
                const auto window = page_window(query);
                if (window && *window < sorted_groups.size()) {
                    std::partial_sort(sorted_groups.begin(), sorted_groups.begin() + static_cast<std::ptrdiff_t>(*window), sorted_groups.end(), comp);
                    sorted_groups.resize(static_cast<size_t>(*window));
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

                const auto window = page_window(query);
                if (window && *window < sorted_keys.size()) {
                    std::partial_sort(sorted_keys.begin(), sorted_keys.begin() + static_cast<std::ptrdiff_t>(*window), sorted_keys.end(), comp);
                    sorted_keys.resize(static_cast<size_t>(*window));
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
            } else if (page_window(query) && !query.distinct && *page_window(query) < filtered_rows.size()) {
                // DISTINCT collapses rows during projection, so the raw matched rows cannot be
                // truncated to the LIMIT here; the post-dedup resize below enforces it instead. The cut is
                // to offset+limit, not limit: an OFFSET skips past rows that are still needed.
                auto limit_start = std::chrono::steady_clock::now();
                filtered_rows.resize(static_cast<size_t>(*page_window(query)));
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

        apply_page(query_res.rows, query);
        query_res.is_paged = true;

        return seastar::make_ready_future<QueryResult>(std::move(query_res));
    });
}
/**
 * @brief Validates database constraints after executing queries containing write operations.
 * @param graph The database graph instance.
 * @return A future resolving when validation completes successfully or throws on violation.
 */
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

/**
 * @brief Executes a pre-parsed and typechecked GqlQuery.
 * @param graph The database graph instance.
 * @param query_val The GqlQuery object.
 * @return A future resolving to the query result formatted as a JSON string.
 */
seastar::future<std::string> GqlExecutor::execute(ragedb::Graph& graph, GqlQuery query_val) {
    GqlTypechecker::typecheck(graph, query_val);

    if (query_val.explain) {
        auto plan = build_query_plan(graph, query_val);
        // Which dispatch path the query would actually run -- a streaming fast path bypasses the plan
        // tree shown below, so surface it on the root row. Whole-query disposition, shown once.
        std::string execution = classify_execution_strategy(query_val);
        std::vector<std::vector<GqlValue>> plan_rows;
        flatten_plan_tree(plan, plan_rows, "", true);

        std::vector<std::string> column_names = { "Operator", "Details", "Outputs" };

        std::string json_res = "[";
        bool first_row = true;
        for (const auto& row : plan_rows) {
            if (!first_row) json_res += ", ";
            json_res += "{";
            bool first_col = true;
            for (size_t i = 0; i < column_names.size(); ++i) {
                if (!first_col) json_res += ", ";
                json_res += "\"" + column_names[i] + "\": " + serialize_gql_value(row[i]);
                first_col = false;
            }
            // flatten_plan_tree emits estimated_rows at index 5; surface it as the operator's row estimate.
            json_res += ", \"Est. Rows\": " + serialize_gql_value(row.size() > 5 ? row[5] : GqlValue());
            std::string cache_status = query_val.plan_cache_hit ? "\"HIT\"" : "\"MISS\"";
            json_res += ", \"Cache\": " + cache_status;
            json_res += ", \"Execution\": " + serialize_gql_value(GqlValue(first_row ? execution : std::string()));
            json_res += "}";
            first_row = false;
        }
        json_res += "]";

        return seastar::make_ready_future<std::string>(json_res);
    }

    if (query_val.clear_cache) {
        return graph.shard.invoke_on_all([](Shard&) {
            GqlQueryCache::local().clear();
            WccCache::local().clear();
            TransitiveReachabilityCache::local().clear();
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
            return graph.shard.invoke_on_all([](Shard&) {
                WccCache::local().clear();
                TransitiveReachabilityCache::local().clear();
            }).then([&graph]() {
                return validate_constraints(graph);
            }).then([result = std::move(result)]() mutable {
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

        // Set operations combine un-paged branch results, so the page applies to the combination here.
        // Every other path already paged its own rows and must not be paged a second time.
        if (!result.is_paged) {
            if (!query.order_by.empty() && !result.is_sorted) {
                // Sort down to the page window, not the bare limit: the OFFSET skips past rows the sort
                // would otherwise have pruned away.
                const auto window = page_window(query);
                sort_combined_result(result, query.order_by,
                                     window ? std::optional<size_t>(static_cast<size_t>(*window)) : std::nullopt);
            }
            apply_page(result.rows, query);
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
/**
 * @brief Helper utility to trim whitespace from the start and end of a string.
 * @param str The string to trim.
 * @return The trimmed string.
 */
static std::string trim(const std::string& str) {
    size_t first = str.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) return "";
    size_t last = str.find_last_not_of(" \t\r\n");
    return str.substr(first, (last - first + 1));
}
/**
 * @brief Parses, optimizes, caches, and executes a GQL query string.
 * @param graph The database graph instance.
 * @param query_str The query string to run.
 * @return A future resolving to the query result formatted as a JSON string.
 */
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

void clear_registries() {
    GqlQueryCache::local().clear();
    GqlVirtualCatalog::local().clear();
}

} // namespace ragedb::gql
