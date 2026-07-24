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

#include "ExecutorInternal.h"
#include "PathTraverser.h"
#include "ExpressionEvaluator.h"
#include "../graph/Graph.h"

#include <algorithm>
#include <memory>
#include <vector>
#include <string>
#include <set>
#include <map>
#include <optional>
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/when_all.hh>

namespace ragedb::gql {

/**
 * @brief Detect whether a query is a pure COUNT over a single node pattern and, if so, populate a
 *        SimpleNodeCountPlan. Returns false (fall back to the normal executor) for any shape that
 *        cannot be answered by a single count-index lookup.
 */
bool try_plan_simple_node_count(const GqlQuery& q, SimpleNodeCountPlan& out) {
    if (q.kind != QueryKind::SINGLE) return false;
    if (q.explain || q.profile) return false;      // keep plan/profile output on the normal path
    if (!q.writes.empty()) return false;
    if (q.where_expr) return false;
    if (q.has_unnested_subquery) return false;
    if (q.distinct) return false;
    if (q.limit.has_value()) return false;
    if (q.offset.has_value()) return false;   // this plan emits the count row directly; nothing pages it
    if (!q.order_by.empty()) return false;
    if (q.matches.size() != 1) return false;
    if (q.returns.size() != 1) return false;

    const auto& item = q.returns[0];
    if (!item.expr || item.expr->kind != ExpressionKind::AGGREGATION) return false;
    const auto* ae = static_cast<const AggregateExpr*>(item.expr.get());
    if (ae->fn_kind != AggregateKind::COUNT) return false;

    const auto& m = q.matches[0];
    if (m.is_optional) return false;
    if (m.shortest_path_kind != ShortestPathKind::NONE) return false;
    if (m.is_khop) return false;
    if (!m.path_variable.empty()) return false;
    if (m.limit.has_value()) return false;
    if (!m.pattern.edges.empty()) return false;
    if (m.pattern.nodes.size() != 1) return false;

    const auto& node = m.pattern.nodes[0];
    if (node.where_expr) return false;
    // A property map value that is an expression is only known once it is resolved against the incoming
    // row, which this plan bypasses; counting through it would silently ignore the constraint.
    if (!node.property_exprs.empty()) return false;

    // The count target must be count(*) or count(<the pattern node's variable>). For a single
    // non-optional node every matched row binds a distinct node, so count(n) == row count.
    if (ae->expr) {
        if (ae->expr->kind != ExpressionKind::VARIABLE) return false;
        const auto* ve = static_cast<const VariableExpr*>(ae->expr.get());
        if (node.variable.empty() || ve->name != node.variable) return false;
    }

    // Only a single literal label (or no label) can be answered by the count endpoints;
    // AND/OR/NOT/WILDCARD label expressions fall back.
    if (node.label_expr) {
        if (node.label_expr->kind != LabelExprKind::LITERAL) return false;
        out.has_label = true;
        out.label = node.label_expr->name;
    }

    // Zero filters -> count all (of the label). Exactly one filter (with a label) -> FindNodeCount.
    // More than one filter has no single count endpoint, so fall back.
    size_t filter_count = node.properties.size() + node.property_filters.size();
    if (filter_count > 1) return false;
    if (filter_count == 1) {
        if (!out.has_label) return false;  // FindNodeCountPeered requires a node type
        out.has_filter = true;
        if (!node.properties.empty()) {
            const auto& kv = *node.properties.begin();
            out.filter_property = kv.first;
            out.filter_op = Operation::EQ;
            out.filter_value = kv.second;
        } else {
            const auto& f = node.property_filters[0];
            out.filter_property = f.property;
            out.filter_op = f.op;
            out.filter_value = f.value;
        }
    }

    out.column_name = return_item_column_name(item, 0);

    out.multiplier = q.count_multiplication_factor;
    return true;
}

static bool node_label_literal(const PatternNode& n, std::string& out_label) {
    if (!n.label_expr) { out_label = ""; return true; }
    if (n.label_expr->kind == LabelExprKind::LITERAL) { out_label = n.label_expr->name; return true; }
    return false;
}

bool plan_streaming_edge_aggregate(const GqlQuery& q, EdgeAggPlan& out) {
    if (q.kind != QueryKind::SINGLE) return false;
    if (q.explain || q.profile) return false;
    if (!q.writes.empty()) return false;
    if (q.where_expr) return false;
    if (q.has_unnested_subquery) return false;
    if (q.distinct) return false;
    if (q.matches.size() != 1) return false;

    const auto& m = q.matches[0];
    if (m.is_optional) return false;
    if (m.shortest_path_kind != ShortestPathKind::NONE) return false;
    if (m.is_khop) return false;
    if (!m.path_variable.empty()) return false;
    if (m.limit.has_value()) return false;
    if (m.pattern.nodes.size() != 2 || m.pattern.edges.size() != 1) return false;

    const auto& edge = m.pattern.edges[0];
    if (edge.is_variable_length) return false;
    if (edge.where_expr) return false;
    if (!edge.properties.empty() || !edge.property_filters.empty()) return false;
    if (!edge.label_expr || edge.label_expr->kind != LabelExprKind::LITERAL) return false;
    out.rel_type = edge.label_expr->name;

    // Property map values that are expressions are only known once resolved against the incoming row,
    // which this plan bypasses; planning through them would silently drop the constraint.
    for (const auto& n : m.pattern.nodes) {
        if (!n.property_exprs.empty()) return false;
    }
    if (!edge.property_exprs.empty()) return false;

    for (const auto& item : q.returns) find_aggregates(item.expr.get(), out.aggs);
    for (const auto& spec : q.order_by) find_aggregates(spec.expr.get(), out.aggs);
    if (out.aggs.empty()) return false;

    // DISTINCT aggregates (e.g. count(DISTINCT x)) need per-group value dedup, which the streaming
    // accumulator does not perform; collect_list gathers a list the accumulator does not build. Both
    // fall back to the normal grouped aggregation path.
    for (const auto* agg : out.aggs) {
        if (agg->distinct) return false;
        // COLLECT, STDDEV and PERCENTILE need the whole value set (a list, sum-of-squares, or a sort),
        // which the streaming accumulator does not carry, so they take the materialising path.
        if (agg->fn_kind == AggregateKind::COLLECT ||
            agg->fn_kind == AggregateKind::STDDEV_POP || agg->fn_kind == AggregateKind::STDDEV_SAMP ||
            agg->fn_kind == AggregateKind::PERCENTILE_CONT || agg->fn_kind == AggregateKind::PERCENTILE_DISC) {
            return false;
        }
    }

    // Aggregates must be count(*) or over a single variable / one of its properties.
    std::string agg_var;
    for (const auto* agg : out.aggs) {
        if (!agg->expr) continue;  // count(*)
        std::string v;
        if (agg->expr->kind == ExpressionKind::VARIABLE) v = static_cast<const VariableExpr*>(agg->expr.get())->name;
        else if (agg->expr->kind == ExpressionKind::PROPERTY_LOOKUP) v = static_cast<const PropertyLookupExpr*>(agg->expr.get())->variable;
        else return false;
        if (v.empty()) return false;
        if (agg_var.empty()) agg_var = v;
        else if (agg_var != v) return false;  // aggregates span two variables
    }

    const auto& n0 = m.pattern.nodes[0];
    const auto& n1 = m.pattern.nodes[1];
    if (n0.variable.empty() || n1.variable.empty()) return false;
    // A self-loop pattern (both endpoints share one variable) needs the source==target join the
    // streaming runner never applies; it would count every edge instead of self-loops only.
    if (n0.variable == n1.variable) return false;

    const PatternNode* group_node = nullptr;
    const PatternNode* agg_node = nullptr;
    if (!agg_var.empty()) {
        if (agg_var == n0.variable) { agg_node = &n0; group_node = &n1; }
        else if (agg_var == n1.variable) { agg_node = &n1; group_node = &n0; }
        else return false;
    }

    // Grouping keys: the non-aggregate RETURN items; must reference a single variable.
    std::set<std::string> nonagg_vars;
    for (const auto& item : q.returns) {
        if (has_aggregates(item.expr.get())) continue;
        std::string v;
        if (item.expr->kind == ExpressionKind::VARIABLE) v = static_cast<const VariableExpr*>(item.expr.get())->name;
        else if (item.expr->kind == ExpressionKind::PROPERTY_LOOKUP) v = static_cast<const PropertyLookupExpr*>(item.expr.get())->variable;
        else return false;
        if (v.empty()) return false;
        nonagg_vars.insert(v);
        out.grouping_keys.push_back(item.expr.get());
    }
    if (nonagg_vars.size() > 1) return false;

    if (!group_node) {
        // pure count(*): choose the group node from the grouping keys, else default to n1.
        if (!nonagg_vars.empty()) {
            const std::string& gv = *nonagg_vars.begin();
            if (gv == n0.variable) { group_node = &n0; agg_node = &n1; }
            else if (gv == n1.variable) { group_node = &n1; agg_node = &n0; }
            else return false;
        } else {
            group_node = &n1; agg_node = &n0;
        }
    }
    if (!nonagg_vars.empty() && *nonagg_vars.begin() != group_node->variable) return false;

    std::string group_label, agg_label;
    if (!node_label_literal(*group_node, group_label)) return false;
    if (!node_label_literal(*agg_node, agg_label)) return false;
    if (!group_node->properties.empty() || !group_node->property_filters.empty() || group_node->where_expr) return false;
    if (!agg_node->properties.empty() || !agg_node->property_filters.empty() || agg_node->where_expr) return false;

    // Direction from group node toward aggregated node. RIGHT means nodes[0]->nodes[1].
    bool group_is_n0 = (group_node == &n0);
    if (edge.direction == EdgeDirection::ANY) out.dir = Direction::BOTH;
    else if (edge.direction == EdgeDirection::RIGHT) out.dir = group_is_n0 ? Direction::OUT : Direction::IN;
    else out.dir = group_is_n0 ? Direction::IN : Direction::OUT;

    // Non-aggregate ORDER BY expressions are evaluated against the group representative, which
    // binds only the group variable; a sort key on any other variable would evaluate to NIL and
    // order the groups arbitrarily (dropping the wrong ones under LIMIT).
    for (const auto& spec : q.order_by) {
        if (has_aggregates(spec.expr.get())) continue;
        std::string v;
        if (spec.expr->kind == ExpressionKind::VARIABLE) v = static_cast<const VariableExpr*>(spec.expr.get())->name;
        else if (spec.expr->kind == ExpressionKind::PROPERTY_LOOKUP) v = static_cast<const PropertyLookupExpr*>(spec.expr.get())->variable;
        else return false;
        if (v != group_node->variable) return false;
    }

    out.group_node = group_node;
    out.agg_node = agg_node;
    out.group_var = group_node->variable;
    out.agg_var = agg_node->variable;
    out.agg_label = agg_label;
    out.multiplier = q.count_multiplication_factor;
    return true;
}

/**
 * @brief Execute a group-anchored streaming aggregate (see EdgeAggPlan). Iterates the group-side
 *        nodes, folds each one's aggregated-side neighbours into per-group accumulators, then
 *        projects/sorts/limits the (small) group set exactly as the normal aggregate path does.
 */
seastar::future<QueryResult> run_streaming_edge_aggregate(
        ragedb::Graph& graph, std::shared_ptr<GqlQuery> query_ptr, EdgeAggPlan plan) {
    auto state = std::make_shared<EdgeAggRunState>();
    auto plan_ptr = std::make_shared<EdgeAggPlan>(std::move(plan));

    // Keep the group node's properties intact (grouping keys read them); the default pruner would
    // strip every property.
    ProjectionPruner group_pruner;
    group_pruner.whole_objects.insert(plan_ptr->group_var);

    // Whether any accumulator evaluates an expression against the row: pure count(*) folds never
    // look at the neighbour binding, so the per-neighbour row write is skipped entirely for them.
    const bool needs_neighbor_binding = std::any_of(plan_ptr->aggs.begin(), plan_ptr->aggs.end(),
        [](const AggregateExpr* a) { return a->expr != nullptr; });

    return get_start_nodes(graph, *plan_ptr->group_node, 0, group_pruner)
    .then([&graph, state, plan_ptr, needs_neighbor_binding](std::vector<Node> group_nodes) {
        auto nodes = std::make_shared<std::vector<Node>>(std::move(group_nodes));
        // Bounded fan-out instead of one-at-a-time: each group's neighbour fetch is an RPC, and
        // awaiting them serially leaves the reactor idle for num_groups x latency.
        return seastar::max_concurrent_for_each(nodes->begin(), nodes->end(), 32,
            [&graph, state, plan_ptr, needs_neighbor_binding](Node& g) {
                uint64_t gid = g.getId();
                return graph.shard.local().NodeGetNeighborsPeered(gid, plan_ptr->dir, plan_ptr->rel_type)
                .then([state, plan_ptr, needs_neighbor_binding, g = std::move(g)](std::vector<Node> neighbors) {
                    const auto& plan = *plan_ptr;
                    GqlRow rep;
                    rep.bindings[plan.group_var] = GqlValue(g);

                    bool any = false;
                    EdgeAggGroupState* gstate = nullptr;
                    GqlRow row;  // reused across neighbours: only the aggregated binding changes
                    for (auto& n : neighbors) {
                        if (!plan.agg_label.empty() && n.getType() != plan.agg_label) continue;
                        if (!any) {
                            std::vector<GqlValue> key;
                            for (const auto* gk : plan.grouping_keys) key.push_back(evaluate_expression(rep, gk));
                            gstate = &state->groups[key];
                            if (!gstate->initialized) {
                                gstate->rep = rep;
                                for (const auto* agg : plan.aggs) gstate->accs.push_back(EdgeAggAccumulator::make(agg));
                                gstate->initialized = true;
                            }
                            if (needs_neighbor_binding) row = rep;
                            any = true;
                        }
                        if (needs_neighbor_binding) {
                            row.bindings[plan.agg_var] = GqlValue(std::move(n));
                        }
                        for (auto& acc : gstate->accs) acc.add(row);
                    }
                });
            }
        ).then([state, plan_ptr, nodes] { (void)nodes; });
    }).then([query_ptr, state, plan_ptr] {
        const auto& query = *query_ptr;
        const auto& plan = *plan_ptr;

        QueryResult result;
        for (size_t i = 0; i < query.returns.size(); ++i) {
            result.column_names.push_back(return_item_column_name(query.returns[i], i));
        }

        // Ungrouped aggregate over an empty expansion still yields one row (e.g. count -> 0).
        if (state->groups.empty() && plan.grouping_keys.empty()) {
            EdgeAggGroupState empty_group;
            empty_group.rep = GqlRow{};
            for (const auto* agg : plan.aggs) empty_group.accs.push_back(EdgeAggAccumulator::make(agg));
            empty_group.initialized = true;
            state->groups[{}] = std::move(empty_group);
        }

        struct GroupRow { std::vector<GqlValue> projected; std::vector<GqlValue> sort_keys; };
        std::vector<GroupRow> rows;
        for (auto& [key, gstate] : state->groups) {
            (void)key;
            std::map<const AggregateExpr*, GqlValue> agg_results;
            for (size_t i = 0; i < plan.aggs.size(); ++i) {
                agg_results[plan.aggs[i]] = gstate.accs[i].finalize(plan.multiplier);
            }
            GroupRow gr;
            for (const auto& item : query.returns) {
                gr.projected.push_back(evaluate_group_expression(gstate.rep, agg_results, item.expr.get()));
            }
            for (const auto& spec : query.order_by) {
                gr.sort_keys.push_back(evaluate_group_expression(gstate.rep, agg_results, spec.expr.get()));
            }
            rows.push_back(std::move(gr));
        }

        const auto window = page_window(query);
        if (!query.order_by.empty()) {
            auto comp = [&query](const GroupRow& a, const GroupRow& b) {
                for (size_t i = 0; i < query.order_by.size(); ++i) {
                    int cmp = compare_gql_values(a.sort_keys[i], b.sort_keys[i]);
                    if (cmp != 0) return query.order_by[i].ascending ? (cmp < 0) : (cmp > 0);
                }
                return false;
            };
            if (window && *window < rows.size()) {
                std::partial_sort(rows.begin(), rows.begin() + static_cast<std::ptrdiff_t>(*window), rows.end(), comp);
                rows.resize(static_cast<size_t>(*window));
            } else {
                std::stable_sort(rows.begin(), rows.end(), comp);
            }
        } else if (window && *window < rows.size()) {
            rows.resize(static_cast<size_t>(*window));
        }
        apply_page(rows, query);
        result.is_paged = true;

        for (auto& gr : rows) result.rows.push_back(std::move(gr.projected));
        return result;
    });
}

/**
 * @brief Whether a query's single match statement can be driven through the streaming traversal
 *        sink: a plain (non-optional, non-search, non-fast-path) pattern with at least one edge
 *        and nothing that needs the materialised row set as a whole. The streamed shapes are the
 *        expansion-heavy ones that OOM when collected (tasks 018/020).
 */
static bool match_streamable(const MatchStatement& m) {
    if (m.is_optional || m.is_search || m.is_propagate || m.is_khop) return false;
    if (m.shortest_path_kind != ShortestPathKind::NONE) return false;
    if (m.algebraic_path_count || m.khop_count_only) return false;
    if (m.equivalence_partition_lookup || m.transitive_reachability_lookup) return false;
    if (!m.path_variable.empty()) return false;
    if (m.limit.has_value()) return false;
    // A plain node scan with no edges streams too: its rows are the scanned nodes themselves, which the
    // traversal pages and hands to the sink. Excluding it sent every aggregate or top-K over a bare label
    // (a date-filtered count of Posts, say) down the materialising path, which holds the whole label --
    // every node WITH its properties -- in one vector before the fold ever runs.
    return true;
}

bool stream_eligible(const GqlQuery& q) {
    if (q.kind != QueryKind::SINGLE) return false;
    if (q.explain || q.profile) return false;
    if (!q.writes.empty()) return false;
    if (q.has_unnested_subquery) return false;
    if (q.matches.size() != 1) return false;
    if (!q.let_bindings.empty()) return false; // LET adds computed columns; use the materialising path
    if (!q.for_bindings.empty()) return false; // FOR multiplies rows; the streamed heap counts matches
    return match_streamable(q.matches[0]);
}

/**
 * @brief Whether a grouped/ungrouped aggregate can be streamed through a MULTI-match chain: every
 *        match is individually streamable and the segment carries no shape that needs the whole
 *        materialised row set. This bounds the FoF + expansion + count(DISTINCT) class (tasks 018/029)
 *        that OOM-crashes when the full (frontier x expansion) join is collected before aggregating.
 *        Cross-match DIFFERENT-EDGES uniqueness is enforced on the completed row in the fold sink.
 */
bool stream_group_eligible(const GqlQuery& q) {
    if (q.kind != QueryKind::SINGLE) return false;
    if (q.explain || q.profile) return false;
    if (!q.writes.empty()) return false;
    if (q.has_unnested_subquery) return false;
    if (!q.let_bindings.empty()) return false;
    if (!q.for_bindings.empty()) return false; // FOR multiplies rows before the aggregate folds them
    if (q.matches.empty()) return false;
    for (const auto& m : q.matches) {
        if (!match_streamable(m)) return false;
    }
    return true;
}

std::string classify_execution_strategy(GqlQuery& q) {
    // The eligibility predicates deliberately return false under EXPLAIN/PROFILE (those need the
    // general plan tree, not a streaming bypass), so clear the flags for the probe and restore them.
    const bool saved_explain = q.explain;
    const bool saved_profile = q.profile;
    q.explain = false;
    q.profile = false;

    std::string strategy = "Materialising";
    if (q.let_bindings.empty() && q.for_bindings.empty()) {
        SimpleNodeCountPlan cplan;
        EdgeAggPlan eplan;
        if (try_plan_simple_node_count(q, cplan)) {
            strategy = "Streaming (node-count)";
        } else if (plan_streaming_edge_aggregate(q, eplan)) {
            strategy = "Streaming (edge-aggregate)";
        }
    }
    if (strategy == "Materialising") {
        bool has_agg = false;
        for (const auto& item : q.returns) {
            if (item.expr && has_aggregates(item.expr.get())) { has_agg = true; break; }
        }
        if (!has_agg) {
            for (const auto& spec : q.order_by) {
                if (has_aggregates(spec.expr.get())) { has_agg = true; break; }
            }
        }
        if (stream_eligible(q) && !q.distinct && !q.order_by.empty() && q.limit.has_value() && !has_agg) {
            strategy = "Streaming (top-K heap)";
        } else if (stream_group_eligible(q) && !q.distinct && has_agg) {
            std::vector<const AggregateExpr*> aggs;
            for (const auto& item : q.returns) find_aggregates(item.expr.get(), aggs);
            for (const auto& spec : q.order_by) find_aggregates(spec.expr.get(), aggs);
            bool needs_mat = false;
            for (const auto* a : aggs) {
                if (a->fn_kind == AggregateKind::COLLECT ||
                    a->fn_kind == AggregateKind::STDDEV_POP || a->fn_kind == AggregateKind::STDDEV_SAMP ||
                    a->fn_kind == AggregateKind::PERCENTILE_CONT || a->fn_kind == AggregateKind::PERCENTILE_DISC) {
                    needs_mat = true;
                    break;
                }
            }
            if (!aggs.empty() && !needs_mat) strategy = "Streaming (group fold)";
        }
    }

    q.explain = saved_explain;
    q.profile = saved_profile;
    return strategy;
}

/**
 * @brief Streamed ORDER BY + LIMIT: fold every matched row into a bounded top-K heap as the
 *        traversal produces it, so peak memory is K rows plus one traversal chunk instead of the
 *        whole expansion (the IC2/IC9 OOM class). Row-level filters (path modes, residual WHERE)
 *        are applied before the heap; the projection runs over the K winners at the end.
 */
seastar::future<QueryResult> run_streaming_topk(
        ragedb::Graph& graph, std::shared_ptr<GqlQuery> query_ptr,
        std::optional<std::vector<GqlRow>> incoming, const ProjectionPruner& pruner) {
    struct TopKEntry {
        std::vector<GqlValue> keys;
        GqlRow row;
    };
    struct TopKState {
        std::vector<TopKEntry> heap;
        size_t k = 0;
    };
    auto st = std::make_shared<TopKState>();
    // The heap must retain the whole page window (offset + limit): an OFFSET skips past the first rows of
    // the ordered result, so keeping only `limit` of them would discard exactly what the page returns.
    st->k = static_cast<size_t>(page_window(*query_ptr).value_or(*query_ptr->limit));

    // Orders entries by the ORDER BY specs; the heap is a max-heap under this comparator, so its
    // front is the current worst kept entry.
    auto entry_less = [query_ptr](const TopKEntry& a, const TopKEntry& b) {
        for (size_t i = 0; i < query_ptr->order_by.size(); ++i) {
            int cmp = compare_gql_values(a.keys[i], b.keys[i]);
            if (cmp != 0) return query_ptr->order_by[i].ascending ? (cmp < 0) : (cmp > 0);
        }
        return false;
    };

    auto sink = std::make_shared<GqlRowSink>();
    sink->consume = [st, query_ptr, entry_less](std::vector<GqlRow> rows) {
        const auto& stmt = query_ptr->matches[0];
        for (auto& r : rows) {
            if (!satisfies_match_path_modes(r, stmt.match_mode, stmt.path_mode, stmt.pattern)) continue;
            if (query_ptr->where_expr && !evaluate_expression(r, query_ptr->where_expr.get()).is_truthy()) continue;
            TopKEntry e;
            e.keys.reserve(query_ptr->order_by.size());
            for (const auto& spec : query_ptr->order_by) {
                e.keys.push_back(evaluate_expression(r, spec.expr.get()));
            }
            if (st->heap.size() < st->k) {
                e.row = std::move(r);
                st->heap.push_back(std::move(e));
                std::push_heap(st->heap.begin(), st->heap.end(), entry_less);
            } else if (st->k > 0 && entry_less(e, st->heap.front())) {
                e.row = std::move(r);
                std::pop_heap(st->heap.begin(), st->heap.end(), entry_less);
                st->heap.back() = std::move(e);
                std::push_heap(st->heap.begin(), st->heap.end(), entry_less);
            }
        }
        return seastar::make_ready_future<>();
    };

    auto rows_in = std::make_shared<std::vector<GqlRow>>(
        incoming.has_value() ? std::move(*incoming) : std::vector<GqlRow>{ GqlRow{} });
    return seastar::max_concurrent_for_each(rows_in->begin(), rows_in->end(), gql_stream_concurrency,
        [&graph, query_ptr, pruner, sink](GqlRow& in) {
            const auto& stmt = query_ptr->matches[0];
            return traverse_path_pattern(graph, stmt.pattern, in, 0, pruner, "", true, false,
                                         stmt.path_mode, sink.get()).discard_result();
        })
    .then([st, query_ptr, entry_less, rows_in, sink] {
        std::sort(st->heap.begin(), st->heap.end(), entry_less);
        apply_page(st->heap, *query_ptr);
        QueryResult res;
        res.is_sorted = true;
        res.is_paged = true;
        for (size_t i = 0; i < query_ptr->returns.size(); ++i) {
            res.column_names.push_back(return_item_column_name(query_ptr->returns[i], i));
        }
        for (auto& e : st->heap) {
            std::vector<GqlValue> projected;
            projected.reserve(query_ptr->returns.size());
            for (const auto& item : query_ptr->returns) {
                projected.push_back(evaluate_expression(e.row, item.expr.get()));
            }
            res.rows.push_back(std::move(projected));
        }
        return res;
    });
}

/**
 * @brief Collect the user-named edge variables that participate in a DIFFERENT EDGES match (GQL's
 *        default). Auto-generated (`_`-prefixed) edge variables are excluded, matching the batch path.
 */
static std::set<std::string> collect_different_edge_vars(const GqlQuery& q) {
    std::set<std::string> vars;
    for (const auto& stmt : q.matches) {
        if (stmt.match_mode == MatchMode::DIFFERENT_EDGES) {
            for (const auto& edge : stmt.pattern.edges) {
                if (!edge.variable.empty() && edge.variable[0] != '_') vars.insert(edge.variable);
            }
        }
    }
    return vars;
}

/** @brief Whether the relationships bound to the DIFFERENT-EDGES variables in a completed row are all
 *         distinct (the cross-match GQL uniqueness the batch path enforces at PathTraverser.cpp). */
static bool row_edges_all_distinct(const GqlRow& row, const std::set<std::string>& diff_edge_vars) {
    if (diff_edge_vars.empty()) return true;
    std::vector<uint64_t> rel_ids;
    for (const auto& var : diff_edge_vars) {
        auto it = row.bindings.find(var);
        if (it == row.bindings.end()) continue;
        const auto& val = it->second;
        if (val.type == GqlValue::RELATIONSHIP) {
            rel_ids.push_back(val.relationship->getId());
        } else if (val.type == GqlValue::RELATIONSHIP_LIST) {
            for (const auto& r : *val.relationship_list) rel_ids.push_back(r.getId());
        } else if (val.type == GqlValue::PATH) {
            for (const auto& r : val.path->GetRelationships()) rel_ids.push_back(r.getId());
        }
    }
    std::set<uint64_t> uniq(rel_ids.begin(), rel_ids.end());
    return uniq.size() == rel_ids.size();
}

/**
 * @brief Stream one input row through matches[idx..], emitting each fully-joined row to final_sink.
 *        Cross-match binding works because traverse_path_pattern uses already-bound variables in the
 *        row as pattern anchors; each match's path/mode constraint is checked on its own output before
 *        driving the next match. This keeps a multi-match expansion (FoF -> forum -> post) bounded to
 *        one traversal chunk instead of the whole join.
 */
static seastar::future<> stream_match_chain(
        ragedb::Graph& graph, std::shared_ptr<GqlQuery> query_ptr,
        std::shared_ptr<const ProjectionPruner> pruner, size_t idx, GqlRow row,
        std::shared_ptr<GqlRowSink> final_sink) {
    if (idx >= query_ptr->matches.size()) {
        return final_sink->consume(std::vector<GqlRow>{ std::move(row) });
    }
    const auto& stmt = query_ptr->matches[idx];
    auto next_sink = std::make_shared<GqlRowSink>();
    next_sink->consume = [&graph, query_ptr, pruner, idx, final_sink,
                          mode = stmt.match_mode, pmode = stmt.path_mode,
                          pattern = &stmt.pattern](std::vector<GqlRow> rows) {
        auto rows_ptr = std::make_shared<std::vector<GqlRow>>(std::move(rows));
        return seastar::max_concurrent_for_each(*rows_ptr, gql_stream_inner_concurrency,
            [&graph, query_ptr, pruner, idx, final_sink, mode, pmode, pattern](GqlRow& r) {
                if (!satisfies_match_path_modes(r, mode, pmode, *pattern)) {
                    return seastar::make_ready_future<>();
                }
                return stream_match_chain(graph, query_ptr, pruner, idx + 1, std::move(r), final_sink);
            }).finally([rows_ptr] {});
    };
    return traverse_path_pattern(graph, stmt.pattern, row, 0, *pruner, "", true, false,
                                 stmt.path_mode, next_sink.get())
        .discard_result().finally([next_sink] {});
}

/**
 * @brief Streamed grouped aggregation over one or more match statements (with or without piped input
 *        rows): every fully-joined row folds into per-group incremental accumulators as it is
 *        produced, so peak memory is O(groups) instead of the whole expansion (the FoF DISTINCT ->
 *        expand -> aggregate crash class, tasks 018/029). Grouping keys are the non-aggregate RETURN
 *        items, the group representative is its first row, and the final project/sort/limit runs over
 *        the group set.
 */
seastar::future<QueryResult> run_streaming_group_fold(
        ragedb::Graph& graph, std::shared_ptr<GqlQuery> query_ptr,
        std::optional<std::vector<GqlRow>> incoming, const ProjectionPruner& pruner,
        std::vector<const Expression*> grouping_keys, std::vector<const AggregateExpr*> aggs) {
    struct GroupState {
        GqlRow rep;
        std::vector<EdgeAggAccumulator> accs;
    };
    struct FoldState {
        std::map<std::vector<GqlValue>, GroupState, GqlValueVectorLess> groups;
    };
    auto st = std::make_shared<FoldState>();
    auto keys_ptr = std::make_shared<std::vector<const Expression*>>(std::move(grouping_keys));
    auto aggs_ptr = std::make_shared<std::vector<const AggregateExpr*>>(std::move(aggs));

    auto diff_edge_vars = std::make_shared<std::set<std::string>>(collect_different_edge_vars(*query_ptr));
    auto sink = std::make_shared<GqlRowSink>();
    sink->consume = [st, query_ptr, keys_ptr, aggs_ptr, diff_edge_vars](std::vector<GqlRow> rows) {
        for (auto& r : rows) {
            // Per-match path/mode constraints were checked in the chain; here the row is fully joined,
            // so apply the cross-match DIFFERENT-EDGES uniqueness and the segment WHERE, then fold.
            if (!row_edges_all_distinct(r, *diff_edge_vars)) continue;
            if (query_ptr->where_expr && !evaluate_expression(r, query_ptr->where_expr.get()).is_truthy()) continue;
            std::vector<GqlValue> key;
            key.reserve(keys_ptr->size());
            for (const auto* gk : *keys_ptr) key.push_back(evaluate_expression(r, gk));
            auto [it, inserted] = st->groups.try_emplace(std::move(key));
            if (inserted) {
                it->second.rep = r;
                it->second.accs.reserve(aggs_ptr->size());
                for (const auto* agg : *aggs_ptr) it->second.accs.push_back(EdgeAggAccumulator::make(agg));
            }
            for (auto& acc : it->second.accs) acc.add(r);
        }
        return seastar::make_ready_future<>();
    };

    auto pruner_ptr = std::make_shared<const ProjectionPruner>(pruner);
    auto rows_in = std::make_shared<std::vector<GqlRow>>(
        incoming.has_value() ? std::move(*incoming) : std::vector<GqlRow>{ GqlRow{} });
    return seastar::max_concurrent_for_each(rows_in->begin(), rows_in->end(), gql_stream_concurrency,
        [&graph, query_ptr, pruner_ptr, sink](GqlRow& in) {
            return stream_match_chain(graph, query_ptr, pruner_ptr, 0, in, sink);
        })
    .then([st, query_ptr, keys_ptr, aggs_ptr, rows_in, sink] {
        const auto& query = *query_ptr;

        QueryResult result;
        for (size_t i = 0; i < query.returns.size(); ++i) {
            result.column_names.push_back(return_item_column_name(query.returns[i], i));
        }

        // Ungrouped aggregate over an empty expansion still yields one row (e.g. count -> 0).
        if (st->groups.empty() && keys_ptr->empty()) {
            GroupState empty_group;
            empty_group.rep = GqlRow{};
            for (const auto* agg : *aggs_ptr) empty_group.accs.push_back(EdgeAggAccumulator::make(agg));
            st->groups[{}] = std::move(empty_group);
        }

        struct GroupRow {
            std::vector<GqlValue> projected;
            std::vector<GqlValue> sort_keys;
        };
        std::vector<GroupRow> rows;
        rows.reserve(st->groups.size());
        for (auto& [key, gstate] : st->groups) {
            (void)key;
            std::map<const AggregateExpr*, GqlValue> agg_results;
            for (size_t i = 0; i < aggs_ptr->size(); ++i) {
                agg_results[(*aggs_ptr)[i]] = gstate.accs[i].finalize(query.count_multiplication_factor);
            }
            GroupRow gr;
            for (const auto& item : query.returns) {
                gr.projected.push_back(evaluate_group_expression(gstate.rep, agg_results, item.expr.get()));
            }
            for (const auto& spec : query.order_by) {
                gr.sort_keys.push_back(evaluate_group_expression(gstate.rep, agg_results, spec.expr.get()));
            }
            rows.push_back(std::move(gr));
        }

        const auto window = page_window(query);
        if (!query.order_by.empty()) {
            auto comp = [&query](const GroupRow& a, const GroupRow& b) {
                for (size_t i = 0; i < query.order_by.size(); ++i) {
                    int cmp = compare_gql_values(a.sort_keys[i], b.sort_keys[i]);
                    if (cmp != 0) return query.order_by[i].ascending ? (cmp < 0) : (cmp > 0);
                }
                return false;
            };
            if (window && *window < rows.size()) {
                std::partial_sort(rows.begin(), rows.begin() + static_cast<std::ptrdiff_t>(*window), rows.end(), comp);
                rows.resize(static_cast<size_t>(*window));
            } else {
                std::stable_sort(rows.begin(), rows.end(), comp);
            }
        } else if (window && *window < rows.size()) {
            rows.resize(static_cast<size_t>(*window));
        }
        apply_page(rows, query);
        result.is_paged = true;

        for (auto& gr : rows) result.rows.push_back(std::move(gr.projected));
        return result;
    });
}

} // namespace ragedb::gql
