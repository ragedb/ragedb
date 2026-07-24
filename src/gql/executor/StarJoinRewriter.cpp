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

#include "StarJoinRewriter.h"
#include "PathTraverser.h"
#include "JoinHelpers.h"
#include <seastar/core/when_all.hh>
#include <seastar/core/thread.hh>
#include <algorithm>
#include <chrono>

namespace ragedb::gql {

std::optional<StarJoinCandidate> find_star_join_candidate(
    const std::vector<MatchStatement>& matches,
    size_t match_idx,
    const std::set<std::string>& incoming_vars
) {
    if (matches.size() - match_idx < 2) {
        return std::nullopt;
    }

    std::vector<std::set<std::string>> remaining_vars;
    for (size_t i = match_idx; i < matches.size(); ++i) {
        std::set<std::string> vars;
        if (matches[i].is_search || matches[i].is_propagate) {
            if (!matches[i].yield_var.empty()) vars.insert(matches[i].yield_var);
            if (!matches[i].yield_score_var.empty()) vars.insert(matches[i].yield_score_var);
            if (!matches[i].yield_depth_var.empty()) vars.insert(matches[i].yield_depth_var);
        } else {
            for (const auto& node : matches[i].pattern.nodes) {
                if (!node.variable.empty()) vars.insert(node.variable);
            }
            for (const auto& edge : matches[i].pattern.edges) {
                if (!edge.variable.empty()) vars.insert(edge.variable);
            }
        }
        remaining_vars.push_back(std::move(vars));
    }

    std::set<std::string> all_candidates;
    for (const auto& vars : remaining_vars) {
        all_candidates.insert(vars.begin(), vars.end());
    }

    for (const auto& central_var : all_candidates) {
        if (central_var.rfind("_exists_", 0) == 0) {
            continue;
        }

        std::vector<size_t> indices;
        for (size_t i = 0; i < remaining_vars.size(); ++i) {
            if (remaining_vars[i].count(central_var)) {
                indices.push_back(match_idx + i);
            }
        }

        if (indices.size() < 2) {
            continue;
        }

        bool valid = true;
        for (size_t i = 0; i < indices.size(); ++i) {
            for (size_t j = i + 1; j < indices.size(); ++j) {
                size_t idx_i = indices[i] - match_idx;
                size_t idx_j = indices[j] - match_idx;
                for (const auto& var : remaining_vars[idx_i]) {
                    if (var != central_var && remaining_vars[idx_j].count(var)) {
                        valid = false;
                        break;
                    }
                }
                if (!valid) break;
            }
            if (!valid) break;
        }

        if (!valid) continue;

        for (size_t idx : indices) {
            size_t rel_idx = idx - match_idx;
            for (const auto& var : remaining_vars[rel_idx]) {
                if (var != central_var && incoming_vars.count(var)) {
                    valid = false;
                    break;
                }
            }
            if (!valid) break;
        }

        if (valid) {
            return StarJoinCandidate{ central_var, std::move(indices) };
        }
    }

    return std::nullopt;
}

seastar::future<IntermediateResult> execute_match_chain_factorized(
    ragedb::Graph& graph,
    std::vector<MatchStatement> matches,
    size_t match_idx,
    IntermediateResult incoming,
    size_t limit,
    const ProjectionPruner& pruner,
    std::string sort_property,
    bool sort_ascending,
    bool sort_by_id,
    std::shared_ptr<GqlQuery> query_ptr
) {
    if (match_idx >= matches.size()) {
        return seastar::make_ready_future<IntermediateResult>(std::move(incoming));
    }

    const auto& stmt = matches[match_idx];
    if (stmt.is_optional && stmt.optional_group_id >= 0) {
        size_t group_end_idx = match_idx;
        while (group_end_idx + 1 < matches.size() &&
               matches[group_end_idx + 1].optional_group_id == stmt.optional_group_id) {
            group_end_idx++;
        }

        std::vector<MatchStatement> group_matches;
        for (size_t idx = match_idx; idx <= group_end_idx; ++idx) {
            MatchStatement copy = matches[idx];
            copy.is_optional = false; // Run group matches as non-optional
            group_matches.push_back(std::move(copy));
        }

        // Collect all variables introduced in this optional group
        std::set<std::string> group_vars;
        for (const auto& group_stmt : group_matches) {
            for (const auto& node : group_stmt.pattern.nodes) {
                if (!node.variable.empty()) group_vars.insert(node.variable);
            }
            for (const auto& edge : group_stmt.pattern.edges) {
                if (!edge.variable.empty()) group_vars.insert(edge.variable);
            }
            if (!group_stmt.path_variable.empty()) {
                group_vars.insert(group_stmt.path_variable);
            }
        }

        incoming.ensure_flat();
        std::vector<seastar::future<std::vector<GqlRow>>> futs;
        for (const auto& row : incoming.rows) {
            // Run the non-optional group matches for this single row
            futs.push_back(
                execute_match_chain_factorized(graph, group_matches, 0, IntermediateResult(std::vector<GqlRow>{row}), limit, pruner, "", true, false, query_ptr)
                .then([row, group_vars](IntermediateResult res) {
                    if (res.rows.empty()) {
                        // Group failed to match: return the original row with all group vars bound to NULL
                        GqlRow opt_row = row;
                        for (const auto& var : group_vars) {
                            if (opt_row.bindings.find(var) == opt_row.bindings.end()) {
                                opt_row.bindings[var] = GqlValue();
                            }
                        }
                        return std::vector<GqlRow>{opt_row};
                    }
                    res.ensure_flat();
                    return res.rows;
                })
            );
        }

        return seastar::when_all_succeed(futs.begin(), futs.end())
        .then([&graph, matches = std::move(matches), group_end_idx, limit, pruner, sort_property, sort_ascending, sort_by_id, query_ptr](std::vector<std::vector<GqlRow>> nested) mutable {
            std::vector<GqlRow> next_rows;
            for (const auto& vec : nested) {
                next_rows.insert(next_rows.end(), vec.begin(), vec.end());
                if (limit > 0 && next_rows.size() >= limit) {
                    next_rows.resize(limit);
                    break;
                }
            }
            return execute_match_chain_factorized(graph, std::move(matches), group_end_idx + 1, IntermediateResult(std::move(next_rows)), limit, pruner, sort_property, sort_ascending, sort_by_id, query_ptr);
        });
    }
    if (stmt.is_propagate) {
        // CALL algo.propagate(...) YIELD node, value, depth: a value-propagating first-claim BFS whose
        // arguments are correlated expressions evaluated per incoming row. The kernel
        // (Shard::PropagateBFSPeered) blocks on cross-shard futures internally (.get0()), which aborts on
        // the reactor thread (the HTTP/GQL path, smp>1). Run it inside a seastar::thread via seastar::async
        // -- the same bridge the alglib reachability/equivalence fast paths use -- then continue
        // asynchronously, fetching each reached node and cross-joining the yielded columns with the row.
        if (stmt.propagate_args.size() < 8) {
            throw std::runtime_error("algo.propagate expects at least 8 arguments "
                                     "(seeds, values, relTypes, direction, maxDepth, valueProp, order, cap)");
        }
        std::vector<std::shared_ptr<Expression>> args = stmt.propagate_args;
        std::string yield_node = stmt.yield_var;
        std::string yield_value = stmt.yield_score_var;
        std::string yield_depth = stmt.yield_depth_var;

        auto as_double = [](const GqlValue& v) -> double {
            if (v.type == GqlValue::PROPERTY) {
                if (std::holds_alternative<double>(v.property)) return std::get<double>(v.property);
                if (std::holds_alternative<int64_t>(v.property)) return static_cast<double>(std::get<int64_t>(v.property));
            }
            return 0.0;
        };
        auto as_string = [](const GqlValue& v) -> std::string {
            if (v.type == GqlValue::PROPERTY && std::holds_alternative<std::string>(v.property))
                return std::get<std::string>(v.property);
            return "";
        };
        auto as_int = [](const GqlValue& v) -> int64_t {
            if (v.type == GqlValue::PROPERTY) {
                if (std::holds_alternative<int64_t>(v.property)) return std::get<int64_t>(v.property);
                if (std::holds_alternative<double>(v.property)) return static_cast<int64_t>(std::get<double>(v.property));
            }
            return 0;
        };
        auto to_lower = [](std::string s) {
            std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
            return s;
        };

        incoming.ensure_flat();
        std::vector<seastar::future<std::vector<GqlRow>>> row_futs;
        for (const auto& in_row : incoming.rows) {
            std::vector<uint64_t> seeds;
            if (auto elems = as_list_elements(evaluate_expression(in_row, args[0].get()))) {
                for (const auto& e : *elems) if (e.type == GqlValue::NODE && e.node) seeds.push_back(e.node->getId());
            }
            std::vector<double> vals;
            if (auto elems = as_list_elements(evaluate_expression(in_row, args[1].get()))) {
                for (const auto& e : *elems) vals.push_back(as_double(e));
            }
            std::vector<std::string> rel_types;
            if (auto elems = as_list_elements(evaluate_expression(in_row, args[2].get()))) {
                for (const auto& e : *elems) rel_types.push_back(as_string(e));
            }
            std::string dir_s = to_lower(as_string(evaluate_expression(in_row, args[3].get())));
            Direction direction = (dir_s == "in") ? Direction::IN : (dir_s == "both") ? Direction::BOTH : Direction::OUT;
            uint32_t max_depth = static_cast<uint32_t>(as_int(evaluate_expression(in_row, args[4].get())));
            std::string value_prop = as_string(evaluate_expression(in_row, args[5].get()));
            bool order_desc = (to_lower(as_string(evaluate_expression(in_row, args[6].get()))) == "desc");
            uint32_t trunc_limit = static_cast<uint32_t>(as_int(evaluate_expression(in_row, args[7].get())));

            row_futs.push_back(
                seastar::async([&graph, seeds = std::move(seeds), vals = std::move(vals), rel_types = std::move(rel_types),
                                direction, max_depth, value_prop, order_desc, trunc_limit] {
                    return graph.shard.local().PropagateBFSPeered(seeds, vals, rel_types, direction, max_depth,
                                                                  value_prop, order_desc, trunc_limit, 0.0, "", 0, 0);
                }).then([&graph, in_row, yield_node, yield_value, yield_depth](std::vector<Shard::PropagateResult> results) {
                    std::vector<seastar::future<GqlRow>> per;
                    per.reserve(results.size());
                    for (const auto& r : results) {
                        double value = r.value;
                        int64_t depth = static_cast<int64_t>(r.depth);
                        per.push_back(graph.shard.local().NodeGetPeered(r.node).then(
                            [in_row, yield_node, yield_value, yield_depth, value, depth](Node node) {
                                GqlRow row = in_row;
                                row.bindings[yield_node] = GqlValue(std::move(node));
                                if (!yield_value.empty()) row.bindings[yield_value] = GqlValue(property_type_t(value));
                                if (!yield_depth.empty()) row.bindings[yield_depth] = GqlValue(property_type_t(depth));
                                return row;
                            }));
                    }
                    return seastar::when_all_succeed(per.begin(), per.end())
                        .then([](std::vector<GqlRow> rows) { return rows; });
                })
            );
        }

        return seastar::when_all_succeed(row_futs.begin(), row_futs.end())
        .then([&graph, matches = std::move(matches), match_idx, limit, pruner, sort_property, sort_ascending, sort_by_id, query_ptr](std::vector<std::vector<GqlRow>> nested) mutable {
            std::vector<GqlRow> next_rows;
            for (auto& v : nested) {
                next_rows.insert(next_rows.end(), std::make_move_iterator(v.begin()), std::make_move_iterator(v.end()));
                if (limit > 0 && next_rows.size() >= limit) { next_rows.resize(limit); break; }
            }
            return execute_match_chain_factorized(graph, std::move(matches), match_idx + 1, IntermediateResult(std::move(next_rows)), limit, pruner, sort_property, sort_ascending, sort_by_id, query_ptr);
        });
    }
    if (stmt.is_search) {
        bool is_node = graph.shard.local().NodeTypesGet().count(stmt.search_type) > 0;
        seastar::future<std::vector<std::pair<uint64_t, double>>> search_fut = is_node ?
            graph.shard.local().NodeIndexFTSSearchPeered(stmt.search_type, stmt.search_properties, stmt.search_string, stmt.search_options) :
            graph.shard.local().RelationshipIndexFTSSearchPeered(stmt.search_type, stmt.search_properties, stmt.search_string, stmt.search_options);

        auto start = std::chrono::steady_clock::now();
        return search_fut.then([&graph, stmt, is_node, matches = std::move(matches), match_idx, incoming = std::move(incoming), limit, pruner, query_ptr, start](std::vector<std::pair<uint64_t, double>> search_res) mutable {
            if (search_res.empty()) {
                IntermediateResult empty_res = stmt.is_optional ? incoming : IntermediateResult::empty();
                return execute_match_chain_factorized(graph, std::move(matches), match_idx + 1, std::move(empty_res), limit, pruner, "", true, false, query_ptr);
            }

            std::vector<seastar::future<GqlRow>> row_futs;
            for (const auto& [id, score] : search_res) {
                if (is_node) {
                    row_futs.push_back(graph.shard.local().NodeGetPeered(id).then([yield_var = stmt.yield_var, yield_score_var = stmt.yield_score_var, score](Node node) {
                        GqlRow row;
                        row.bindings[yield_var] = GqlValue(node);
                        row.bindings[yield_score_var] = GqlValue(property_type_t(score));
                        return row;
                    }));
                } else {
                    row_futs.push_back(graph.shard.local().RelationshipGetPeered(id).then([yield_var = stmt.yield_var, yield_score_var = stmt.yield_score_var, score](Relationship rel) {
                        GqlRow row;
                        row.bindings[yield_var] = GqlValue(rel);
                        row.bindings[yield_score_var] = GqlValue(property_type_t(score));
                        return row;
                    }));
                }
            }

            return seastar::when_all_succeed(row_futs.begin(), row_futs.end())
            .then([&graph, matches = std::move(matches), match_idx, incoming = std::move(incoming), stmt, limit, pruner, query_ptr, start](std::vector<GqlRow> search_rows) mutable {
                if (query_ptr && query_ptr->profile) {
                    auto end = std::chrono::steady_clock::now();
                    auto node = query_ptr->plan_nodes["match_" + std::to_string(stmt.id)];
                    if (node) {
                        node->actual_rows = search_rows.size();
                        node->time_ms = std::chrono::duration<double, std::milli>(end - start).count();
                    }
                }

                IntermediateResult search_res(std::move(search_rows));
                IntermediateResult joined;
                if (incoming.rows.size() == 1 && incoming.rows[0].bindings.empty()) {
                    joined = std::move(search_res);
                } else {
                    auto join_start = std::chrono::steady_clock::now();
                    if (stmt.is_optional) {
                        joined = left_outer_join(std::move(incoming), std::move(search_res), incoming.freevars(), {stmt.yield_var, stmt.yield_score_var});
                    } else {
                        joined = natural_join(std::move(incoming), std::move(search_res), limit);
                    }
                    if (query_ptr && query_ptr->profile) {
                        auto join_end = std::chrono::steady_clock::now();
                        std::string join_key = (stmt.is_optional ? "left_outer_join_" : "natural_join_") + std::to_string(stmt.id);
                        auto join_node = query_ptr->plan_nodes[join_key];
                        if (join_node) {
                            join_node->actual_rows = joined.rows.size();
                            join_node->time_ms = std::chrono::duration<double, std::milli>(join_end - join_start).count();
                        }
                    }
                }

                return execute_match_chain_factorized(graph, std::move(matches), match_idx + 1, std::move(joined), limit, pruner, "", true, false, query_ptr);
            });
        });
    }

    auto incoming_vars = incoming.freevars();
    
    if (auto candidate = find_star_join_candidate(matches, match_idx, incoming_vars)) {
        std::string central_var = candidate->central_var;
        const auto& indices = candidate->match_indices;

        if (incoming_vars.count(central_var)) {
            std::vector<MatchStatement> branch_matches;
            std::vector<MatchStatement> remaining_matches;
            std::set<size_t> S_set(indices.begin(), indices.end());

            for (size_t i = match_idx; i < matches.size(); ++i) {
                if (S_set.count(i)) {
                    branch_matches.push_back(std::move(matches[i]));
                } else {
                    remaining_matches.push_back(std::move(matches[i]));
                }
            }

            std::vector<seastar::future<IntermediateResult>> futs;
            for (auto& branch_stmt : branch_matches) {
                std::vector<MatchStatement> single_stmt_vec;
                single_stmt_vec.push_back(std::move(branch_stmt));
                futs.push_back(execute_match_chain_factorized(graph, std::move(single_stmt_vec), 0, incoming, limit, pruner, "", true, false, query_ptr));
            }

            return seastar::when_all_succeed(futs.begin(), futs.end())
            .then([&graph, remaining_matches = std::move(remaining_matches), limit, pruner, sort_property, sort_ascending, sort_by_id, query_ptr, central_var](std::vector<IntermediateResult> branch_results) mutable {
                if (branch_results.empty()) {
                    return seastar::make_ready_future<IntermediateResult>(IntermediateResult::empty());
                }
                
                auto start = std::chrono::steady_clock::now();
                IntermediateResult joined = std::move(branch_results[0]);
                for (size_t i = 1; i < branch_results.size(); ++i) {
                    joined = natural_join(std::move(joined), std::move(branch_results[i]), limit);
                }

                if (query_ptr && query_ptr->profile) {
                    auto end = std::chrono::steady_clock::now();
                    auto node = query_ptr->plan_nodes["star_join_" + central_var];
                    if (node) {
                        node->actual_rows = joined.rows.size();
                        node->time_ms = std::chrono::duration<double, std::milli>(end - start).count();
                    }
                }

                if (remaining_matches.empty()) {
                    return seastar::make_ready_future<IntermediateResult>(std::move(joined));
                }

                return execute_match_chain_factorized(graph, std::move(remaining_matches), 0, std::move(joined), limit, pruner, sort_property, sort_ascending, sort_by_id, query_ptr);
            });
        } else {
            size_t first_idx = indices[0];
            std::vector<MatchStatement> first_stmt_vec;
            first_stmt_vec.push_back(std::move(matches[first_idx]));
            
            std::vector<MatchStatement> remaining_matches;
            for (size_t i = match_idx; i < matches.size(); ++i) {
                if (i != first_idx) {
                    remaining_matches.push_back(std::move(matches[i]));
                }
            }

            return execute_match_chain_factorized(graph, std::move(first_stmt_vec), 0, std::move(incoming), limit, pruner, "", true, false, query_ptr)
            .then([&graph, remaining_matches = std::move(remaining_matches), limit, pruner, sort_property, sort_ascending, sort_by_id, query_ptr](IntermediateResult res_first) mutable {
                return execute_match_chain_factorized(graph, std::move(remaining_matches), 0, std::move(res_first), limit, pruner, sort_property, sort_ascending, sort_by_id, query_ptr);
            });
        }
    }

    bool has_shared = false;
    for (const auto& node : stmt.pattern.nodes) {
        if (!node.variable.empty() && incoming_vars.count(node.variable)) {
            has_shared = true;
            break;
        }
    }
    if (!has_shared) {
        for (const auto& edge : stmt.pattern.edges) {
            if (!edge.variable.empty() && incoming_vars.count(edge.variable)) {
                has_shared = true;
                break;
            }
        }
    }

    // A property map value that is an expression is resolved against the incoming row, so a statement
    // carrying one is never independent of those rows even when it shares no pattern variable with them:
    // running it standalone would evaluate the expression against an empty row and match nothing. Drive
    // it per row instead.
    if (!has_shared) {
        for (const auto& node : stmt.pattern.nodes) {
            if (!node.property_exprs.empty()) {
                has_shared = true;
                break;
            }
        }
    }
    if (!has_shared) {
        for (const auto& edge : stmt.pattern.edges) {
            if (!edge.property_exprs.empty()) {
                has_shared = true;
                break;
            }
        }
    }

    if (!has_shared && !incoming_vars.empty()) {
        auto start = std::chrono::steady_clock::now();
        size_t stmt_limit = stmt.limit ? *stmt.limit : limit;
        return traverse_path_pattern(graph, stmt.pattern, GqlRow{}, stmt_limit, pruner)
        .then([&graph, matches = std::move(matches), match_idx, incoming = std::move(incoming), stmt, limit, pruner, query_ptr, start](std::vector<GqlRow> pattern_rows) mutable {
            if (query_ptr && query_ptr->profile) {
                auto end = std::chrono::steady_clock::now();
                auto node = query_ptr->plan_nodes["match_" + std::to_string(stmt.id)];
                if (node) {
                    node->actual_rows = pattern_rows.size();
                    node->time_ms = std::chrono::duration<double, std::milli>(end - start).count();
                }
            }

            IntermediateResult pattern_res(std::move(pattern_rows));
            IntermediateResult joined;
            auto join_start = std::chrono::steady_clock::now();
            if (stmt.is_optional) {
                std::set<std::string> new_vars;
                for (const auto& node : stmt.pattern.nodes) {
                    if (!node.variable.empty()) new_vars.insert(node.variable);
                }
                for (const auto& edge : stmt.pattern.edges) {
                    if (!edge.variable.empty()) new_vars.insert(edge.variable);
                }
                joined = left_outer_join(std::move(incoming), std::move(pattern_res), incoming.freevars(), new_vars);
            } else {
                joined = natural_join(std::move(incoming), std::move(pattern_res), limit);
            }

            if (query_ptr && query_ptr->profile) {
                auto join_end = std::chrono::steady_clock::now();
                std::string join_key = (stmt.is_optional ? "left_outer_join_" : "natural_join_") + std::to_string(stmt.id);
                auto node = query_ptr->plan_nodes[join_key];
                if (node) {
                    node->actual_rows = joined.rows.size();
                    node->time_ms = std::chrono::duration<double, std::milli>(join_end - join_start).count();
                }
            }

            return execute_match_chain_factorized(graph, std::move(matches), match_idx + 1, std::move(joined), limit, pruner, "", true, false, query_ptr);
        });
    } else {
        incoming.ensure_flat();
        if (limit > 0 && incoming.rows.size() > limit) {
            incoming.rows.resize(limit);
        }
        std::vector<seastar::future<std::vector<GqlRow>>> futs;
        size_t stmt_limit = stmt.limit ? *stmt.limit : limit;
        for (const auto& row : incoming.rows) {
            if (match_idx == 0) {
                futs.push_back(traverse_match_statement(graph, stmt, row, stmt_limit, pruner, sort_property, sort_ascending, sort_by_id));
            } else {
                futs.push_back(traverse_match_statement(graph, stmt, row, stmt_limit, pruner));
            }
        }

        auto start = std::chrono::steady_clock::now();
        return seastar::when_all_succeed(futs.begin(), futs.end())
        .then([&graph, matches = std::move(matches), match_idx, incoming = std::move(incoming), limit, pruner, sort_property, sort_by_id, query_ptr, start, stmt](std::vector<std::vector<GqlRow>> nested) mutable {
            std::vector<GqlRow> next_rows;
            for (const auto& vec : nested) {
                next_rows.insert(next_rows.end(), vec.begin(), vec.end());
                if (limit > 0 && next_rows.size() >= limit) {
                    next_rows.resize(limit);
                    break;
                }
            }

            if (query_ptr && query_ptr->profile) {
                auto end = std::chrono::steady_clock::now();
                auto node = query_ptr->plan_nodes["match_" + std::to_string(stmt.id)];
                if (node) {
                    node->actual_rows = next_rows.size();
                    node->time_ms = std::chrono::duration<double, std::milli>(end - start).count();
                }
            }

            IntermediateResult res(std::move(next_rows));
            if (match_idx == 0 && (!sort_property.empty() || sort_by_id)) {
                res.is_sorted = true;
            } else {
                res.is_sorted = incoming.is_sorted;
            }
            return execute_match_chain_factorized(graph, std::move(matches), match_idx + 1, std::move(res), limit, pruner, "", true, false, query_ptr);
        });
    }
}

} // namespace ragedb::gql
