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
#include <algorithm>

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
        for (const auto& node : matches[i].pattern.nodes) {
            if (!node.variable.empty()) vars.insert(node.variable);
        }
        for (const auto& edge : matches[i].pattern.edges) {
            if (!edge.variable.empty()) vars.insert(edge.variable);
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

seastar::future<IntermediateResult> execute_match_chain_factorized(ragedb::Graph& graph, std::vector<MatchStatement> matches, size_t match_idx, IntermediateResult incoming, size_t limit, const ProjectionPruner& pruner, std::string sort_property, bool sort_ascending, bool sort_by_id) {
    if (match_idx >= matches.size()) {
        return seastar::make_ready_future<IntermediateResult>(std::move(incoming));
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
                futs.push_back(execute_match_chain_factorized(graph, std::move(single_stmt_vec), 0, incoming, limit, pruner));
            }

            return seastar::when_all_succeed(futs.begin(), futs.end())
            .then([&graph, remaining_matches = std::move(remaining_matches), limit, pruner, sort_property, sort_ascending, sort_by_id](std::vector<IntermediateResult> branch_results) mutable {
                if (branch_results.empty()) {
                    return seastar::make_ready_future<IntermediateResult>(IntermediateResult::empty());
                }
                
                IntermediateResult joined = std::move(branch_results[0]);
                for (size_t i = 1; i < branch_results.size(); ++i) {
                    joined = natural_join(std::move(joined), std::move(branch_results[i]), limit);
                }

                if (remaining_matches.empty()) {
                    return seastar::make_ready_future<IntermediateResult>(std::move(joined));
                }

                return execute_match_chain_factorized(graph, std::move(remaining_matches), 0, std::move(joined), limit, pruner, sort_property, sort_ascending, sort_by_id);
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

            return execute_match_chain_factorized(graph, std::move(first_stmt_vec), 0, std::move(incoming), limit, pruner)
            .then([&graph, remaining_matches = std::move(remaining_matches), limit, pruner, sort_property, sort_ascending, sort_by_id](IntermediateResult res_first) mutable {
                return execute_match_chain_factorized(graph, std::move(remaining_matches), 0, std::move(res_first), limit, pruner, sort_property, sort_ascending, sort_by_id);
            });
        }
    }

    const auto stmt = matches[match_idx];

    bool has_shared = false;
    for (const auto& node : stmt.pattern.nodes) {
        if (!node.variable.empty() && incoming_vars.count(node.variable)) {
            has_shared = true;
            break;
        }
    }
    for (const auto& edge : stmt.pattern.edges) {
        if (!edge.variable.empty() && incoming_vars.count(edge.variable)) {
            has_shared = true;
            break;
        }
    }

    if (!has_shared && !incoming_vars.empty()) {
        return traverse_path_pattern(graph, stmt.pattern, GqlRow{}, limit, pruner)
        .then([&graph, matches = std::move(matches), match_idx, incoming = std::move(incoming), stmt, limit, pruner](std::vector<GqlRow> pattern_rows) mutable {
            IntermediateResult pattern_res(std::move(pattern_rows));
            IntermediateResult joined;
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
            return execute_match_chain_factorized(graph, std::move(matches), match_idx + 1, std::move(joined), limit, pruner);
        });
    } else {
        incoming.ensure_flat();
        if (limit > 0 && incoming.rows.size() > limit) {
            incoming.rows.resize(limit);
        }
        std::vector<seastar::future<std::vector<GqlRow>>> futs;
        for (const auto& row : incoming.rows) {
            if (match_idx == 0) {
                futs.push_back(traverse_match_statement(graph, stmt, row, limit, pruner, sort_property, sort_ascending, sort_by_id));
            } else {
                futs.push_back(traverse_match_statement(graph, stmt, row, limit, pruner));
            }
        }

        return seastar::when_all_succeed(futs.begin(), futs.end())
        .then([&graph, matches = std::move(matches), match_idx, incoming = std::move(incoming), limit, pruner, sort_property, sort_ascending, sort_by_id](std::vector<std::vector<GqlRow>> nested) mutable {
            std::vector<GqlRow> next_rows;
            for (const auto& vec : nested) {
                next_rows.insert(next_rows.end(), vec.begin(), vec.end());
                if (limit > 0 && next_rows.size() >= limit) {
                    next_rows.resize(limit);
                    break;
                }
            }
            IntermediateResult res(std::move(next_rows));
            if (match_idx == 0 && (!sort_property.empty() || sort_by_id)) {
                res.is_sorted = true;
            } else {
                res.is_sorted = incoming.is_sorted;
            }
            return execute_match_chain_factorized(graph, std::move(matches), match_idx + 1, std::move(res), limit, pruner);
        });
    }
}

} // namespace ragedb::gql
