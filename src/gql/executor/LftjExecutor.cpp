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

#include "LftjExecutor.h"
#include "HoneycombExecutor.h"
#include <seastar/core/smp.hh>
#include <seastar/core/when_all.hh>
#include <limits>
#include <algorithm>
#include <functional>
#include <unordered_map>
#include <unordered_set>
#include <chrono>
#include "../../graph/Graph.h"

namespace ragedb::gql {

// 1. Galloping seek algorithm (exponential search)
static size_t galloping_seek(const std::vector<uint64_t>& values, size_t start, size_t end, uint64_t target) {
    if (start >= end) return end;
    if (values[start] >= target) return start;

    size_t step = 1;
    size_t prev = start;
    size_t curr = start + step;

    while (curr < end && values[curr] < target) {
        prev = curr;
        step *= 2;
        curr = start + step;
    }

    if (curr >= end) {
        curr = end - 1;
    }

    // Binary search in [prev, curr]
    auto it = std::lower_bound(values.begin() + static_cast<std::ptrdiff_t>(prev),
                               values.begin() + static_cast<std::ptrdiff_t>(curr) + 1, target);
    return static_cast<size_t>(std::distance(values.begin(), it));
}

// Iterator representing pointer at a trie level of a relation
struct LeapfrogIterator {
    const std::vector<uint64_t>* values;
    size_t start;
    size_t end;
    size_t curr_idx;
    
    uint64_t key() const {
        return (*values)[curr_idx];
    }
    
    bool is_at_end() const {
        return curr_idx >= end;
    }
};

// 2. Leapfrog triejoin multi-way intersection logic (WCOJ intersection loop)
static std::vector<uint64_t> leapfrog_intersect(std::vector<LeapfrogIterator>& iters) {
    std::vector<uint64_t> result;
    if (iters.empty()) return result;
    
    for (const auto& it : iters) {
        if (it.is_at_end()) return result;
    }
    
    size_t M = iters.size();
    std::vector<size_t> p(M);
    for (size_t i = 0; i < M; ++i) p[i] = i;

    auto sort_p = [&]() {
        std::sort(p.begin(), p.end(), [&](size_t a, size_t b) {
            return iters[a].key() < iters[b].key();
        });
    };
    sort_p();

    while (true) {
        size_t smallest_idx = p[0];
        size_t largest_idx = p[M - 1];
        uint64_t smallest_val = iters[smallest_idx].key();
        uint64_t largest_val = iters[largest_idx].key();
        
        if (smallest_val == largest_val) {
            result.push_back(smallest_val);
            iters[smallest_idx].curr_idx = galloping_seek(*iters[smallest_idx].values, iters[smallest_idx].curr_idx + 1, iters[smallest_idx].end, smallest_val + 1);
            if (iters[smallest_idx].is_at_end()) {
                break;
            }
        } else {
            iters[smallest_idx].curr_idx = galloping_seek(*iters[smallest_idx].values, iters[smallest_idx].curr_idx, iters[smallest_idx].end, largest_val);
            if (iters[smallest_idx].is_at_end()) {
                break;
            }
        }
        
        size_t i = 0;
        while (i < M - 1 && iters[p[i]].key() > iters[p[i + 1]].key()) {
            std::swap(p[i], p[i + 1]);
            i++;
        }
    }
    return result;
}

// 3. Helper to build contiguous CoCo trie level arrays
struct TupleRange { size_t start; size_t end; };
static CoCoIndex build_coco_index(const std::vector<std::vector<uint64_t>>& tuples, size_t k) {
    CoCoIndex index;
    if (tuples.empty()) return index;
    index.levels.resize(k);
    
    std::vector<TupleRange> current_ranges = { {0, tuples.size()} };
    
    for (size_t j = 0; j < k; ++j) {
        auto& level = index.levels[j];
        level.offsets.push_back(0);
        std::vector<TupleRange> next_ranges;
        
        for (const auto& rng : current_ranges) {
            if (rng.start < rng.end) {
                uint64_t last_val = tuples[rng.start][j];
                level.values.push_back(last_val);
                size_t segment_start = rng.start;
                for (size_t r = rng.start + 1; r < rng.end; ++r) {
                    uint64_t val = tuples[r][j];
                    if (val != last_val) {
                        next_ranges.push_back({segment_start, r});
                        last_val = val;
                        level.values.push_back(val);
                        segment_start = r;
                    }
                }
                next_ranges.push_back({segment_start, rng.end});
            }
            level.offsets.push_back(level.values.size());
        }
        current_ranges = std::move(next_ranges);
    }
    return index;
}

seastar::future<IntermediateResult> LftjExecutor::execute(
    ragedb::Graph& graph,
    const std::vector<MatchStatement>& matches,
    size_t limit
) {
    if (matches.empty()) {
        return seastar::make_ready_future<IntermediateResult>(IntermediateResult::empty());
    }

    // Assign unique names to anonymous nodes and edges
    std::vector<std::vector<std::string>> node_vars(matches.size());
    std::vector<std::vector<std::string>> edge_vars(matches.size());
    for (size_t m_idx = 0; m_idx < matches.size(); ++m_idx) {
        if (matches[m_idx].is_search || matches[m_idx].is_propagate) continue;
        node_vars[m_idx].resize(matches[m_idx].pattern.nodes.size());
        for (size_t n_idx = 0; n_idx < matches[m_idx].pattern.nodes.size(); ++n_idx) {
            std::string nv = matches[m_idx].pattern.nodes[n_idx].variable;
            if (nv.empty()) {
                nv = "_n_anon_" + std::to_string(m_idx) + "_" + std::to_string(n_idx);
            }
            node_vars[m_idx][n_idx] = nv;
        }
        edge_vars[m_idx].resize(matches[m_idx].pattern.edges.size());
        for (size_t e_idx = 0; e_idx < matches[m_idx].pattern.edges.size(); ++e_idx) {
            std::string ev = matches[m_idx].pattern.edges[e_idx].variable;
            if (ev.empty()) {
                ev = "_e_anon_" + std::to_string(m_idx) + "_" + std::to_string(e_idx);
            }
            edge_vars[m_idx][e_idx] = ev;
        }
    }

    std::vector<seastar::future<std::vector<Relationship>>> rels_futs;
    struct RelationMeta {
        std::string u;
        std::string e;
        std::string v;
        EdgeDirection dir;
    };
    std::vector<RelationMeta> rel_metas;

    for (size_t m_idx = 0; m_idx < matches.size(); ++m_idx) {
        if (matches[m_idx].is_search || matches[m_idx].is_propagate) continue;
        const auto& pattern = matches[m_idx].pattern;
        for (size_t e_idx = 0; e_idx < pattern.edges.size(); ++e_idx) {
            const auto& edge = pattern.edges[e_idx];
            std::string rel_type = "";
            if (edge.label_expr && edge.label_expr->kind == LabelExprKind::LITERAL) {
                rel_type = edge.label_expr->name;
            }
            
            if (!rel_type.empty()) {
                auto count_fut = graph.shard.local().AllRelationshipsCountPeered(rel_type)
                .then([&graph, rel_type](uint64_t count) {
                    return graph.shard.local().AllRelationshipsPeered(rel_type, 0, count);
                });
                rels_futs.push_back(std::move(count_fut));
            } else {
                auto count_fut = graph.shard.local().AllRelationshipsCountPeered()
                .then([&graph](uint64_t count) {
                    return graph.shard.local().AllRelationshipsPeered(0, count);
                });
                rels_futs.push_back(std::move(count_fut));
            }
            
            rel_metas.push_back({
                node_vars[m_idx][e_idx],
                edge_vars[m_idx][e_idx],
                node_vars[m_idx][e_idx+1],
                edge.direction
            });
        }
    }

    if (rels_futs.empty()) {
        return seastar::make_ready_future<IntermediateResult>(IntermediateResult::empty());
    }

    return seastar::when_all_succeed(rels_futs.begin(), rels_futs.end())
    .then([&graph, rel_metas, matches, limit, node_vars, edge_vars](std::vector<std::vector<Relationship>> all_rels) {
        std::unordered_map<std::string, uint16_t> var_type_ids;
        for (size_t m_idx = 0; m_idx < matches.size(); ++m_idx) {
            if (matches[m_idx].is_search || matches[m_idx].is_propagate) continue;
            for (size_t n_idx = 0; n_idx < matches[m_idx].pattern.nodes.size(); ++n_idx) {
                const auto& node = matches[m_idx].pattern.nodes[n_idx];
                if (node.label_expr && node.label_expr->kind == LabelExprKind::LITERAL) {
                    uint16_t type_id = graph.shard.local().NodeTypeGetTypeId(node.label_expr->name);
                    var_type_ids[node_vars[m_idx][n_idx]] = type_id;
                }
            }
        }

        std::vector<HoneycombRelation> relations;
        for (size_t i = 0; i < all_rels.size(); ++i) {
            const auto& rels = all_rels[i];
            const auto& meta = rel_metas[i];

            HoneycombRelation hr;
            hr.variables = { meta.u, meta.e, meta.v };
            hr.columns.resize(3);
            hr.columns[0].reserve(rels.size());
            hr.columns[1].reserve(rels.size());
            hr.columns[2].reserve(rels.size());

            bool check_u = var_type_ids.count(meta.u) > 0;
            uint16_t u_type_id = check_u ? var_type_ids[meta.u] : 0;
            bool check_v = var_type_ids.count(meta.v) > 0;
            uint16_t v_type_id = check_v ? var_type_ids[meta.v] : 0;

            for (const auto& r : rels) {
                uint64_t src = r.getStartingNodeId();
                uint64_t dst = r.getEndingNodeId();
                uint64_t edge_id = r.getId();

                if (meta.dir == EdgeDirection::RIGHT) {
                    if (check_u && Shard::externalToTypeId(src) != u_type_id) continue;
                    if (check_v && Shard::externalToTypeId(dst) != v_type_id) continue;
                    hr.columns[0].push_back(src);
                    hr.columns[1].push_back(edge_id);
                    hr.columns[2].push_back(dst);
                } else if (meta.dir == EdgeDirection::LEFT) {
                    if (check_u && Shard::externalToTypeId(dst) != u_type_id) continue;
                    if (check_v && Shard::externalToTypeId(src) != v_type_id) continue;
                    hr.columns[0].push_back(dst);
                    hr.columns[1].push_back(edge_id);
                    hr.columns[2].push_back(src);
                } else {
                    if ((!check_u || Shard::externalToTypeId(src) == u_type_id) &&
                        (!check_v || Shard::externalToTypeId(dst) == v_type_id)) {
                        hr.columns[0].push_back(src);
                        hr.columns[1].push_back(edge_id);
                        hr.columns[2].push_back(dst);
                    }
                    if ((!check_u || Shard::externalToTypeId(dst) == u_type_id) &&
                        (!check_v || Shard::externalToTypeId(src) == v_type_id)) {
                        hr.columns[0].push_back(dst);
                        hr.columns[1].push_back(edge_id);
                        hr.columns[2].push_back(src);
                    }
                }
            }
            relations.push_back(std::move(hr));
        }

        std::vector<std::string> global_vars;
        std::set<std::string> seen_vars;
        for (size_t m_idx = 0; m_idx < matches.size(); ++m_idx) {
            if (matches[m_idx].is_search || matches[m_idx].is_propagate) continue;
            for (const auto& nv : node_vars[m_idx]) {
                if (seen_vars.insert(nv).second) {
                    global_vars.push_back(nv);
                }
            }
            for (const auto& ev : edge_vars[m_idx]) {
                if (seen_vars.insert(ev).second) {
                    global_vars.push_back(ev);
                }
            }
        }

        std::set<std::string> edge_variables_set;
        for (const auto& ev_list : edge_vars) {
            for (const auto& ev : ev_list) {
                edge_variables_set.insert(ev);
            }
        }

        // Run WCOJ index building and solve synchronously on Seastar thread
        std::unordered_map<std::string, size_t> global_var_indices;
        for (size_t i = 0; i < global_vars.size(); ++i) {
            global_var_indices[global_vars[i]] = i;
        }

        std::vector<CoCoIndex> indexes;
        indexes.reserve(relations.size());

        for (auto& R : relations) {
            size_t k = R.variables.size();
            if (k == 0 || R.columns.empty() || R.columns[0].empty()) {
                indexes.push_back(CoCoIndex{});
                continue;
            }

            struct ColMapping {
                std::string var;
                size_t orig_col_idx;
            };
            std::vector<ColMapping> mappings;
            for (size_t i = 0; i < k; ++i) {
                mappings.push_back({R.variables[i], i});
            }
            std::sort(mappings.begin(), mappings.end(), [&](const ColMapping& a, const ColMapping& b) {
                return global_var_indices[a.var] < global_var_indices[b.var];
            });

            std::vector<std::vector<uint64_t>> sorted_columns(k);
            std::vector<std::string> sorted_variables(k);
            for (size_t i = 0; i < k; ++i) {
                sorted_variables[i] = mappings[i].var;
                sorted_columns[i] = std::move(R.columns[mappings[i].orig_col_idx]);
            }
            R.variables = std::move(sorted_variables);
            R.columns = std::move(sorted_columns);

            size_t N = R.columns[0].size();
            std::vector<std::vector<uint64_t>> tuples(N, std::vector<uint64_t>(k));
            for (size_t r = 0; r < N; ++r) {
                for (size_t c = 0; c < k; ++c) {
                    tuples[r][c] = R.columns[c][r];
                }
            }

            std::sort(tuples.begin(), tuples.end());
            indexes.push_back(build_coco_index(tuples, k));
        }

        std::vector<std::vector<uint64_t>> results_ids;
        std::vector<std::vector<size_t>> parent_idx(relations.size());
        for (size_t i = 0; i < relations.size(); ++i) {
            parent_idx[i].assign(relations[i].variables.size() + 1, 0);
        }

        std::set<uint64_t> bound_edges;
        std::vector<uint64_t> current_binding(global_vars.size());

        struct RelCoverage {
            size_t rel_idx;
            size_t level;
        };
        std::vector<std::vector<RelCoverage>> coverage(global_vars.size());
        for (size_t d = 0; d < global_vars.size(); ++d) {
            const auto& var = global_vars[d];
            for (size_t i = 0; i < relations.size(); ++i) {
                const auto& R = relations[i];
                for (size_t j = 0; j < R.variables.size(); ++j) {
                    if (R.variables[j] == var) {
                       coverage[d].push_back({i, j});
                    }
                }
            }
        }

        std::function<void(size_t)> solve = [&](size_t depth) {
            if (limit > 0 && results_ids.size() >= limit) return;
            if (depth == global_vars.size()) {
                results_ids.push_back(current_binding);
                return;
            }

            const auto& var = global_vars[depth];
            bool is_edge_var = edge_variables_set.count(var) > 0;

            std::vector<LeapfrogIterator> iters;
            iters.reserve(coverage[depth].size());

            for (const auto& cov : coverage[depth]) {
                size_t i = cov.rel_idx;
                size_t j = cov.level;
                if (indexes[i].levels.empty()) return;
                const auto& level = indexes[i].levels[j];
                if (level.values.empty()) return;

                size_t p = parent_idx[i][j];
                if (p + 1 >= level.offsets.size()) return;

                size_t start = level.offsets[p];
                size_t end = level.offsets[p + 1];
                if (start < end) {
                    iters.push_back({ &level.values, start, end, start });
                } else {
                    return;
                }
            }

            if (iters.empty()) return;

            std::vector<uint64_t> intersection = leapfrog_intersect(iters);
            for (uint64_t v : intersection) {
                if (is_edge_var) {
                    if (bound_edges.count(v)) continue;
                    bound_edges.insert(v);
                }

                current_binding[depth] = v;

                for (const auto& cov : coverage[depth]) {
                    size_t i = cov.rel_idx;
                    size_t j = cov.level;
                    const auto& level = indexes[i].levels[j];
                    size_t p = parent_idx[i][j];
                    size_t start = level.offsets[p];
                    size_t end = level.offsets[p + 1];
                    size_t idx = galloping_seek(level.values, start, end, v);
                    parent_idx[i][j + 1] = idx;
                }

                solve(depth + 1);

                if (is_edge_var) {
                    bound_edges.erase(v);
                }
            }
        };

        solve(0);

        if (results_ids.empty()) {
            return seastar::make_ready_future<IntermediateResult>(IntermediateResult::empty());
        }

        std::vector<uint64_t> node_ids;
        std::vector<uint64_t> rel_ids;
        std::set<uint64_t> unique_nodes;
        std::set<uint64_t> unique_rels;

        std::vector<bool> var_is_node(global_vars.size());
        for (size_t i = 0; i < global_vars.size(); ++i) {
            var_is_node[i] = (edge_variables_set.count(global_vars[i]) == 0);
        }

        for (const auto& row : results_ids) {
            for (size_t i = 0; i < global_vars.size(); ++i) {
                uint64_t id = row[i];
                if (var_is_node[i]) {
                    if (unique_nodes.insert(id).second) {
                        node_ids.push_back(id);
                    }
                } else {
                    if (unique_rels.insert(id).second) {
                        rel_ids.push_back(id);
                    }
                }
            }
        }

        auto get_nodes_fut = graph.shard.local().NodesGetPeered(node_ids);
        auto get_rels_fut = graph.shard.local().RelationshipsGetPeered(rel_ids);

        return seastar::when_all_succeed(std::move(get_nodes_fut), std::move(get_rels_fut))
        .then([results_ids = std::move(results_ids), global_vars, var_is_node, node_ids, rel_ids](auto tuple) {
            auto nodes = std::move(std::get<0>(tuple));
            auto rels = std::move(std::get<1>(tuple));

            std::unordered_map<uint64_t, Node> node_map;
            for (size_t i = 0; i < node_ids.size(); ++i) {
                node_map[node_ids[i]] = nodes[i];
            }

            std::unordered_map<uint64_t, Relationship> rel_map;
            for (size_t i = 0; i < rel_ids.size(); ++i) {
                rel_map[rel_ids[i]] = rels[i];
            }

            std::vector<GqlRow> rows;
            rows.reserve(results_ids.size());
            for (const auto& row_ids : results_ids) {
                GqlRow row;
                for (size_t i = 0; i < global_vars.size(); ++i) {
                    uint64_t id = row_ids[i];
                    if (var_is_node[i]) {
                        row.bindings[global_vars[i]] = GqlValue(node_map[id]);
                    } else {
                        row.bindings[global_vars[i]] = GqlValue(rel_map[id]);
                    }
                }
                rows.push_back(std::move(row));
            }
            return IntermediateResult(std::move(rows));
        });
    });
}

} // namespace ragedb::gql
