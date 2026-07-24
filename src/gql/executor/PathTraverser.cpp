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

#include "PathTraverser.h"
#include "JoinHelpers.h"
#include "Join.h"
#include "../../graph/cache/WccCache.h"
#include "../../graph/cache/TransitiveReachabilityCache.h"
#include <seastar/core/when_all.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/loop.hh>
#include <seastar/util/later.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <algorithm>
#include <unordered_set>
#include <iterator>
#include <numeric>
#include <limits>

/**
 * @file PathTraverser.cpp
 * @brief Implementation of path traversal, index lookups, and step-wise GQL pattern matching.
 */
namespace ragedb::gql {

seastar::future<std::vector<Node>> get_start_nodes(ragedb::Graph& graph, const PatternNode& node, size_t limit, const ProjectionPruner& pruner, std::string sort_property, bool sort_ascending, bool sort_by_id) {
    std::string single_label = "";
    if (node.label_expr && node.label_expr->kind == LabelExprKind::LITERAL) {
        single_label = node.label_expr->name;
    }

    struct FilterInfo {
        std::string property;
        Operation op;
        property_type_t value;
    };
    std::vector<FilterInfo> all_filters;
    for (const auto& [prop, val] : node.properties) {
        all_filters.push_back({prop, Operation::EQ, val});
    }
    for (const auto& filter : node.property_filters) {
        all_filters.push_back({filter.property, filter.op, filter.value});
    }

    auto sort_nodes = [sort_property, sort_ascending, sort_by_id](std::vector<Node>& list) {
        if (!sort_property.empty()) {
            std::stable_sort(list.begin(), list.end(), [&sort_property, sort_ascending](const Node& a, const Node& b) {
                property_type_t val_a = a.getProperty(sort_property);
                property_type_t val_b = b.getProperty(sort_property);
                int cmp = compare_properties(val_a, val_b);
                if (cmp != 0) {
                    return sort_ascending ? (cmp < 0) : (cmp > 0);
                }
                return a.getId() < b.getId();
            });
        } else if (sort_by_id) {
            std::stable_sort(list.begin(), list.end(), [sort_ascending](const Node& a, const Node& b) {
                return sort_ascending ? (a.getId() < b.getId()) : (a.getId() > b.getId());
            });
        }
    };

    // Determine how many candidate start nodes to scan. An explicit query LIMIT bounds the scan
    // directly; without one we scan every candidate, deriving the bound from the actual candidate
    // count so results (and aggregates built on them) are never silently truncated, while the
    // underlying reserve()/accumulation stays bounded. The multi-filter intersection relies on this
    // too: each per-filter list is bounded by the label's total node count, so no filter list is
    // truncated before the leapfrog join.
    seastar::future<size_t> scan_limit_fut = (limit > 0)
        ? seastar::make_ready_future<size_t>(limit)
        : (single_label.empty()
            ? graph.shard.local().AllNodesCountPeered().then([](uint64_t count) { return static_cast<size_t>(count); })
            : graph.shard.local().AllNodesCountPeered(single_label).then([](uint64_t count) { return static_cast<size_t>(count); }));

    return scan_limit_fut.then([&graph, single_label, all_filters](size_t scan_limit) {
        if (all_filters.size() > 1 && !single_label.empty()) {
            std::vector<seastar::future<std::vector<Node>>> futs;
            for (const auto& filter : all_filters) {
                futs.push_back(graph.shard.local().FindNodesPeered(single_label, filter.property, filter.op, filter.value, 0, scan_limit));
            }
            return seastar::when_all_succeed(futs.begin(), futs.end())
            .then([scan_limit](std::vector<std::vector<Node>> node_lists) {
                if (node_lists.empty()) return std::vector<Node>{};

                std::vector<std::vector<uint64_t>> id_lists;
                for (const auto& list : node_lists) {
                    std::vector<uint64_t> ids;
                    for (const auto& n : list) {
                        ids.push_back(n.getId());
                    }
                    std::sort(ids.begin(), ids.end());
                    id_lists.push_back(std::move(ids));
                }

                std::vector<uint64_t> intersected_ids = leapfrogJoin(id_lists);

                std::unordered_set<uint64_t> valid_ids(intersected_ids.begin(), intersected_ids.end());
                std::vector<Node> result;
                for (const auto& n : node_lists[0]) {
                    if (valid_ids.count(n.getId())) {
                        result.push_back(n);
                        if (result.size() >= scan_limit) {
                            break;
                        }
                    }
                }
                return result;
            });
        } else if (all_filters.size() == 1 && !single_label.empty()) {
            const auto& filter = all_filters[0];
            return graph.shard.local().FindNodesPeered(single_label, filter.property, filter.op, filter.value, 0, scan_limit);
        } else if (!single_label.empty()) {
            return graph.shard.local().AllNodesPeered(single_label, 0, scan_limit);
        }
        return graph.shard.local().AllNodesPeered(0, scan_limit);
    }).then([&graph, degree_opt_info = node.degree_opt_info, var = node.variable, pruner, sort_nodes](std::vector<Node> result_list) mutable {
        if (degree_opt_info.empty()) {
            sort_nodes(result_list);
            if (pruner.should_prune(var)) {
                auto keys = pruner.get_keys(var);
                for (auto& n : result_list) {
                    n.pruneProperties(keys);
                }
            }
            return seastar::make_ready_future<std::vector<Node>>(std::move(result_list));
        }

        std::vector<seastar::future<>> futs;
        auto shared_list = std::make_shared<std::vector<Node>>(std::move(result_list));

        for (auto& n : *shared_list) {
            for (const auto& info : degree_opt_info) {
                seastar::future<uint64_t> deg_fut = info.rel_types.empty()
                    ? graph.shard.local().NodeGetDegreePeered(n.getId(), info.direction)
                    : (info.rel_types.size() == 1
                        ? graph.shard.local().NodeGetDegreePeered(n.getId(), info.direction, info.rel_types[0])
                        : graph.shard.local().NodeGetDegreePeered(n.getId(), info.direction, info.rel_types));
                
                Node* node_ptr = &n;
                futs.push_back(deg_fut.then([node_ptr, prop_name = info.property_name](uint64_t deg) {
                    node_ptr->setProperty(prop_name, static_cast<int64_t>(deg));
                }));
            }
        }

        return seastar::when_all_succeed(futs.begin(), futs.end())
        .then([shared_list, var, pruner, sort_nodes]() mutable {
            std::vector<Node> final_list = std::move(*shared_list);
            sort_nodes(final_list);
            if (pruner.should_prune(var)) {
                auto keys = pruner.get_keys(var);
                for (auto& n : final_list) {
                    n.pruneProperties(keys);
                }
            }
            return final_list;
        });
    });
}

/**
 * @brief Fetch one page of candidate start nodes in scan order. Only filterless scans
 *        are paged: AllNodesPeered distributes skip/limit across shards from per-shard counts, so
 *        each page is genuinely bounded (unlike the filtered scan paths, which fetch skip+limit
 *        rows from every shard per call).
 */
static seastar::future<std::vector<Node>> get_start_nodes_page(
        ragedb::Graph& graph, const PatternNode& node, uint64_t skip, uint64_t count,
        const ProjectionPruner& pruner) {
    std::string single_label = "";
    if (node.label_expr && node.label_expr->kind == LabelExprKind::LITERAL) {
        single_label = node.label_expr->name;
    }

    seastar::future<std::vector<Node>> page_fut = single_label.empty()
        ? graph.shard.local().AllNodesPeered(skip, count)
        : graph.shard.local().AllNodesPeered(single_label, skip, count);

    return page_fut.then([var = node.variable, pruner](std::vector<Node> page) {
        if (pruner.should_prune(var)) {
            auto keys = pruner.get_keys(var);
            for (auto& n : page) {
                n.pruneProperties(keys);
            }
        }
        return page;
    });
}

/**
 * @brief Whether the pattern's start-node scan can be driven page by page: an unbound, filterless
 *        anchor whose candidates come back in plain scan order (no sort, no degree population).
 *        Anchors with property filters keep the one-shot scan: their filtered fetch paths are not
 *        memory-bounded per page, and the filters already narrow the scan.
 */
static bool start_scan_is_pageable(const PatternNode& node, const std::string& sort_property, bool sort_by_id) {
    if (!sort_property.empty() || sort_by_id) return false;
    if (!node.degree_opt_info.empty()) return false;
    if (!node.properties.empty() || !node.property_filters.empty()) return false;
    return true;
}

struct PathHop {
    std::vector<Relationship> rels;
    std::vector<Node> nodes;
};

/**
 * @brief Streaming consumer for completed var-length paths. When a traversal is given one, each finished
 *        path is handed over in batches and nothing is retained, so peak memory is the in-flight branches
 *        plus one batch instead of every path in the expansion. Without one, the traversal accumulates
 *        and returns the whole path set, which one person's KNOWS{1,3} neighbourhood at SF1 (over a
 *        million paths, each carrying the nodes and relationships along it) cannot fit in.
 */
struct PathHopSink {
    std::function<seastar::future<>(std::vector<PathHop>)> consume;
    std::vector<PathHop> buffer;
    size_t batch = 1024;

    seastar::future<> add(PathHop hop) {
        buffer.push_back(std::move(hop));
        if (buffer.size() < batch) {
            return seastar::make_ready_future<>();
        }
        return flush();
    }

    seastar::future<> flush() {
        if (buffer.empty()) {
            return seastar::make_ready_future<>();
        }
        auto pending = std::move(buffer);
        buffer.clear();
        return consume(std::move(pending));
    }
};

bool satisfies_match_path_modes(const GqlRow& row, MatchMode match_mode, PathMode path_mode, const PathPattern& pattern) {
    // 1. Path Mode constraints
    std::vector<uint64_t> node_ids;
    std::vector<uint64_t> rel_ids;

    // Gather node IDs from the path pattern bindings
    for (size_t i = 0; i < pattern.nodes.size(); ++i) {
        auto it = row.bindings.find("_n_" + std::to_string(i));
        if (it != row.bindings.end() && it->second.type == GqlValue::NODE) {
            node_ids.push_back(it->second.node->getId());
        }
    }

    // Gather relationship IDs from the path pattern bindings
    for (size_t i = 0; i < pattern.edges.size(); ++i) {
        auto it = row.bindings.find("_e_" + std::to_string(i));
        if (it != row.bindings.end()) {
            if (it->second.type == GqlValue::RELATIONSHIP) {
                rel_ids.push_back(it->second.relationship->getId());
            } else if (it->second.type == GqlValue::RELATIONSHIP_LIST) {
                for (const auto& rel : *it->second.relationship_list) {
                    rel_ids.push_back(rel.getId());
                }
            }
        }
    }

    if (rel_ids.empty()) {
        return true;
    }

    // Check Path Mode
    if (path_mode == PathMode::TRAIL) {
        // No repeated relationships
        std::set<uint64_t> unique_rels(rel_ids.begin(), rel_ids.end());
        if (unique_rels.size() != rel_ids.size()) {
            return false;
        }
    } else if (path_mode == PathMode::ACYCLIC) {
        // No repeated nodes
        std::set<uint64_t> unique_nodes(node_ids.begin(), node_ids.end());
        if (unique_nodes.size() != node_ids.size()) {
            return false;
        }
    } else if (path_mode == PathMode::SIMPLE) {
        // No repeated nodes, except first and last can be same
        if (node_ids.size() > 1) {
            bool first_last_same = (node_ids.front() == node_ids.back());
            std::set<uint64_t> unique_nodes;
            if (first_last_same) {
                unique_nodes.insert(node_ids.begin(), node_ids.end() - 1);
                if (unique_nodes.size() != node_ids.size() - 1) {
                    return false;
                }
            } else {
                unique_nodes.insert(node_ids.begin(), node_ids.end());
                if (unique_nodes.size() != node_ids.size()) {
                    return false;
                }
            }
        }
    }

    // 2. Match Mode constraints
    if (match_mode == MatchMode::DIFFERENT_EDGES) {
        // No relationship ID is bound to more than one variable
        std::vector<uint64_t> bound_rel_ids;
        for (const auto& [var, val] : row.bindings) {
            if (!var.empty() && var[0] == '_') {
                continue;
            }
            if (val.type == GqlValue::RELATIONSHIP) {
                bound_rel_ids.push_back(val.relationship->getId());
            } else if (val.type == GqlValue::RELATIONSHIP_LIST) {
                for (const auto& r : *val.relationship_list) {
                    bound_rel_ids.push_back(r.getId());
                }
            }
        }
        std::set<uint64_t> unique_bound_rels(bound_rel_ids.begin(), bound_rel_ids.end());
        if (unique_bound_rels.size() != bound_rel_ids.size()) {
            return false;
        }
    }

    return true;
}

static seastar::future<std::vector<PathHop>> traverse_var_len_async(
    ragedb::Graph& graph,
    uint64_t current_node_id,
    const PatternEdge& edge,
    const PatternNode& next_node,
    uint64_t current_depth,
    std::vector<uint64_t> visited_rel_ids,
    std::vector<Relationship> current_path_rels,
    std::vector<Node> current_path_nodes,
    size_t limit,
    const ProjectionPruner& pruner,
    PathMode path_mode,
    PathHopSink* hop_sink = nullptr,
    std::shared_ptr<uint64_t> walk_path_count = nullptr
) {
    // Base Case 1: Exceeded maximum hops allowed
    if (current_depth > edge.max_hops) {
        return seastar::make_ready_future<std::vector<PathHop>>();
    }

    // walk_path_count is non-null only for an unbounded quantifier under WALK -- the case with no finite
    // result on cyclic data. Fail loudly at the depth cap (catches a low-branching cycle) rather than
    // diverge. The path-count budget below catches the exponential case.
    if (walk_path_count && current_depth > gql_var_len_walk_depth_cap) {
        return seastar::make_exception_future<std::vector<PathHop>>(std::runtime_error(
            "An unbounded quantifier under the WALK path mode did not terminate within " +
            std::to_string(gql_var_len_walk_depth_cap) +
            " hops -- a walk may re-cross an edge on cyclic data, so the result is unbounded. Give the "
            "quantifier an upper bound, or restrict the paths (TRAIL / ACYCLIC / SIMPLE)."));
    }

    // Base Case 2: Within allowed hop range, record the current path. With a sink the finished path is
    // handed over immediately and nothing accumulates here.
    std::vector<PathHop> local_results;
    seastar::future<> recorded = seastar::make_ready_future<>();
    if (current_depth >= edge.min_hops) {
        // An unbounded WALK on a dense/clique-like cycle explodes exponentially at shallow depths, well
        // before the depth cap; bound the total paths recorded so it fails loudly instead of exhausting
        // the heap. Counted here because this is the one place a path is finalised, on both the
        // materialising and the streaming route.
        if (walk_path_count && ++(*walk_path_count) > gql_var_len_walk_path_budget) {
            return seastar::make_exception_future<std::vector<PathHop>>(std::runtime_error(
                "An unbounded quantifier under the WALK path mode produced more than " +
                std::to_string(gql_var_len_walk_path_budget) +
                " paths -- a walk may re-cross an edge on cyclic data, so the result is unbounded. Give "
                "the quantifier an upper bound, or restrict the paths (TRAIL / ACYCLIC / SIMPLE)."));
        }
        if (hop_sink) {
            recorded = hop_sink->add(PathHop{current_path_rels, current_path_nodes});
        } else {
            local_results.push_back({current_path_rels, current_path_nodes});
        }
    }

    // Base Case 3: Reached maximum hops allowed, stop traversing further
    if (current_depth == edge.max_hops) {
        return recorded.then([local_results = std::move(local_results)]() mutable {
            return std::move(local_results);
        });
    }

    // Determine traversal direction
    Direction dir = Direction::BOTH;
    if (edge.direction == EdgeDirection::RIGHT) dir = Direction::OUT;
    else if (edge.direction == EdgeDirection::LEFT) dir = Direction::IN;

    std::string edge_type = "";
    if (edge.label_expr && edge.label_expr->kind == LabelExprKind::LITERAL) {
        edge_type = edge.label_expr->name;
    }

    // Callback to process retrieved relationships on the current node
    auto handle_rels = [&graph, edge, next_node, current_node_id, current_depth, visited_rel_ids = std::move(visited_rel_ids),
           current_path_rels = std::move(current_path_rels), current_path_nodes = std::move(current_path_nodes),
           local_results = std::move(local_results), limit, pruner, path_mode, hop_sink, walk_path_count](std::vector<Relationship> rels) mutable {

        if (pruner.should_prune(edge.variable)) {
            auto keys = pruner.get_keys(edge.variable);
            for (auto& r : rels) {
                r.pruneProperties(keys);
            }
        }

        if (edge.max_cardinality_limit.has_value()) {
            size_t max_limit = *edge.max_cardinality_limit;
            if (rels.size() > max_limit) {
                rels.resize(max_limit);
            }
        }

        // Select the relationships this node actually branches on, so the expansion below can be bounded
        // over exactly those.
        auto branches = std::make_shared<std::vector<Relationship>>();
        for (auto& rel : rels) {
            uint64_t target_id = (rel.getStartingNodeId() == current_node_id) ? rel.getEndingNodeId() : rel.getStartingNodeId();

            if (path_mode == PathMode::TRAIL) {
                if (std::find(visited_rel_ids.begin(), visited_rel_ids.end(), rel.getId()) != visited_rel_ids.end()) {
                    continue;
                }
            } else if (path_mode == PathMode::ACYCLIC) {
                if (std::find_if(current_path_nodes.begin(), current_path_nodes.end(), [target_id](const Node& n) { return n.getId() == target_id; }) != current_path_nodes.end()) {
                    continue;
                }
            } else if (path_mode == PathMode::SIMPLE) {
                bool already_visited = false;
                for (size_t idx = 0; idx < current_path_nodes.size(); ++idx) {
                    if (current_path_nodes[idx].getId() == target_id) {
                        if (idx == 0 && current_depth + 1 >= edge.min_hops) {
                            // allowed as final node, but cannot traverse further
                        } else {
                            already_visited = true;
                        }
                    }
                }
                if (already_visited) {
                    continue;
                }
            }

            if (edge.label_expr && !matches_label_expr(rel.getType(), edge.label_expr)) {
                continue;
            }
            if (!matches_properties(rel.getProperties(), edge.properties) || !matches_filters(rel.getProperties(), edge.property_filters)) {
                continue;
            }
            if (edge.where_expr) {
                GqlRow temp_row;
                if (!edge.variable.empty()) {
                    temp_row.bindings[edge.variable] = GqlValue(rel);
                }
                if (!evaluate_expression(temp_row, edge.where_expr.get()).is_truthy()) {
                    continue;
                }
            }

            branches->push_back(std::move(rel));
        }

        if (branches->empty()) {
            return seastar::make_ready_future<std::vector<PathHop>>(std::move(local_results));
        }

        // Expand the branches with a bounded number in flight. Each branch carries its own copy of the
        // path built so far, so expanding every branch of a high-degree node at once made live memory
        // the product of that degree and the path length. Results land in per-branch slots, keeping the
        // accumulated order identical to the order the relationships came back in.
        auto branch_results = std::make_shared<std::vector<std::vector<PathHop>>>(branches->size());
        auto indices = std::make_shared<std::vector<size_t>>(branches->size());
        std::iota(indices->begin(), indices->end(), 0);

        const size_t branch_concurrency = gql_var_len_branch_concurrency > 0 ? gql_var_len_branch_concurrency : 1;

        return seastar::max_concurrent_for_each(indices->begin(), indices->end(), branch_concurrency,
        [&graph, branches, branch_results, edge, next_node, current_node_id, current_depth, visited_rel_ids,
         current_path_rels, current_path_nodes, limit, pruner, path_mode, hop_sink, walk_path_count](size_t branch_idx) {
            const Relationship& rel = (*branches)[branch_idx];
            uint64_t target_id = (rel.getStartingNodeId() == current_node_id) ? rel.getEndingNodeId() : rel.getStartingNodeId();

            return graph.shard.local().NodeGetPeered(target_id)
            .then([&graph, branch_idx, branch_results, rel, edge, next_node, current_depth, visited_rel_ids,
                   current_path_rels, current_path_nodes, target_id, limit, pruner, path_mode, hop_sink, walk_path_count](Node target_node) mutable {
                if (pruner.should_prune(next_node.variable)) {
                    target_node.pruneProperties(pruner.get_keys(next_node.variable));
                }
                visited_rel_ids.push_back(rel.getId());
                current_path_rels.push_back(rel);
                current_path_nodes.push_back(target_node);

                uint64_t next_depth = current_depth + 1;
                auto descend = [&graph, target_id, edge, next_node, next_depth,
                                visited_rel_ids = std::move(visited_rel_ids),
                                current_path_rels = std::move(current_path_rels),
                                current_path_nodes = std::move(current_path_nodes),
                                limit, pruner, path_mode, hop_sink, walk_path_count]() mutable {
                    return traverse_var_len_async(
                        graph,
                        target_id,
                        edge,
                        next_node,
                        next_depth,
                        std::move(visited_rel_ids),
                        std::move(current_path_rels),
                        std::move(current_path_nodes),
                        limit,
                        pruner,
                        path_mode,
                        hop_sink,
                        walk_path_count
                    );
                };

                seastar::future<std::vector<PathHop>> descended = (next_depth % 5 == 0)
                    ? seastar::yield().then(std::move(descend))
                    : descend();

                return descended.then([branch_idx, branch_results](std::vector<PathHop> hops) {
                    (*branch_results)[branch_idx] = std::move(hops);
                });
            });
        }).then([local_results = std::move(local_results), branches, indices, branch_results, limit]() mutable {
            for (auto& branch_res : *branch_results) {
                local_results.insert(local_results.end(),
                                     std::make_move_iterator(branch_res.begin()),
                                     std::make_move_iterator(branch_res.end()));
                if (limit > 0 && local_results.size() >= limit) {
                    local_results.resize(limit);
                    break;
                }
            }
            return std::move(local_results);
        });
    };

    return recorded.then([&graph, current_node_id, dir, edge_type, handle_rels = std::move(handle_rels)]() mutable {
        if (edge_type.empty()) {
            return graph.shard.local().NodeGetRelationshipsPeered(current_node_id, dir).then(std::move(handle_rels));
        }
        return graph.shard.local().NodeGetRelationshipsPeered(current_node_id, dir, edge_type).then(std::move(handle_rels));
    });
}

/**
 * @brief Turns completed var-length paths into rows, applying the far node's label and property
 *        constraints and any binding it already carries. Shared by the materialising var-length step and
 *        the streamed one so the two cannot drift apart.
 */
static std::vector<GqlRow> var_len_hops_to_rows(const GqlRow& row, const PatternEdge& edge,
                                                const PatternNode& next_node, size_t node_idx,
                                                const std::vector<PathHop>& hops) {
    std::vector<GqlRow> out;
    for (const auto& hop : hops) {
        Node final_node;
        if (hop.nodes.empty()) {
            if (edge.min_hops == 0) {
                // 0-hop transition: final node is the start node itself
                auto start_it = row.bindings.find("_n_" + std::to_string(node_idx));
                if (start_it != row.bindings.end() && start_it->second.type == GqlValue::NODE) {
                    final_node = *start_it->second.node;
                } else {
                    continue;
                }
            } else {
                continue;
            }
        } else {
            final_node = hop.nodes.back();
        }

        if (next_node.label_expr && !matches_label_expr(final_node.getType(), next_node.label_expr)) {
            continue;
        }
        if (!matches_properties(final_node.getProperties(), next_node.properties) || !matches_filters(final_node.getProperties(), next_node.property_filters)) {
            continue;
        }

        auto bound_it = row.bindings.find(next_node.variable);
        if (bound_it != row.bindings.end() && bound_it->second.type == GqlValue::NODE) {
            if (bound_it->second.node->getId() != final_node.getId()) {
                continue;
            }
        }

        GqlRow new_row = row;
        new_row.bindings[edge.variable] = GqlValue(hop.rels);
        new_row.bindings["_e_" + std::to_string(node_idx)] = GqlValue(hop.rels);
        new_row.bindings[next_node.variable] = GqlValue(final_node);
        new_row.bindings["_n_" + std::to_string(node_idx + 1)] = GqlValue(final_node);
        out.push_back(std::move(new_row));
    }
    return out;
}

static seastar::future<std::vector<GqlRow>> traverse_step(ragedb::Graph& graph, const GqlRow& row, const PatternEdge& edge, const PatternNode& next_node, size_t node_idx, size_t limit, const ProjectionPruner& pruner, PathMode path_mode) {
    auto it = row.bindings.find("_n_" + std::to_string(node_idx));
    if (it == row.bindings.end()) {
        return seastar::make_ready_future<std::vector<GqlRow>>();
    }
    uint64_t src_id = it->second.node->getId();

    if (edge.is_variable_length) {
        // A counter guards only the divergent case -- unbounded quantifier under WALK; null disables the
        // guard for bounded quantifiers and restrictors, which are already finite.
        auto walk_path_count = (path_mode == PathMode::WALK &&
                                edge.max_hops == std::numeric_limits<uint64_t>::max())
            ? std::make_shared<uint64_t>(0) : nullptr;
        return traverse_var_len_async(graph, src_id, edge, next_node, 0, {}, {}, {}, limit, pruner, path_mode,
                                      nullptr, walk_path_count)
        .then([row, edge, next_node, node_idx](std::vector<PathHop> hops) {
            return var_len_hops_to_rows(row, edge, next_node, node_idx, hops);
        });
    }

    Direction dir = Direction::BOTH;
    if (edge.direction == EdgeDirection::RIGHT) dir = Direction::OUT;
    else if (edge.direction == EdgeDirection::LEFT) dir = Direction::IN;

    std::string edge_type = "";
    if (edge.label_expr && edge.label_expr->kind == LabelExprKind::LITERAL) {
        edge_type = edge.label_expr->name;
    }

    auto handle_rels = [row, edge, next_node, src_id, node_idx, &graph, limit, pruner](std::vector<Relationship> rels) {
        if (pruner.should_prune(edge.variable)) {
            auto keys = pruner.get_keys(edge.variable);
            for (auto& r : rels) {
                r.pruneProperties(keys);
            }
        }

        if (edge.max_cardinality_limit.has_value()) {
            size_t max_limit = *edge.max_cardinality_limit;
            if (rels.size() > max_limit) {
                rels.resize(max_limit);
            }
        }

        // Bound-target semi-join: when the far node is already bound (a piped/earlier binding),
        // we already hold that Node. A verification match like `(forum)-[hm:HAS_MEMBER]->(f)` then only
        // needs to keep the relationship(s) whose other end IS the bound node -- so build the row from the
        // matched rel directly instead of fetching every neighbour node (one NodeGetPeered per rel) and
        // discarding all but the bound one. That per-rel node fetch was the FoF verification blow-up.
        std::optional<uint64_t> bound_target_id;
        std::optional<Node> bound_target_node;
        {
            auto bit = row.bindings.find(next_node.variable);
            if (bit != row.bindings.end() && bit->second.type == GqlValue::NODE && bit->second.node.has_value()) {
                bound_target_id = bit->second.node->getId();
                bound_target_node = bit->second.node;
            }
        }
        auto build_bound_row = [row, edge, next_node, node_idx, pruner, bound_target_node](const Relationship& rel) -> std::optional<GqlRow> {
            Node target_node = *bound_target_node;
            if (pruner.should_prune(next_node.variable)) {
                target_node.pruneProperties(pruner.get_keys(next_node.variable));
            }
            if (next_node.label_expr && !matches_label_expr(target_node.getType(), next_node.label_expr)) return std::nullopt;
            if (!matches_properties(target_node.getProperties(), next_node.properties) || !matches_filters(target_node.getProperties(), next_node.property_filters)) return std::nullopt;
            GqlRow new_row = row;
            new_row.bindings[edge.variable] = GqlValue(rel);
            new_row.bindings["_e_" + std::to_string(node_idx)] = GqlValue(rel);
            new_row.bindings[next_node.variable] = GqlValue(target_node);
            new_row.bindings["_n_" + std::to_string(node_idx + 1)] = GqlValue(target_node);
            if (edge.where_expr && !evaluate_expression(new_row, edge.where_expr.get()).is_truthy()) return std::nullopt;
            if (next_node.where_expr && !evaluate_expression(new_row, next_node.where_expr.get()).is_truthy()) return std::nullopt;
            return new_row;
        };

        if (limit > 0) {
            struct LoopState {
                size_t rel_idx = 0;
                std::vector<GqlRow> results;
            };
            auto state = std::make_shared<LoopState>();
            auto run_loop = std::make_shared<std::function<seastar::future<std::vector<GqlRow>>()>>();
            *run_loop = [state, rels = std::move(rels), src_id, &graph, limit, edge, next_node, node_idx, row, pruner, run_loop, bound_target_id, build_bound_row]() -> seastar::future<std::vector<GqlRow>> {
                if (state->results.size() >= limit || state->rel_idx >= rels.size()) {
                    if (state->results.size() > limit) {
                        state->results.resize(limit);
                    }
                    return seastar::make_ready_future<std::vector<GqlRow>>(std::move(state->results));
                }

                const auto& rel = rels[state->rel_idx++];
                if (edge.label_expr && !matches_label_expr(rel.getType(), edge.label_expr)) {
                    return (*run_loop)();
                }
                if (!matches_properties(rel.getProperties(), edge.properties) || !matches_filters(rel.getProperties(), edge.property_filters)) {
                    return (*run_loop)();
                }

                uint64_t target_id = (rel.getStartingNodeId() == src_id) ? rel.getEndingNodeId() : rel.getStartingNodeId();
                if (bound_target_id) {
                    if (target_id == *bound_target_id) {
                        if (auto r = build_bound_row(rel)) state->results.push_back(std::move(*r));
                    }
                    return (*run_loop)();
                }
                return graph.shard.local().NodeGetPeered(target_id)
                .then([row, rel, edge, next_node, node_idx, pruner, state, run_loop](Node target_node) mutable {
                    if (pruner.should_prune(next_node.variable)) {
                        target_node.pruneProperties(pruner.get_keys(next_node.variable));
                    }
                    if (next_node.label_expr && !matches_label_expr(target_node.getType(), next_node.label_expr)) {
                        return (*run_loop)();
                    }
                    if (!matches_properties(target_node.getProperties(), next_node.properties) || !matches_filters(target_node.getProperties(), next_node.property_filters)) {
                        return (*run_loop)();
                    }

                    auto bound_it = row.bindings.find(next_node.variable);
                    if (bound_it != row.bindings.end() && bound_it->second.type == GqlValue::NODE) {
                        if (bound_it->second.node->getId() != target_node.getId()) {
                            return (*run_loop)();
                        }
                    }

                    GqlRow new_row = row;
                    new_row.bindings[edge.variable] = GqlValue(rel);
                    new_row.bindings["_e_" + std::to_string(node_idx)] = GqlValue(rel);
                    new_row.bindings[next_node.variable] = GqlValue(target_node);
                    new_row.bindings["_n_" + std::to_string(node_idx + 1)] = GqlValue(target_node);
                    if (edge.where_expr && !evaluate_expression(new_row, edge.where_expr.get()).is_truthy()) {
                        return (*run_loop)();
                    }
                    if (next_node.where_expr && !evaluate_expression(new_row, next_node.where_expr.get()).is_truthy()) {
                        return (*run_loop)();
                    }

                    state->results.push_back(std::move(new_row));
                    return (*run_loop)();
                });
            };
            return (*run_loop)();
        } else {
            std::vector<seastar::future<std::optional<GqlRow>>> futs;
            for (const auto& rel : rels) {
                if (edge.label_expr && !matches_label_expr(rel.getType(), edge.label_expr)) {
                    continue;
                }
                if (!matches_properties(rel.getProperties(), edge.properties) || !matches_filters(rel.getProperties(), edge.property_filters)) {
                    continue;
                }

                uint64_t target_id = (rel.getStartingNodeId() == src_id) ? rel.getEndingNodeId() : rel.getStartingNodeId();

                if (bound_target_id) {
                    if (target_id == *bound_target_id) {
                        futs.push_back(seastar::make_ready_future<std::optional<GqlRow>>(build_bound_row(rel)));
                    }
                    continue;
                }

                futs.push_back(
                    graph.shard.local().NodeGetPeered(target_id)
                    .then([row, rel, edge, next_node, node_idx, pruner](Node target_node) -> std::optional<GqlRow> {
                        if (pruner.should_prune(next_node.variable)) {
                            target_node.pruneProperties(pruner.get_keys(next_node.variable));
                        }
                        if (next_node.label_expr && !matches_label_expr(target_node.getType(), next_node.label_expr)) {
                            return std::nullopt;
                        }
                        if (!matches_properties(target_node.getProperties(), next_node.properties) || !matches_filters(target_node.getProperties(), next_node.property_filters)) {
                            return std::nullopt;
                        }

                        auto bound_it = row.bindings.find(next_node.variable);
                        if (bound_it != row.bindings.end() && bound_it->second.type == GqlValue::NODE) {
                            if (bound_it->second.node->getId() != target_node.getId()) {
                                return std::nullopt;
                            }
                        }

                        GqlRow new_row = row;
                        new_row.bindings[edge.variable] = GqlValue(rel);
                        new_row.bindings["_e_" + std::to_string(node_idx)] = GqlValue(rel);
                        new_row.bindings[next_node.variable] = GqlValue(target_node);
                        new_row.bindings["_n_" + std::to_string(node_idx + 1)] = GqlValue(target_node);
                        if (edge.where_expr && !evaluate_expression(new_row, edge.where_expr.get()).is_truthy()) {
                            return std::nullopt;
                        }
                        if (next_node.where_expr && !evaluate_expression(new_row, next_node.where_expr.get()).is_truthy()) {
                            return std::nullopt;
                        }
                        return new_row;
                    })
                );
            }
            return seastar::when_all_succeed(futs.begin(), futs.end())
            .then([](std::vector<std::optional<GqlRow>> opts) {
                std::vector<GqlRow> out;
                for (const auto& opt : opts) {
                    if (opt) {
                        out.push_back(*opt);
                    }
                }
                return out;
            });
        }
    };

    if (edge_type.empty()) {
        return graph.shard.local().NodeGetRelationshipsPeered(src_id, dir).then(std::move(handle_rels));
    } else {
        return graph.shard.local().NodeGetRelationshipsPeered(src_id, dir, edge_type).then(std::move(handle_rels));
    }
}

/**
 * @brief Drains one row's var-length expansion straight into the row sink: completed paths are converted
 *        and handed over in batches as the traversal finds them, so a single high-fanout row never
 *        materialises its whole path set. This is the shape that exhausted the heap -- one person's
 *        KNOWS{1,3} neighbourhood at SF1 is over a million paths, each holding the nodes and
 *        relationships along it, and a bare count(*) needed none of their contents.
 */
static seastar::future<> traverse_var_len_step_to_sink(ragedb::Graph& graph, const GqlRow& row,
                                                       const PatternEdge& edge, const PatternNode& next_node,
                                                       size_t node_idx, const ProjectionPruner& pruner,
                                                       PathMode path_mode, GqlRowSink* sink) {
    auto it = row.bindings.find("_n_" + std::to_string(node_idx));
    if (it == row.bindings.end()) {
        return seastar::make_ready_future<>();
    }
    uint64_t src_id = it->second.node->getId();

    auto hop_sink = std::make_shared<PathHopSink>();
    hop_sink->batch = gql_var_len_hop_batch > 0 ? gql_var_len_hop_batch : 1;
    hop_sink->consume = [row, edge, next_node, node_idx, sink](std::vector<PathHop> hops) {
        auto rows = var_len_hops_to_rows(row, edge, next_node, node_idx, hops);
        if (rows.empty()) {
            return seastar::make_ready_future<>();
        }
        return sink->consume(std::move(rows));
    };

    auto walk_path_count = (path_mode == PathMode::WALK &&
                            edge.max_hops == std::numeric_limits<uint64_t>::max())
        ? std::make_shared<uint64_t>(0) : nullptr;
    return traverse_var_len_async(graph, src_id, edge, next_node, 0, {}, {}, {}, 0, pruner, path_mode,
                                  hop_sink.get(), walk_path_count)
    .then([hop_sink](std::vector<PathHop>) {
        return hop_sink->flush();
    });
}

static seastar::future<std::vector<GqlRow>> traverse_path_pattern_iterative(ragedb::Graph& graph, const PathPattern& prep_pattern, size_t step_idx, std::vector<GqlRow> current_step_rows, size_t limit, const ProjectionPruner& pruner, PathMode path_mode, GqlRowSink* sink = nullptr) {
    if (step_idx >= prep_pattern.edges.size()) {
        if (sink) {
            return sink->consume(std::move(current_step_rows)).then([] { return std::vector<GqlRow>{}; });
        }
        return seastar::make_ready_future<std::vector<GqlRow>>(std::move(current_step_rows));
    }

    // Streamed traversal: expand the frontier row by row, each row's expansion draining through
    // all remaining hops to the sink before the next row starts. Peak memory is one row's fanout
    // per level plus the sink's own state, never a whole level's row set (even a modest chunk of
    // hub nodes crossed with their expansions was enough to exhaust memory at SF1).
    if (sink) {
        struct RowLoopState {
            std::vector<GqlRow> rows;
            size_t idx = 0;
        };
        auto st = std::make_shared<RowLoopState>();
        st->rows = std::move(current_step_rows);
        return seastar::repeat([st, &graph, prep_pattern, step_idx, limit, pruner, path_mode, sink] {
            if (st->idx >= st->rows.size()) {
                return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
            }
            const GqlRow& row = st->rows[st->idx++];
            const auto& step_edge = prep_pattern.edges[step_idx];

            // A var-length final step is the one expansion the row-by-row drive does not bound: the row's
            // whole {min,max}-hop neighbourhood would be materialised before any of it reached the sink.
            // Stream it instead.
            if (step_edge.is_variable_length && step_idx + 1 == prep_pattern.edges.size()) {
                return traverse_var_len_step_to_sink(graph, row, step_edge, prep_pattern.nodes[step_idx + 1],
                                                     step_idx, pruner, path_mode, sink)
                    .then([] { return seastar::stop_iteration::no; });
            }

            return traverse_step(graph, row, step_edge, prep_pattern.nodes[step_idx + 1], step_idx, 0, pruner, path_mode)
            .then([st, &graph, prep_pattern, step_idx, limit, pruner, path_mode, sink](std::vector<GqlRow> step_rows) {
                if (step_rows.empty()) {
                    return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no);
                }
                return traverse_path_pattern_iterative(graph, prep_pattern, step_idx + 1, std::move(step_rows), limit, pruner, path_mode, sink)
                    .then([](std::vector<GqlRow>) { return seastar::stop_iteration::no; });
            });
        }).then([st] { return std::vector<GqlRow>{}; });
    }

    const auto& edge = prep_pattern.edges[step_idx];
    const auto& next_node = prep_pattern.nodes[step_idx + 1];

    if (limit > 0) {
        struct LoopState {
            size_t row_idx = 0;
            std::vector<GqlRow> next_rows;
        };
        auto state = std::make_shared<LoopState>();
        auto run_loop = std::make_shared<std::function<seastar::future<std::vector<GqlRow>>()>>();
        *run_loop = [state, current_step_rows = std::move(current_step_rows), &graph, edge, next_node, step_idx, limit, pruner, path_mode, run_loop]() -> seastar::future<std::vector<GqlRow>> {
            if (state->next_rows.size() >= limit || state->row_idx >= current_step_rows.size()) {
                if (state->next_rows.size() > limit) {
                    state->next_rows.resize(limit);
                }
                return seastar::make_ready_future<std::vector<GqlRow>>(std::move(state->next_rows));
            }
            const auto& row = current_step_rows[state->row_idx++];
            return traverse_step(graph, row, edge, next_node, step_idx, limit, pruner, path_mode)
            .then([state, run_loop](std::vector<GqlRow> step_rows) {
                state->next_rows.insert(state->next_rows.end(), step_rows.begin(), step_rows.end());
                return (*run_loop)();
            });
        };
        return (*run_loop)().then([&graph, prep_pattern, step_idx, limit, pruner, path_mode, sink](std::vector<GqlRow> next_rows) {
            return traverse_path_pattern_iterative(graph, prep_pattern, step_idx + 1, std::move(next_rows), limit, pruner, path_mode, sink);
        });
    } else {
        std::vector<seastar::future<std::vector<GqlRow>>> futs;
        for (const auto& row : current_step_rows) {
            futs.push_back(traverse_step(graph, row, edge, next_node, step_idx, limit, pruner, path_mode));
        }

        return seastar::when_all_succeed(futs.begin(), futs.end())
        .then([&graph, prep_pattern, step_idx, limit, pruner, path_mode, sink](std::vector<std::vector<GqlRow>> nested) {
            std::vector<GqlRow> next_rows;
            for (const auto& vec : nested) {
                next_rows.insert(next_rows.end(), vec.begin(), vec.end());
            }
            return traverse_path_pattern_iterative(graph, prep_pattern, step_idx + 1, std::move(next_rows), limit, pruner, path_mode, sink);
        });
    }
}

static seastar::future<std::vector<GqlRow>> traverse_from_relationship_index(
    ragedb::Graph& graph,
    const PathPattern& prep_pattern,
    const GqlRow& base_row,
    size_t limit,
    const ProjectionPruner& pruner,
    PathMode path_mode,
    GqlRowSink* sink = nullptr
) {
    const auto& edge = prep_pattern.edges[0];
    std::string single_label = "";
    if (edge.label_expr && edge.label_expr->kind == LabelExprKind::LITERAL) {
        single_label = edge.label_expr->name;
    }

    std::string indexed_prop = "";
    property_type_t indexed_val;
    Operation indexed_op = Operation::EQ;
    bool found = false;
    for (const auto& [prop, val] : edge.properties) {
        if (graph.shard.local().RelationshipIndexExists(single_label, prop)) {
            indexed_prop = prop;
            indexed_val = val;
            indexed_op = Operation::EQ;
            found = true;
            break;
        }
    }
    if (!found) {
        for (const auto& filter : edge.property_filters) {
            if (filter.op == Operation::EQ && graph.shard.local().RelationshipIndexExists(single_label, filter.property)) {
                indexed_prop = filter.property;
                indexed_val = filter.value;
                indexed_op = filter.op;
                found = true;
                break;
            }
        }
    }

    if (!found) {
        return seastar::make_ready_future<std::vector<GqlRow>>();
    }

    // Without an explicit LIMIT, scan every matching relationship. The bound comes from the actual
    // match count so results and aggregates are never silently truncated while the underlying
    // reserve()/accumulation stays bounded.
    seastar::future<size_t> scan_limit_fut = (limit > 0)
        ? seastar::make_ready_future<size_t>(limit)
        : graph.shard.local().FindRelationshipCountPeered(single_label, indexed_prop, indexed_op, indexed_val).then([](uint64_t count) { return static_cast<size_t>(count); });

    return scan_limit_fut.then([&graph, single_label, indexed_prop, indexed_op, indexed_val](size_t scan_limit) {
        return graph.shard.local().FindRelationshipsPeered(single_label, indexed_prop, indexed_op, indexed_val, 0, scan_limit);
    }).then([&graph, prep_pattern, base_row, limit, pruner, path_mode, sink](std::vector<Relationship> rels) {
        const auto& pattern_edge = prep_pattern.edges[0];
        
        std::vector<Relationship> matched_rels;
        for (const auto& rel : rels) {
            if (pattern_edge.label_expr && !matches_label_expr(rel.getType(), pattern_edge.label_expr)) {
                continue;
            }
            if (!matches_properties(rel.getProperties(), pattern_edge.properties) || !matches_filters(rel.getProperties(), pattern_edge.property_filters)) {
                continue;
            }
            matched_rels.push_back(rel);
        }
        
        if (matched_rels.empty()) {
            return seastar::make_ready_future<std::vector<GqlRow>>();
        }
        
        struct RelEndPoints {
            Relationship rel;
            uint64_t node_0_id;
            uint64_t node_1_id;
        };
        
        std::vector<RelEndPoints> endpoints;
        for (const auto& rel : matched_rels) {
            if (pattern_edge.direction == EdgeDirection::RIGHT) {
                endpoints.push_back({rel, rel.getStartingNodeId(), rel.getEndingNodeId()});
            } else if (pattern_edge.direction == EdgeDirection::LEFT) {
                endpoints.push_back({rel, rel.getEndingNodeId(), rel.getStartingNodeId()});
            } else {
                endpoints.push_back({rel, rel.getStartingNodeId(), rel.getEndingNodeId()});
                endpoints.push_back({rel, rel.getEndingNodeId(), rel.getStartingNodeId()});
            }
        }
        
        std::vector<seastar::future<std::vector<GqlRow>>> row_futs;
        for (const auto& ep : endpoints) {
            std::vector<seastar::future<Node>> node_futs;
            node_futs.push_back(graph.shard.local().NodeGetPeered(ep.node_0_id));
            node_futs.push_back(graph.shard.local().NodeGetPeered(ep.node_1_id));
            
            auto combined = seastar::when_all_succeed(node_futs.begin(), node_futs.end())
            .then([ep, prep_pattern, base_row, pruner](std::vector<Node> nodes) -> std::vector<GqlRow> {
                const auto& node_0 = nodes[0];
                const auto& node_1 = nodes[1];
                const auto& p_node_0 = prep_pattern.nodes[0];
                const auto& p_node_1 = prep_pattern.nodes[1];
                const auto& inner_pattern_edge = prep_pattern.edges[0];
                
                if (p_node_0.label_expr && !matches_label_expr(node_0.getType(), p_node_0.label_expr)) {
                    return {};
                }
                if (!matches_properties(node_0.getProperties(), p_node_0.properties) || !matches_filters(node_0.getProperties(), p_node_0.property_filters)) {
                    return {};
                }
                
                if (p_node_1.label_expr && !matches_label_expr(node_1.getType(), p_node_1.label_expr)) {
                    return {};
                }
                if (!matches_properties(node_1.getProperties(), p_node_1.properties) || !matches_filters(node_1.getProperties(), p_node_1.property_filters)) {
                    return {};
                }
                
                GqlRow new_row = base_row;
                
                Node pruned_node_0 = node_0;
                if (pruner.should_prune(p_node_0.variable)) {
                    pruned_node_0.pruneProperties(pruner.get_keys(p_node_0.variable));
                }
                Node pruned_node_1 = node_1;
                if (pruner.should_prune(p_node_1.variable)) {
                    pruned_node_1.pruneProperties(pruner.get_keys(p_node_1.variable));
                }
                Relationship pruned_rel = ep.rel;
                if (pruner.should_prune(inner_pattern_edge.variable)) {
                    pruned_rel.pruneProperties(pruner.get_keys(inner_pattern_edge.variable));
                }
                
                new_row.bindings[p_node_0.variable] = GqlValue(pruned_node_0);
                new_row.bindings["_n_0"] = GqlValue(pruned_node_0);
                
                new_row.bindings[inner_pattern_edge.variable] = GqlValue(pruned_rel);
                new_row.bindings["_e_0"] = GqlValue(pruned_rel);
                
                new_row.bindings[p_node_1.variable] = GqlValue(pruned_node_1);
                new_row.bindings["_n_1"] = GqlValue(pruned_node_1);

                if (p_node_0.where_expr && !evaluate_expression(new_row, p_node_0.where_expr.get()).is_truthy()) {
                    return {};
                }
                if (inner_pattern_edge.where_expr && !evaluate_expression(new_row, inner_pattern_edge.where_expr.get()).is_truthy()) {
                    return {};
                }
                if (p_node_1.where_expr && !evaluate_expression(new_row, p_node_1.where_expr.get()).is_truthy()) {
                    return {};
                }
                
                return {new_row};
            });
            row_futs.push_back(std::move(combined));
        }
        
        return seastar::when_all_succeed(row_futs.begin(), row_futs.end())
        .then([&graph, prep_pattern, limit, pruner, path_mode, sink](std::vector<std::vector<GqlRow>> nested_rows) {
            std::vector<GqlRow> initial_rows;
            for (const auto& vec : nested_rows) {
                for (const auto& row : vec) {
                    initial_rows.push_back(row);
                    if (limit > 0 && prep_pattern.edges.size() == 1 && initial_rows.size() >= limit) {
                        break;
                    }
                }
                if (limit > 0 && prep_pattern.edges.size() == 1 && initial_rows.size() >= limit) {
                    break;
                }
            }
            
            return traverse_path_pattern_iterative(graph, prep_pattern, 1, std::move(initial_rows), limit, pruner, path_mode, sink);
        });
     });
}

/**
 * @brief Resolves a pattern's non-literal property map values -- a variable bound by an earlier segment,
 *        a LET binding, any computed expression -- against the given row, yielding a pattern whose
 *        property maps are ordinary literals. Everything downstream (the indexed start-node lookup, the
 *        property matchers, the fast-path guards) then needs no special case for them.
 */
static PathPattern bind_property_exprs(const PathPattern& pattern, const GqlRow& row) {
    if (pattern.nodes.empty() && pattern.edges.empty()) {
        return pattern;
    }
    PathPattern bound = pattern;
    for (auto& node : bound.nodes) {
        for (const auto& [prop, expr] : node.property_exprs) {
            node.properties[prop] = evaluate_expression(row, expr.get()).property;
        }
    }
    for (auto& edge : bound.edges) {
        for (const auto& [prop, expr] : edge.property_exprs) {
            edge.properties[prop] = evaluate_expression(row, expr.get()).property;
        }
    }
    return bound;
}

seastar::future<std::vector<GqlRow>> traverse_path_pattern(ragedb::Graph& graph, const PathPattern& pattern, const GqlRow& base_row, size_t limit, const ProjectionPruner& pruner, std::string sort_property, bool sort_ascending, bool sort_by_id, PathMode path_mode, GqlRowSink* sink) {
    PathPattern prep_pattern = bind_property_exprs(pattern, base_row);

    for (size_t j = 0; j < prep_pattern.nodes.size(); ++j) {
        if (prep_pattern.nodes[j].variable.empty()) {
            prep_pattern.nodes[j].variable = "_n_" + std::to_string(j) + "_user_empty";
        }
    }
    for (size_t j = 0; j < prep_pattern.edges.size(); ++j) {
        if (prep_pattern.edges[j].variable.empty()) {
            prep_pattern.edges[j].variable = "_e_" + std::to_string(j) + "_user_empty";
        }
    }

    // anchor on a bound (e.g. piped) node when the written first node is unbound. If the
    // pattern's first node has no binding in base_row but its last node does, reverse the pattern so
    // the traversal starts from the bound node instead of scanning the first node's whole label (a
    // piped `(forum:Forum)-[:CONTAINER_OF]->(post)-[:HAS_CREATOR]->(f)` otherwise scans every Forum
    // per piped f). Reversal is semantically identical -- same paths, edge directions flipped.
    // Restricted to the no-start-sort case so the start-node ordering optimisation is never dropped.
    if (sort_property.empty() && !sort_by_id && prep_pattern.nodes.size() >= 2 &&
        prep_pattern.edges.size() + 1 == prep_pattern.nodes.size() &&
        base_row.bindings.find(prep_pattern.nodes.front().variable) == base_row.bindings.end() &&
        base_row.bindings.find(prep_pattern.nodes.back().variable) != base_row.bindings.end()) {
        std::reverse(prep_pattern.nodes.begin(), prep_pattern.nodes.end());
        std::reverse(prep_pattern.edges.begin(), prep_pattern.edges.end());
        for (auto& edge : prep_pattern.edges) {
            if (edge.direction == EdgeDirection::RIGHT) edge.direction = EdgeDirection::LEFT;
            else if (edge.direction == EdgeDirection::LEFT) edge.direction = EdgeDirection::RIGHT;
        }
    }

    bool has_node_seek = false;
    if (prep_pattern.nodes[0].label_expr && prep_pattern.nodes[0].label_expr->kind == LabelExprKind::LITERAL) {
        std::string label = prep_pattern.nodes[0].label_expr->name;
        for (const auto& [prop, val] : prep_pattern.nodes[0].properties) {
            if (graph.shard.local().NodeIndexExists(label, prop)) {
                has_node_seek = true;
                break;
            }
        }
        if (!has_node_seek) {
            for (const auto& filter : prep_pattern.nodes[0].property_filters) {
                if (filter.op == Operation::EQ && graph.shard.local().NodeIndexExists(label, filter.property)) {
                    has_node_seek = true;
                    break;
                }
            }
        }
    }

    bool has_edge_seek = false;
    if (!prep_pattern.edges.empty()) {
        const auto& edge = prep_pattern.edges[0];
        if (edge.label_expr && edge.label_expr->kind == LabelExprKind::LITERAL) {
            std::string label = edge.label_expr->name;
            for (const auto& [prop, val] : edge.properties) {
                if (graph.shard.local().RelationshipIndexExists(label, prop)) {
                    has_edge_seek = true;
                    break;
                }
            }
            if (!has_edge_seek) {
                for (const auto& filter : edge.property_filters) {
                    if (filter.op == Operation::EQ && graph.shard.local().RelationshipIndexExists(label, filter.property)) {
                        has_edge_seek = true;
                        break;
                    }
                }
            }
        }
    }

    auto start_node_var = prep_pattern.nodes[0].variable;
    auto bound_it = base_row.bindings.find(start_node_var);

    if (bound_it == base_row.bindings.end() && !has_node_seek && has_edge_seek) {
        return traverse_from_relationship_index(graph, prep_pattern, base_row, limit, pruner, path_mode, sink);
    }

    // An unbound anchor used to trigger a full-label materialisation: edge patterns cannot bound the
    // anchor scan by the LIMIT (a start node may yield no match), and a bare node scan was not paged at
    // all. Drive both page by page instead: expand each page, accumulate matches (or hand them to the
    // sink), and stop as soon as the LIMIT is satisfied or the scan is exhausted, keeping peak memory at
    // one page rather than the whole label. With no edges the page's rows ARE the result, so they go
    // straight to the sink -- which is what makes a streamed aggregate over a plain label bounded.
    if (bound_it == base_row.bindings.end() &&
        start_scan_is_pageable(prep_pattern.nodes[0], sort_property, sort_by_id)) {
        struct ChunkScanState {
            std::vector<GqlRow> rows;
            uint64_t skip = 0;
            bool exhausted = false;
        };
        auto state = std::make_shared<ChunkScanState>();
        const uint64_t chunk = gql_scan_chunk_size > 0 ? gql_scan_chunk_size : 65536;
        return seastar::repeat([state, chunk, &graph, prep_pattern, base_row, limit, pruner, path_mode, sink]() {
            return get_start_nodes_page(graph, prep_pattern.nodes[0], state->skip, chunk, pruner)
            .then([state, chunk, &graph, prep_pattern, base_row, limit, pruner, path_mode, sink](std::vector<Node> page) {
                state->exhausted = page.size() < chunk;
                state->skip += page.size();

                std::vector<GqlRow> initial_rows;
                for (const auto& node : page) {
                    if (prep_pattern.nodes[0].label_expr && !matches_label_expr(node.getType(), prep_pattern.nodes[0].label_expr)) {
                        continue;
                    }
                    GqlRow new_row = base_row;
                    new_row.bindings[prep_pattern.nodes[0].variable] = GqlValue(node);
                    new_row.bindings["_n_0"] = GqlValue(node);
                    if (prep_pattern.nodes[0].where_expr && !evaluate_expression(new_row, prep_pattern.nodes[0].where_expr.get()).is_truthy()) {
                        continue;
                    }
                    initial_rows.push_back(std::move(new_row));
                }

                if (initial_rows.empty()) {
                    return seastar::make_ready_future<seastar::stop_iteration>(
                        state->exhausted ? seastar::stop_iteration::yes : seastar::stop_iteration::no);
                }
                return traverse_path_pattern_iterative(graph, prep_pattern, 0, std::move(initial_rows), limit, pruner, path_mode, sink)
                .then([state, limit](std::vector<GqlRow> matched) {
                    for (auto& r : matched) {
                        state->rows.push_back(std::move(r));
                        if (limit > 0 && state->rows.size() >= limit) break;
                    }
                    if (limit > 0 && state->rows.size() >= limit) return seastar::stop_iteration::yes;
                    return state->exhausted ? seastar::stop_iteration::yes : seastar::stop_iteration::no;
                });
            });
        }).then([state]() {
            return seastar::make_ready_future<std::vector<GqlRow>>(std::move(state->rows));
        });
    }

    size_t start_node_limit = prep_pattern.edges.empty() ? limit : 0;
    seastar::future<std::vector<Node>> start_nodes_fut = seastar::make_ready_future<std::vector<Node>>();
    if (bound_it != base_row.bindings.end() && bound_it->second.type == GqlValue::NODE) {
        start_nodes_fut = seastar::make_ready_future<std::vector<Node>>(std::vector<Node>{ *bound_it->second.node });
    } else {
        start_nodes_fut = get_start_nodes(graph, prep_pattern.nodes[0], start_node_limit, pruner, sort_property, sort_ascending, sort_by_id);
    }

    return start_nodes_fut.then([base_row, prep_pattern, &graph, limit, pruner, path_mode, sink](std::vector<Node> start_nodes) {
        std::vector<GqlRow> initial_rows;
        for (const auto& node : start_nodes) {
            if (prep_pattern.nodes[0].label_expr && !matches_label_expr(node.getType(), prep_pattern.nodes[0].label_expr)) {
                continue;
            }
            if (!matches_properties(node.getProperties(), prep_pattern.nodes[0].properties) || !matches_filters(node.getProperties(), prep_pattern.nodes[0].property_filters)) {
                continue;
            }
            GqlRow new_row = base_row;
            new_row.bindings[prep_pattern.nodes[0].variable] = GqlValue(node);
            new_row.bindings["_n_0"] = GqlValue(node);
            if (prep_pattern.nodes[0].where_expr && !evaluate_expression(new_row, prep_pattern.nodes[0].where_expr.get()).is_truthy()) {
                continue;
            }
            initial_rows.push_back(new_row);
            if (limit > 0 && prep_pattern.edges.empty() && initial_rows.size() >= limit) {
                break;
            }
        }

        return traverse_path_pattern_iterative(graph, prep_pattern, 0, std::move(initial_rows), limit, pruner, path_mode, sink);
    });
}

static seastar::future<int64_t> propagate_path_counts(
    ragedb::Graph& graph,
    std::unordered_map<uint64_t, int64_t> current_counts,
    uint16_t hops_remaining,
    Direction dir,
    std::vector<std::string> rel_types
) {
    if (hops_remaining == 0) {
        int64_t total = 0;
        for (const auto& [id, count] : current_counts) {
            total += count;
        }
        return seastar::make_ready_future<int64_t>(total);
    }
    if (current_counts.empty()) {
        return seastar::make_ready_future<int64_t>(0);
    }
    
    std::vector<seastar::future<std::pair<std::vector<uint64_t>, int64_t>>> futs;
    futs.reserve(current_counts.size());
    for (const auto& [u, count] : current_counts) {
        if (count > 0) {
            futs.push_back(
                graph.shard.local().KHopIdsPeered(u, 1, dir, rel_types)
                .then([count](std::vector<uint64_t> neighbors) {
                    return std::make_pair(std::move(neighbors), count);
                })
            );
        }
    }
    
    return seastar::when_all_succeed(futs.begin(), futs.end())
    .then([&graph, hops_remaining, dir, rel_types = std::move(rel_types)](std::vector<std::pair<std::vector<uint64_t>, int64_t>> results) mutable {
        std::unordered_map<uint64_t, int64_t> next_counts;
        for (auto& [neighbors, count] : results) {
            for (uint64_t nbr : neighbors) {
                next_counts[nbr] += count;
            }
        }
        return propagate_path_counts(graph, std::move(next_counts), hops_remaining - 1, dir, std::move(rel_types));
    });
}

seastar::future<std::vector<GqlRow>> traverse_match_statement(ragedb::Graph& graph, const MatchStatement& stmt_in, const GqlRow& row, size_t limit, const ProjectionPruner& pruner, std::string sort_property, bool sort_ascending, bool sort_by_id) {
    // Resolve any non-literal property map values against the incoming row up front, so every path
    // below -- the k-hop and shortest-path searches as well as the general traversal -- sees ordinary
    // literal property maps and needs no special case for them.
    MatchStatement bound_stmt = stmt_in;
    bound_stmt.pattern = bind_property_exprs(stmt_in.pattern, row);
    const MatchStatement& stmt = bound_stmt;

    // Case 0.25: Algebraic Path Count Traversal.
    if (stmt.algebraic_path_count) {
        // Resolve start nodes candidate set.
        seastar::future<std::vector<Node>> start_nodes_fut = seastar::make_ready_future<std::vector<Node>>();
        auto start_node_var = stmt.pattern.nodes[0].variable;
        if (!start_node_var.empty()) {
            auto bound_start_it = row.bindings.find(start_node_var);
            if (bound_start_it != row.bindings.end() && bound_start_it->second.type == GqlValue::NODE) {
                start_nodes_fut = seastar::make_ready_future<std::vector<Node>>(std::vector<Node>{ *bound_start_it->second.node });
            } else {
                start_nodes_fut = get_start_nodes(graph, stmt.pattern.nodes[0], 0, pruner);
            }
        } else {
            start_nodes_fut = get_start_nodes(graph, stmt.pattern.nodes[0], 0, pruner);
        }

        return start_nodes_fut.then([&graph, stmt, row](std::vector<Node> start_nodes) {
            // Filter start node candidates by labels and property constraint expressions.
            std::vector<Node> valid_starts;
            for (const auto& node : start_nodes) {
                if (stmt.pattern.nodes[0].label_expr && !matches_label_expr(node.getType(), stmt.pattern.nodes[0].label_expr)) {
                    continue;
                }
                if (!matches_properties(node.getProperties(), stmt.pattern.nodes[0].properties) || !matches_filters(node.getProperties(), stmt.pattern.nodes[0].property_filters)) {
                    continue;
                }
                valid_starts.push_back(node);
            }

            auto start_node_var = stmt.pattern.nodes[0].variable;
            auto end_node_var = stmt.path_count_target_var;

            if (valid_starts.empty()) {
                if (stmt.is_optional || stmt.optional_group_id >= 0) {
                    GqlRow opt_row = row;
                    if (!start_node_var.empty() && opt_row.bindings.find(start_node_var) == opt_row.bindings.end()) {
                        opt_row.bindings[start_node_var] = GqlValue();
                    }
                    if (!end_node_var.empty() && opt_row.bindings.find(end_node_var) == opt_row.bindings.end()) {
                        opt_row.bindings[end_node_var] = GqlValue();
                    }
                    return seastar::make_ready_future<std::vector<GqlRow>>(std::vector<GqlRow>{opt_row});
                }
                return seastar::make_ready_future<std::vector<GqlRow>>(std::vector<GqlRow>{});
            }

            Direction dir = Direction::BOTH;
            if (stmt.path_count_dir == EdgeDirection::RIGHT) dir = Direction::OUT;
            else if (stmt.path_count_dir == EdgeDirection::LEFT) dir = Direction::IN;

            std::vector<seastar::future<int64_t>> path_futs;
            path_futs.reserve(valid_starts.size());
            for (const auto& s : valid_starts) {
                std::unordered_map<uint64_t, int64_t> start_counts;
                start_counts[s.getId()] = 1;
                path_futs.push_back(propagate_path_counts(graph, std::move(start_counts), stmt.path_count_hops, dir, stmt.path_count_rel_types));
            }

            return seastar::when_all_succeed(path_futs.begin(), path_futs.end())
            .then([row, valid_starts, start_node_var, end_node_var](std::vector<int64_t> path_counts) {
                std::vector<GqlRow> out_rows;
                out_rows.reserve(valid_starts.size());
                for (size_t i = 0; i < valid_starts.size(); ++i) {
                    GqlRow new_row = row;
                    if (!start_node_var.empty()) {
                        new_row.bindings[start_node_var] = GqlValue(valid_starts[i]);
                    }
                    new_row.bindings[end_node_var] = GqlValue(property_type_t(path_counts[i]));
                    out_rows.push_back(std::move(new_row));
                }
                return out_rows;
            });
        });
    }

    // Case 0: K-Hop Traversal.
    if (stmt.is_khop) {
        // Resolve start nodes candidate set.
        seastar::future<std::vector<Node>> start_nodes_fut = seastar::make_ready_future<std::vector<Node>>();
        auto start_node_var = stmt.pattern.nodes[0].variable;
        if (!start_node_var.empty()) {
            auto bound_start_it = row.bindings.find(start_node_var);
            if (bound_start_it != row.bindings.end() && bound_start_it->second.type == GqlValue::NODE) {
                start_nodes_fut = seastar::make_ready_future<std::vector<Node>>(std::vector<Node>{ *bound_start_it->second.node });
            } else {
                start_nodes_fut = get_start_nodes(graph, stmt.pattern.nodes[0], 0, pruner);
            }
        } else {
            start_nodes_fut = get_start_nodes(graph, stmt.pattern.nodes[0], 0, pruner);
        }

        return start_nodes_fut.then([&graph, stmt, row](std::vector<Node> start_nodes) {
            // Filter start node candidates by labels and property constraint expressions.
            std::vector<Node> valid_starts;
            for (const auto& node : start_nodes) {
                if (stmt.pattern.nodes[0].label_expr && !matches_label_expr(node.getType(), stmt.pattern.nodes[0].label_expr)) {
                    continue;
                }
                if (!matches_properties(node.getProperties(), stmt.pattern.nodes[0].properties) || !matches_filters(node.getProperties(), stmt.pattern.nodes[0].property_filters)) {
                    continue;
                }
                valid_starts.push_back(node);
            }

            auto start_node_var = stmt.pattern.nodes[0].variable;
            auto end_node_var = stmt.pattern.nodes[1].variable;

            if (valid_starts.empty()) {
                if (stmt.is_optional || stmt.optional_group_id >= 0) {
                    GqlRow opt_row = row;
                    if (!start_node_var.empty() && opt_row.bindings.find(start_node_var) == opt_row.bindings.end()) {
                        opt_row.bindings[start_node_var] = GqlValue();
                    }
                    if (!end_node_var.empty() && opt_row.bindings.find(end_node_var) == opt_row.bindings.end()) {
                        opt_row.bindings[end_node_var] = GqlValue();
                    }
                    return seastar::make_ready_future<std::vector<GqlRow>>(std::vector<GqlRow>{opt_row});
                }
                return seastar::make_ready_future<std::vector<GqlRow>>(std::vector<GqlRow>{});
            }

            const auto& edge = stmt.pattern.edges[0];
            Direction dir = Direction::BOTH;
            if (edge.direction == EdgeDirection::RIGHT) dir = Direction::OUT;
            else if (edge.direction == EdgeDirection::LEFT) dir = Direction::IN;

            std::vector<std::string> rel_types;
            if (edge.label_expr) {
                auto extract_types = [](auto& self, const LabelExpression* expr, std::vector<std::string>& types) -> void {
                    if (!expr) return;
                    if (expr->kind == LabelExprKind::LITERAL) {
                        types.push_back(expr->name);
                    } else if (expr->kind == LabelExprKind::OR) {
                        self(self, expr->left.get(), types);
                        self(self, expr->right.get(), types);
                    }
                };
                extract_types(extract_types, edge.label_expr.get(), rel_types);
            }

            uint64_t min_hops = edge.is_variable_length ? edge.min_hops : 1;
            uint64_t max_hops = edge.is_variable_length ? edge.max_hops : 1;

            // Check if the end node is bound in row.
            bool end_node_bound = false;
            Node bound_end_node;
            auto bound_end_it = row.bindings.find(end_node_var);
            if (bound_end_it != row.bindings.end() && bound_end_it->second.type == GqlValue::NODE) {
                end_node_bound = true;
                bound_end_node = *bound_end_it->second.node;
            }

            bool end_has_constraints = stmt.pattern.nodes[1].label_expr || 
                                       !stmt.pattern.nodes[1].properties.empty() || 
                                       !stmt.pattern.nodes[1].property_filters.empty() || 
                                       end_node_bound;

            if (stmt.khop_count_only) {
                if (!end_has_constraints) {
                    // Optimized: Use KHopCountPeered directly.
                    std::vector<seastar::future<uint64_t>> futs;
                    for (const auto& s : valid_starts) {
                        futs.push_back(graph.shard.local().KHopCountPeered(s.getId(), max_hops, dir, rel_types));
                    }
                    seastar::future<std::vector<uint64_t>> max_counts_fut = seastar::when_all_succeed(futs.begin(), futs.end());

                    seastar::future<std::vector<uint64_t>> min_counts_fut = seastar::make_ready_future<std::vector<uint64_t>>();
                    if (min_hops > 1) {
                        std::vector<seastar::future<uint64_t>> min_futs;
                        for (const auto& s : valid_starts) {
                            min_futs.push_back(graph.shard.local().KHopCountPeered(s.getId(), min_hops - 1, dir, rel_types));
                        }
                        min_counts_fut = seastar::when_all_succeed(min_futs.begin(), min_futs.end());
                    } else {
                        min_counts_fut = seastar::make_ready_future<std::vector<uint64_t>>(std::vector<uint64_t>(valid_starts.size(), 0));
                    }

                    return seastar::when_all_succeed(std::move(max_counts_fut), std::move(min_counts_fut))
                    .then([row, valid_starts, start_node_var, end_node_var](std::tuple<std::vector<uint64_t>, std::vector<uint64_t>> counts_tuple) {
                        auto& max_counts = std::get<0>(counts_tuple);
                        auto& min_counts = std::get<1>(counts_tuple);
                        std::vector<GqlRow> out_rows;
                        for (size_t i = 0; i < valid_starts.size(); ++i) {
                            int64_t count = static_cast<int64_t>(max_counts[i]) - static_cast<int64_t>(min_counts[i]);
                            if (count < 0) count = 0;
                            GqlRow new_row = row;
                            if (!start_node_var.empty()) {
                                new_row.bindings[start_node_var] = GqlValue(valid_starts[i]);
                            }
                            new_row.bindings[end_node_var] = GqlValue(property_type_t(count));
                            out_rows.push_back(std::move(new_row));
                        }
                        return out_rows;
                    });
                } else {
                    // Count only but end node has constraints/filters or is bound.
                    // We must retrieve the IDs, subtract if min_hops > 1, fetch nodes, filter them, and count.
                    std::vector<seastar::future<std::vector<uint64_t>>> max_futs;
                    for (const auto& s : valid_starts) {
                        max_futs.push_back(graph.shard.local().KHopIdsPeered(s.getId(), max_hops, dir, rel_types));
                    }
                    seastar::future<std::vector<std::vector<uint64_t>>> max_ids_fut = seastar::when_all_succeed(max_futs.begin(), max_futs.end());

                    seastar::future<std::vector<std::vector<uint64_t>>> min_ids_fut = seastar::make_ready_future<std::vector<std::vector<uint64_t>>>();
                    if (min_hops > 1) {
                        std::vector<seastar::future<std::vector<uint64_t>>> min_futs;
                        for (const auto& s : valid_starts) {
                            min_futs.push_back(graph.shard.local().KHopIdsPeered(s.getId(), min_hops - 1, dir, rel_types));
                        }
                        min_ids_fut = seastar::when_all_succeed(min_futs.begin(), min_futs.end());
                    } else {
                        min_ids_fut = seastar::make_ready_future<std::vector<std::vector<uint64_t>>>(std::vector<std::vector<uint64_t>>(valid_starts.size()));
                    }

                    return seastar::when_all_succeed(std::move(max_ids_fut), std::move(min_ids_fut))
                    .then([&graph, stmt, row, valid_starts, start_node_var, end_node_var, end_node_bound, bound_end_node](std::tuple<std::vector<std::vector<uint64_t>>, std::vector<std::vector<uint64_t>>> ids_tuple) {
                        auto& max_ids_list = std::get<0>(ids_tuple);
                        auto& min_ids_list = std::get<1>(ids_tuple);

                        std::vector<seastar::future<std::vector<Node>>> fetch_futs;
                        auto remaining_ids_list = std::make_shared<std::vector<std::vector<uint64_t>>>();
                        for (size_t i = 0; i < valid_starts.size(); ++i) {
                            std::vector<uint64_t> max_ids = max_ids_list[i];
                            std::vector<uint64_t> min_ids = min_ids_list[i];
                            std::sort(max_ids.begin(), max_ids.end());
                            std::sort(min_ids.begin(), min_ids.end());
                            std::vector<uint64_t> diff_ids;
                            std::set_difference(max_ids.begin(), max_ids.end(), min_ids.begin(), min_ids.end(), std::back_inserter(diff_ids));
                            remaining_ids_list->push_back(diff_ids);
                            fetch_futs.push_back(graph.shard.local().NodesGetPeered(diff_ids));
                        }

                        return seastar::when_all_succeed(fetch_futs.begin(), fetch_futs.end())
                        .then([row, stmt, valid_starts, start_node_var, end_node_var, end_node_bound, bound_end_node](std::vector<std::vector<Node>> fetched_nodes_list) {
                            std::vector<GqlRow> out_rows;
                            for (size_t i = 0; i < valid_starts.size(); ++i) {
                                int64_t filtered_count = 0;
                                for (const auto& node : fetched_nodes_list[i]) {
                                    if (end_node_bound && node.getId() != bound_end_node.getId()) {
                                        continue;
                                    }
                                    if (stmt.pattern.nodes[1].label_expr && !matches_label_expr(node.getType(), stmt.pattern.nodes[1].label_expr)) {
                                        continue;
                                    }
                                    if (!matches_properties(node.getProperties(), stmt.pattern.nodes[1].properties) || !matches_filters(node.getProperties(), stmt.pattern.nodes[1].property_filters)) {
                                        continue;
                                    }
                                    GqlRow temp_row = row;
                                    if (!start_node_var.empty()) {
                                        temp_row.bindings[start_node_var] = GqlValue(valid_starts[i]);
                                    }
                                    temp_row.bindings[end_node_var] = GqlValue(node);
                                    if (stmt.pattern.nodes[1].where_expr && !evaluate_expression(temp_row, stmt.pattern.nodes[1].where_expr.get()).is_truthy()) {
                                        continue;
                                    }
                                    filtered_count++;
                                }
                                GqlRow new_row = row;
                                if (!start_node_var.empty()) {
                                    new_row.bindings[start_node_var] = GqlValue(valid_starts[i]);
                                }
                                new_row.bindings[end_node_var] = GqlValue(property_type_t(filtered_count));
                                out_rows.push_back(std::move(new_row));
                            }
                            return out_rows;
                        });
                    });
                }
            } else {
                // khop_count_only is false. Return rows binding start_node and end_node (each valid neighbor node).
                std::vector<seastar::future<std::vector<uint64_t>>> max_futs;
                for (const auto& s : valid_starts) {
                    max_futs.push_back(graph.shard.local().KHopIdsPeered(s.getId(), max_hops, dir, rel_types));
                }
                seastar::future<std::vector<std::vector<uint64_t>>> max_ids_fut = seastar::when_all_succeed(max_futs.begin(), max_futs.end());

                seastar::future<std::vector<std::vector<uint64_t>>> min_ids_fut = seastar::make_ready_future<std::vector<std::vector<uint64_t>>>();
                if (min_hops > 1) {
                    std::vector<seastar::future<std::vector<uint64_t>>> min_futs;
                    for (const auto& s : valid_starts) {
                        min_futs.push_back(graph.shard.local().KHopIdsPeered(s.getId(), min_hops - 1, dir, rel_types));
                    }
                    min_ids_fut = seastar::when_all_succeed(min_futs.begin(), min_futs.end());
                } else {
                    min_ids_fut = seastar::make_ready_future<std::vector<std::vector<uint64_t>>>(std::vector<std::vector<uint64_t>>(valid_starts.size()));
                }

                return seastar::when_all_succeed(std::move(max_ids_fut), std::move(min_ids_fut))
                .then([&graph, stmt, row, valid_starts, start_node_var, end_node_var, end_node_bound, bound_end_node](std::tuple<std::vector<std::vector<uint64_t>>, std::vector<std::vector<uint64_t>>> ids_tuple) {
                    auto& max_ids_list = std::get<0>(ids_tuple);
                    auto& min_ids_list = std::get<1>(ids_tuple);

                    std::vector<seastar::future<std::vector<Node>>> fetch_futs;
                    for (size_t i = 0; i < valid_starts.size(); ++i) {
                        std::vector<uint64_t> max_ids = max_ids_list[i];
                        std::vector<uint64_t> min_ids = min_ids_list[i];
                        std::sort(max_ids.begin(), max_ids.end());
                        std::sort(min_ids.begin(), min_ids.end());
                        std::vector<uint64_t> diff_ids;
                        std::set_difference(max_ids.begin(), max_ids.end(), min_ids.begin(), min_ids.end(), std::back_inserter(diff_ids));
                        fetch_futs.push_back(graph.shard.local().NodesGetPeered(diff_ids));
                    }

                    return seastar::when_all_succeed(fetch_futs.begin(), fetch_futs.end())
                    .then([row, stmt, valid_starts, start_node_var, end_node_var, end_node_bound, bound_end_node](std::vector<std::vector<Node>> fetched_nodes_list) {
                        std::vector<GqlRow> out_rows;
                        for (size_t i = 0; i < valid_starts.size(); ++i) {
                            bool has_any_match = false;
                            for (const auto& node : fetched_nodes_list[i]) {
                                if (end_node_bound && node.getId() != bound_end_node.getId()) {
                                    continue;
                                }
                                if (stmt.pattern.nodes[1].label_expr && !matches_label_expr(node.getType(), stmt.pattern.nodes[1].label_expr)) {
                                    continue;
                                }
                                if (!matches_properties(node.getProperties(), stmt.pattern.nodes[1].properties) || !matches_filters(node.getProperties(), stmt.pattern.nodes[1].property_filters)) {
                                    continue;
                                }
                                GqlRow temp_row = row;
                                if (!start_node_var.empty()) {
                                    temp_row.bindings[start_node_var] = GqlValue(valid_starts[i]);
                                }
                                temp_row.bindings[end_node_var] = GqlValue(node);
                                if (stmt.pattern.nodes[1].where_expr && !evaluate_expression(temp_row, stmt.pattern.nodes[1].where_expr.get()).is_truthy()) {
                                    continue;
                                }
                                has_any_match = true;
                                GqlRow new_row = row;
                                if (!start_node_var.empty()) {
                                    new_row.bindings[start_node_var] = GqlValue(valid_starts[i]);
                                }
                                new_row.bindings[end_node_var] = GqlValue(node);
                                out_rows.push_back(std::move(new_row));
                            }
                            if (!has_any_match && (stmt.is_optional || stmt.optional_group_id >= 0)) {
                                GqlRow opt_row = row;
                                if (!start_node_var.empty()) {
                                    opt_row.bindings[start_node_var] = GqlValue(valid_starts[i]);
                                }
                                opt_row.bindings[end_node_var] = GqlValue();
                                out_rows.push_back(std::move(opt_row));
                            }
                        }
                        return out_rows;
                    });
                });
            }
        });
    }

    // Case 0.5: Equivalence Partition Lookup (Phase 26)
    if (stmt.equivalence_partition_lookup) {
        auto start_node_var = stmt.pattern.nodes[0].variable;
        auto end_node_var = stmt.pattern.nodes[1].variable;
        const auto& edge = stmt.pattern.edges[0];
        std::string rel_type = "";
        if (edge.label_expr && edge.label_expr->kind == LabelExprKind::LITERAL) {
            rel_type = edge.label_expr->name;
        }

        using PartitionPtr = std::shared_ptr<const WccCache::PartitionMap>;
        seastar::future<PartitionPtr> partitions_fut = seastar::make_ready_future<PartitionPtr>();
        if (auto cached = WccCache::local().get(rel_type)) {
            partitions_fut = seastar::make_ready_future<PartitionPtr>(std::move(cached));
        } else {
            // WeaklyConnectedComponentsPeered blocks on cross-shard futures internally (.get0()),
            // which aborts when run on the reactor thread (the HTTP/GQL path, smp>1). Run it inside a
            // seastar::thread where waiting is legal, then cache and continue asynchronously.
            partitions_fut = seastar::async([&graph, rel_type] {
                return graph.shard.local().WeaklyConnectedComponentsPeered(rel_type, "", "", "", "", true, false);
            }).then([rel_type](std::map<uint64_t, uint64_t> wcc) {
                return WccCache::local().set(rel_type, std::move(wcc));
            });
        }

        seastar::future<std::vector<Node>> start_nodes_fut = seastar::make_ready_future<std::vector<Node>>();
        if (!start_node_var.empty()) {
            auto bound_start_it = row.bindings.find(start_node_var);
            if (bound_start_it != row.bindings.end() && bound_start_it->second.type == GqlValue::NODE) {
                start_nodes_fut = seastar::make_ready_future<std::vector<Node>>(std::vector<Node>{ *bound_start_it->second.node });
            } else {
                start_nodes_fut = get_start_nodes(graph, stmt.pattern.nodes[0], 0, pruner);
            }
        } else {
            start_nodes_fut = get_start_nodes(graph, stmt.pattern.nodes[0], 0, pruner);
        }

        seastar::future<std::vector<Node>> end_nodes_fut = seastar::make_ready_future<std::vector<Node>>();
        if (!end_node_var.empty()) {
            auto bound_end_it = row.bindings.find(end_node_var);
            if (bound_end_it != row.bindings.end() && bound_end_it->second.type == GqlValue::NODE) {
                end_nodes_fut = seastar::make_ready_future<std::vector<Node>>(std::vector<Node>{ *bound_end_it->second.node });
            } else {
                end_nodes_fut = get_start_nodes(graph, stmt.pattern.nodes[1], 0, pruner);
            }
        } else {
            end_nodes_fut = get_start_nodes(graph, stmt.pattern.nodes[1], 0, pruner);
        }

        return seastar::when_all_succeed(std::move(partitions_fut), std::move(start_nodes_fut), std::move(end_nodes_fut))
        .then([stmt, row, start_node_var, end_node_var, rel_type](std::tuple<PartitionPtr, std::vector<Node>, std::vector<Node>> results)
                -> seastar::future<std::vector<GqlRow>> {
            auto wcc_ptr = std::move(std::get<0>(results));
            const auto& wcc = *wcc_ptr;
            auto start_nodes = std::move(std::get<1>(results));
            auto end_nodes = std::move(std::get<2>(results));

            std::vector<Node> valid_starts;
            for (const auto& node : start_nodes) {
                if (stmt.pattern.nodes[0].label_expr && !matches_label_expr(node.getType(), stmt.pattern.nodes[0].label_expr)) {
                    continue;
                }
                if (!matches_properties(node.getProperties(), stmt.pattern.nodes[0].properties) || !matches_filters(node.getProperties(), stmt.pattern.nodes[0].property_filters)) {
                    continue;
                }
                valid_starts.push_back(node);
            }

            std::vector<Node> valid_ends;
            for (const auto& node : end_nodes) {
                if (stmt.pattern.nodes[1].label_expr && !matches_label_expr(node.getType(), stmt.pattern.nodes[1].label_expr)) {
                    continue;
                }
                if (!matches_properties(node.getProperties(), stmt.pattern.nodes[1].properties) || !matches_filters(node.getProperties(), stmt.pattern.nodes[1].property_filters)) {
                    continue;
                }
                valid_ends.push_back(node);
            }

            // When neither endpoint is bound this pairs every valid start against every valid end
            // (O(V^2)); yield periodically so the enumeration stays cooperative on the reactor.
            std::vector<GqlRow> out_rows;
            uint64_t work = 0;
            for (const auto& s : valid_starts) {
                uint64_t s_id = s.getId();
                auto s_it = wcc.find(s_id);
                if (s_it == wcc.end()) continue;
                uint64_t s_root = s_it->second;

                for (const auto& e : valid_ends) {
                    uint64_t e_id = e.getId();
                    auto e_it = wcc.find(e_id);
                    if (e_it == wcc.end()) continue;
                    uint64_t e_root = e_it->second;

                    if (s_root == e_root) {
                        GqlRow new_row = row;
                        if (!start_node_var.empty()) {
                            new_row.bindings[start_node_var] = GqlValue(s);
                        }
                        new_row.bindings["_n_0"] = GqlValue(s);
                        if (!end_node_var.empty()) {
                            new_row.bindings[end_node_var] = GqlValue(e);
                        }
                        new_row.bindings["_n_1"] = GqlValue(e);

                        if (!stmt.path_variable.empty()) {
                            new_row.bindings[stmt.path_variable] = GqlValue(Path({s, e}, {}));
                        }
                        out_rows.push_back(std::move(new_row));
                    }
                    if ((++work & 0xFFFF) == 0) co_await seastar::coroutine::maybe_yield();
                }
            }

            if (out_rows.empty() && stmt.is_optional) {
                GqlRow opt_row = row;
                if (!start_node_var.empty() && opt_row.bindings.find(start_node_var) == opt_row.bindings.end()) {
                    opt_row.bindings[start_node_var] = GqlValue();
                }
                if (!end_node_var.empty() && opt_row.bindings.find(end_node_var) == opt_row.bindings.end()) {
                    opt_row.bindings[end_node_var] = GqlValue();
                }
                co_return std::vector<GqlRow>{opt_row};
            }

            co_return out_rows;
        });
    }

    // Case 0.6: Transitive Reachability Index Lookup (Phase 23)
    if (stmt.transitive_reachability_lookup) {
        auto start_node_var = stmt.pattern.nodes[0].variable;
        auto end_node_var = stmt.pattern.nodes[1].variable;
        const auto& edge = stmt.pattern.edges[0];
        std::string rel_type = "";
        if (edge.label_expr && edge.label_expr->kind == LabelExprKind::LITERAL) {
            rel_type = edge.label_expr->name;
        }

        using DescendantsPtr = std::shared_ptr<const TransitiveReachabilityCache::DescendantsMap>;
        seastar::future<DescendantsPtr> descendants_fut = seastar::make_ready_future<DescendantsPtr>();
        if (auto cached = TransitiveReachabilityCache::local().get(rel_type)) {
            descendants_fut = seastar::make_ready_future<DescendantsPtr>(std::move(cached));
        } else {
            // ReachablePeered blocks on cross-shard futures internally (.get0()), which aborts on the
            // reactor thread (the HTTP/GQL path, smp>1). Run it inside a seastar::thread where waiting
            // is legal, then build the descendants index and cache it asynchronously.
            descendants_fut = seastar::async([&graph, rel_type] {
                return graph.shard.local().ReachablePeered(rel_type, "", "", "", "", true, false);
            }).then([rel_type](std::vector<std::pair<uint64_t, uint64_t>> pairs) {
                std::map<uint64_t, std::unordered_set<uint64_t>> descendants;
                for (const auto& p : pairs) {
                    descendants[p.first].insert(p.second);
                }
                return TransitiveReachabilityCache::local().set(rel_type, std::move(descendants));
            });
        }

        return descendants_fut.then([&graph, stmt, row, start_node_var, end_node_var, rel_type, pruner](DescendantsPtr descendants_ptr) {
        const auto& start_pat = stmt.pattern.nodes[0];
        const auto& end_pat = stmt.pattern.nodes[1];
        // An unbound endpoint with no label/property/filter constraint would scan the whole graph via
        // get_start_nodes. Derive its candidates from the reachability map instead (start side = the
        // map's keys, end side = the reachable set), bounding memory by the relation's node set rather
        // than the entire graph. Labeled/constrained endpoints keep get_start_nodes (bounded by label).
        bool start_unconstrained = !start_pat.label_expr && start_pat.properties.empty() &&
            start_pat.property_filters.empty() && !start_pat.where_expr;
        bool end_unconstrained = !end_pat.label_expr && end_pat.properties.empty() &&
            end_pat.property_filters.empty() && !end_pat.where_expr;

        // The unconstrained candidate id lists are the same for every incoming row, so build them
        // once per relationship type (cached alongside the closure) rather than re-deriving per row.
        std::shared_ptr<const TransitiveReachabilityCache::Candidates> cand;
        if (start_unconstrained || end_unconstrained) {
            cand = TransitiveReachabilityCache::local().get_candidates(rel_type);
        }

        seastar::future<std::vector<Node>> start_nodes_fut = seastar::make_ready_future<std::vector<Node>>();
        bool start_resolved = false;
        if (!start_node_var.empty()) {
            auto bound_start_it = row.bindings.find(start_node_var);
            if (bound_start_it != row.bindings.end() && bound_start_it->second.type == GqlValue::NODE) {
                start_nodes_fut = seastar::make_ready_future<std::vector<Node>>(std::vector<Node>{ *bound_start_it->second.node });
                start_resolved = true;
            }
        }
        if (!start_resolved) {
            if (start_unconstrained && cand) {
                start_nodes_fut = graph.shard.local().NodesGetPeered(cand->key_ids);
            } else {
                start_nodes_fut = get_start_nodes(graph, stmt.pattern.nodes[0], 0, pruner);
            }
        }

        seastar::future<std::vector<Node>> end_nodes_fut = seastar::make_ready_future<std::vector<Node>>();
        bool end_resolved = false;
        if (!end_node_var.empty()) {
            auto bound_end_it = row.bindings.find(end_node_var);
            if (bound_end_it != row.bindings.end() && bound_end_it->second.type == GqlValue::NODE) {
                end_nodes_fut = seastar::make_ready_future<std::vector<Node>>(std::vector<Node>{ *bound_end_it->second.node });
                end_resolved = true;
            }
        }
        if (!end_resolved) {
            if (end_unconstrained && cand) {
                end_nodes_fut = graph.shard.local().NodesGetPeered(cand->end_ids);
            } else {
                end_nodes_fut = get_start_nodes(graph, stmt.pattern.nodes[1], 0, pruner);
            }
        }

        return seastar::when_all_succeed(std::move(start_nodes_fut), std::move(end_nodes_fut))
        .then([stmt, row, start_node_var, end_node_var, descendants_ptr](std::tuple<std::vector<Node>, std::vector<Node>> results) {
            const auto& descendants = *descendants_ptr;
            auto start_nodes = std::move(std::get<0>(results));
            auto end_nodes = std::move(std::get<1>(results));

            std::vector<Node> valid_starts;
            for (const auto& node : start_nodes) {
                if (stmt.pattern.nodes[0].label_expr && !matches_label_expr(node.getType(), stmt.pattern.nodes[0].label_expr)) {
                    continue;
                }
                if (!matches_properties(node.getProperties(), stmt.pattern.nodes[0].properties) || !matches_filters(node.getProperties(), stmt.pattern.nodes[0].property_filters)) {
                    continue;
                }
                valid_starts.push_back(node);
            }

            std::vector<Node> valid_ends;
            for (const auto& node : end_nodes) {
                if (stmt.pattern.nodes[1].label_expr && !matches_label_expr(node.getType(), stmt.pattern.nodes[1].label_expr)) {
                    continue;
                }
                if (!matches_properties(node.getProperties(), stmt.pattern.nodes[1].properties) || !matches_filters(node.getProperties(), stmt.pattern.nodes[1].property_filters)) {
                    continue;
                }
                valid_ends.push_back(node);
            }

            std::vector<GqlRow> out_rows;
            for (const auto& s : valid_starts) {
                uint64_t s_id = s.getId();
                auto s_it = descendants.find(s_id);
                if (s_it == descendants.end()) continue;
                const auto& desc_set = s_it->second;

                for (const auto& e : valid_ends) {
                    uint64_t e_id = e.getId();
                    if (desc_set.count(e_id) > 0) {
                        GqlRow new_row = row;
                        if (!start_node_var.empty()) {
                            new_row.bindings[start_node_var] = GqlValue(s);
                        }
                        new_row.bindings["_n_0"] = GqlValue(s);
                        if (!end_node_var.empty()) {
                            new_row.bindings[end_node_var] = GqlValue(e);
                        }
                        new_row.bindings["_n_1"] = GqlValue(e);

                        if (!stmt.path_variable.empty()) {
                            new_row.bindings[stmt.path_variable] = GqlValue(Path({s, e}, {}));
                        }
                        out_rows.push_back(std::move(new_row));
                    }
                }
            }

            if (out_rows.empty() && stmt.is_optional) {
                GqlRow opt_row = row;
                if (!start_node_var.empty() && opt_row.bindings.find(start_node_var) == opt_row.bindings.end()) {
                    opt_row.bindings[start_node_var] = GqlValue();
                }
                if (!end_node_var.empty() && opt_row.bindings.find(end_node_var) == opt_row.bindings.end()) {
                    opt_row.bindings[end_node_var] = GqlValue();
                }
                return std::vector<GqlRow>{opt_row};
            }

            return out_rows;
        });
        });
    }

    // Case 1: Shortest Path Traversal.
    // If the Match statement specifies a shortest path selector (ALL, ANY, K, K_GROUP),
    // we bypass standard traversal and perform sharded BFS pathfinding between all valid start/end node pairs.
    if (stmt.shortest_path_kind != ShortestPathKind::NONE) {
        // Resolve start nodes candidate set.
        seastar::future<std::vector<Node>> start_nodes_fut = seastar::make_ready_future<std::vector<Node>>();
        auto start_node_var = stmt.pattern.nodes[0].variable;
        auto bound_start_it = row.bindings.find(start_node_var);
        if (bound_start_it != row.bindings.end() && bound_start_it->second.type == GqlValue::NODE) {
            start_nodes_fut = seastar::make_ready_future<std::vector<Node>>(std::vector<Node>{ *bound_start_it->second.node });
        } else {
            start_nodes_fut = get_start_nodes(graph, stmt.pattern.nodes[0], 0, pruner);
        }

        // Resolve end nodes candidate set.
        seastar::future<std::vector<Node>> end_nodes_fut = seastar::make_ready_future<std::vector<Node>>();
        auto end_node_var = stmt.pattern.nodes[1].variable;
        auto bound_end_it = row.bindings.find(end_node_var);
        if (bound_end_it != row.bindings.end() && bound_end_it->second.type == GqlValue::NODE) {
            end_nodes_fut = seastar::make_ready_future<std::vector<Node>>(std::vector<Node>{ *bound_end_it->second.node });
        } else {
            end_nodes_fut = get_start_nodes(graph, stmt.pattern.nodes[1], 0, pruner);
        }

        return seastar::when_all_succeed(std::move(start_nodes_fut), std::move(end_nodes_fut))
        .then([&graph, stmt, row, start_node_var, end_node_var](std::tuple<std::vector<Node>, std::vector<Node>> nodes_tuple) {
            auto& start_nodes = std::get<0>(nodes_tuple);
            auto& end_nodes = std::get<1>(nodes_tuple);

            // Filter start node candidates by labels and property constraint expressions.
            std::vector<Node> valid_starts;
            for (const auto& node : start_nodes) {
                if (stmt.pattern.nodes[0].label_expr && !matches_label_expr(node.getType(), stmt.pattern.nodes[0].label_expr)) {
                    continue;
                }
                if (!matches_properties(node.getProperties(), stmt.pattern.nodes[0].properties) || !matches_filters(node.getProperties(), stmt.pattern.nodes[0].property_filters)) {
                    continue;
                }
                GqlRow temp_row = row;
                if (!start_node_var.empty()) {
                    temp_row.bindings[start_node_var] = GqlValue(node);
                }
                if (stmt.pattern.nodes[0].where_expr && !evaluate_expression(temp_row, stmt.pattern.nodes[0].where_expr.get()).is_truthy()) {
                    continue;
                }
                valid_starts.push_back(node);
            }

            // Filter end node candidates by labels and property constraint expressions.
            std::vector<Node> valid_ends;
            for (const auto& node : end_nodes) {
                if (stmt.pattern.nodes[1].label_expr && !matches_label_expr(node.getType(), stmt.pattern.nodes[1].label_expr)) {
                    continue;
                }
                if (!matches_properties(node.getProperties(), stmt.pattern.nodes[1].properties) || !matches_filters(node.getProperties(), stmt.pattern.nodes[1].property_filters)) {
                    continue;
                }
                GqlRow temp_row = row;
                if (!end_node_var.empty()) {
                    temp_row.bindings[end_node_var] = GqlValue(node);
                }
                if (stmt.pattern.nodes[1].where_expr && !evaluate_expression(temp_row, stmt.pattern.nodes[1].where_expr.get()).is_truthy()) {
                    continue;
                }
                valid_ends.push_back(node);
            }

            // Map GQL edge direction to RageDB internal Direction enum.
            const auto& edge = stmt.pattern.edges[0];
            Direction dir = Direction::BOTH;
            if (edge.direction == EdgeDirection::RIGHT) dir = Direction::OUT;
            else if (edge.direction == EdgeDirection::LEFT) dir = Direction::IN;

            // Restrict relationships to the parsed label literal if specified.
            std::vector<std::string> rel_types;
            if (edge.label_expr && edge.label_expr->kind == LabelExprKind::LITERAL) {
                rel_types.push_back(edge.label_expr->name);
            }

            uint64_t min_hops = edge.is_variable_length ? edge.min_hops : 1;
            uint64_t max_hops = edge.is_variable_length ? edge.max_hops : 15;

            // Enumerate the (start node, end node) partition pairs to search.
            struct PartitionInfo {
                Node start_node;
                Node end_node;
            };
            auto partitions = std::make_shared<std::vector<PartitionInfo>>();
            partitions->reserve(valid_starts.size() * valid_ends.size());
            for (const auto& s : valid_starts) {
                for (const auto& e : valid_ends) {
                    partitions->push_back({s, e});
                }
            }

            if (partitions->empty()) {
                return seastar::make_ready_future<std::vector<GqlRow>>(std::vector<GqlRow>{});
            }

            // Resolve the edge cost function once for the weighted selectors, rather than rebuilding
            // it for every pair.
            // `ANY SHORTEST ... COST r.w` selects the single cheapest-by-weight path, not fewest hops:
            // when a COST is present, drive ANY through the weighted search just like CHEAPEST.
            const bool any_weighted = stmt.shortest_path_kind == ShortestPathKind::ANY &&
                                      !stmt.pattern.edges.empty() && stmt.pattern.edges[0].cost_expr != nullptr;
            std::function<double(const Relationship&)> cost_fn;
            if (stmt.shortest_path_kind == ShortestPathKind::CHEAPEST ||
                stmt.shortest_path_kind == ShortestPathKind::ALL_CHEAPEST ||
                stmt.shortest_path_kind == ShortestPathKind::CHEAPEST_K ||
                any_weighted) {
                std::shared_ptr<Expression> cost_expr = nullptr;
                std::string cost_edge_var = "";
                if (!stmt.pattern.edges.empty()) {
                    cost_expr = stmt.pattern.edges[0].cost_expr;
                    cost_edge_var = stmt.pattern.edges[0].variable;
                }

                if (cost_expr) {
                    cost_fn = [cost_expr, cost_edge_var](const Relationship& rel) -> double {
                        GqlRow temp_row;
                        if (!cost_edge_var.empty()) {
                            temp_row.bindings[cost_edge_var] = GqlValue(rel);
                        }
                        GqlValue val = evaluate_expression(temp_row, cost_expr.get());
                        if (std::holds_alternative<double>(val.property)) {
                            return std::get<double>(val.property);
                        } else if (std::holds_alternative<int64_t>(val.property)) {
                            return static_cast<double>(std::get<int64_t>(val.property));
                        }
                        return 1.0;
                    };
                } else {
                    cost_fn = [](const Relationship& rel) -> double {
                        auto val = rel.getProperty("weight");
                        if (std::holds_alternative<double>(val)) {
                            return std::get<double>(val);
                        } else if (std::holds_alternative<int64_t>(val)) {
                            return static_cast<double>(std::get<int64_t>(val));
                        }
                        return 1.0;
                    };
                }
            }

            // Search the pairs with a bounded number in flight. Every pair runs an independent BFS
            // holding its own frontier and result paths, so launching the whole cartesian product at
            // once -- and holding every pair's paths until the last one resolves -- makes peak memory
            // O(pairs). A pattern with many candidate endpoints (one person to every Person of a given
            // first name) exhausts the heap that way. Each pair folds its rows into its own slot, so
            // the output stays in partition order regardless of the order the searches complete in.
            auto row_groups = std::make_shared<std::vector<std::vector<GqlRow>>>(partitions->size());
            auto indices = std::make_shared<std::vector<size_t>>(partitions->size());
            std::iota(indices->begin(), indices->end(), 0);

            const size_t concurrency = gql_shortest_path_concurrency > 0 ? gql_shortest_path_concurrency : 1;

            return seastar::max_concurrent_for_each(indices->begin(), indices->end(), concurrency,
            [&graph, stmt, row, start_node_var, end_node_var, edge_var = edge.variable, rel_types, dir,
             min_hops, max_hops, cost_fn, any_weighted, partitions, row_groups](size_t index) {
                const auto& partition = (*partitions)[index];
                seastar::future<std::vector<Path>> search = seastar::make_ready_future<std::vector<Path>>();

                if (stmt.shortest_path_kind == ShortestPathKind::CHEAPEST || any_weighted) {
                    search = graph.shard.local().ShortestWeightedPathPeered(
                        partition.start_node.getId(), partition.end_node.getId(), dir, rel_types, cost_fn
                    ).then([](std::optional<WeightedPath> opt_wpath) {
                        std::vector<Path> paths;
                        if (opt_wpath) {
                            paths.push_back(Path(opt_wpath->GetNodes(), opt_wpath->GetRelationships()));
                        }
                        return paths;
                    });
                } else if (stmt.shortest_path_kind == ShortestPathKind::ALL_CHEAPEST) {
                    search = graph.shard.local().AllCheapestWeightedPathsPeered(
                        partition.start_node.getId(), partition.end_node.getId(), dir, rel_types, cost_fn
                    ).then([](std::vector<WeightedPath> wpaths) {
                        std::vector<Path> paths;
                        for (const auto& wp : wpaths) {
                            paths.push_back(Path(wp.GetNodes(), wp.GetRelationships()));
                        }
                        return paths;
                    });
                } else if (stmt.shortest_path_kind == ShortestPathKind::CHEAPEST_K) {
                    search = graph.shard.local().KCheapestWeightedPathsPeered(
                        partition.start_node.getId(), partition.end_node.getId(), dir, rel_types, cost_fn,
                        stmt.shortest_path_k
                    ).then([](std::vector<WeightedPath> wpaths) {
                        std::vector<Path> paths;
                        for (const auto& wp : wpaths) {
                            paths.push_back(Path(wp.GetNodes(), wp.GetRelationships()));
                        }
                        return paths;
                    });
                } else {
                    search = graph.shard.local().ShortestPathsPeered(
                        partition.start_node.getId(), partition.end_node.getId(), dir, rel_types,
                        min_hops, max_hops, stmt.shortest_path_kind, stmt.shortest_path_k
                    );
                }

                // Bind variables for the start/end nodes, traversed edge relationships list, and path object.
                return search.then([row, stmt, start_node_var, end_node_var, edge_var, index, partitions, row_groups]
                                   (std::vector<Path> paths) {
                    const auto& partition = (*partitions)[index];
                    auto& slot = (*row_groups)[index];
                    for (const auto& path : paths) {
                        GqlRow new_row = row;
                        new_row.bindings[start_node_var] = GqlValue(partition.start_node);
                        new_row.bindings["_n_0"] = GqlValue(partition.start_node);
                        new_row.bindings[end_node_var] = GqlValue(partition.end_node);
                        new_row.bindings["_n_1"] = GqlValue(partition.end_node);

                        if (!edge_var.empty()) {
                            new_row.bindings[edge_var] = GqlValue(path.GetRelationships());
                        }
                        new_row.bindings["_e_0"] = GqlValue(path.GetRelationships());

                        new_row.bindings[stmt.path_variable] = GqlValue(path);
                        slot.push_back(new_row);
                    }
                });
            }).then([partitions, indices, row_groups] {
                std::vector<GqlRow> out_rows;
                for (auto& group : *row_groups) {
                    out_rows.insert(out_rows.end(), std::make_move_iterator(group.begin()),
                                    std::make_move_iterator(group.end()));
                }
                return out_rows;
            });
        });
    }

    // Case 2: Standard MATCH Traversal.
    return traverse_path_pattern(graph, stmt.pattern, row, limit, pruner, sort_property, sort_ascending, sort_by_id, stmt.path_mode)
    .then([row, stmt](std::vector<GqlRow> traversed_rows) {
        // Enforce Match Mode and Path Mode filtering
        std::vector<GqlRow> filtered_rows;
        for (auto&& traversed_row : traversed_rows) {
            if (satisfies_match_path_modes(traversed_row, stmt.match_mode, stmt.path_mode, stmt.pattern)) {
                filtered_rows.push_back(std::move(traversed_row));
            }
        }
        traversed_rows = std::move(filtered_rows);

        // Handle OPTIONAL MATCH returning null bindings if no matches found.
        if (traversed_rows.empty() && stmt.is_optional) {
            GqlRow opt_row = row;
            for (const auto& node : stmt.pattern.nodes) {
                if (!node.variable.empty() && opt_row.bindings.find(node.variable) == opt_row.bindings.end()) {
                    opt_row.bindings[node.variable] = GqlValue();
                }
            }
            for (const auto& edge : stmt.pattern.edges) {
                if (!edge.variable.empty() && opt_row.bindings.find(edge.variable) == opt_row.bindings.end()) {
                    opt_row.bindings[edge.variable] = GqlValue();
                }
            }
            return std::vector<GqlRow>{opt_row};
        }

        // If a path variable was assigned to this pattern (e.g. p = (a)-[b]->(c)),
        // reconstruct the Path object from the individual node/edge bindings and assign it.
        if (!stmt.path_variable.empty()) {
            std::vector<GqlRow> updated_rows;
            for (const auto& traversed_row : traversed_rows) {
                GqlRow new_row = traversed_row;
                std::vector<Node> path_nodes;
                for (size_t i = 0; i < stmt.pattern.nodes.size(); ++i) {
                    auto nit = new_row.bindings.find("_n_" + std::to_string(i));
                    if (nit != new_row.bindings.end() && nit->second.type == GqlValue::NODE) {
                        path_nodes.push_back(*nit->second.node);
                    }
                }
                std::vector<Relationship> path_rels;
                for (size_t i = 0; i < stmt.pattern.edges.size(); ++i) {
                    auto eit = new_row.bindings.find("_e_" + std::to_string(i));
                    if (eit != new_row.bindings.end()) {
                        if (eit->second.type == GqlValue::RELATIONSHIP) {
                            path_rels.push_back(*eit->second.relationship);
                        } else if (eit->second.type == GqlValue::RELATIONSHIP_LIST) {
                            for (const auto& rel : *eit->second.relationship_list) {
                                path_rels.push_back(rel);
                            }
                        }
                    }
                }
                new_row.bindings[stmt.path_variable] = GqlValue(Path(path_nodes, path_rels));
                updated_rows.push_back(std::move(new_row));
            }
            return updated_rows;
        }

        return traversed_rows;
    });
}

} // namespace ragedb::gql
