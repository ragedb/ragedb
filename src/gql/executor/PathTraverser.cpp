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
#include <seastar/core/when_all.hh>
#include <algorithm>
#include <unordered_set>
#include <iterator>

/**
 * @file PathTraverser.cpp
 * @brief Implementation of path traversal, index lookups, and step-wise GQL pattern matching.
 */
namespace ragedb::gql {

bool matches_label_expr(const std::string& actual_type, const std::shared_ptr<LabelExpression>& expr) {
    if (!expr) return true;
    switch (expr->kind) {
        case LabelExprKind::LITERAL:
            return actual_type == expr->name;
        case LabelExprKind::NOT:
            return !matches_label_expr(actual_type, expr->expr);
        case LabelExprKind::AND:
            return matches_label_expr(actual_type, expr->left) && matches_label_expr(actual_type, expr->right);
        case LabelExprKind::OR:
            return matches_label_expr(actual_type, expr->left) || matches_label_expr(actual_type, expr->right);
    }
    return false;
}

seastar::future<std::vector<Node>> get_start_nodes(ragedb::Graph& graph, const PatternNode& node, size_t limit, const ProjectionPruner& pruner, std::string sort_property, bool sort_ascending, bool sort_by_id) {
    size_t scan_limit = (limit > 0) ? limit : 100000;
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
    
    seastar::future<std::vector<Node>> fut = seastar::make_ready_future<std::vector<Node>>();
    if (all_filters.size() > 1 && !single_label.empty()) {
        std::vector<seastar::future<std::vector<Node>>> futs;
        for (const auto& filter : all_filters) {
            futs.push_back(graph.shard.local().FindNodesPeered(single_label, filter.property, filter.op, filter.value, 0, scan_limit));
        }
        fut = seastar::when_all_succeed(futs.begin(), futs.end())
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
        fut = graph.shard.local().FindNodesPeered(single_label, filter.property, filter.op, filter.value, 0, scan_limit);
    } else if (!single_label.empty()) {
        fut = graph.shard.local().AllNodesPeered(single_label, 0, scan_limit);
    } else {
        fut = graph.shard.local().AllNodesPeered(0, scan_limit);
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

    return fut.then([&graph, degree_opt_info = node.degree_opt_info, var = node.variable, pruner, sort_nodes](std::vector<Node> result_list) mutable {
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

struct PathHop {
    std::vector<Relationship> rels;
    std::vector<Node> nodes;
};

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
    const ProjectionPruner& pruner
) {
    // Base Case 1: Exceeded maximum hops allowed
    if (current_depth > edge.max_hops) {
        return seastar::make_ready_future<std::vector<PathHop>>();
    }

    std::vector<PathHop> local_results;
    // Base Case 2: Within allowed hop range, record the current path
    if (current_depth >= edge.min_hops) {
        local_results.push_back({current_path_rels, current_path_nodes});
    }

    // Base Case 3: Reached maximum hops allowed, stop traversing further
    if (current_depth == edge.max_hops) {
        return seastar::make_ready_future<std::vector<PathHop>>(std::move(local_results));
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
           local_results = std::move(local_results), limit, pruner](std::vector<Relationship> rels) mutable {

        if (pruner.should_prune(edge.variable)) {
            auto keys = pruner.get_keys(edge.variable);
            for (auto& r : rels) {
                r.pruneProperties(keys);
            }
        }

        std::vector<seastar::future<std::vector<PathHop>>> branch_futs;
        for (const auto& rel : rels) {
            if (std::find(visited_rel_ids.begin(), visited_rel_ids.end(), rel.getId()) != visited_rel_ids.end()) {
                continue;
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

            uint64_t target_id = (rel.getStartingNodeId() == current_node_id) ? rel.getEndingNodeId() : rel.getStartingNodeId();

            branch_futs.push_back(
                graph.shard.local().NodeGetPeered(target_id)
                .then([&graph, rel, edge, next_node, current_depth, visited_rel_ids, current_path_rels, current_path_nodes, target_id, limit, pruner](Node target_node) mutable {
                    if (pruner.should_prune(next_node.variable)) {
                        target_node.pruneProperties(pruner.get_keys(next_node.variable));
                    }
                    visited_rel_ids.push_back(rel.getId());
                    current_path_rels.push_back(rel);
                    current_path_nodes.push_back(target_node);

                    return traverse_var_len_async(
                        graph,
                        target_id,
                        edge,
                        next_node,
                        current_depth + 1,
                        std::move(visited_rel_ids),
                        std::move(current_path_rels),
                        std::move(current_path_nodes),
                        limit,
                        pruner
                    );
                })
            );
        }

        if (branch_futs.empty()) {
            return seastar::make_ready_future<std::vector<PathHop>>(std::move(local_results));
        }

        return seastar::when_all_succeed(branch_futs.begin(), branch_futs.end())
        .then([local_results = std::move(local_results), limit](std::vector<std::vector<PathHop>> nested_branches) mutable {
            for (auto& branch_res : nested_branches) {
                local_results.insert(local_results.end(), branch_res.begin(), branch_res.end());
                if (limit > 0 && local_results.size() >= limit) {
                    local_results.resize(limit);
                    break;
                }
            }
            return local_results;
        });
    };

    if (edge_type.empty()) {
        return graph.shard.local().NodeGetRelationshipsPeered(current_node_id, dir).then(std::move(handle_rels));
    } else {
        return graph.shard.local().NodeGetRelationshipsPeered(current_node_id, dir, edge_type).then(std::move(handle_rels));
    }
}

static seastar::future<std::vector<GqlRow>> traverse_step(ragedb::Graph& graph, const GqlRow& row, const PatternEdge& edge, const PatternNode& next_node, size_t node_idx, size_t limit, const ProjectionPruner& pruner) {
    auto it = row.bindings.find("_n_" + std::to_string(node_idx));
    if (it == row.bindings.end()) {
        return seastar::make_ready_future<std::vector<GqlRow>>();
    }
    uint64_t src_id = it->second.node->getId();

    if (edge.is_variable_length) {
        return traverse_var_len_async(graph, src_id, edge, next_node, 0, {}, {}, {}, limit, pruner)
        .then([row, edge, next_node, node_idx](std::vector<PathHop> hops) {
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

                GqlRow new_row = row;
                new_row.bindings[edge.variable] = GqlValue(hop.rels);
                new_row.bindings["_e_" + std::to_string(node_idx)] = GqlValue(hop.rels);
                new_row.bindings[next_node.variable] = GqlValue(final_node);
                new_row.bindings["_n_" + std::to_string(node_idx + 1)] = GqlValue(final_node);
                out.push_back(new_row);
            }
            return out;
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
        std::vector<seastar::future<std::optional<GqlRow>>> futs;
        for (const auto& rel : rels) {
            if (edge.label_expr && !matches_label_expr(rel.getType(), edge.label_expr)) {
                continue;
            }
            if (!matches_properties(rel.getProperties(), edge.properties) || !matches_filters(rel.getProperties(), edge.property_filters)) {
                continue;
            }

            uint64_t target_id = (rel.getStartingNodeId() == src_id) ? rel.getEndingNodeId() : rel.getStartingNodeId();

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
        .then([limit](std::vector<std::optional<GqlRow>> opts) {
            std::vector<GqlRow> out;
            for (const auto& opt : opts) {
                if (opt) {
                    out.push_back(*opt);
                    if (limit > 0 && out.size() >= limit) {
                        break;
                    }
                }
            }
            return out;
        });
    };

    if (edge_type.empty()) {
        return graph.shard.local().NodeGetRelationshipsPeered(src_id, dir).then(std::move(handle_rels));
    } else {
        return graph.shard.local().NodeGetRelationshipsPeered(src_id, dir, edge_type).then(std::move(handle_rels));
    }
}

static seastar::future<std::vector<GqlRow>> traverse_path_pattern_iterative(ragedb::Graph& graph, const PathPattern& prep_pattern, size_t step_idx, std::vector<GqlRow> current_step_rows, size_t limit, const ProjectionPruner& pruner) {
    if (step_idx >= prep_pattern.edges.size()) {
        return seastar::make_ready_future<std::vector<GqlRow>>(std::move(current_step_rows));
    }

    const auto& edge = prep_pattern.edges[step_idx];
    const auto& next_node = prep_pattern.nodes[step_idx + 1];

    std::vector<seastar::future<std::vector<GqlRow>>> futs;
    for (const auto& row : current_step_rows) {
        futs.push_back(traverse_step(graph, row, edge, next_node, step_idx, limit, pruner));
    }

    return seastar::when_all_succeed(futs.begin(), futs.end())
    .then([&graph, prep_pattern, step_idx, limit, pruner](std::vector<std::vector<GqlRow>> nested) {
        std::vector<GqlRow> next_rows;
        for (const auto& vec : nested) {
            next_rows.insert(next_rows.end(), vec.begin(), vec.end());
            if (limit > 0 && next_rows.size() >= limit) {
                next_rows.resize(limit);
                break;
            }
        }
        return traverse_path_pattern_iterative(graph, prep_pattern, step_idx + 1, std::move(next_rows), limit, pruner);
    });
}

static seastar::future<std::vector<GqlRow>> traverse_from_relationship_index(
    ragedb::Graph& graph,
    const PathPattern& prep_pattern,
    const GqlRow& base_row,
    size_t limit,
    const ProjectionPruner& pruner
) {
    size_t scan_limit = (limit > 0) ? limit : 100000;
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

    return graph.shard.local().FindRelationshipsPeered(single_label, indexed_prop, indexed_op, indexed_val, 0, scan_limit)
    .then([&graph, prep_pattern, base_row, limit, pruner](std::vector<Relationship> rels) {
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
        .then([&graph, prep_pattern, limit, pruner](std::vector<std::vector<GqlRow>> nested_rows) {
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
            
            return traverse_path_pattern_iterative(graph, prep_pattern, 1, std::move(initial_rows), limit, pruner);
        });
    });
}

seastar::future<std::vector<GqlRow>> traverse_path_pattern(ragedb::Graph& graph, const PathPattern& pattern, const GqlRow& base_row, size_t limit, const ProjectionPruner& pruner, std::string sort_property, bool sort_ascending, bool sort_by_id) {
    PathPattern prep_pattern = pattern;
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
        return traverse_from_relationship_index(graph, prep_pattern, base_row, limit, pruner);
    }

    size_t start_node_limit = prep_pattern.edges.empty() ? limit : 0;
    seastar::future<std::vector<Node>> start_nodes_fut = seastar::make_ready_future<std::vector<Node>>();
    if (bound_it != base_row.bindings.end() && bound_it->second.type == GqlValue::NODE) {
        start_nodes_fut = seastar::make_ready_future<std::vector<Node>>(std::vector<Node>{ *bound_it->second.node });
    } else {
        start_nodes_fut = get_start_nodes(graph, prep_pattern.nodes[0], start_node_limit, pruner, sort_property, sort_ascending, sort_by_id);
    }

    return start_nodes_fut.then([base_row, prep_pattern, &graph, limit, pruner](std::vector<Node> start_nodes) {
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

        return traverse_path_pattern_iterative(graph, prep_pattern, 0, std::move(initial_rows), limit, pruner);
    });
}

seastar::future<std::vector<GqlRow>> traverse_match_statement(ragedb::Graph& graph, const MatchStatement& stmt, const GqlRow& row, size_t limit, const ProjectionPruner& pruner, std::string sort_property, bool sort_ascending, bool sort_by_id) {
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
                if (stmt.is_optional) {
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
                            if (!has_any_match && stmt.is_optional) {
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

            // Compute shortest paths within each (start node, end node) partition pair.
            std::vector<seastar::future<std::vector<Path>>> traversal_futs;
            struct PartitionInfo {
                Node start_node;
                Node end_node;
            };
            auto partitions = std::make_shared<std::vector<PartitionInfo>>();

            if (stmt.shortest_path_kind == ShortestPathKind::CHEAPEST) {
                std::string weight_property = "weight";
                if (stmt.cost_expr && stmt.cost_expr->kind == ExpressionKind::PROPERTY_LOOKUP) {
                    auto prop_expr = std::dynamic_pointer_cast<PropertyLookupExpr>(stmt.cost_expr);
                    if (prop_expr) {
                        weight_property = prop_expr->property;
                    }
                }
                for (const auto& s : valid_starts) {
                    for (const auto& e : valid_ends) {
                        partitions->push_back({s, e});
                        seastar::future<std::vector<Path>> fut = graph.shard.local().ShortestWeightedPathPeered(
                            s.getId(),
                            e.getId(),
                            dir,
                            rel_types,
                            weight_property
                        ).then([](std::optional<WeightedPath> opt_wpath) {
                            std::vector<Path> paths;
                            if (opt_wpath) {
                                paths.push_back(Path(opt_wpath->GetNodes(), opt_wpath->GetRelationships()));
                            }
                            return paths;
                        });
                        traversal_futs.push_back(std::move(fut));
                    }
                }
            } else {
                for (const auto& s : valid_starts) {
                    for (const auto& e : valid_ends) {
                        partitions->push_back({s, e});
                        traversal_futs.push_back(
                            graph.shard.local().ShortestPathsPeered(
                                s.getId(),
                                e.getId(),
                                dir,
                                rel_types,
                                min_hops,
                                max_hops,
                                stmt.shortest_path_kind,
                                stmt.shortest_path_k
                            )
                        );
                    }
                }
            }

            if (traversal_futs.empty()) {
                return seastar::make_ready_future<std::vector<GqlRow>>(std::vector<GqlRow>{});
            }

            // Once all partition-level shortest path searches finish, bind results to GqlValue.
            return seastar::when_all_succeed(traversal_futs.begin(), traversal_futs.end())
            .then([row, stmt, start_node_var, end_node_var, partitions, edge_var = edge.variable](std::vector<std::vector<Path>> path_results) {
                std::vector<GqlRow> out_rows;
                for (size_t i = 0; i < path_results.size(); ++i) {
                    const auto& partition = (*partitions)[i];
                    const auto& paths = path_results[i];

                    // Bind variables for the start/end nodes, traversed edge relationships list, and path object.
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
                        out_rows.push_back(new_row);
                    }
                }
                return out_rows;
            });
        });
    }

    // Case 2: Standard MATCH Traversal.
    return traverse_path_pattern(graph, stmt.pattern, row, limit, pruner, sort_property, sort_ascending, sort_by_id)
    .then([row, stmt](std::vector<GqlRow> traversed_rows) {
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
