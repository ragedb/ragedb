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

namespace ragedb::gql {

void collect_accessed_properties(const Expression* expr,
                                 std::map<std::string, std::set<std::string>>& accessed_props,
                                 std::set<std::string>& whole_objects) {
    if (!expr) return;
    switch (expr->kind) {
        case ExpressionKind::VARIABLE: {
            auto* var = static_cast<const VariableExpr*>(expr);
            whole_objects.insert(var->name);
            break;
        }
        case ExpressionKind::PROPERTY_LOOKUP: {
            auto* prop_lookup = static_cast<const PropertyLookupExpr*>(expr);
            accessed_props[prop_lookup->variable].insert(prop_lookup->property);
            break;
        }
        case ExpressionKind::UNARY_OP: {
            auto* un = static_cast<const UnaryOpExpr*>(expr);
            collect_accessed_properties(un->expr.get(), accessed_props, whole_objects);
            break;
        }
        case ExpressionKind::BINARY_OP: {
            auto* bin = static_cast<const BinaryOpExpr*>(expr);
            collect_accessed_properties(bin->left.get(), accessed_props, whole_objects);
            collect_accessed_properties(bin->right.get(), accessed_props, whole_objects);
            break;
        }
        case ExpressionKind::AGGREGATION: {
            auto* agg = static_cast<const AggregateExpr*>(expr);
            collect_accessed_properties(agg->expr.get(), accessed_props, whole_objects);
            break;
        }
        case ExpressionKind::LITERAL:
        default:
            break;
    }
}

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

seastar::future<std::vector<Node>> get_start_nodes(ragedb::Graph& graph, const PatternNode& node, size_t limit = 0, const ProjectionPruner& pruner = {}, std::string sort_property = "", bool sort_ascending = true, bool sort_by_id = false) {
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
    if (current_depth > edge.max_hops) {
        return seastar::make_ready_future<std::vector<PathHop>>();
    }

    std::vector<PathHop> local_results;
    if (current_depth >= edge.min_hops) {
        local_results.push_back({current_path_rels, current_path_nodes});
    }

    if (current_depth == edge.max_hops) {
        return seastar::make_ready_future<std::vector<PathHop>>(std::move(local_results));
    }

    Direction dir = Direction::BOTH;
    if (edge.direction == EdgeDirection::RIGHT) dir = Direction::OUT;
    else if (edge.direction == EdgeDirection::LEFT) dir = Direction::IN;

    std::string edge_type = "";
    if (edge.label_expr && edge.label_expr->kind == LabelExprKind::LITERAL) {
        edge_type = edge.label_expr->name;
    }

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
                if (hop.nodes.empty()) {
                    continue;
                }
                const Node& final_node = hop.nodes.back();
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

seastar::future<std::vector<GqlRow>> traverse_path_pattern(ragedb::Graph& graph, const PathPattern& pattern, const GqlRow& base_row, size_t limit = 0, const ProjectionPruner& pruner = {}, std::string sort_property = "", bool sort_ascending = true, bool sort_by_id = false) {
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

    auto start_node_var = prep_pattern.nodes[0].variable;
    auto bound_it = base_row.bindings.find(start_node_var);

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
            initial_rows.push_back(new_row);
            if (limit > 0 && prep_pattern.edges.empty() && initial_rows.size() >= limit) {
                break;
            }
        }

        return traverse_path_pattern_iterative(graph, prep_pattern, 0, std::move(initial_rows), limit, pruner);
    });
}

seastar::future<std::vector<GqlRow>> traverse_match_statement(ragedb::Graph& graph, const MatchStatement& stmt, const GqlRow& row, size_t limit = 0, const ProjectionPruner& pruner = {}, std::string sort_property = "", bool sort_ascending = true, bool sort_by_id = false) {
    return traverse_path_pattern(graph, stmt.pattern, row, limit, pruner, sort_property, sort_ascending, sort_by_id)
    .then([row, stmt](std::vector<GqlRow> traversed_rows) {
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
        return traversed_rows;
    });
}

struct StarJoinCandidate {
    std::string central_var;
    std::vector<size_t> match_indices;
};

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
