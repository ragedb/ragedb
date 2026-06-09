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
#include <sstream>
#include <unordered_set>
#include <algorithm>
#include <cstdlib>
#include <chrono>
#include <seastar/core/when_all.hh>

namespace ragedb::gql {

/**
 * @brief Retrieves the starting nodes for a pattern match.
 * 
 * Inspects node pattern label and properties. Utilizes index scans if a property and a label
 * are supplied. Falls back to AllNodes scan either labeled or global if properties are absent.
 * 
 * @param graph The RageDB graph instance.
 * @param node The vertex pattern definition.
 * @return seastar::future<std::vector<Node>> Future wrapping matching start nodes.
 */
seastar::future<std::vector<Node>> get_start_nodes(ragedb::Graph& graph, const PatternNode& node) {
    if (!node.properties.empty() && !node.label.empty()) {
        auto it = node.properties.begin();
        std::string prop = it->first;
        property_type_t val = it->second;
        return graph.shard.local().FindNodesPeered(node.label, prop, Operation::EQ, val, 0, 100000);
    } else if (!node.label.empty()) {
        return graph.shard.local().AllNodesPeered(node.label, 0, 100000);
    } else {
        return graph.shard.local().AllNodesPeered(0, 100000);
    }
}

/**
 * @brief Asynchronously traverses a single edge pattern hop from a given row node.
 * 
 * Evaluates edge label, properties, direction (incoming, outgoing, undirected), and matching
 * properties of the target destination node. Binds the traversed edge and target node variables.
 * 
 * @param graph The RageDB graph instance.
 * @param row The current query row (source context).
 * @param edge The pattern edge describing the hop.
 * @param next_node The pattern node that we must reach.
 * @param node_idx The current node index in the traversal chain.
 * @return seastar::future<std::vector<GqlRow>> Future wrapping new rows representing extended paths.
 */
seastar::future<std::vector<GqlRow>> traverse_step(ragedb::Graph& graph, const GqlRow& row, const PatternEdge& edge, const PatternNode& next_node, size_t node_idx) {
    auto it = row.bindings.find("_n_" + std::to_string(node_idx));
    if (it == row.bindings.end()) {
        return seastar::make_ready_future<std::vector<GqlRow>>();
    }
    uint64_t src_id = it->second.node->getId();

    Direction dir = Direction::BOTH;
    if (edge.direction == EdgeDirection::RIGHT) dir = Direction::OUT;
    else if (edge.direction == EdgeDirection::LEFT) dir = Direction::IN;

    return graph.shard.local().NodeGetRelationshipsPeered(src_id, dir, edge.label)
    .then([row, edge, next_node, src_id, node_idx, &graph](std::vector<Relationship> rels) {
        std::vector<seastar::future<std::optional<GqlRow>>> futs;
        for (const auto& rel : rels) {
            if (!matches_properties(rel.getProperties(), edge.properties)) {
                continue;
            }

            uint64_t target_id = (rel.getStartingNodeId() == src_id) ? rel.getEndingNodeId() : rel.getStartingNodeId();

            futs.push_back(
                graph.shard.local().NodeGetPeered(target_id)
                .then([row, rel, edge, next_node, node_idx](Node target_node) -> std::optional<GqlRow> {
                    if (!next_node.label.empty() && target_node.getType() != next_node.label) {
                        return std::nullopt;
                    }
                    if (!matches_properties(target_node.getProperties(), next_node.properties)) {
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
        .then([](std::vector<std::optional<GqlRow>> opts) {
            std::vector<GqlRow> out;
            for (const auto& opt : opts) {
                if (opt) out.push_back(*opt);
            }
            return out;
        });
    });
}

/**
 * @brief Recursively traverses a path pattern, processing one edge-node step at a time.
 * 
 * @param graph The RageDB graph instance.
 * @param prep_pattern The prepared path pattern containing filled variables.
 * @param step_idx Current edge index in the path pattern.
 * @param current_step_rows Rows generated by the previous step.
 * @return seastar::future<std::vector<GqlRow>> Future wrapping the final traversed rows.
 */
seastar::future<std::vector<GqlRow>> traverse_path_pattern_iterative(ragedb::Graph& graph, const PathPattern& prep_pattern, size_t step_idx, std::vector<GqlRow> current_step_rows) {
    if (step_idx >= prep_pattern.edges.size()) {
        return seastar::make_ready_future<std::vector<GqlRow>>(std::move(current_step_rows));
    }

    const auto& edge = prep_pattern.edges[step_idx];
    const auto& next_node = prep_pattern.nodes[step_idx + 1];

    std::vector<seastar::future<std::vector<GqlRow>>> futs;
    for (const auto& row : current_step_rows) {
        futs.push_back(traverse_step(graph, row, edge, next_node, step_idx));
    }

    return seastar::when_all_succeed(futs.begin(), futs.end())
    .then([&graph, prep_pattern, step_idx](std::vector<std::vector<GqlRow>> nested) {
        std::vector<GqlRow> next_rows;
        for (const auto& vec : nested) {
            next_rows.insert(next_rows.end(), vec.begin(), vec.end());
        }
        return traverse_path_pattern_iterative(graph, prep_pattern, step_idx + 1, std::move(next_rows));
    });
}

/**
 * @brief Entry point for traversing a path pattern.
 * 
 * Generates default variable names for unlabeled/unnamed nodes/edges, resolves start nodes,
 * and initiates the recursive traversal.
 * 
 * @param graph The RageDB graph instance.
 * @param pattern The GQL path pattern to evaluate.
 * @param base_row The base row context.
 * @return seastar::future<std::vector<GqlRow>> Future wrapping matches.
 */
seastar::future<std::vector<GqlRow>> traverse_path_pattern(ragedb::Graph& graph, const PathPattern& pattern, const GqlRow& base_row) {
    // Prep pattern with variables if missing
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

    seastar::future<std::vector<Node>> start_nodes_fut = seastar::make_ready_future<std::vector<Node>>();
    if (bound_it != base_row.bindings.end() && bound_it->second.type == GqlValue::NODE) {
        start_nodes_fut = seastar::make_ready_future<std::vector<Node>>(std::vector<Node>{ *bound_it->second.node });
    } else {
        start_nodes_fut = get_start_nodes(graph, prep_pattern.nodes[0]);
    }

    return start_nodes_fut.then([base_row, prep_pattern, &graph](std::vector<Node> start_nodes) {
        std::vector<GqlRow> initial_rows;
        for (const auto& node : start_nodes) {
            if (!matches_properties(node.getProperties(), prep_pattern.nodes[0].properties)) {
                continue;
            }
            GqlRow new_row = base_row;
            new_row.bindings[prep_pattern.nodes[0].variable] = GqlValue(node);
            new_row.bindings["_n_0"] = GqlValue(node);
            initial_rows.push_back(new_row);
        }

        return traverse_path_pattern_iterative(graph, prep_pattern, 0, std::move(initial_rows));
    });
}

/**
 * @brief Traverses a single GQL MATCH/OPTIONAL MATCH statement.
 * 
 * If OPTIONAL MATCH generates no results, binds all newly introduced variables to null values
 * and retains the incoming row.
 * 
 * @param graph The RageDB graph instance.
 * @param stmt The MatchStatement AST node.
 * @param row The row context.
 * @return seastar::future<std::vector<GqlRow>> Future wrapping evaluated rows.
 */
seastar::future<std::vector<GqlRow>> traverse_match_statement(ragedb::Graph& graph, const MatchStatement& stmt, const GqlRow& row) {
    return traverse_path_pattern(graph, stmt.pattern, row)
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

/**
 * @brief Recursively executes a chain of sequential MATCH statements.
 * 
 * Multiplies (cartesian product) rows along the match traversal pathways.
 * 
 * @param graph The RageDB graph instance.
 * @param matches List of match statements to execute.
 * @param match_idx Current match index.
 * @param current_rows Accumulated rows up to this point.
 * @return seastar::future<std::vector<GqlRow>> Future wrapping the combined matched rows.
 */
seastar::future<std::vector<GqlRow>> execute_match_chain(ragedb::Graph& graph, const std::vector<MatchStatement>& matches, size_t match_idx, std::vector<GqlRow> current_rows) {
    if (match_idx >= matches.size()) {
        return seastar::make_ready_future<std::vector<GqlRow>>(std::move(current_rows));
    }

    const auto& stmt = matches[match_idx];

    std::vector<seastar::future<std::vector<GqlRow>>> futs;
    for (const auto& row : current_rows) {
        futs.push_back(traverse_match_statement(graph, stmt, row));
    }

    return seastar::when_all_succeed(futs.begin(), futs.end())
    .then([&graph, matches, match_idx, current_rows = std::move(current_rows)](std::vector<std::vector<GqlRow>> nested) {
        std::vector<GqlRow> next_rows;
        for (const auto& vec : nested) {
            next_rows.insert(next_rows.end(), vec.begin(), vec.end());
        }
        return execute_match_chain(graph, matches, match_idx + 1, std::move(next_rows));
    });
}

/**
 * @brief Helper sorting structure representing a row mapped to its sort keys.
 */
struct RowSortKey {
    std::vector<GqlValue> keys;
    GqlRow row;
};

/**
 * @brief Main execution entry point for the GQL engine.
 * 
 * Performs MATCH chain query paths, applies global WHERE filtering, applies side-effects
 * (writes), sorts the final results via ORDER BY, serializes return projected columns
 * to formatted JSON arrays, and handles limits.
 * 
 * @param graph The RageDB graph instance.
 * @param query_val The parsed GQL Query configuration.
 * @return seastar::future<std::string> Future containing serialized JSON array results.
 */
seastar::future<std::string> GqlExecutor::execute(ragedb::Graph& graph, GqlQuery query_val) {
    auto query_ptr = std::make_shared<GqlQuery>(std::move(query_val));
    std::vector<GqlRow> initial_rows = { GqlRow{} };

    return execute_match_chain(graph, query_ptr->matches, 0, std::move(initial_rows))
    .then([&graph, query_ptr](std::vector<GqlRow> matched_rows) {
        const auto& query = *query_ptr;
        // Filter rows using global WHERE condition
        std::vector<GqlRow> filtered_rows;
        for (auto& row : matched_rows) {
            if (!query.where_expr || evaluate_expression(row, query.where_expr.get()).is_truthy()) {
                filtered_rows.push_back(std::move(row));
            }
        }

        // Execute write operations if any
        if (query.writes.empty()) {
            return seastar::make_ready_future<std::vector<GqlRow>>(std::move(filtered_rows));
        }

        std::vector<seastar::future<GqlRow>> futs;
        for (auto& row : filtered_rows) {
            futs.push_back(execute_writes_for_row(graph, query_ptr, 0, std::move(row)));
        }
        return seastar::when_all_succeed(futs.begin(), futs.end());
    })
    .then([query_ptr](std::vector<GqlRow> written_rows) {
        const auto& query = *query_ptr;
        std::vector<GqlRow> filtered_rows = std::move(written_rows);

        // Apply ORDER BY
        if (!query.order_by.empty()) {
            std::vector<RowSortKey> sorted_keys;
            for (auto& row : filtered_rows) {
                RowSortKey rk;
                rk.row = row;
                for (const auto& spec : query.order_by) {
                    rk.keys.push_back(evaluate_expression(row, spec.expr.get()));
                }
                sorted_keys.push_back(std::move(rk));
            }

            std::stable_sort(sorted_keys.begin(), sorted_keys.end(), [&query](const RowSortKey& a, const RowSortKey& b) {
                for (size_t i = 0; i < query.order_by.size(); ++i) {
                    int cmp = compare_gql_values(a.keys[i], b.keys[i]);
                    if (cmp != 0) {
                        return query.order_by[i].ascending ? (cmp < 0) : (cmp > 0);
                    }
                }
                return false;
            });

            filtered_rows.clear();
            for (auto& rk : sorted_keys) {
                filtered_rows.push_back(std::move(rk.row));
            }
        }

        // Format projected Return items as JSON rows
        std::vector<std::string> results;
        std::unordered_set<std::string> seen;

        for (const auto& row : filtered_rows) {
            std::string serialized_row = "{";
            bool first = true;
            for (size_t i = 0; i < query.returns.size(); ++i) {
                const auto& item = query.returns[i];
                GqlValue val = evaluate_expression(row, item.expr.get());

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
                    } else {
                        key = "column_" + std::to_string(i);
                    }
                }

                if (!first) serialized_row += ", ";
                serialized_row += "\"" + key + "\": " + serialize_gql_value(val);
                first = false;
            }
            serialized_row += "}";

            if (query.distinct) {
                if (seen.insert(serialized_row).second) {
                    results.push_back(serialized_row);
                }
            } else {
                results.push_back(serialized_row);
            }
        }

        // Apply LIMIT
        if (query.limit && results.size() > *query.limit) {
            results.resize(*query.limit);
        }

        // Output final JSON Array
        std::string json_res = "[";
        bool first = true;
        for (const auto& r : results) {
            if (!first) json_res += ", ";
            json_res += r;
            first = false;
        }
        json_res += "]";

        return seastar::make_ready_future<std::string>(json_res);
    });
}

} // namespace ragedb::gql
