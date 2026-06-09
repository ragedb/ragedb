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
 * @brief Helper to check if an expression contains aggregate functions.
 */
bool has_aggregates(const Expression* expr) {
    if (!expr) return false;
    if (expr->kind == ExpressionKind::AGGREGATION) return true;
    if (expr->kind == ExpressionKind::UNARY_OP) {
        auto* un = static_cast<const UnaryOpExpr*>(expr);
        return has_aggregates(un->expr.get());
    }
    if (expr->kind == ExpressionKind::BINARY_OP) {
        auto* bin = static_cast<const BinaryOpExpr*>(expr);
        return has_aggregates(bin->left.get()) || has_aggregates(bin->right.get());
    }
    return false;
}

/**
 * @brief Recursively collects all pointers to AggregateExpr within an expression tree.
 */
void find_aggregates(const Expression* expr, std::vector<const AggregateExpr*>& aggregates) {
    if (!expr) return;
    if (expr->kind == ExpressionKind::AGGREGATION) {
        aggregates.push_back(static_cast<const AggregateExpr*>(expr));
        return;
    }
    if (expr->kind == ExpressionKind::UNARY_OP) {
        auto* un = static_cast<const UnaryOpExpr*>(expr);
        find_aggregates(un->expr.get(), aggregates);
    } else if (expr->kind == ExpressionKind::BINARY_OP) {
        auto* bin = static_cast<const BinaryOpExpr*>(expr);
        find_aggregates(bin->left.get(), aggregates);
        find_aggregates(bin->right.get(), aggregates);
    }
}

/**
 * @brief Evaluates an expression (including sub-expressions containing aggregates) for an aggregated group.
 * 
 * Uses pre-computed aggregate values for AggregateExpr nodes and falls back to evaluating
 * non-aggregate sub-expressions on the group's representative row.
 */
GqlValue evaluate_group_expression(const GqlRow& representative, const std::map<const AggregateExpr*, GqlValue>& aggregate_results, const Expression* expr) {
    if (!expr) return GqlValue();

    switch (expr->kind) {
        case ExpressionKind::AGGREGATION: {
            auto* agg = static_cast<const AggregateExpr*>(expr);
            auto it = aggregate_results.find(agg);
            if (it != aggregate_results.end()) {
                return it->second;
            }
            return GqlValue();
        }
        case ExpressionKind::LITERAL: {
            auto* lit = static_cast<const LiteralExpr*>(expr);
            return GqlValue(lit->value);
        }
        case ExpressionKind::VARIABLE: {
            auto* var = static_cast<const VariableExpr*>(expr);
            auto it = representative.bindings.find(var->name);
            if (it != representative.bindings.end()) {
                return it->second;
            }
            return GqlValue();
        }
        case ExpressionKind::PROPERTY_LOOKUP: {
            auto* prop_lookup = static_cast<const PropertyLookupExpr*>(expr);
            auto it = representative.bindings.find(prop_lookup->variable);
            if (it != representative.bindings.end()) {
                const auto& val = it->second;
                if (val.type == GqlValue::NODE) {
                    return GqlValue(val.node->getProperty(prop_lookup->property));
                } else if (val.type == GqlValue::RELATIONSHIP) {
                    return GqlValue(val.relationship->getProperty(prop_lookup->property));
                }
            }
            return GqlValue();
        }
        case ExpressionKind::UNARY_OP: {
            auto* un = static_cast<const UnaryOpExpr*>(expr);
            auto val = evaluate_group_expression(representative, aggregate_results, un->expr.get());
            if (un->op == UnaryOpKind::NOT) {
                return GqlValue(!val.is_truthy());
            } else if (un->op == UnaryOpKind::NEG) {
                if (val.type == GqlValue::PROPERTY) {
                    if (std::holds_alternative<int64_t>(val.property)) {
                        return GqlValue(-std::get<int64_t>(val.property));
                    }
                    if (std::holds_alternative<double>(val.property)) {
                        return GqlValue(-std::get<double>(val.property));
                    }
                }
                return GqlValue();
            }
            return GqlValue();
        }
        case ExpressionKind::BINARY_OP: {
            auto* bin = static_cast<const BinaryOpExpr*>(expr);
            if (bin->op == BinaryOpKind::AND) {
                auto lhs = evaluate_group_expression(representative, aggregate_results, bin->left.get());
                if (!lhs.is_truthy()) return GqlValue(false);
                auto rhs = evaluate_group_expression(representative, aggregate_results, bin->right.get());
                return GqlValue(rhs.is_truthy());
            }
            if (bin->op == BinaryOpKind::OR) {
                auto lhs = evaluate_group_expression(representative, aggregate_results, bin->left.get());
                if (lhs.is_truthy()) return GqlValue(true);
                auto rhs = evaluate_group_expression(representative, aggregate_results, bin->right.get());
                return GqlValue(rhs.is_truthy());
            }

            auto lhs = evaluate_group_expression(representative, aggregate_results, bin->left.get());
            auto rhs = evaluate_group_expression(representative, aggregate_results, bin->right.get());

            if (lhs.type == GqlValue::NIL || rhs.type == GqlValue::NIL) {
                return GqlValue();
            }

            if (bin->op == BinaryOpKind::EQ) {
                return GqlValue(compare_gql_values(lhs, rhs) == 0);
            }
            if (bin->op == BinaryOpKind::NE) {
                return GqlValue(compare_gql_values(lhs, rhs) != 0);
            }
            if (bin->op == BinaryOpKind::LT) {
                return GqlValue(compare_gql_values(lhs, rhs) < 0);
            }
            if (bin->op == BinaryOpKind::LE) {
                return GqlValue(compare_gql_values(lhs, rhs) <= 0);
            }
            if (bin->op == BinaryOpKind::GT) {
                return GqlValue(compare_gql_values(lhs, rhs) > 0);
            }
            if (bin->op == BinaryOpKind::GE) {
                return GqlValue(compare_gql_values(lhs, rhs) >= 0);
            }

            if (lhs.type == GqlValue::PROPERTY && rhs.type == GqlValue::PROPERTY) {
                if (std::holds_alternative<int64_t>(lhs.property) && std::holds_alternative<int64_t>(rhs.property)) {
                    int64_t l = std::get<int64_t>(lhs.property);
                    int64_t r = std::get<int64_t>(rhs.property);
                    if (bin->op == BinaryOpKind::ADD) return GqlValue(l + r);
                    if (bin->op == BinaryOpKind::SUB) return GqlValue(l - r);
                    if (bin->op == BinaryOpKind::MUL) return GqlValue(l * r);
                    if (bin->op == BinaryOpKind::DIV) return r != 0 ? GqlValue(l / r) : GqlValue();
                }
                if (std::holds_alternative<double>(lhs.property) || std::holds_alternative<double>(rhs.property)) {
                    double l = std::holds_alternative<double>(lhs.property) ? std::get<double>(lhs.property) : static_cast<double>(std::get<int64_t>(lhs.property));
                    double r = std::holds_alternative<double>(rhs.property) ? std::get<double>(rhs.property) : static_cast<double>(std::get<int64_t>(rhs.property));
                    if (bin->op == BinaryOpKind::ADD) return GqlValue(l + r);
                    if (bin->op == BinaryOpKind::SUB) return GqlValue(l - r);
                    if (bin->op == BinaryOpKind::MUL) return GqlValue(l * r);
                    if (bin->op == BinaryOpKind::DIV) return r != 0.0 ? GqlValue(l / r) : GqlValue();
                }
                if (std::holds_alternative<std::string>(lhs.property) && std::holds_alternative<std::string>(rhs.property)) {
                    if (bin->op == BinaryOpKind::ADD) {
                        return GqlValue(std::get<std::string>(lhs.property) + std::get<std::string>(rhs.property));
                    }
                }
            }
            return GqlValue();
        }
    }
    return GqlValue();
}

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

        // Detect if there are aggregate expressions anywhere in projected items or order by spec
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

        std::vector<std::string> results;
        std::unordered_set<std::string> seen;

        if (contains_aggregates) {
            // Collect all unique AggregateExpr pointer targets in the query
            std::vector<const AggregateExpr*> aggregate_exprs;
            for (const auto& item : query.returns) {
                find_aggregates(item.expr.get(), aggregate_exprs);
            }
            for (const auto& spec : query.order_by) {
                find_aggregates(spec.expr.get(), aggregate_exprs);
            }

            // Identify non-aggregate grouping key expressions from query returns
            std::vector<const Expression*> grouping_keys;
            for (const auto& item : query.returns) {
                if (!has_aggregates(item.expr.get())) {
                    grouping_keys.push_back(item.expr.get());
                }
            }

            // Group the matching rows by evaluated grouping keys
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

            // Standard GQL behavior: if inputs are empty and there are only aggregates, return a single default row
            if (groups.empty() && grouping_keys.empty()) {
                GqlGroup default_group;
                default_group.representative = GqlRow{};
                groups[{}] = default_group;
            }

            // Evaluate aggregates and build projected results for each group
            struct GroupSortKey {
                std::vector<GqlValue> sort_keys;
                std::string serialized_row;
            };
            std::vector<GroupSortKey> sorted_groups;

            for (const auto& [key, group] : groups) {
                // Pre-compute all aggregate values for this group
                std::map<const AggregateExpr*, GqlValue> aggregate_results;
                for (const auto* agg : aggregate_exprs) {
                    if (agg->fn_kind == AggregateKind::COUNT) {
                        int64_t count = 0;
                        if (!agg->expr) {
                            // count(*)
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
                            // AVG
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

                // Serialize projected group columns
                std::string serialized_row = "{";
                bool first = true;
                for (size_t i = 0; i < query.returns.size(); ++i) {
                    const auto& item = query.returns[i];
                    GqlValue val = evaluate_group_expression(group.representative, aggregate_results, item.expr.get());

                    std::string return_key;
                    if (item.alias) {
                        return_key = *item.alias;
                    } else {
                        if (item.expr->kind == ExpressionKind::PROPERTY_LOOKUP) {
                            auto* pl = static_cast<const PropertyLookupExpr*>(item.expr.get());
                            return_key = pl->variable + "." + pl->property;
                        } else if (item.expr->kind == ExpressionKind::VARIABLE) {
                            auto* ve = static_cast<const VariableExpr*>(item.expr.get());
                            return_key = ve->name;
                        } else if (item.expr->kind == ExpressionKind::AGGREGATION) {
                            auto* ae = static_cast<const AggregateExpr*>(item.expr.get());
                            std::string fn_name;
                            if (ae->fn_kind == AggregateKind::COUNT) fn_name = "count";
                            else if (ae->fn_kind == AggregateKind::SUM) fn_name = "sum";
                            else if (ae->fn_kind == AggregateKind::AVG) fn_name = "avg";
                            else if (ae->fn_kind == AggregateKind::MIN) fn_name = "min";
                            else fn_name = "max";

                            if (!ae->expr) {
                                return_key = fn_name + "(*)";
                            } else if (ae->expr->kind == ExpressionKind::PROPERTY_LOOKUP) {
                                auto* pl = static_cast<const PropertyLookupExpr*>(ae->expr.get());
                                return_key = fn_name + "(" + pl->variable + "." + pl->property + ")";
                            } else if (ae->expr->kind == ExpressionKind::VARIABLE) {
                                auto* ve = static_cast<const VariableExpr*>(ae->expr.get());
                                return_key = fn_name + "(" + ve->name + ")";
                            } else {
                                return_key = fn_name + "(expr)";
                            }
                        } else {
                            return_key = "column_" + std::to_string(i);
                        }
                    }

                    if (!first) serialized_row += ", ";
                    serialized_row += "\"" + return_key + "\": " + serialize_gql_value(val);
                    first = false;
                }
                serialized_row += "}";

                // Evaluate sorting specifications for this group
                GroupSortKey gsk;
                gsk.serialized_row = serialized_row;
                for (const auto& spec : query.order_by) {
                    gsk.sort_keys.push_back(evaluate_group_expression(group.representative, aggregate_results, spec.expr.get()));
                }
                sorted_groups.push_back(std::move(gsk));
            }

            // Apply sorting to the groups
            if (!query.order_by.empty()) {
                std::stable_sort(sorted_groups.begin(), sorted_groups.end(), [&query](const GroupSortKey& a, const GroupSortKey& b) {
                    for (size_t i = 0; i < query.order_by.size(); ++i) {
                        int cmp = compare_gql_values(a.sort_keys[i], b.sort_keys[i]);
                        if (cmp != 0) {
                            return query.order_by[i].ascending ? (cmp < 0) : (cmp > 0);
                        }
                    }
                    return false;
                });
            }

            // Format results with distinct constraints
            for (const auto& gsk : sorted_groups) {
                if (query.distinct) {
                    if (seen.insert(gsk.serialized_row).second) {
                        results.push_back(gsk.serialized_row);
                    }
                } else {
                    results.push_back(gsk.serialized_row);
                }
            }
        } else {
            // Apply ORDER BY to raw rows
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

            // Format return projection items for raw rows
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
