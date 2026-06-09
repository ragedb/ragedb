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
#include "GqlTypechecker.h"
#include <sstream>
#include <unordered_set>
#include <set>
#include <map>
#include <algorithm>
#include <cstdlib>
#include <chrono>
#include <seastar/core/when_all.hh>

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
    std::string single_label = "";
    if (node.label_expr && node.label_expr->kind == LabelExprKind::LITERAL) {
        single_label = node.label_expr->name;
    }
    if (!node.properties.empty() && !single_label.empty()) {
        auto it = node.properties.begin();
        std::string prop = it->first;
        property_type_t val = it->second;
        return graph.shard.local().FindNodesPeered(single_label, prop, Operation::EQ, val, 0, 100000);
    } else if (!single_label.empty()) {
        return graph.shard.local().AllNodesPeered(single_label, 0, 100000);
    } else {
        return graph.shard.local().AllNodesPeered(0, 100000);
    }
}

struct PathHop {
    std::vector<Relationship> rels;
    std::vector<Node> nodes;
};

seastar::future<std::vector<PathHop>> traverse_var_len_async(
    ragedb::Graph& graph,
    uint64_t current_node_id,
    const PatternEdge& edge,
    const PatternNode& next_node,
    uint64_t current_depth,
    std::vector<uint64_t> visited_rel_ids,
    std::vector<Relationship> current_path_rels,
    std::vector<Node> current_path_nodes
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
           local_results = std::move(local_results)](std::vector<Relationship> rels) mutable {

        std::vector<seastar::future<std::vector<PathHop>>> branch_futs;
        for (const auto& rel : rels) {
            // Relationship uniqueness check (edge isomorphic path matching)
            if (std::find(visited_rel_ids.begin(), visited_rel_ids.end(), rel.getId()) != visited_rel_ids.end()) {
                continue;
            }

            // Check relationship type/properties
            if (edge.label_expr && !matches_label_expr(rel.getType(), edge.label_expr)) {
                continue;
            }
            if (!matches_properties(rel.getProperties(), edge.properties)) {
                continue;
            }

            uint64_t target_id = (rel.getStartingNodeId() == current_node_id) ? rel.getEndingNodeId() : rel.getStartingNodeId();

            branch_futs.push_back(
                graph.shard.local().NodeGetPeered(target_id)
                .then([&graph, rel, edge, next_node, current_depth, visited_rel_ids, current_path_rels, current_path_nodes, target_id](Node target_node) mutable {
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
                        std::move(current_path_nodes)
                    );
                })
            );
        }

        if (branch_futs.empty()) {
            return seastar::make_ready_future<std::vector<PathHop>>(std::move(local_results));
        }

        return seastar::when_all_succeed(branch_futs.begin(), branch_futs.end())
        .then([local_results = std::move(local_results)](std::vector<std::vector<PathHop>> nested_branches) mutable {
            for (auto& branch_res : nested_branches) {
                local_results.insert(local_results.end(), branch_res.begin(), branch_res.end());
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

    if (edge.is_variable_length) {
        return traverse_var_len_async(graph, src_id, edge, next_node, 0, {}, {}, {})
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
                if (!matches_properties(final_node.getProperties(), next_node.properties)) {
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

    auto handle_rels = [row, edge, next_node, src_id, node_idx, &graph](std::vector<Relationship> rels) {
        std::vector<seastar::future<std::optional<GqlRow>>> futs;
        for (const auto& rel : rels) {
            if (edge.label_expr && !matches_label_expr(rel.getType(), edge.label_expr)) {
                continue;
            }
            if (!matches_properties(rel.getProperties(), edge.properties)) {
                continue;
            }

            uint64_t target_id = (rel.getStartingNodeId() == src_id) ? rel.getEndingNodeId() : rel.getStartingNodeId();

            futs.push_back(
                graph.shard.local().NodeGetPeered(target_id)
                .then([row, rel, edge, next_node, node_idx](Node target_node) -> std::optional<GqlRow> {
                    if (next_node.label_expr && !matches_label_expr(target_node.getType(), next_node.label_expr)) {
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
    };

    if (edge_type.empty()) {
        return graph.shard.local().NodeGetRelationshipsPeered(src_id, dir).then(std::move(handle_rels));
    } else {
        return graph.shard.local().NodeGetRelationshipsPeered(src_id, dir, edge_type).then(std::move(handle_rels));
    }
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
            if (prep_pattern.nodes[0].label_expr && !matches_label_expr(node.getType(), prep_pattern.nodes[0].label_expr)) {
                continue;
            }
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
struct QueryResult {
    std::vector<std::string> column_names;
    std::vector<std::vector<GqlValue>> rows;
};

static void sort_combined_result(QueryResult& res, const std::vector<SortSpec>& order_by) {
    if (order_by.empty()) return;
    
    std::stable_sort(res.rows.begin(), res.rows.end(), [&res, &order_by](const std::vector<GqlValue>& a, const std::vector<GqlValue>& b) {
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
    });
}

static seastar::future<QueryResult> execute_query_internal(ragedb::Graph& graph, std::shared_ptr<GqlQuery> query_ptr) {
    if (query_ptr->kind != QueryKind::SINGLE) {
        auto left_ptr = std::shared_ptr<GqlQuery>(query_ptr->left.release());
        auto right_ptr = std::shared_ptr<GqlQuery>(query_ptr->right.release());

        return execute_query_internal(graph, left_ptr)
        .then([&graph, right_ptr, query_ptr](QueryResult left_res) {
            return execute_query_internal(graph, right_ptr)
            .then([left_res = std::move(left_res), query_ptr](QueryResult right_res) {
                if (left_res.column_names.size() != right_res.column_names.size()) {
                    throw std::runtime_error("All subqueries in a GQL Set operation must return the same number of columns");
                }

                QueryResult combined_res;
                combined_res.column_names = left_res.column_names;

                if (query_ptr->kind == QueryKind::UNION_ALL) {
                    combined_res.rows = std::move(left_res.rows);
                    for (auto& r : right_res.rows) {
                        combined_res.rows.push_back(std::move(r));
                    }
                } else if (query_ptr->kind == QueryKind::UNION) {
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
                }

                return combined_res;
            });
        });
    }

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

        QueryResult query_res;

        // Resolve column names
        for (size_t i = 0; i < query.returns.size(); ++i) {
            const auto& item = query.returns[i];
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
                } else if (item.expr->kind == ExpressionKind::AGGREGATION) {
                    auto* ae = static_cast<const AggregateExpr*>(item.expr.get());
                    std::string fn_name;
                    if (ae->fn_kind == AggregateKind::COUNT) fn_name = "count";
                    else if (ae->fn_kind == AggregateKind::SUM) fn_name = "sum";
                    else if (ae->fn_kind == AggregateKind::AVG) fn_name = "avg";
                    else if (ae->fn_kind == AggregateKind::MIN) fn_name = "min";
                    else fn_name = "max";

                    if (!ae->expr) {
                        key = fn_name + "(*)";
                    } else if (ae->expr->kind == ExpressionKind::PROPERTY_LOOKUP) {
                        auto* pl = static_cast<const PropertyLookupExpr*>(ae->expr.get());
                        key = fn_name + "(" + pl->variable + "." + pl->property + ")";
                    } else if (ae->expr->kind == ExpressionKind::VARIABLE) {
                        auto* ve = static_cast<const VariableExpr*>(ae->expr.get());
                        key = fn_name + "(" + ve->name + ")";
                    } else {
                        key = fn_name + "(expr)";
                    }
                } else {
                    key = "column_" + std::to_string(i);
                }
            }
            query_res.column_names.push_back(key);
        }

        std::unordered_set<std::string> seen_distinct;

        if (contains_aggregates) {
            std::vector<const AggregateExpr*> aggregate_exprs;
            for (const auto& item : query.returns) {
                find_aggregates(item.expr.get(), aggregate_exprs);
            }
            for (const auto& spec : query.order_by) {
                find_aggregates(spec.expr.get(), aggregate_exprs);
            }

            std::vector<const Expression*> grouping_keys;
            for (const auto& item : query.returns) {
                if (!has_aggregates(item.expr.get())) {
                    grouping_keys.push_back(item.expr.get());
                }
            }

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
                std::map<const AggregateExpr*, GqlValue> aggregate_results;
                for (const auto* agg : aggregate_exprs) {
                    if (agg->fn_kind == AggregateKind::COUNT) {
                        int64_t count = 0;
                        if (!agg->expr) {
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
        }

        if (query.limit && query_res.rows.size() > *query.limit) {
            query_res.rows.resize(*query.limit);
        }

        return seastar::make_ready_future<QueryResult>(std::move(query_res));
    });
}

seastar::future<std::string> GqlExecutor::execute(ragedb::Graph& graph, GqlQuery query_val) {
    GqlTypechecker::typecheck(graph, query_val);

    if (query_val.schema_op.has_value()) {
        return GqlCatalog::execute_schema_op(graph, *query_val.schema_op);
    }

    auto query_ptr = std::make_shared<GqlQuery>(std::move(query_val));

    return execute_query_internal(graph, query_ptr)
    .then([query_ptr](QueryResult result) {
        const auto& query = *query_ptr;

        if (!query.order_by.empty()) {
            sort_combined_result(result, query.order_by);
        }

        if (query.limit && result.rows.size() > *query.limit) {
            result.rows.resize(*query.limit);
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

} // namespace ragedb::gql
