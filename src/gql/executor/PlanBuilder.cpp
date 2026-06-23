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

#include "PlanBuilder.h"
#include "StarJoinRewriter.h"
#include "ExpressionEvaluator.h"
#include <sstream>
#include <unordered_set>
#include <unordered_map>
#include <set>
#include <map>
#include <algorithm>
#include <limits>

namespace ragedb::gql {

/**
 * @brief Helper function to recursively describe a label expression (e.g. A & !B).
 */
static std::string describe_label_expr(const std::shared_ptr<LabelExpression>& expr) {
    if (!expr) return "";
    switch (expr->kind) {
        case LabelExprKind::LITERAL:
            return expr->name;
        case LabelExprKind::NOT:
            return "!" + describe_label_expr(expr->expr);
        case LabelExprKind::AND:
            return "(" + describe_label_expr(expr->left) + " & " + describe_label_expr(expr->right) + ")";
        case LabelExprKind::OR:
            return "(" + describe_label_expr(expr->left) + " | " + describe_label_expr(expr->right) + ")";
    }
    return "";
}

/**
 * @brief Helper function to format a path pattern into a standard GQL string representation.
 * 
 * E.g., translates internal nodes and edges into "(a:Person)-[e:KNOWS]->(b:Person)".
 */
static std::string describe_pattern(const PathPattern& pattern) {
    std::string res;
    for (size_t i = 0; i < pattern.nodes.size(); ++i) {
        if (i > 0) {
            const auto& edge = pattern.edges[i - 1];
            std::string edge_str = "-";
            if (edge.direction == EdgeDirection::LEFT) edge_str = "<-";
            
            edge_str += "[";
            if (!edge.variable.empty()) edge_str += edge.variable;
            if (edge.label_expr) {
                edge_str += ":" + describe_label_expr(edge.label_expr);
            }
            if (edge.is_variable_length) {
                edge_str += "*";
                if (edge.min_hops != 1 || edge.max_hops != std::numeric_limits<uint64_t>::max()) {
                    edge_str += std::to_string(edge.min_hops) + ".." + (edge.max_hops == std::numeric_limits<uint64_t>::max() ? "" : std::to_string(edge.max_hops));
                }
            }
            edge_str += "]";
            
            if (edge.direction == EdgeDirection::RIGHT) edge_str += "->";
            else edge_str += "-";
            
            res += edge_str;
        }
        
        const auto& node = pattern.nodes[i];
        std::string node_str = "(";
        if (!node.variable.empty()) node_str += node.variable;
        if (node.label_expr) {
            node_str += ":" + describe_label_expr(node.label_expr);
        }
        node_str += ")";
        res += node_str;
    }
    return res;
}

/**
 * @brief Helper function to join a set of string variables with a delimiter.
 */
static std::string join_strings(const std::set<std::string>& strings, const std::string& delimiter) {
    std::string res;
    for (const auto& s : strings) {
        if (!res.empty()) res += delimiter;
        res += s;
    }
    return res;
}

/**
 * @brief Recursively builds the plan tree for a sequence of MATCH statements.
 * 
 * Analyzes variables and relationships to decide between Index Seeks, Scans,
 * expansions, natural/left-outer joins, and star join rewriter strategies.
 */
static std::shared_ptr<PlanNode> build_match_plan(
    ragedb::Graph& graph,
    const std::vector<MatchStatement>& matches,
    size_t match_idx,
    std::set<std::string>& incoming_vars,
    std::shared_ptr<PlanNode> input_plan
) {
    if (match_idx >= matches.size()) {
        return input_plan;
    }

    // Check if the remaining matches can be rewritten as a star join
    if (auto candidate = find_star_join_candidate(matches, match_idx, incoming_vars)) {
        std::string central_var = candidate->central_var;
        const auto& indices = candidate->match_indices;

        if (incoming_vars.count(central_var)) {
            std::vector<MatchStatement> branch_matches;
            std::vector<MatchStatement> remaining_matches;
            std::set<size_t> S_set(indices.begin(), indices.end());

            for (size_t i = match_idx; i < matches.size(); ++i) {
                if (S_set.count(i)) {
                    branch_matches.push_back(matches[i]);
                } else {
                    remaining_matches.push_back(matches[i]);
                }
            }

            auto join_node = std::make_shared<PlanNode>();
            join_node->operator_name = "NaturalJoin";
            join_node->detail = "Star join on central variable: " + central_var;
            join_node->key = "star_join_" + central_var;
            
            std::set<std::string> current_vars = incoming_vars;
            for (const auto& branch_stmt : branch_matches) {
                std::vector<MatchStatement> single_stmt_vec = { branch_stmt };
                auto branch_plan = build_match_plan(graph, single_stmt_vec, 0, current_vars, input_plan);
                if (branch_plan) {
                    join_node->children.push_back(branch_plan);
                }
                for (const auto& node : branch_stmt.pattern.nodes) {
                    if (!node.variable.empty()) current_vars.insert(node.variable);
                }
                for (const auto& edge : branch_stmt.pattern.edges) {
                    if (!edge.variable.empty()) current_vars.insert(edge.variable);
                }
            }
            join_node->variables = join_strings(current_vars, ", ");

            if (remaining_matches.empty()) {
                return join_node;
            } else {
                return build_match_plan(graph, remaining_matches, 0, current_vars, join_node);
            }
        } else {
            size_t first_idx = indices[0];
            std::vector<MatchStatement> first_stmt_vec = { matches[first_idx] };
            
            std::vector<MatchStatement> remaining_matches;
            for (size_t i = match_idx; i < matches.size(); ++i) {
                if (i != first_idx) {
                    remaining_matches.push_back(matches[i]);
                }
            }

            auto first_plan = build_match_plan(graph, first_stmt_vec, 0, incoming_vars, input_plan);
            return build_match_plan(graph, remaining_matches, 0, incoming_vars, first_plan);
        }
    }

    const auto& stmt = matches[match_idx];

    // Determine if any variables in the current statement are shared with incoming variables
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

    // If no shared variables, it's a disconnected match which requires a cartesian/natural join
    if (!has_shared && !incoming_vars.empty()) {
        std::set<std::string> pattern_vars;
        std::vector<MatchStatement> single_stmt_vec = { stmt };
        auto pattern_plan = build_match_plan(graph, single_stmt_vec, 0, pattern_vars, nullptr);

        auto join_node = std::make_shared<PlanNode>();
        join_node->operator_name = stmt.is_optional ? "LeftOuterJoin" : "NaturalJoin";
        join_node->detail = stmt.is_optional ? "Optional match join" : "Disconnected match join";
        join_node->key = (stmt.is_optional ? "left_outer_join_" : "natural_join_") + std::to_string(stmt.id);
        if (input_plan) {
            join_node->children.push_back(input_plan);
        }
        if (pattern_plan) {
            join_node->children.push_back(pattern_plan);
        }
        
        for (const auto& var : pattern_vars) {
            incoming_vars.insert(var);
        }
        join_node->variables = join_strings(incoming_vars, ", ");

        std::vector<MatchStatement> remaining_matches(matches.begin() + match_idx + 1, matches.end());
        return build_match_plan(graph, remaining_matches, 0, incoming_vars, join_node);
    } else {
        auto node = std::make_shared<PlanNode>();
        if (incoming_vars.empty()) {
            // Seek optimization checks: check if we can seek by node/relationship index instead of scan
            bool has_node_seek = false;
            std::string node_indexed_prop = "";
            const auto& start_node = stmt.pattern.nodes[0];
            if (start_node.label_expr && start_node.label_expr->kind == LabelExprKind::LITERAL) {
                std::string label = start_node.label_expr->name;
                for (const auto& [prop, val] : start_node.properties) {
                    if (graph.shard.local().NodeIndexExists(label, prop)) {
                        has_node_seek = true;
                        node_indexed_prop = prop;
                        break;
                    }
                }
                if (!has_node_seek) {
                    for (const auto& filter : start_node.property_filters) {
                        if (filter.op == Operation::EQ && graph.shard.local().NodeIndexExists(label, filter.property)) {
                            has_node_seek = true;
                            node_indexed_prop = filter.property;
                            break;
                        }
                    }
                }
            }

            bool has_edge_seek = false;
            std::string edge_indexed_prop = "";
            if (!has_node_seek && !stmt.pattern.edges.empty()) {
                const auto& edge = stmt.pattern.edges[0];
                if (edge.label_expr && edge.label_expr->kind == LabelExprKind::LITERAL) {
                    std::string label = edge.label_expr->name;
                    for (const auto& [prop, val] : edge.properties) {
                        if (graph.shard.local().RelationshipIndexExists(label, prop)) {
                            has_edge_seek = true;
                            edge_indexed_prop = prop;
                            break;
                        }
                    }
                    if (!has_edge_seek) {
                        for (const auto& filter : edge.property_filters) {
                            if (filter.op == Operation::EQ && graph.shard.local().RelationshipIndexExists(label, filter.property)) {
                                has_edge_seek = true;
                                edge_indexed_prop = filter.property;
                                break;
                            }
                        }
                    }
                }
            }

            if (has_node_seek) {
                node->operator_name = "NodeIndexSeek";
                node->detail = "Seek (" + start_node.variable + ":" + start_node.label_expr->name + "(" + node_indexed_prop + ")) via index";
            } else if (has_edge_seek) {
                node->operator_name = "RelationshipIndexSeek";
                const auto& edge = stmt.pattern.edges[0];
                node->detail = "Seek [" + edge.variable + ":" + edge.label_expr->name + "(" + edge_indexed_prop + ")] via index";
            } else {
                node->operator_name = "Scan / Traverse";
                node->detail = describe_pattern(stmt.pattern);
            }
        } else {
            node->operator_name = stmt.is_optional ? "OptionalExpand" : "Expand / Traverse";
            node->detail = describe_pattern(stmt.pattern);
        }
        node->key = "match_" + std::to_string(stmt.id);
        if (input_plan) {
            node->children.push_back(input_plan);
        }
        
        for (const auto& n : stmt.pattern.nodes) {
            if (!n.variable.empty()) incoming_vars.insert(n.variable);
        }
        for (const auto& e : stmt.pattern.edges) {
            if (!e.variable.empty()) incoming_vars.insert(e.variable);
        }
        node->variables = join_strings(incoming_vars, ", ");

        std::vector<MatchStatement> remaining_matches(matches.begin() + match_idx + 1, matches.end());
        return build_match_plan(graph, remaining_matches, 0, incoming_vars, node);
    }
}

/**
 * @brief Helper function to build a plan for a single (non-union/set) GQL query.
 */
static std::shared_ptr<PlanNode> build_single_query_plan(ragedb::Graph& graph, const GqlQuery& query) {
    std::set<std::string> incoming_vars;
    std::shared_ptr<PlanNode> current = build_match_plan(graph, query.matches, 0, incoming_vars, nullptr);

    if (!query.writes.empty()) {
        auto write_node = std::make_shared<PlanNode>();
        write_node->operator_name = "Write";
        write_node->key = "write";
        std::string details;
        for (const auto& w : query.writes) {
            if (!details.empty()) details += ", ";
            if (w.type == WriteOp::Type::INSERT) {
                details += "INSERT " + describe_pattern(w.insert_pattern);
            } else if (w.type == WriteOp::Type::SET) {
                details += "SET " + w.set_var + "." + w.set_prop;
            } else if (w.type == WriteOp::Type::REMOVE) {
                details += "REMOVE " + w.remove_var + "." + w.remove_prop;
            } else if (w.type == WriteOp::Type::DELETE_OP) {
                details += (w.detach ? "DETACH DELETE " : "DELETE ") + w.delete_var;
            }
        }
        write_node->detail = details;
        write_node->variables = join_strings(incoming_vars, ", ");
        if (current) write_node->children.push_back(current);
        current = write_node;
    }

    if (query.where_expr) {
        auto filter_node = std::make_shared<PlanNode>();
        filter_node->operator_name = "Filter";
        filter_node->key = "filter";
        filter_node->detail = "WHERE expression";
        filter_node->variables = join_strings(incoming_vars, ", ");
        if (current) filter_node->children.push_back(current);
        current = filter_node;
    }

    bool query_contains_aggregates = false;
    for (const auto& item : query.returns) {
        if (has_aggregates(item.expr.get())) {
            query_contains_aggregates = true;
            break;
        }
    }
    if (!query_contains_aggregates) {
        for (const auto& spec : query.order_by) {
            if (has_aggregates(spec.expr.get())) {
                query_contains_aggregates = true;
                break;
            }
        }
    }

    if (query_contains_aggregates) {
        auto agg_node = std::make_shared<PlanNode>();
        agg_node->operator_name = "Aggregate";
        agg_node->key = "aggregate";
        agg_node->variables = join_strings(incoming_vars, ", ");
        if (current) agg_node->children.push_back(current);
        current = agg_node;
    }

    if (query.distinct) {
        auto distinct_node = std::make_shared<PlanNode>();
        distinct_node->operator_name = "Distinct";
        distinct_node->key = "distinct";
        distinct_node->variables = join_strings(incoming_vars, ", ");
        if (current) distinct_node->children.push_back(current);
        current = distinct_node;
    }

    if (!query.order_by.empty()) {
        auto sort_node = std::make_shared<PlanNode>();
        sort_node->operator_name = "Sort";
        sort_node->key = "sort";
        std::string details;
        for (const auto& spec : query.order_by) {
            if (!details.empty()) details += ", ";
            details += spec.ascending ? "ASC" : "DESC";
        }
        sort_node->detail = details;
        sort_node->variables = join_strings(incoming_vars, ", ");
        if (current) sort_node->children.push_back(current);
        current = sort_node;
    }

    if (query.limit) {
        auto limit_node = std::make_shared<PlanNode>();
        limit_node->operator_name = "Limit";
        limit_node->key = "limit";
        limit_node->detail = std::to_string(*query.limit);
        limit_node->variables = join_strings(incoming_vars, ", ");
        if (current) limit_node->children.push_back(current);
        current = limit_node;
    }

    auto produce_node = std::make_shared<PlanNode>();
    produce_node->operator_name = "ProduceResults";
    produce_node->key = "produce_results";
    std::string returns_str;
    for (const auto& item : query.returns) {
        if (!returns_str.empty()) returns_str += ", ";
        if (item.alias) {
            returns_str += *item.alias;
        } else {
            returns_str += "expr";
        }
    }
    produce_node->detail = returns_str;
    produce_node->variables = returns_str;
    if (current) produce_node->children.push_back(current);
    current = produce_node;

    return current;
}

std::shared_ptr<PlanNode> build_query_plan(ragedb::Graph& graph, const GqlQuery& query) {
    if (query.clear_cache) {
        auto node = std::make_shared<PlanNode>();
        node->operator_name = "ClearCache";
        node->detail = "CALL CLEAR CACHE";
        node->key = "clear_cache";
        return node;
    }
    if (query.schema_op) {
        auto node = std::make_shared<PlanNode>();
        node->operator_name = "SchemaOperation";
        node->detail = query.schema_op->name;
        node->key = "schema_op";
        return node;
    }
    if (query.kind != QueryKind::SINGLE) {
        auto node = std::make_shared<PlanNode>();
        if (query.kind == QueryKind::UNION) node->operator_name = "Union";
        else if (query.kind == QueryKind::UNION_ALL) node->operator_name = "UnionAll";
        else if (query.kind == QueryKind::INTERSECT) node->operator_name = "Intersect";
        else if (query.kind == QueryKind::INTERSECT_ALL) node->operator_name = "IntersectAll";
        
        node->key = "set_operation";
        if (query.left) {
            node->children.push_back(build_query_plan(graph, *query.left));
        }
        if (query.right) {
            node->children.push_back(build_query_plan(graph, *query.right));
        }
        return node;
    }
    return build_single_query_plan(graph, query);
}

void index_plan_nodes(
    const std::shared_ptr<PlanNode>& node,
    std::map<std::string, std::shared_ptr<PlanNode>>& plan_nodes
) {
    if (!node) return;
    if (!node->key.empty()) {
        plan_nodes[node->key] = node;
    }
    for (const auto& child : node->children) {
        index_plan_nodes(child, plan_nodes);
    }
}

void flatten_plan_tree(
    const std::shared_ptr<PlanNode>& node,
    std::vector<std::vector<GqlValue>>& rows,
    std::string indent,
    bool is_last
) {
    if (!node) return;

    std::vector<GqlValue> row;
    std::string prefix = "";
    if (!indent.empty()) {
        prefix = indent + (is_last ? "└─ " : "├─ ");
    }
    row.push_back(GqlValue(prefix + node->operator_name));
    row.push_back(GqlValue(node->detail));
    row.push_back(GqlValue(node->variables));
    row.push_back(node->actual_rows ? GqlValue(*node->actual_rows) : GqlValue());
    row.push_back(node->time_ms ? GqlValue(*node->time_ms) : GqlValue());
    row.push_back(node->estimated_rows ? GqlValue(*node->estimated_rows) : GqlValue());

    rows.push_back(row);

    std::string next_indent = indent;
    if (!indent.empty()) {
        next_indent += is_last ? "   " : "│  ";
    } else {
        next_indent = "";
    }

    for (size_t i = 0; i < node->children.size(); ++i) {
        bool last_child = (i == node->children.size() - 1);
        flatten_plan_tree(node->children[i], rows, next_indent, last_child);
    }
}

} // namespace ragedb::gql
