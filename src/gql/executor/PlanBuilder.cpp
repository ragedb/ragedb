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
#include "../GqlExecutor.h"
#include <sstream>
#include <unordered_set>
#include <unordered_map>
#include <set>
#include <map>
#include <algorithm>
#include <limits>
#include <optional>

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
        case LabelExprKind::WILDCARD:
            return "%";
    }
    return "";
}

/**
 * @brief Helper function to format a path pattern into a standard GQL string representation.
 * 
 * E.g., translates internal nodes and edges into "(a:Person)-[e:KNOWS]->(b:Person)".
 */
/// Renders a literal property value for a plan detail. Only the scalar alternatives appear in a pattern
/// constraint; anything else is shown as "?".
static std::string describe_property_value(const property_type_t& v) {
    if (std::holds_alternative<std::string>(v)) return "'" + std::get<std::string>(v) + "'";
    if (std::holds_alternative<int64_t>(v)) return std::to_string(std::get<int64_t>(v));
    if (std::holds_alternative<double>(v)) {
        std::ostringstream oss; oss << std::get<double>(v); return oss.str();
    }
    if (std::holds_alternative<bool>(v)) return std::get<bool>(v) ? "true" : "false";
    return "?";
}

static std::string describe_op(Operation op) {
    switch (op) {
        case Operation::EQ:  return "=";
        case Operation::NEQ: return "<>";
        case Operation::GT:  return ">";
        case Operation::GTE: return ">=";
        case Operation::LT:  return "<";
        case Operation::LTE: return "<=";
        case Operation::STARTS_WITH: return " STARTS WITH ";
        case Operation::CONTAINS:    return " CONTAINS ";
        case Operation::ENDS_WITH:   return " ENDS WITH ";
        default: return " ? ";
    }
}

/// Renders the property constraints on a node or edge -- both an inline literal map and the filters the
/// optimizer pushed into the scan -- as a `{...}` suffix. Without this a bound anchor
/// (`(a:Person {id: 1})` or `(a:Person) FILTER a.id = 1`, both pushed to property_filters) rendered
/// identically to an unbound full scan, hiding the single fact that decides whether a query seeks or scans.
static std::string describe_property_constraints(const std::map<std::string, property_type_t>& properties,
                                                 const std::vector<PropertyFilter>& filters) {
    std::vector<std::string> parts;
    for (const auto& [prop, val] : properties) {
        parts.push_back(prop + " = " + describe_property_value(val));
    }
    for (const auto& f : filters) {
        parts.push_back(f.property + describe_op(f.op) + describe_property_value(f.value));
    }
    if (parts.empty()) return "";
    std::string out = " {";
    for (size_t i = 0; i < parts.size(); ++i) {
        if (i > 0) out += ", ";
        out += parts[i];
    }
    return out + "}";
}

/// Renders the common predicate shapes for a plan detail, so a Filter operator shows the actual condition
/// (`p.age > 30`) rather than a generic "WHERE expression". Shapes it does not know fall back to a short
/// label so the plan stays readable rather than dumping a whole AST.
static std::string describe_expression(const Expression* expr) {
    if (!expr) return "";
    switch (expr->kind) {
        case ExpressionKind::LITERAL:
            return describe_property_value(static_cast<const LiteralExpr*>(expr)->value);
        case ExpressionKind::VARIABLE:
            return static_cast<const VariableExpr*>(expr)->name;
        case ExpressionKind::PROPERTY_LOOKUP: {
            auto* p = static_cast<const PropertyLookupExpr*>(expr);
            return p->variable + "." + p->property;
        }
        case ExpressionKind::BINARY_OP: {
            auto* b = static_cast<const BinaryOpExpr*>(expr);
            std::string op;
            switch (b->op) {
                case BinaryOpKind::AND: op = " AND "; break;
                case BinaryOpKind::OR:  op = " OR ";  break;
                case BinaryOpKind::ADD: op = " + ";   break;
                case BinaryOpKind::SUB: op = " - ";   break;
                case BinaryOpKind::MUL: op = " * ";   break;
                case BinaryOpKind::DIV: op = " / ";   break;
                case BinaryOpKind::CONCAT: op = " || "; break;
                case BinaryOpKind::EQ: op = " = ";  break;
                case BinaryOpKind::NE: op = " <> "; break;
                case BinaryOpKind::LT: op = " < ";  break;
                case BinaryOpKind::LE: op = " <= "; break;
                case BinaryOpKind::GT: op = " > ";  break;
                case BinaryOpKind::GE: op = " >= "; break;
                default: op = " ? "; break;
            }
            return describe_expression(b->left.get()) + op + describe_expression(b->right.get());
        }
        case ExpressionKind::CAST:      return "CAST(...)";
        case ExpressionKind::IS_LABELED: return "IS LABELED";
        case ExpressionKind::IS_DIRECTED: return "IS DIRECTED";
        case ExpressionKind::IS_SOURCE_DEST: return "IS SOURCE/DESTINATION OF";
        case ExpressionKind::AGGREGATION: return "<aggregate>";
        default:
            return "<expr>";
    }
}

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
            edge_str += describe_property_constraints(edge.properties, edge.property_filters);
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
        node_str += describe_property_constraints(node.properties, node.property_filters);
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

// ===== Cardinality estimation ===================================================================
// Rough per-operator row estimates for EXPLAIN. Base scans use the real label / relationship counts
// from the local shard; filters, fan-out and paging apply simple heuristics. These are indicative,
// not optimiser-grade: there is no distinct-value or histogram statistic to draw on, so equality on a
// non-id property is assumed ~10% selective and a range ~33%.
static double node_scan_selectivity(const PatternNode& n, bool& has_id_eq) {
    has_id_eq = false;
    double sel = 1.0;
    for (const auto& [prop, val] : n.properties) {
        if (prop == "id") has_id_eq = true;
        else sel *= 0.1;
    }
    for (const auto& f : n.property_filters) {
        if (f.property == "id" && f.op == Operation::EQ) has_id_eq = true;
        else if (f.op == Operation::EQ) sel *= 0.1;
        else sel *= 0.33;
    }
    return sel;
}

static int64_t estimate_node_scan(ragedb::Graph& graph, const PatternNode& n) {
    std::string label;
    if (n.label_expr && n.label_expr->kind == LabelExprKind::LITERAL) label = n.label_expr->name;
    int64_t base = label.empty() ? static_cast<int64_t>(graph.shard.local().AllNodesCount())
                                  : static_cast<int64_t>(graph.shard.local().NodeCount(label));
    bool has_id_eq = false;
    double sel = node_scan_selectivity(n, has_id_eq);
    if (has_id_eq) return 1;  // id equality selects a single node
    int64_t est = static_cast<int64_t>(static_cast<double>(base) * sel);
    return est < 1 ? 1 : est;
}

// Estimate the rows a single match statement produces: the anchor node's scan, fanned out along each
// edge by the relationship type's average degree, narrowed by each subsequent node's filters.
static int64_t estimate_match_cardinality(ragedb::Graph& graph, const MatchStatement& stmt) {
    if (stmt.pattern.nodes.empty()) return 1;
    int64_t est = estimate_node_scan(graph, stmt.pattern.nodes[0]);
    const int64_t total_nodes = static_cast<int64_t>(graph.shard.local().AllNodesCount());
    for (size_t i = 0; i < stmt.pattern.edges.size(); ++i) {
        const auto& e = stmt.pattern.edges[i];
        std::string rt;
        if (e.label_expr && e.label_expr->kind == LabelExprKind::LITERAL) rt = e.label_expr->name;
        int64_t rels = rt.empty() ? static_cast<int64_t>(graph.shard.local().AllRelationshipsCount())
                                  : static_cast<int64_t>(graph.shard.local().RelationshipCount(rt));
        double fanout = total_nodes > 0 ? static_cast<double>(rels) / static_cast<double>(total_nodes) : 1.0;
        est = static_cast<int64_t>(static_cast<double>(est) * fanout);
        if (i + 1 < stmt.pattern.nodes.size()) {
            bool hid = false;
            double sel = node_scan_selectivity(stmt.pattern.nodes[i + 1], hid);
            est = hid ? 1 : static_cast<int64_t>(static_cast<double>(est) * sel);
        }
        if (est < 1) est = 1;
    }
    return est;
}

// The estimate carried by an already-built child operator, if any.
static std::optional<int64_t> child_estimate(const std::shared_ptr<PlanNode>& n) {
    if (!n || n->children.empty()) return std::nullopt;
    return n->children.front()->estimated_rows;
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

        std::vector<MatchStatement> remaining_matches(matches.begin() + static_cast<std::ptrdiff_t>(match_idx) + 1, matches.end());
        return build_match_plan(graph, remaining_matches, 0, incoming_vars, join_node);
    } else {
        auto node = std::make_shared<PlanNode>();
        if (stmt.algebraic_path_count) {
            node->operator_name = "AlgebraicPathCountJoin";
            node->detail = "Algebraic Path Count (" + std::to_string(stmt.path_count_hops) + " hops)";
        } else if (incoming_vars.empty()) {
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
        // Estimated rows this match produces (indicative). A piped match is driven by the incoming
        // rows, so scale the pattern estimate by the input's cardinality relative to a bare scan.
        node->estimated_rows = estimate_match_cardinality(graph, stmt);
        if (input_plan) {
            node->children.push_back(input_plan);
            if (input_plan->estimated_rows && !stmt.pattern.nodes.empty()) {
                int64_t anchor = estimate_node_scan(graph, stmt.pattern.nodes[0]);
                if (anchor > 0) {
                    double ratio = static_cast<double>(*input_plan->estimated_rows) / static_cast<double>(anchor);
                    node->estimated_rows = std::max<int64_t>(1, static_cast<int64_t>(static_cast<double>(*node->estimated_rows) * ratio));
                }
            }
        }

        for (const auto& n : stmt.pattern.nodes) {
            if (!n.variable.empty()) incoming_vars.insert(n.variable);
        }
        for (const auto& e : stmt.pattern.edges) {
            if (!e.variable.empty()) incoming_vars.insert(e.variable);
        }
        if (stmt.algebraic_path_count && !stmt.path_count_target_var.empty()) {
            incoming_vars.insert(stmt.path_count_target_var);
        }
        node->variables = join_strings(incoming_vars, ", ");

        std::vector<MatchStatement> remaining_matches(matches.begin() + static_cast<std::ptrdiff_t>(match_idx) + 1, matches.end());
        return build_match_plan(graph, remaining_matches, 0, incoming_vars, node);
    }
}

static bool is_query_cyclic(const std::vector<MatchStatement>& matches) {
    std::unordered_map<std::string, std::vector<std::pair<std::string, std::string>>> adj_list;
    std::set<std::string> node_vars;
    
    for (size_t m_idx = 0; m_idx < matches.size(); ++m_idx) {
        const auto& match = matches[m_idx];
        if (match.is_search || match.is_propagate) continue;
        const auto& pattern = match.pattern;
        for (size_t i = 0; i < pattern.edges.size(); ++i) {
            std::string u = pattern.nodes[i].variable;
            std::string v = pattern.nodes[i+1].variable;
            std::string e = pattern.edges[i].variable;
            if (u.empty()) u = "_n_anon_" + std::to_string(m_idx) + "_" + std::to_string(i);
            if (v.empty()) v = "_n_anon_" + std::to_string(m_idx) + "_" + std::to_string(i+1);
            if (e.empty()) e = "_e_anon_" + std::to_string(m_idx) + "_" + std::to_string(i);
            
            node_vars.insert(u);
            node_vars.insert(v);
            adj_list[u].push_back({v, e});
            adj_list[v].push_back({u, e});
        }
    }
    
    std::unordered_set<std::string> visited;
    std::function<bool(const std::string&, const std::string&)> dfs = [&](const std::string& curr, const std::string& parent_edge) -> bool {
        visited.insert(curr);
        for (const auto& neighbor_info : adj_list[curr]) {
            std::string neighbor = neighbor_info.first;
            std::string edge_var = neighbor_info.second;
            if (edge_var == parent_edge) continue;
            if (visited.count(neighbor)) {
                return true;
            }
            if (dfs(neighbor, edge_var)) {
                return true;
            }
        }
        return false;
    };
    
    for (const auto& node : node_vars) {
        if (!visited.count(node)) {
            if (dfs(node, "")) return true;
        }
    }
    return false;
}

/**
 * @brief Helper function to build a plan for a single (non-union/set) GQL query.
 */
static std::shared_ptr<PlanNode> build_single_query_plan(ragedb::Graph& graph, const GqlQuery& query) {
    std::set<std::string> incoming_vars;
    std::shared_ptr<PlanNode> current;

    bool use_honeycomb = false;
    bool use_lftj = false;
    if (is_query_cyclic(query.matches)) {
        uint64_t total_rels = 0;
        for (const auto& match : query.matches) {
            if (match.is_search || match.is_propagate) continue;
            for (const auto& edge : match.pattern.edges) {
                std::string rel_type = "";
                if (edge.label_expr && edge.label_expr->kind == LabelExprKind::LITERAL) {
                    rel_type = edge.label_expr->name;
                }
                if (!rel_type.empty()) {
                    total_rels += graph.shard.local().RelationshipCount(rel_type);
                } else {
                    total_rels += graph.shard.local().AllRelationshipsCount();
                }
            }
        }
        if (GqlExecutor::force_enable_honeycomb) {
            use_honeycomb = true;
        } else if (GqlExecutor::force_enable_lftj) {
            use_lftj = true;
        } else {
            if (total_rels > 10000) {
                if (!GqlExecutor::force_disable_honeycomb) {
                    use_honeycomb = true;
                } else if (!GqlExecutor::force_disable_lftj) {
                    use_lftj = true;
                }
            } else {
                if (!GqlExecutor::force_disable_lftj) {
                    use_lftj = true;
                } else if (!GqlExecutor::force_disable_honeycomb) {
                    use_honeycomb = true;
                }
            }
        }
    }

    if (use_honeycomb) {
        auto hc_node = std::make_shared<PlanNode>();
        hc_node->operator_name = "HoneycombJoin";
        hc_node->detail = "Offloaded parallel WCOJ grid-partitioned join";
        hc_node->key = "honeycomb_join";
        
        std::set<std::string> vars;
        for (const auto& match : query.matches) {
            if (match.is_search || match.is_propagate) continue;
            for (const auto& node : match.pattern.nodes) {
                if (!node.variable.empty()) vars.insert(node.variable);
            }
            for (const auto& edge : match.pattern.edges) {
                if (!edge.variable.empty()) vars.insert(edge.variable);
            }
        }
        hc_node->variables = join_strings(vars, ", ");
        for (const auto& v : vars) {
            incoming_vars.insert(v);
        }
        current = hc_node;
    } else if (use_lftj) {
        auto lftj_node = std::make_shared<PlanNode>();
        lftj_node->operator_name = "LftjJoin";
        lftj_node->detail = "Reactor-local synchronous WCOJ join";
        lftj_node->key = "lftj_join";
        
        std::set<std::string> vars;
        for (const auto& match : query.matches) {
            if (match.is_search || match.is_propagate) continue;
            for (const auto& node : match.pattern.nodes) {
                if (!node.variable.empty()) vars.insert(node.variable);
            }
            for (const auto& edge : match.pattern.edges) {
                if (!edge.variable.empty()) vars.insert(edge.variable);
            }
        }
        lftj_node->variables = join_strings(vars, ", ");
        for (const auto& v : vars) {
            incoming_vars.insert(v);
        }
        current = lftj_node;
    } else {
        current = build_match_plan(graph, query.matches, 0, incoming_vars, nullptr);
    }


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
        filter_node->detail = describe_expression(query.where_expr.get());
        filter_node->variables = join_strings(incoming_vars, ", ");
        if (current) filter_node->children.push_back(current);
        if (auto c = child_estimate(filter_node)) {
            filter_node->estimated_rows = std::max<int64_t>(1, static_cast<int64_t>(static_cast<double>(*c) * 0.5));
        }
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
        // A global aggregate yields one row; a grouped one yields at most its input (no distinct-value
        // statistic to estimate the group count from, so the child is an upper bound).
        bool grouped = !query.group_by.empty();
        for (const auto& item : query.returns) {
            if (item.expr && !has_aggregates(item.expr.get())) { grouped = true; break; }
        }
        if (!grouped) agg_node->estimated_rows = 1;
        else if (auto c = child_estimate(agg_node)) agg_node->estimated_rows = *c;
        current = agg_node;
    }

    if (query.distinct) {
        auto distinct_node = std::make_shared<PlanNode>();
        distinct_node->operator_name = "Distinct";
        distinct_node->key = "distinct";
        distinct_node->variables = join_strings(incoming_vars, ", ");
        if (current) distinct_node->children.push_back(current);
        if (auto c = child_estimate(distinct_node)) distinct_node->estimated_rows = *c;  // upper bound
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
        if (auto c = child_estimate(sort_node)) sort_node->estimated_rows = *c;  // ordering preserves count
        current = sort_node;
    }

    if (query.limit) {
        auto limit_node = std::make_shared<PlanNode>();
        limit_node->operator_name = "Limit";
        limit_node->key = "limit";
        limit_node->detail = std::to_string(*query.limit);
        limit_node->variables = join_strings(incoming_vars, ", ");
        if (current) limit_node->children.push_back(current);
        {
            int64_t lim = static_cast<int64_t>(*query.limit);
            auto c = child_estimate(limit_node);
            limit_node->estimated_rows = c ? std::min<int64_t>(lim, *c) : lim;
        }
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
    if (auto c = child_estimate(produce_node)) produce_node->estimated_rows = *c;
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
        else if (query.kind == QueryKind::EXCEPT) node->operator_name = "Except";
        else if (query.kind == QueryKind::EXCEPT_ALL) node->operator_name = "ExceptAll";
        
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
