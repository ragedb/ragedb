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
#include <sstream>
#include <unordered_set>
#include <algorithm>
#include <cstdlib>
#include <chrono>
#include <seastar/core/when_all.hh>
#include <seastar/json/json_elements.hh>

namespace ragedb::gql {

struct GqlValue {
    enum Type { NIL, NODE, RELATIONSHIP, PROPERTY } type = NIL;
    std::optional<Node> node;
    std::optional<Relationship> relationship;
    property_type_t property = std::monostate{};

    GqlValue() : type(NIL) {}
    explicit GqlValue(Node n) : type(NODE), node(std::move(n)) {}
    explicit GqlValue(Relationship r) : type(RELATIONSHIP), relationship(std::move(r)) {}
    explicit GqlValue(property_type_t p) : type(PROPERTY), property(std::move(p)) {}

    [[nodiscard]] bool is_truthy() const {
        if (type == PROPERTY) {
            if (std::holds_alternative<bool>(property)) {
                return std::get<bool>(property);
            }
        }
        return type != NIL;
    }
};

struct GqlRow {
    std::map<std::string, GqlValue> bindings;
};

inline int compare_properties(const property_type_t& lhs, const property_type_t& rhs) {
    if (lhs.index() == rhs.index()) {
        if (std::holds_alternative<int64_t>(lhs)) {
            int64_t l = std::get<int64_t>(lhs);
            int64_t r = std::get<int64_t>(rhs);
            return (l < r) ? -1 : ((l > r) ? 1 : 0);
        }
        if (std::holds_alternative<double>(lhs)) {
            double l = std::get<double>(lhs);
            double r = std::get<double>(rhs);
            return (l < r) ? -1 : ((l > r) ? 1 : 0);
        }
        if (std::holds_alternative<std::string>(lhs)) {
            const auto& l = std::get<std::string>(lhs);
            const auto& r = std::get<std::string>(rhs);
            return (l < r) ? -1 : ((l > r) ? 1 : 0);
        }
        if (std::holds_alternative<bool>(lhs)) {
            bool l = std::get<bool>(lhs);
            bool r = std::get<bool>(rhs);
            return (l < r) ? -1 : ((l > r) ? 1 : 0);
        }
    } else {
        if (std::holds_alternative<int64_t>(lhs) && std::holds_alternative<double>(rhs)) {
            double l = static_cast<double>(std::get<int64_t>(lhs));
            double r = std::get<double>(rhs);
            return (l < r) ? -1 : ((l > r) ? 1 : 0);
        }
        if (std::holds_alternative<double>(lhs) && std::holds_alternative<int64_t>(rhs)) {
            double l = std::get<double>(lhs);
            double r = static_cast<double>(std::get<int64_t>(rhs));
            return (l < r) ? -1 : ((l > r) ? 1 : 0);
        }
    }
    return 0;
}

inline int compare_gql_values(const GqlValue& a, const GqlValue& b) {
    if (a.type != b.type) return (a.type < b.type) ? -1 : 1;
    if (a.type == GqlValue::PROPERTY) {
        return compare_properties(a.property, b.property);
    }
    if (a.type == GqlValue::NODE) {
        uint64_t la = a.node->getId();
        uint64_t lb = b.node->getId();
        return (la < lb) ? -1 : ((la > lb) ? 1 : 0);
    }
    if (a.type == GqlValue::RELATIONSHIP) {
        uint64_t la = a.relationship->getId();
        uint64_t lb = b.relationship->getId();
        return (la < lb) ? -1 : ((la > lb) ? 1 : 0);
    }
    return 0;
}

inline bool matches_properties(const std::map<std::string, property_type_t>& target, const std::map<std::string, property_type_t>& pattern) {
    for (const auto& [key, val] : pattern) {
        auto it = target.find(key);
        if (it == target.end()) return false;
        if (compare_properties(it->second, val) != 0) return false;
    }
    return true;
}

GqlValue evaluate_expression(const GqlRow& row, const Expression* expr) {
    if (!expr) return GqlValue();

    switch (expr->kind) {
        case ExpressionKind::LITERAL: {
            auto* lit = static_cast<const LiteralExpr*>(expr);
            return GqlValue(lit->value);
        }
        case ExpressionKind::VARIABLE: {
            auto* var = static_cast<const VariableExpr*>(expr);
            auto it = row.bindings.find(var->name);
            if (it != row.bindings.end()) {
                return it->second;
            }
            return GqlValue();
        }
        case ExpressionKind::PROPERTY_LOOKUP: {
            auto* prop_lookup = static_cast<const PropertyLookupExpr*>(expr);
            auto it = row.bindings.find(prop_lookup->variable);
            if (it != row.bindings.end()) {
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
            auto val = evaluate_expression(row, un->expr.get());
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
                auto lhs = evaluate_expression(row, bin->left.get());
                if (!lhs.is_truthy()) return GqlValue(false);
                auto rhs = evaluate_expression(row, bin->right.get());
                return GqlValue(rhs.is_truthy());
            }
            if (bin->op == BinaryOpKind::OR) {
                auto lhs = evaluate_expression(row, bin->left.get());
                if (lhs.is_truthy()) return GqlValue(true);
                auto rhs = evaluate_expression(row, bin->right.get());
                return GqlValue(rhs.is_truthy());
            }

            auto lhs = evaluate_expression(row, bin->left.get());
            auto rhs = evaluate_expression(row, bin->right.get());

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

std::string serialize_gql_value(const GqlValue& val) {
    if (val.type == GqlValue::NIL) {
        return "null";
    }
    if (val.type == GqlValue::PROPERTY) {
        switch (val.property.index()) {
            case 0: return "null";
            case 1: return std::get<bool>(val.property) ? "true" : "false";
            case 2: return std::to_string(std::get<int64_t>(val.property));
            case 3: return std::to_string(std::get<double>(val.property));
            case 4: return seastar::json::formatter::to_json(std::get<std::string>(val.property));
            case 5: {
                std::string s = "[";
                bool init = true;
                for (bool x : std::get<std::vector<bool>>(val.property)) {
                    if (!init) s += ",";
                    s += (x ? "true" : "false");
                    init = false;
                }
                s += "]";
                return s;
            }
            case 6: {
                std::string s = "[";
                bool init = true;
                for (int64_t x : std::get<std::vector<int64_t>>(val.property)) {
                    if (!init) s += ",";
                    s += std::to_string(x);
                    init = false;
                }
                s += "]";
                return s;
            }
            case 7: {
                std::string s = "[";
                bool init = true;
                for (double x : std::get<std::vector<double>>(val.property)) {
                    if (!init) s += ",";
                    s += std::to_string(x);
                    init = false;
                }
                s += "]";
                return s;
            }
            case 8: {
                std::string s = "[";
                bool init = true;
                for (const auto& x : std::get<std::vector<std::string>>(val.property)) {
                    if (!init) s += ",";
                    s += seastar::json::formatter::to_json(x);
                    init = false;
                }
                s += "]";
                return s;
            }
        }
    }
    if (val.type == GqlValue::NODE) {
        std::string s = "{\"id\": " + std::to_string(val.node->getId()) + ", \"type\": \"" + val.node->getType() + "\", \"key\": \"" + val.node->getKey() + "\", \"properties\": {";
        bool init = true;
        for (const auto& [k, v] : val.node->getProperties()) {
            if (!init) s += ", ";
            s += "\"" + k + "\": " + serialize_gql_value(GqlValue(v));
            init = false;
        }
        s += "}}";
        return s;
    }
    if (val.type == GqlValue::RELATIONSHIP) {
        std::string s = "{\"id\": " + std::to_string(val.relationship->getId()) + ", \"type\": \"" + val.relationship->getType() + "\", \"from\": " + std::to_string(val.relationship->getStartingNodeId()) + ", \"to\": " + std::to_string(val.relationship->getEndingNodeId()) + ", \"properties\": {";
        bool init = true;
        for (const auto& [k, v] : val.relationship->getProperties()) {
            if (!init) s += ", ";
            s += "\"" + k + "\": " + serialize_gql_value(GqlValue(v));
            init = false;
        }
        s += "}}";
        return s;
    }
    return "null";
}

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

struct RowSortKey {
    std::vector<GqlValue> keys;
    GqlRow row;
};

std::string serialize_properties_to_json(const std::map<std::string, property_type_t>& props) {
    std::string json = "{";
    bool first = true;
    for (const auto& [k, v] : props) {
        if (!first) json += ", ";
        json += "\"" + k + "\": " + serialize_gql_value(GqlValue(v));
        first = false;
    }
    json += "}";
    return json;
}

seastar::future<GqlRow> execute_writes_for_row(ragedb::Graph& graph, std::shared_ptr<GqlQuery> query_ptr, size_t write_idx, GqlRow row) {
    if (write_idx >= query_ptr->writes.size()) {
        return seastar::make_ready_future<GqlRow>(std::move(row));
    }
    const auto& op = query_ptr->writes[write_idx];

    if (op.type == WriteOp::Type::SET) {
        auto val = evaluate_expression(row, op.set_expr.get());
        auto it = row.bindings.find(op.set_var);
        if (it != row.bindings.end()) {
            if (it->second.type == GqlValue::NODE) {
                uint64_t id = it->second.node->getId();
                return graph.shard.local().NodeSetPropertyPeered(id, op.set_prop, val.property)
                .then([id, set_var = op.set_var, query_ptr, write_idx, row = std::move(row), &graph](bool success) mutable {
                    if (success) {
                        return graph.shard.local().NodeGetPeered(id)
                        .then([row = std::move(row), set_var](Node new_node) mutable {
                            row.bindings[set_var] = GqlValue(new_node);
                            return row;
                        });
                    }
                    return seastar::make_ready_future<GqlRow>(std::move(row));
                }).then([&graph, query_ptr, write_idx](GqlRow r) {
                    return execute_writes_for_row(graph, query_ptr, write_idx + 1, std::move(r));
                });
            } else if (it->second.type == GqlValue::RELATIONSHIP) {
                uint64_t id = it->second.relationship->getId();
                return graph.shard.local().RelationshipSetPropertyPeered(id, op.set_prop, val.property)
                .then([id, set_var = op.set_var, query_ptr, write_idx, row = std::move(row), &graph](bool success) mutable {
                    if (success) {
                        return graph.shard.local().RelationshipGetPeered(id)
                        .then([row = std::move(row), set_var](Relationship new_rel) mutable {
                            row.bindings[set_var] = GqlValue(new_rel);
                            return row;
                        });
                    }
                    return seastar::make_ready_future<GqlRow>(std::move(row));
                }).then([&graph, query_ptr, write_idx](GqlRow r) {
                    return execute_writes_for_row(graph, query_ptr, write_idx + 1, std::move(r));
                });
            }
        }
        return execute_writes_for_row(graph, query_ptr, write_idx + 1, std::move(row));
    }

    if (op.type == WriteOp::Type::REMOVE) {
        auto it = row.bindings.find(op.remove_var);
        if (it != row.bindings.end()) {
            if (it->second.type == GqlValue::NODE) {
                uint64_t id = it->second.node->getId();
                return graph.shard.local().NodeDeletePropertyPeered(id, op.remove_prop)
                .then([id, remove_var = op.remove_var, query_ptr, write_idx, row = std::move(row), &graph](bool success) mutable {
                    if (success) {
                        return graph.shard.local().NodeGetPeered(id)
                        .then([row = std::move(row), remove_var](Node new_node) mutable {
                            row.bindings[remove_var] = GqlValue(new_node);
                            return row;
                        });
                    }
                    return seastar::make_ready_future<GqlRow>(std::move(row));
                }).then([&graph, query_ptr, write_idx](GqlRow r) {
                    return execute_writes_for_row(graph, query_ptr, write_idx + 1, std::move(r));
                });
            } else if (it->second.type == GqlValue::RELATIONSHIP) {
                uint64_t id = it->second.relationship->getId();
                return graph.shard.local().RelationshipDeletePropertyPeered(id, op.remove_prop)
                .then([id, remove_var = op.remove_var, query_ptr, write_idx, row = std::move(row), &graph](bool success) mutable {
                    if (success) {
                        return graph.shard.local().RelationshipGetPeered(id)
                        .then([row = std::move(row), remove_var](Relationship new_rel) mutable {
                            row.bindings[remove_var] = GqlValue(new_rel);
                            return row;
                        });
                    }
                    return seastar::make_ready_future<GqlRow>(std::move(row));
                }).then([&graph, query_ptr, write_idx](GqlRow r) {
                    return execute_writes_for_row(graph, query_ptr, write_idx + 1, std::move(r));
                });
            }
        }
        return execute_writes_for_row(graph, query_ptr, write_idx + 1, std::move(row));
    }

    if (op.type == WriteOp::Type::DELETE_OP) {
        auto it = row.bindings.find(op.delete_var);
        if (it != row.bindings.end()) {
            if (it->second.type == GqlValue::NODE) {
                uint64_t id = it->second.node->getId();
                return graph.shard.local().NodeRemovePeered(id)
                .then([delete_var = op.delete_var, query_ptr, write_idx, row = std::move(row)](bool success) mutable {
                    if (success) {
                        row.bindings[delete_var] = GqlValue();
                    }
                    return row;
                }).then([&graph, query_ptr, write_idx](GqlRow r) {
                    return execute_writes_for_row(graph, query_ptr, write_idx + 1, std::move(r));
                });
            } else if (it->second.type == GqlValue::RELATIONSHIP) {
                uint64_t id = it->second.relationship->getId();
                return graph.shard.local().RelationshipRemovePeered(id)
                .then([delete_var = op.delete_var, query_ptr, write_idx, row = std::move(row)](bool success) mutable {
                    if (success) {
                        row.bindings[delete_var] = GqlValue();
                    }
                    return row;
                }).then([&graph, query_ptr, write_idx](GqlRow r) {
                    return execute_writes_for_row(graph, query_ptr, write_idx + 1, std::move(r));
                });
            }
        }
        return execute_writes_for_row(graph, query_ptr, write_idx + 1, std::move(row));
    }

    if (op.type == WriteOp::Type::INSERT) {
        struct InsertState {
            PathPattern pattern;
            GqlRow row;
            std::vector<uint64_t> node_ids;
        };
        auto state = std::make_shared<InsertState>();
        state->pattern = op.insert_pattern;
        state->row = std::move(row);
        state->node_ids.resize(op.insert_pattern.nodes.size(), 0);

        auto insert_node_func = std::make_shared<std::function<seastar::future<>(size_t)>>();
        *insert_node_func = [state, &graph, insert_node_func](size_t i) -> seastar::future<> {
            if (i >= state->pattern.nodes.size()) {
                return seastar::make_ready_future<>();
            }
            const auto& node = state->pattern.nodes[i];
            if (!node.variable.empty()) {
                auto it = state->row.bindings.find(node.variable);
                if (it != state->row.bindings.end() && it->second.type == GqlValue::NODE) {
                    state->node_ids[i] = it->second.node->getId();
                    return (*insert_node_func)(i + 1);
                }
            }

            std::string key;
            auto& node_props = state->pattern.nodes[i].properties;
            auto key_it = node_props.find("key");
            if (key_it != node_props.end() && std::holds_alternative<std::string>(key_it->second)) {
                key = std::get<std::string>(key_it->second);
            } else {
                key = std::to_string(std::chrono::high_resolution_clock::now().time_since_epoch().count()) + "_" + std::to_string(rand());
            }

            return graph.shard.local().NodeAddPeered(node.label, key, serialize_properties_to_json(node.properties))
            .then([state, i, &graph, node_var = node.variable](uint64_t new_id) {
                state->node_ids[i] = new_id;
                return graph.shard.local().NodeGetPeered(new_id)
                .then([state, node_var](Node new_node) {
                    if (!node_var.empty()) {
                        state->row.bindings[node_var] = GqlValue(new_node);
                    }
                });
            }).then([i, insert_node_func]() {
                return (*insert_node_func)(i + 1);
            });
        };

        auto insert_edge_func = std::make_shared<std::function<seastar::future<>(size_t)>>();
        *insert_edge_func = [state, &graph, insert_edge_func](size_t i) -> seastar::future<> {
            if (i >= state->pattern.edges.size()) {
                return seastar::make_ready_future<>();
            }
            const auto& edge = state->pattern.edges[i];
            uint64_t start = state->node_ids[i];
            uint64_t end = state->node_ids[i + 1];

            return graph.shard.local().RelationshipAddPeered(edge.label, start, end, serialize_properties_to_json(edge.properties))
            .then([state, &graph, i, edge_var = edge.variable](uint64_t new_rel_id) {
                return graph.shard.local().RelationshipGetPeered(new_rel_id)
                .then([state, edge_var](Relationship new_rel) {
                    if (!edge_var.empty()) {
                        state->row.bindings[edge_var] = GqlValue(new_rel);
                    }
                });
            }).then([i, insert_edge_func]() {
                return (*insert_edge_func)(i + 1);
            });
        };

        return (*insert_node_func)(0)
        .then([insert_edge_func]() {
            return (*insert_edge_func)(0);
        }).then([state, query_ptr, write_idx, &graph, insert_node_func, insert_edge_func]() mutable {
            return execute_writes_for_row(graph, query_ptr, write_idx + 1, std::move(state->row));
        });
    }

    return execute_writes_for_row(graph, query_ptr, write_idx + 1, std::move(row));
}

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
