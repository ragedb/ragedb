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

#include "GqlTypechecker.h"
#include <stdexcept>
#include <algorithm>

namespace ragedb::gql {

std::string to_string(GqlType type) {
    switch (type) {
        case GqlType::ANY: return "ANY";
        case GqlType::VOID: return "VOID";
        case GqlType::BOOLEAN: return "BOOLEAN";
        case GqlType::INTEGER: return "INTEGER";
        case GqlType::DOUBLE: return "DOUBLE";
        case GqlType::STRING: return "STRING";
        case GqlType::BOOLEAN_LIST: return "BOOLEAN_LIST";
        case GqlType::INTEGER_LIST: return "INTEGER_LIST";
        case GqlType::DOUBLE_LIST: return "DOUBLE_LIST";
        case GqlType::STRING_LIST: return "STRING_LIST";
        case GqlType::NODE: return "NODE";
        case GqlType::RELATIONSHIP: return "RELATIONSHIP";
        case GqlType::PATH: return "PATH";
        case GqlType::RELATIONSHIP_LIST: return "RELATIONSHIP_LIST";
    }
    return "UNKNOWN";
}

GqlTypechecker::GqlTypechecker(ragedb::Graph& g) : graph(g) {
    all_node_labels = graph.shard.local().NodeTypesGetPeered();
    all_relationship_labels = graph.shard.local().RelationshipTypesGetPeered();
}

std::set<std::string> GqlTypechecker::evaluate_label_expr(const std::shared_ptr<LabelExpression>& label_expr, bool is_node) {
    const auto& universe = is_node ? all_node_labels : all_relationship_labels;
    if (!label_expr) {
        return universe;
    }

    switch (label_expr->kind) {
        case LabelExprKind::LITERAL: {
            std::string name = label_expr->name;
            if (universe.find(name) == universe.end()) {
                throw std::runtime_error((is_node ? "NodeType '" : "RelationshipType '") + name + "' does not exist in schema");
            }
            return {name};
        }
        case LabelExprKind::NOT: {
            std::set<std::string> inner = evaluate_label_expr(label_expr->expr, is_node);
            std::set<std::string> result;
            std::set_difference(universe.begin(), universe.end(), inner.begin(), inner.end(),
                                std::inserter(result, result.begin()));
            return result;
        }
        case LabelExprKind::AND: {
            std::set<std::string> left_set = evaluate_label_expr(label_expr->left, is_node);
            std::set<std::string> right_set = evaluate_label_expr(label_expr->right, is_node);
            std::set<std::string> result;
            std::set_intersection(left_set.begin(), left_set.end(), right_set.begin(), right_set.end(),
                                  std::inserter(result, result.begin()));
            if (result.empty()) {
                throw std::runtime_error("Label expression reduces to empty set (disjoint conjunction)");
            }
            return result;
        }
        case LabelExprKind::OR: {
            std::set<std::string> left_set = evaluate_label_expr(label_expr->left, is_node);
            std::set<std::string> right_set = evaluate_label_expr(label_expr->right, is_node);
            std::set<std::string> result;
            std::set_union(left_set.begin(), left_set.end(), right_set.begin(), right_set.end(),
                           std::inserter(result, result.begin()));
            return result;
        }
    }
    return {};
}

void GqlTypechecker::meet_variable(const std::string& name, GqlType type, const std::set<std::string>& labels) {
    auto it = env.find(name);
    if (it != env.end()) {
        if (it->second.type != type && it->second.type != GqlType::ANY && type != GqlType::ANY) {
            throw std::runtime_error("Variable '" + name + "' has conflicting types: " + to_string(it->second.type) + " vs " + to_string(type));
        }
        if (type != GqlType::ANY) {
            it->second.type = type;
        }
        std::set<std::string> intersection;
        std::set_intersection(it->second.labels.begin(), it->second.labels.end(), labels.begin(), labels.end(),
                              std::inserter(intersection, intersection.begin()));
        if (intersection.empty()) {
            throw std::runtime_error("Variable '" + name + "' has no possible matching types due to disjoint label constraints");
        }
        it->second.labels = std::move(intersection);
    } else {
        if (labels.empty()) {
            throw std::runtime_error("Variable '" + name + "' matches no valid types in the database schema");
        }
        env[name] = {type, labels};
    }
}

static GqlType type_from_variant(const property_type_t& val) {
    if (std::holds_alternative<std::monostate>(val)) return GqlType::VOID;
    if (std::holds_alternative<bool>(val)) return GqlType::BOOLEAN;
    if (std::holds_alternative<int64_t>(val)) return GqlType::INTEGER;
    if (std::holds_alternative<double>(val)) return GqlType::DOUBLE;
    if (std::holds_alternative<std::string>(val)) return GqlType::STRING;
    if (std::holds_alternative<std::vector<bool>>(val)) return GqlType::BOOLEAN_LIST;
    if (std::holds_alternative<std::vector<int64_t>>(val)) return GqlType::INTEGER_LIST;
    if (std::holds_alternative<std::vector<double>>(val)) return GqlType::DOUBLE_LIST;
    if (std::holds_alternative<std::vector<std::string>>(val)) return GqlType::STRING_LIST;
    return GqlType::ANY;
}

static bool is_numeric(GqlType t) {
    return t == GqlType::INTEGER || t == GqlType::DOUBLE || t == GqlType::ANY;
}

static bool is_list(GqlType t) {
    return t == GqlType::BOOLEAN_LIST || t == GqlType::INTEGER_LIST || t == GqlType::DOUBLE_LIST || t == GqlType::STRING_LIST || t == GqlType::RELATIONSHIP_LIST;
}

static GqlType parse_type_string(const std::string& type_str) {
    if (type_str == "boolean") return GqlType::BOOLEAN;
    if (type_str == "integer") return GqlType::INTEGER;
    if (type_str == "double") return GqlType::DOUBLE;
    if (type_str == "string") return GqlType::STRING;
    if (type_str == "boolean_list") return GqlType::BOOLEAN_LIST;
    if (type_str == "integer_list") return GqlType::INTEGER_LIST;
    if (type_str == "double_list") return GqlType::DOUBLE_LIST;
    if (type_str == "string_list") return GqlType::STRING_LIST;
    return GqlType::ANY;
}

GqlType GqlTypechecker::get_property_type(const std::string& var_name, const std::string& prop_name) {
    auto it = env.find(var_name);
    if (it == env.end()) {
        throw std::runtime_error("Variable '" + var_name + "' is not bound");
    }

    if (prop_name == "key" && (it->second.type == GqlType::NODE || it->second.type == GqlType::ANY)) {
        return GqlType::STRING;
    }

    if (it->second.type != GqlType::NODE && it->second.type != GqlType::RELATIONSHIP && it->second.type != GqlType::ANY) {
        throw std::runtime_error("Cannot lookup property '" + prop_name + "' on non-graph variable '" + var_name + "' of type " + to_string(it->second.type));
    }

    bool is_node = (it->second.type == GqlType::NODE);
    std::set<GqlType> types;
    for (const auto& label : it->second.labels) {
        std::string type_str = is_node ? 
            graph.shard.local().NodePropertyTypeGet(label, prop_name) :
            graph.shard.local().RelationshipPropertyTypeGet(label, prop_name);
        if (!type_str.empty()) {
            types.insert(parse_type_string(type_str));
        }
    }

    if (types.empty()) {
        throw std::runtime_error("Property '" + prop_name + "' does not exist on any of the matched types for variable '" + var_name + "'");
    }

    if (types.size() > 1) {
        // Conflicting types on different matching schema labels; return ANY to allow gradual checking
        return GqlType::ANY;
    }

    return *types.begin();
}

static void check_property_compatibility(GqlType prop_type, GqlType val_type, const std::string& prop_name) {
    if (prop_type == GqlType::ANY || val_type == GqlType::ANY || val_type == GqlType::VOID) {
        return;
    }
    if (prop_type != val_type) {
        // Allow numeric coercion (assigning an integer to a double property)
        if (prop_type == GqlType::DOUBLE && val_type == GqlType::INTEGER) {
            return;
        }
        throw std::runtime_error("Type mismatch for property '" + prop_name + "': expected " + to_string(prop_type) + ", got " + to_string(val_type));
    }
}

void GqlTypechecker::check_path_pattern(const PathPattern& path_pattern) {
    for (const auto& node : path_pattern.nodes) {
        std::set<std::string> node_labels = evaluate_label_expr(node.label_expr, true);
        if (!node.variable.empty()) {
            meet_variable(node.variable, GqlType::NODE, node_labels);
            for (const auto& prop : node.properties) {
                GqlType prop_type = get_property_type(node.variable, prop.first);
                GqlType val_type = type_from_variant(prop.second);
                check_property_compatibility(prop_type, val_type, prop.first);
            }
        } else {
            // Check literal properties against node labels without a variable binding
            for (const auto& prop : node.properties) {
                if (prop.first == "key") {
                    GqlType val_type = type_from_variant(prop.second);
                    if (val_type != GqlType::STRING && val_type != GqlType::ANY && val_type != GqlType::VOID) {
                        throw std::runtime_error("Type mismatch for built-in property 'key': expected STRING, got " + to_string(val_type));
                    }
                    continue;
                }
                std::set<GqlType> types;
                for (const auto& label : node_labels) {
                    std::string type_str = graph.shard.local().NodePropertyTypeGet(label, prop.first);
                    if (!type_str.empty()) {
                        types.insert(parse_type_string(type_str));
                    }
                }
                if (types.empty()) {
                    throw std::runtime_error("Property '" + prop.first + "' does not exist on any of the matched node labels");
                }
                GqlType val_type = type_from_variant(prop.second);
                bool compatible = false;
                for (GqlType t : types) {
                    if (t == GqlType::ANY || val_type == GqlType::ANY || t == val_type || (t == GqlType::DOUBLE && val_type == GqlType::INTEGER)) {
                        compatible = true;
                        break;
                    }
                }
                if (!compatible) {
                    throw std::runtime_error("Type mismatch for property '" + prop.first + "'");
                }
            }
        }
    }

    for (const auto& edge : path_pattern.edges) {
        std::set<std::string> edge_labels = evaluate_label_expr(edge.label_expr, false);
        GqlType edge_type = edge.is_variable_length ? GqlType::RELATIONSHIP_LIST : GqlType::RELATIONSHIP;
        if (!edge.variable.empty()) {
            meet_variable(edge.variable, edge_type, edge_labels);
            if (!edge.is_variable_length) {
                for (const auto& prop : edge.properties) {
                    GqlType prop_type = get_property_type(edge.variable, prop.first);
                    GqlType val_type = type_from_variant(prop.second);
                    check_property_compatibility(prop_type, val_type, prop.first);
                }
            } else if (!edge.properties.empty()) {
                throw std::runtime_error("Property filters are not supported on variable-length edge bindings");
            }
        } else {
            // Check literal properties against edge labels without a variable binding
            for (const auto& prop : edge.properties) {
                std::set<GqlType> types;
                for (const auto& label : edge_labels) {
                    std::string type_str = graph.shard.local().RelationshipPropertyTypeGet(label, prop.first);
                    if (!type_str.empty()) {
                        types.insert(parse_type_string(type_str));
                    }
                }
                if (types.empty()) {
                    throw std::runtime_error("Property '" + prop.first + "' does not exist on any of the matched relationship labels");
                }
                GqlType val_type = type_from_variant(prop.second);
                bool compatible = false;
                for (GqlType t : types) {
                    if (t == GqlType::ANY || val_type == GqlType::ANY || t == val_type || (t == GqlType::DOUBLE && val_type == GqlType::INTEGER)) {
                        compatible = true;
                        break;
                    }
                }
                if (!compatible) {
                    throw std::runtime_error("Type mismatch for property '" + prop.first + "'");
                }
            }
        }
    }
}

GqlType GqlTypechecker::check_expression(const Expression& expr) {
    switch (expr.kind) {
        case ExpressionKind::LITERAL: {
            const auto& lit = static_cast<const LiteralExpr&>(expr);
            return type_from_variant(lit.value);
        }
        case ExpressionKind::VARIABLE: {
            const auto& var = static_cast<const VariableExpr&>(expr);
            auto it = env.find(var.name);
            if (it == env.end()) {
                throw std::runtime_error("Variable '" + var.name + "' is not bound");
            }
            return it->second.type;
        }
        case ExpressionKind::PROPERTY_LOOKUP: {
            const auto& prop = static_cast<const PropertyLookupExpr&>(expr);
            return get_property_type(prop.variable, prop.property);
        }
        case ExpressionKind::UNARY_OP: {
            const auto& un = static_cast<const UnaryOpExpr&>(expr);
            GqlType t = check_expression(*un.expr);
            if (un.op == UnaryOpKind::NOT) {
                if (t != GqlType::BOOLEAN && t != GqlType::ANY) {
                    throw std::runtime_error("Logical NOT operand must be BOOLEAN, got " + to_string(t));
                }
                return GqlType::BOOLEAN;
            } else if (un.op == UnaryOpKind::NEG) {
                if (!is_numeric(t)) {
                    throw std::runtime_error("Numeric negation operand must be numeric, got " + to_string(t));
                }
                return t;
            }
            break;
        }
        case ExpressionKind::BINARY_OP: {
            const auto& bin = static_cast<const BinaryOpExpr&>(expr);
            GqlType t1 = check_expression(*bin.left);
            GqlType t2 = check_expression(*bin.right);

            if (bin.op == BinaryOpKind::AND || bin.op == BinaryOpKind::OR) {
                if ((t1 != GqlType::BOOLEAN && t1 != GqlType::ANY) || (t2 != GqlType::BOOLEAN && t2 != GqlType::ANY)) {
                    throw std::runtime_error("Logical binary operands must be BOOLEAN, got " + to_string(t1) + " and " + to_string(t2));
                }
                return GqlType::BOOLEAN;
            }

            if (bin.op == BinaryOpKind::ADD || bin.op == BinaryOpKind::SUB || bin.op == BinaryOpKind::MUL || bin.op == BinaryOpKind::DIV) {
                if (!is_numeric(t1) || !is_numeric(t2)) {
                    throw std::runtime_error("Arithmetic operands must be numeric, got " + to_string(t1) + " and " + to_string(t2));
                }
                if (t1 == GqlType::DOUBLE || t2 == GqlType::DOUBLE) {
                    return GqlType::DOUBLE;
                }
                return GqlType::INTEGER;
            }

            // Comparison operators (=, !=, <, <=, >, >=)
            if (bin.op == BinaryOpKind::EQ || bin.op == BinaryOpKind::NE ||
                bin.op == BinaryOpKind::LT || bin.op == BinaryOpKind::LE ||
                bin.op == BinaryOpKind::GT || bin.op == BinaryOpKind::GE) {
                if (t1 == GqlType::ANY || t2 == GqlType::ANY || t1 == GqlType::VOID || t2 == GqlType::VOID) {
                    return GqlType::BOOLEAN;
                }
                if (is_numeric(t1) && is_numeric(t2)) {
                    return GqlType::BOOLEAN;
                }
                if (is_list(t1) || is_list(t2)) {
                    if (t1 != t2) {
                        throw std::runtime_error("Cannot compare list type " + to_string(t1) + " with " + to_string(t2));
                    }
                    return GqlType::BOOLEAN;
                }
                if (t1 != t2) {
                    throw std::runtime_error("Incompatible types for comparison: " + to_string(t1) + " and " + to_string(t2));
                }
                return GqlType::BOOLEAN;
            }
            break;
        }
        case ExpressionKind::AGGREGATION: {
            const auto& agg = static_cast<const AggregateExpr&>(expr);
            if (agg.fn_kind == AggregateKind::COUNT) {
                if (agg.expr) {
                    check_expression(*agg.expr);
                }
                return GqlType::INTEGER;
            }
            if (!agg.expr) {
                throw std::runtime_error("Aggregation function expects an argument");
            }
            GqlType t = check_expression(*agg.expr);
            if (agg.fn_kind == AggregateKind::SUM || agg.fn_kind == AggregateKind::AVG) {
                if (!is_numeric(t)) {
                    throw std::runtime_error("Aggregation input must be numeric, got " + to_string(t));
                }
                return agg.fn_kind == AggregateKind::AVG ? GqlType::DOUBLE : t;
            }
            if (agg.fn_kind == AggregateKind::MIN || agg.fn_kind == AggregateKind::MAX) {
                if (t == GqlType::NODE || t == GqlType::RELATIONSHIP || t == GqlType::PATH || is_list(t)) {
                    throw std::runtime_error("Cannot aggregate non-scalar type " + to_string(t));
                }
                return t;
            }
            break;
        }
    }
    return GqlType::ANY;
}

void GqlTypechecker::check_write_op(const WriteOp& write_op) {
    switch (write_op.type) {
        case WriteOp::Type::INSERT: {
            check_path_pattern(write_op.insert_pattern);
            break;
        }
        case WriteOp::Type::SET: {
            auto it = env.find(write_op.set_var);
            if (it == env.end()) {
                throw std::runtime_error("Variable '" + write_op.set_var + "' is not bound");
            }
            GqlType prop_type = get_property_type(write_op.set_var, write_op.set_prop);
            GqlType val_type = check_expression(*write_op.set_expr);
            check_property_compatibility(prop_type, val_type, write_op.set_prop);
            break;
        }
        case WriteOp::Type::REMOVE: {
            auto it = env.find(write_op.remove_var);
            if (it == env.end()) {
                throw std::runtime_error("Variable '" + write_op.remove_var + "' is not bound");
            }
            // Verify that the property exists
            get_property_type(write_op.remove_var, write_op.remove_prop);
            break;
        }
        case WriteOp::Type::DELETE_OP: {
            auto it = env.find(write_op.delete_var);
            if (it == env.end()) {
                throw std::runtime_error("Variable '" + write_op.delete_var + "' is not bound");
            }
            if (it->second.type != GqlType::NODE && it->second.type != GqlType::RELATIONSHIP) {
                throw std::runtime_error("DELETE expects a NODE or RELATIONSHIP variable, got " + to_string(it->second.type));
            }
            break;
        }
    }
}

void GqlTypechecker::check_return_item(const ReturnItem& return_item) {
    if (return_item.expr) {
        check_expression(*return_item.expr);
    }
}

void GqlTypechecker::check_query(const GqlQuery& query) {
    if (query.kind != QueryKind::SINGLE) {
        if (query.left) {
            check_query(*query.left);
        }
        if (query.right) {
            check_query(*query.right);
        }
        return;
    }

    if (query.schema_op) {
        // DDL bypasses standard pattern typing
        return;
    }

    // Process all MATCH patterns first to populate variables in env
    for (const auto& match : query.matches) {
        check_path_pattern(match.pattern);
    }

    // Process WHERE condition
    if (query.where_expr) {
        GqlType t = check_expression(*query.where_expr);
        if (t != GqlType::BOOLEAN && t != GqlType::ANY) {
            throw std::runtime_error("WHERE filter expression must evaluate to BOOLEAN, got " + to_string(t));
        }
    }

    // Process WRITE operations
    for (const auto& write : query.writes) {
        check_write_op(write);
    }

    // Process RETURN items
    for (const auto& ret : query.returns) {
        check_return_item(ret);
    }

    // Process ORDER BY
    for (const auto& sort : query.order_by) {
        check_expression(*sort.expr);
    }
}

void GqlTypechecker::typecheck(ragedb::Graph& graph, const GqlQuery& query) {
    GqlTypechecker tc(graph);
    tc.check_query(query);
}

} // namespace ragedb::gql
