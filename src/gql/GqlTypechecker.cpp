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
#include <limits>
#include <stdexcept>
#include <algorithm>
#include <optional>

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
        case LabelExprKind::WILDCARD: {
            return universe;
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
        if (type == GqlType::NODE || type == GqlType::RELATIONSHIP) {
            std::set<std::string> intersection;
            std::set_intersection(it->second.labels.begin(), it->second.labels.end(), labels.begin(), labels.end(),
                                  std::inserter(intersection, intersection.begin()));
            if (intersection.empty()) {
                if (it->second.labels.empty() && labels.empty()) {
                    // Compatible empty sets (e.g. from negated wildcard !%)
                } else {
                    throw std::runtime_error("Variable '" + name + "' has no possible matching types due to disjoint label constraints");
                }
            }
            it->second.labels = std::move(intersection);
        }
    } else {
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

    // Bypass typechecking for synthetic degree properties injected by the optimizer at execution time.
    // Both prefixes are used: `_degree_p_opt` from the count->degree-sum rewrite, and `_deg_v_REL_DIR`
    // from the size()/COUNT{} degree rewrite (DegreeConstraintPruner). They hold integer degree values
    // and are populated during the anchor scan, so the schema has no declaration for them.
    if (prop_name.rfind("_deg", 0) == 0) {
        return GqlType::INTEGER;
    }

    if (it->second.type != GqlType::NODE && it->second.type != GqlType::RELATIONSHIP && it->second.type != GqlType::ANY) {
        throw std::runtime_error("Cannot lookup property '" + prop_name + "' on non-graph variable '" + var_name + "' of type " + to_string(it->second.type));
    }

    bool is_node = (it->second.type == GqlType::NODE);
    if (it->second.labels.empty()) {
        return GqlType::ANY;
    }
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
        if (node.where_expr) {
            GqlType t = check_expression(*node.where_expr);
            if (t != GqlType::BOOLEAN && t != GqlType::ANY) {
                throw std::runtime_error("Inline node pattern WHERE expression must evaluate to BOOLEAN, got " + to_string(t));
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
        if (edge.where_expr) {
            GqlType t = check_expression(*edge.where_expr);
            if (t != GqlType::BOOLEAN && t != GqlType::ANY) {
                throw std::runtime_error("Inline relationship pattern WHERE expression must evaluate to BOOLEAN, got " + to_string(t));
            }
        }
        if (edge.cost_expr) {
            // In a COST expression the edge variable denotes a SINGLE relationship (the cost is applied
            // per edge), even for a variable-length edge where the variable is otherwise a
            // RELATIONSHIP_LIST. Rebind it to RELATIONSHIP for the check, then restore.
            const bool rebind = edge.is_variable_length && !edge.variable.empty();
            std::optional<VariableSchema> saved;
            if (rebind) {
                auto it = env.find(edge.variable);
                if (it != env.end()) saved = it->second;
                env[edge.variable] = VariableSchema{GqlType::RELATIONSHIP, edge_labels};
            }
            GqlType t = check_expression(*edge.cost_expr);
            if (rebind) {
                if (saved) env[edge.variable] = *saved;
                else env.erase(edge.variable);
            }
            if (t != GqlType::INTEGER && t != GqlType::DOUBLE && t != GqlType::ANY) {
                throw std::runtime_error("COST expression must evaluate to a numeric type, got " + to_string(t));
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
        case ExpressionKind::IS_NULL_CHECK: {
            const auto& is_null = static_cast<const IsNullExpr&>(expr);
            check_expression(*is_null.expr);
            return GqlType::BOOLEAN;
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

            if (bin.op == BinaryOpKind::CONCAT) {
                if ((t1 != GqlType::STRING && t1 != GqlType::ANY && t1 != GqlType::VOID) ||
                    (t2 != GqlType::STRING && t2 != GqlType::ANY && t2 != GqlType::VOID)) {
                    throw std::runtime_error("Concatenation operands must be STRING, got " + to_string(t1) + " and " + to_string(t2));
                }
                return GqlType::STRING;
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

            if (bin.op == BinaryOpKind::STARTS_WITH || bin.op == BinaryOpKind::ENDS_WITH || bin.op == BinaryOpKind::CONTAINS) {
                if ((t1 != GqlType::STRING && t1 != GqlType::ANY && t1 != GqlType::VOID) ||
                    (t2 != GqlType::STRING && t2 != GqlType::ANY && t2 != GqlType::VOID)) {
                    throw std::runtime_error("String comparison operands must be STRING, got " + to_string(t1) + " and " + to_string(t2));
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
            if (agg.fn_kind == AggregateKind::STDDEV_POP || agg.fn_kind == AggregateKind::STDDEV_SAMP) {
                if (!is_numeric(t)) {
                    throw std::runtime_error("Aggregation input must be numeric, got " + to_string(t));
                }
                return GqlType::DOUBLE;
            }
            if (agg.fn_kind == AggregateKind::PERCENTILE_CONT || agg.fn_kind == AggregateKind::PERCENTILE_DISC) {
                if (!is_numeric(t)) {
                    throw std::runtime_error("Percentile input must be numeric, got " + to_string(t));
                }
                if (!agg.arg2) {
                    throw std::runtime_error("Percentile function expects a fraction argument");
                }
                GqlType ft = check_expression(*agg.arg2);
                if (!is_numeric(ft)) {
                    throw std::runtime_error("Percentile fraction must be numeric, got " + to_string(ft));
                }
                return GqlType::DOUBLE;
            }
            if (agg.fn_kind == AggregateKind::MIN || agg.fn_kind == AggregateKind::MAX) {
                if (t == GqlType::NODE || t == GqlType::RELATIONSHIP || t == GqlType::PATH || is_list(t)) {
                    throw std::runtime_error("Cannot aggregate non-scalar type " + to_string(t));
                }
                return t;
            }
            break;
        }
        case ExpressionKind::SIZE_OP: {
            // COUNT { ... } / size([...]) yields a match count.
            return GqlType::INTEGER;
        }
        case ExpressionKind::EXISTS: {
            const auto& exists = static_cast<const ExistsExpr&>(expr);
            auto parent_env = this->env;
            for (const auto& match : exists.matches) {
                if (match.is_propagate) {
                    meet_variable(match.yield_var, GqlType::NODE, {});
                    if (!match.yield_score_var.empty()) meet_variable(match.yield_score_var, GqlType::DOUBLE, {});
                    if (!match.yield_depth_var.empty()) meet_variable(match.yield_depth_var, GqlType::INTEGER, {});
                } else if (match.is_search) {
                    bool is_node = graph.shard.local().NodeTypesGet().count(match.search_type) > 0;
                    bool is_rel = graph.shard.local().RelationshipTypesGet().count(match.search_type) > 0;
                    if (!is_node && !is_rel) {
                        throw std::runtime_error("Search type '" + match.search_type + "' does not exist in the schema");
                    }
                    meet_variable(match.yield_var, is_node ? GqlType::NODE : GqlType::RELATIONSHIP, {match.search_type});
                    meet_variable(match.yield_score_var, GqlType::DOUBLE, {});
                } else {
                    check_path_pattern(match.pattern);
                    // Register path variable (e.g. p) as GqlType::PATH in typechecker environment
                    if (!match.path_variable.empty()) {
                        meet_variable(match.path_variable, GqlType::PATH, {});
                    }
                }
            }
            if (exists.where_expr) {
                GqlType t = check_expression(*exists.where_expr);
                if (t != GqlType::BOOLEAN && t != GqlType::ANY) {
                    throw std::runtime_error("EXISTS subquery WHERE filter expression must evaluate to BOOLEAN, got " + to_string(t));
                }
            }
            this->env = std::move(parent_env);
            return GqlType::BOOLEAN;
        }
        case ExpressionKind::FUNCTION_CALL: {
            const auto& fc = static_cast<const FunctionCallExpr&>(expr);
            // property_exists(element, propertyName): the second argument is a property-name identifier,
            // not a bound value, so it is not resolved as a variable.
            if (fc.name == "property_exists") {
                if (!fc.args.empty()) check_expression(*fc.args[0]);
                return GqlType::BOOLEAN;
            }
            for (const auto& a : fc.args) {
                check_expression(*a);
            }
            if (fc.name == "length" || fc.name == "zoned_datetime" || fc.name == "datetime" ||
                fc.name == "date" || fc.name == "localdatetime" || fc.name == "element_id") {
                return GqlType::INTEGER;
            }
            if (fc.name == "all_different" || fc.name == "same") {
                return GqlType::BOOLEAN;
            }
            return GqlType::ANY;
        }
        case ExpressionKind::CASE_WHEN: {
            const auto& ce = static_cast<const CaseExpr&>(expr);
            for (const auto& branch : ce.branches) {
                check_expression(*branch.first);
                check_expression(*branch.second);
            }
            if (ce.else_expr) {
                check_expression(*ce.else_expr);
            }
            return GqlType::ANY;
        }
        case ExpressionKind::IN_LIST: {
            const auto& in = static_cast<const InExpr&>(expr);
            check_expression(*in.value);
            check_expression(*in.list);
            return GqlType::BOOLEAN;
        }
        case ExpressionKind::CAST: {
            const auto& c = static_cast<const CastExpr&>(expr);
            check_expression(*c.value);
            switch (c.target) {
                case CastType::STRING:  return GqlType::STRING;
                case CastType::INTEGER: return GqlType::INTEGER;
                case CastType::FLOAT:   return GqlType::DOUBLE;
                case CastType::BOOLEAN: return GqlType::BOOLEAN;
            }
            return GqlType::ANY;
        }
        case ExpressionKind::IS_LABELED: {
            const auto& l = static_cast<const IsLabeledExpr&>(expr);
            check_expression(*l.value);
            return GqlType::BOOLEAN;
        }
        case ExpressionKind::IS_DIRECTED: {
            const auto& d = static_cast<const IsDirectedExpr&>(expr);
            check_expression(*d.value);
            return GqlType::BOOLEAN;
        }
        case ExpressionKind::IS_SOURCE_DEST: {
            const auto& s = static_cast<const IsSourceDestExpr&>(expr);
            check_expression(*s.value);
            check_expression(*s.edge);
            return GqlType::BOOLEAN;
        }
        case ExpressionKind::LIST_LITERAL: {
            const auto& le = static_cast<const ListExpr&>(expr);
            for (const auto& element : le.elements) {
                if (element) check_expression(*element);
            }
            return GqlType::ANY;
        }
        case ExpressionKind::LIST_INDEX: {
            const auto& ie = static_cast<const IndexExpr&>(expr);
            if (ie.list) check_expression(*ie.list);
            if (ie.index) check_expression(*ie.index);
            return GqlType::ANY;
        }
        case ExpressionKind::LIST_COMPREHENSION: {
            const auto& lc = static_cast<const ListComprehensionExpr&>(expr);
            if (lc.list) check_expression(*lc.list);
            // The iteration variable is bound in a child scope; register it (type ANY, like FOR) so the
            // filter/projection can reference it. It is not statically typed -- the list may hold anything.
            if (!lc.variable.empty()) meet_variable(lc.variable, GqlType::ANY, {});
            if (lc.filter) check_expression(*lc.filter);
            if (lc.projection) check_expression(*lc.projection);
            return GqlType::ANY;
        }
        case ExpressionKind::QUANTIFIED_PREDICATE: {
            const auto& qp = static_cast<const QuantifiedPredicateExpr&>(expr);
            if (qp.list) check_expression(*qp.list);
            if (!qp.variable.empty()) meet_variable(qp.variable, GqlType::ANY, {});
            if (qp.predicate) check_expression(*qp.predicate);
            return GqlType::BOOLEAN;
        }
        case ExpressionKind::TEMPORAL_FIELD: {
            const auto& tf = static_cast<const TemporalFieldExpr&>(expr);
            if (tf.value) check_expression(*tf.value);
            return GqlType::INTEGER;
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
            if (write_op.set_prop == "key") {
                throw std::runtime_error("Cannot set the internal 'key' property");
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
            if (write_op.remove_prop == "key") {
                throw std::runtime_error("Cannot remove the internal 'key' property");
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

/**
 * @brief Derive the variable environment a completed WITH segment pipes into the next one: each
 *        projected column becomes a binding under its output name (alias, or the variable's own
 *        name). Called after the segment was checked, so its expressions are known to be valid.
 */
std::map<std::string, VariableSchema> GqlTypechecker::project_returns(const GqlQuery& segment) {
    std::map<std::string, VariableSchema> projected;
    for (const auto& item : segment.returns) {
        if (!item.expr) continue;
        std::string source_var;
        if (item.expr->kind == ExpressionKind::VARIABLE) {
            source_var = static_cast<const VariableExpr*>(item.expr.get())->name;
        }
        std::string name = item.alias ? *item.alias : source_var;
        if (name.empty()) continue;  // non-variable items without alias are rejected at parse time

        VariableSchema schema;
        if (!source_var.empty()) {
            auto it = env.find(source_var);
            if (it != env.end()) schema = it->second;
        } else {
            schema.type = check_expression(*item.expr);
        }
        projected[name] = schema;
    }
    return projected;
}

void GqlTypechecker::check_query(const GqlQuery& query) {
    if (query.clear_cache) {
        return;
    }

    // WITH-pipeline queries are checked segment by segment: each segment sees only the bindings
    // the previous segment projected (plus whatever its own MATCH binds), mirroring execution.
    if (!query.with_segments.empty()) {
        std::map<std::string, VariableSchema> piped;
        for (const auto& seg : query.with_segments) {
            GqlTypechecker seg_tc(graph);
            seg_tc.env = std::move(piped);
            seg_tc.check_segment_body(*seg);
            piped = seg_tc.project_returns(*seg);
        }
        GqlTypechecker final_tc(graph);
        final_tc.env = std::move(piped);
        final_tc.check_segment_body(query);
        return;
    }

    check_segment_body(query);
}

void GqlTypechecker::check_segment_body(const GqlQuery& query) {
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
        const auto& op = *query.schema_op;
        if (op.op == SchemaOperation::Op::CREATE_NODE_TYPE || op.op == SchemaOperation::Op::CREATE_REL_TYPE) {
            for (const auto& prop : op.properties) {
                if (prop.first == "key") {
                    throw std::runtime_error("Cannot define a property named 'key' on schema types as it is an internal property");
                }
            }
        } else if (op.op == SchemaOperation::Op::ALTER_NODE_TYPE || op.op == SchemaOperation::Op::ALTER_REL_TYPE) {
            if (op.alter_op == SchemaOperation::AlterOp::ADD && op.alter_property_name == "key") {
                throw std::runtime_error("Cannot define a property named 'key' on schema types as it is an internal property");
            }
        }
        return;
    }

    // Scoped subquery `CALL (imports) { ... }`: validate the subquery seeing only the imported outer
    // variables, then expose its projected output columns to this segment (as ANY, since a UNION body's
    // branch types are checked in their own scopes). This lets the segment's own RETURN reference them.
    if (query.call_subquery) {
        GqlTypechecker sub_tc(graph);
        for (const auto& v : query.call_import_vars) {
            auto it = env.find(v);
            if (it != env.end()) sub_tc.env[v] = it->second;
        }
        sub_tc.check_query(*query.call_subquery);
        const GqlQuery* branch = query.call_subquery.get();
        while (branch->kind != QueryKind::SINGLE && branch->left) {
            branch = branch->left.get();
        }
        for (const auto& item : branch->returns) {
            std::string name;
            if (item.alias) {
                name = *item.alias;
            } else if (item.expr && item.expr->kind == ExpressionKind::VARIABLE) {
                name = static_cast<const VariableExpr*>(item.expr.get())->name;
            }
            if (!name.empty()) {
                meet_variable(name, GqlType::ANY, {});
            }
        }
    }

    // Process all MATCH patterns first to populate variables in env
    for (const auto& match : query.matches) {
        if (match.is_propagate) {
            // algo.propagate yields a node, a double value and an integer depth.
            meet_variable(match.yield_var, GqlType::NODE, {});
            if (!match.yield_score_var.empty()) meet_variable(match.yield_score_var, GqlType::DOUBLE, {});
            if (!match.yield_depth_var.empty()) meet_variable(match.yield_depth_var, GqlType::INTEGER, {});
        } else if (match.is_search) {
            bool is_node = graph.shard.local().NodeTypesGet().count(match.search_type) > 0;
            bool is_rel = graph.shard.local().RelationshipTypesGet().count(match.search_type) > 0;
            if (!is_node && !is_rel) {
                throw std::runtime_error("Search type '" + match.search_type + "' does not exist in the schema");
            }
            meet_variable(match.yield_var, is_node ? GqlType::NODE : GqlType::RELATIONSHIP, {match.search_type});
            meet_variable(match.yield_score_var, GqlType::DOUBLE, {});
        } else {
            if (match.is_khop) {
                if (match.pattern.nodes.size() != 2 || match.pattern.edges.size() != 1) {
                    throw std::runtime_error("KHOP pattern must contain exactly 2 nodes and 1 edge");
                }
                if (!match.pattern.edges[0].is_variable_length) {
                    throw std::runtime_error("KHOP edge pattern must specify repetition range");
                }
                if (match.pattern.nodes[1].variable.empty()) {
                    throw std::runtime_error("KHOP end node must specify a variable name");
                }
            }
            check_path_pattern(match.pattern);
            // Register path variable (e.g. p) as GqlType::PATH in typechecker environment
            if (!match.path_variable.empty()) {
                meet_variable(match.path_variable, GqlType::PATH, {});
            }
            if (match.algebraic_path_count && !match.path_count_target_var.empty()) {
                meet_variable(match.path_count_target_var, GqlType::INTEGER, {});
            }
            // cost_expr typechecking is now performed inside check_path_pattern per edge.
        }
    }

    // Register ISO GQL FOR variables before LET, matching execution order: FOR expands the working table
    // first, so a LET may reference the element variable. The element type is not known statically (the
    // list may hold anything), so it is ANY.
    for (const auto& binding : query.for_bindings) {
        if (binding.list_expr) {
            check_expression(*binding.list_expr);
        }
        if (!binding.variable.empty()) {
            meet_variable(binding.variable, GqlType::ANY, {});
        }
    }

    // Register ISO GQL LET bindings so the segment's WHERE/FILTER/RETURN can reference them.
    for (const auto& let : query.let_bindings) {
        GqlType t = check_expression(*let.expr);
        if (let.alias) {
            meet_variable(*let.alias, t, {});
        }
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

    // Process GROUP BY: each grouping variable must be bound.
    for (const auto& g : query.group_by) {
        if (g) check_expression(*g);
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
