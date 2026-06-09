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

#include "GqlValue.h"
#include <seastar/json/json_elements.hh>
#include <algorithm>
#include <sstream>

namespace ragedb::gql {

/**
 * @brief Compare two property_type_t variants.
 * 
 * Supports comparison between identical variant types (int64_t, double, std::string, bool)
 * and cross-type numerical comparisons between int64_t and double.
 * 
 * @param lhs Left property variant.
 * @param rhs Right property variant.
 * @return int -1 if lhs < rhs, 1 if lhs > rhs, 0 if equal or incompatible.
 */
int compare_properties(const property_type_t& lhs, const property_type_t& rhs) {
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
        // Handle cross-type numerical comparison
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

/**
 * @brief Compare two GqlValues.
 * 
 * Compares by GqlValue::Type first. For properties, compares using compare_properties.
 * For nodes and relationships, compares using their underlying graph ID.
 * 
 * @param a First GqlValue.
 * @param b Second GqlValue.
 * @return int -1 if a < b, 1 if a > b, 0 if equal.
 */
int compare_gql_values(const GqlValue& a, const GqlValue& b) {
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

/**
 * @brief Checks if a map of target properties matches a GQL pattern's properties.
 * 
 * Matches successfully if target contains all pattern keys, with equal values.
 * 
 * @param target The map of actual node/edge properties.
 * @param pattern The map of properties specified in the GQL query pattern.
 * @return true If the target matches the pattern.
 * @return false Otherwise.
 */
bool matches_properties(const std::map<std::string, property_type_t>& target, const std::map<std::string, property_type_t>& pattern) {
    for (const auto& [key, val] : pattern) {
        auto it = target.find(key);
        if (it == target.end()) return false;
        if (compare_properties(it->second, val) != 0) return false;
    }
    return true;
}

/**
 * @brief Evaluates an AST expression node against a query row's bindings.
 * 
 * Evaluates variables, literals, properties, unary operations (NOT, NEG),
 * and binary operations (AND, OR, logical comparisons, arithmetic additions/subtractions).
 * 
 * @param row The row representing the query context and bindings.
 * @param expr The AST expression node to evaluate.
 * @return GqlValue The result of the evaluation.
 */
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

/**
 * @brief Serializes a GqlValue (Nil, Node, Relationship, or Property variant) to a JSON string.
 * 
 * Maps typed property types (booleans, integers, doubles, strings, array vectors) to
 * compliant JSON notations.
 * 
 * @param val The GqlValue to serialize.
 * @return std::string JSON representation.
 */
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

/**
 * @brief Serializes a property map into a JSON object string.
 * 
 * @param props The property map to format.
 * @return std::string JSON object string.
 */
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

} // namespace ragedb::gql
