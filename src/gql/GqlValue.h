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

#ifndef RAGEDB_GQLVALUE_H
#define RAGEDB_GQLVALUE_H

#include <optional>
#include <map>
#include <string>
#include <variant>
#include <vector>
#include "../graph/Graph.h"
#include "../graph/paths/Path.h"
#include "GqlAst.h"

namespace ragedb::gql {

/**
 * @brief Represents a value computed during GQL query execution.
 * 
 * Can wrap null/nil, a graph node, a relationship, or a property type variant.
 */
struct GqlValue {
    enum Type { NIL, NODE, RELATIONSHIP, RELATIONSHIP_LIST, PROPERTY, PATH } type = NIL; ///< Type tag for the held value.
    std::optional<Node> node;                       ///< Wrapped Node value.
    std::optional<Relationship> relationship;       ///< Wrapped Relationship value.
    std::optional<std::vector<Relationship>> relationship_list; ///< Wrapped list of Relationships.
    std::optional<Path> path;                       ///< Wrapped Path value.
    property_type_t property = std::monostate{};    ///< Wrapped property value (primitive variants).

    GqlValue() : type(NIL) {}
    explicit GqlValue(Node n) : type(NODE), node(std::move(n)) {}
    explicit GqlValue(Relationship r) : type(RELATIONSHIP), relationship(std::move(r)) {}
    explicit GqlValue(std::vector<Relationship> rl) : type(RELATIONSHIP_LIST), relationship_list(std::move(rl)) {}
    explicit GqlValue(property_type_t p) : type(PROPERTY), property(std::move(p)) {}
    explicit GqlValue(Path pt) : type(PATH), path(std::move(pt)) {} ///< Construct a GqlValue wrapping a Path.

    /**
     * @brief Checks whether the GqlValue evaluates to a truthy value (used in WHERE filters, AND/OR logic).
     * 
     * Null/Nil is falsy. A boolean property is truthy if true. All other types are truthy.
     */
    [[nodiscard]] bool is_truthy() const {
        if (type == PROPERTY) {
            if (std::holds_alternative<bool>(property)) {
                return std::get<bool>(property);
            }
        }
        return type != NIL;
    }
};

/**
 * @brief Represents a row of query execution context, mapping variable names to their bound GqlValues.
 */
struct GqlRow {
    std::map<std::string, GqlValue> bindings;
};

/**
 * @brief Compares two properties.
 * 
 * Supporting integers, doubles, strings, booleans, and cross-type numerical comparisons.
 * Returns -1 if lhs < rhs, 1 if lhs > rhs, or 0 if equal/incomparable.
 */
int compare_properties(const property_type_t& lhs, const property_type_t& rhs);

/**
 * @brief Compares two GqlValues for ordering and equality checks.
 * 
 * Compares types first, then properties by value, and nodes/relationships by their graph ID.
 */
int compare_gql_values(const GqlValue& a, const GqlValue& b);

/**
 * @brief Checks if a map of properties matches a specified GQL pattern's properties.
 * 
 * For a match, the target properties must contain all keys in the pattern, with equal values.
 */
bool matches_properties(const std::map<std::string, property_type_t>& target, const std::map<std::string, property_type_t>& pattern);

/**
 * @brief Checks if a map of properties satisfies a list of pushed down property filters.
 */
bool matches_filters(const std::map<std::string, property_type_t>& target, const std::vector<PropertyFilter>& filters);

/**
 * @brief Evaluates an AST expression tree against a GqlRow context.
 * 
 * Resolves literals, variable lookups, property extractions, unary/binary operators, and math operations.
 */
GqlValue evaluate_expression(const GqlRow& row, const Expression* expr);

/**
 * @brief Serializes a GqlValue to its JSON string representation.
 */
std::string serialize_gql_value(const GqlValue& val);

/**
 * @brief Helper function to serialize a property map into a JSON object string.
 */
std::string serialize_properties_to_json(const std::map<std::string, property_type_t>& props);

} // namespace ragedb::gql

#endif // RAGEDB_GQLVALUE_H
