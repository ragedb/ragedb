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
#include <functional>
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
    enum Type { NIL, NODE, RELATIONSHIP, RELATIONSHIP_LIST, PROPERTY, PATH, LIST } type = NIL; ///< Type tag for the held value.
    std::optional<Node> node;                       ///< Wrapped Node value.
    std::optional<Relationship> relationship;       ///< Wrapped Relationship value.
    std::optional<std::vector<Relationship>> relationship_list; ///< Wrapped list of Relationships.
    std::optional<Path> path;                       ///< Wrapped Path value.
    std::shared_ptr<std::vector<GqlValue>> list;    ///< Wrapped heterogeneous list (collect_list result).
    property_type_t property = std::monostate{};    ///< Wrapped property value (primitive variants).

    GqlValue() : type(NIL) {}
    explicit GqlValue(Node n) : type(NODE), node(std::move(n)) {}
    explicit GqlValue(Relationship r) : type(RELATIONSHIP), relationship(std::move(r)) {}
    explicit GqlValue(std::vector<Relationship> rl) : type(RELATIONSHIP_LIST), relationship_list(std::move(rl)) {}
    explicit GqlValue(property_type_t p) : type(PROPERTY), property(std::move(p)) {}
    explicit GqlValue(Path pt) : type(PATH), path(std::move(pt)) {} ///< Construct a GqlValue wrapping a Path.
    explicit GqlValue(std::vector<GqlValue> l) : type(LIST), list(std::make_shared<std::vector<GqlValue>>(std::move(l))) {} ///< Construct a GqlValue wrapping a list.

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

/// Extracts a temporal component (year/month/day/hour/minute/second) from an epoch-millisecond UTC
/// datetime as an integer; returns NULL for an unrecognized field.
GqlValue gql_temporal_field(int64_t epoch_ms, const std::string& field);

/**
 * @brief Evaluates a scalar function call (e.g. length(path), zoned_datetime('2010-01-01')) against a
 *        row. Shared by the row and group expression evaluators. The parser rejects any name this does not
 *        implement, so an unimplemented function cannot reach here and silently evaluate to NULL.
 */
GqlValue evaluate_scalar_function(const GqlRow& row, const FunctionCallExpr* fc);

/**
 * @brief Applies a scalar function, evaluating each argument through eval_arg. The group evaluator passes
 *        an eval_arg that resolves nested aggregates from its results map, so a scalar function wrapping an
 *        aggregate -- cardinality(collect_list(x)) -- sees the aggregated value rather than a per-row NULL.
 */
GqlValue evaluate_scalar_function_with(const FunctionCallExpr* fc,
                                       const std::function<GqlValue(const Expression*)>& eval_arg);

/**
 * @brief Whether a (lower-cased) scalar function name is one the evaluator implements. Lives beside
 *        evaluate_scalar_function so the allowlist cannot drift from the dispatch it guards.
 */
bool is_supported_scalar_function(const std::string& lower_name);

/// The supported scalar function names, comma-separated, for error messages.
std::string supported_scalar_function_list();

/**
 * @brief Whether a node or relationship type satisfies a label expression (LITERAL / AND / OR / NOT /
 *        wildcard). Used both by pattern matching and by the `IS LABELED` predicate.
 */
bool matches_label_expr(const std::string& actual_type, const std::shared_ptr<LabelExpression>& expr);

/**
 * @brief Converts a value to a CAST target type, yielding NULL when it has no representation there (e.g.
 *        CAST('abc' AS INTEGER)). Shared by the row and group expression evaluators.
 */
GqlValue apply_cast(const GqlValue& value, CastType target);

/**
 * @brief Evaluates `IS [NOT] LABELED` for an already-evaluated operand: true when the bound node or
 *        relationship carries the label. A non-entity operand (or an unbound one) is NULL, not false, so
 *        it propagates like any other unknown.
 */
GqlValue apply_is_labeled(const GqlValue& value, const std::shared_ptr<LabelExpression>& label_expr, bool negated);

/**
 * @brief Views a value as a sequence of elements, for the things that are lists in GQL but are not all
 *        the LIST type at runtime: a LIST, a relationship list, and a list-valued property (a stored
 *        vector of bools/ints/doubles/strings). Returns nullopt for anything that is not a list, which
 *        FOR expands to no rows. Used by FOR (UNWIND).
 */
std::optional<std::vector<GqlValue>> as_list_elements(const GqlValue& value);

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
