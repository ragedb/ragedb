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

#ifndef RAGEDB_GQLAST_H
#define RAGEDB_GQLAST_H

#include <string>
#include <vector>
#include <map>
#include <memory>
#include <variant>
#include <optional>
#include "../graph/PropertyType.h"
#include "../graph/Operation.h"
#include "../graph/Direction.h"

namespace ragedb::gql {

struct DegreePopulateInfo {
    std::string property_name;
    std::vector<std::string> rel_types;
    Direction direction;
};

struct PropertyFilter {
    std::string property;
    Operation op;
    property_type_t value;
};

/**
 * @brief Identifies the type of an Expression AST node.
 */
enum class ExpressionKind {
    LITERAL,          ///< A static value (e.g. 5, "Alice", true)
    VARIABLE,         ///< An identifier reference (e.g. p, m)
    PROPERTY_LOOKUP,  ///< Property extraction (e.g. p.name, m.title)
    UNARY_OP,         ///< Unary operations (e.g. NOT, -x)
    BINARY_OP,        ///< Binary operations (e.g. AND, OR, +, =, <)
    AGGREGATION       ///< GQL Aggregate function (e.g. COUNT, SUM, AVG, MIN, MAX)
};

/**
 * @brief Types of aggregate functions supported by GQL.
 */
enum class AggregateKind {
    COUNT,
    SUM,
    AVG,
    MIN,
    MAX
};

/**
 * @brief Unary operator kinds.
 */
enum class UnaryOpKind {
    NOT,  ///< Logical NOT (e.g. NOT condition)
    NEG   ///< Numeric Negation (e.g. -age)
};

/**
 * @brief Binary operator kinds.
 */
enum class BinaryOpKind {
    AND, OR,                 ///< Logical conjunction/disjunction
    ADD, SUB, MUL, DIV,      ///< Arithmetic operators (+, -, *, /)
    EQ, NE, LT, LE, GT, GE,  ///< Comparison operators (=, !=, <, <=, >, >=)
    IS, AS                   ///< Keywords used in label specification and projections
};

/**
 * @brief Base struct for all GQL expression nodes.
 */
struct Expression {
    ExpressionKind kind;
    virtual ~Expression() = default;
};

/**
 * @brief Represents a literal value expression in the AST.
 */
struct LiteralExpr : public Expression {
    property_type_t value; ///< Holds the variant of property types (bool, string, int64_t, double, etc.)
    explicit LiteralExpr(property_type_t val) {
        kind = ExpressionKind::LITERAL;
        value = std::move(val);
    }
};

/**
 * @brief Represents a variable reference expression in the AST.
 */
struct VariableExpr : public Expression {
    std::string name; ///< The identifier of the referenced variable.
    explicit VariableExpr(std::string n) {
        kind = ExpressionKind::VARIABLE;
        name = std::move(n);
    }
};

/**
 * @brief Represents a property retrieval from a variable (e.g., node.property).
 */
struct PropertyLookupExpr : public Expression {
    std::string variable; ///< Variable referencing the node/relationship.
    std::string property; ///< Property key to retrieve.
    PropertyLookupExpr(std::string var, std::string prop) {
        kind = ExpressionKind::PROPERTY_LOOKUP;
        variable = std::move(var);
        property = std::move(prop);
    }
};

/**
 * @brief Represents a unary operation expression.
 */
struct UnaryOpExpr : public Expression {
    UnaryOpKind op;                      ///< The unary operator kind.
    std::unique_ptr<Expression> expr;    ///< Target expression operand.
    UnaryOpExpr(UnaryOpKind o, std::unique_ptr<Expression> e) {
        kind = ExpressionKind::UNARY_OP;
        op = o;
        expr = std::move(e);
    }
};

/**
 * @brief Represents a binary operation expression.
 */
struct BinaryOpExpr : public Expression {
    BinaryOpKind op;                     ///< The binary operator kind.
    std::unique_ptr<Expression> left;    ///< Left expression operand.
    std::unique_ptr<Expression> right;   ///< Right expression operand.
    BinaryOpExpr(BinaryOpKind o, std::unique_ptr<Expression> l, std::unique_ptr<Expression> r) {
        kind = ExpressionKind::BINARY_OP;
        op = o;
        left = std::move(l);
        right = std::move(r);
    }
};

/**
 * @brief Represents an aggregate function expression (e.g. COUNT(p), SUM(p.age)).
 */
struct AggregateExpr : public Expression {
    AggregateKind fn_kind;              ///< The kind of aggregate function.
    std::unique_ptr<Expression> expr;   ///< Expression target to aggregate (nullptr for COUNT(*)).
    AggregateExpr(AggregateKind kind_val, std::unique_ptr<Expression> e) {
        kind = ExpressionKind::AGGREGATION;
        fn_kind = kind_val;
        expr = std::move(e);
    }
};


enum class LabelExprKind {
    LITERAL,
    NOT,
    AND,
    OR
};

struct LabelExpression {
    LabelExprKind kind;
    std::string name; // For LITERAL
    std::shared_ptr<LabelExpression> left;  // For AND/OR
    std::shared_ptr<LabelExpression> right; // For AND/OR
    std::shared_ptr<LabelExpression> expr;  // For NOT
};

/**
 * @brief Represents a node pattern in MATCH or INSERT statements (e.g. (p:Person {name: 'Alice'})).
 */
struct PatternNode {
    std::string variable;                             ///< Optional variable name.
    std::shared_ptr<LabelExpression> label_expr;       ///< Optional label expression.
    std::map<std::string, property_type_t> properties; ///< Inline property map filter or payload.
    std::vector<PropertyFilter> property_filters;     ///< Pushed down property filters.
    std::vector<DegreePopulateInfo> degree_opt_info;  ///< Instructions to populate degree properties for optimization.
};

/**
 * @brief Represents directionality of relationship patterns.
 */
enum class EdgeDirection {
    RIGHT, ///< Outgoing relationship: -[e]->
    LEFT,  ///< Incoming relationship: <-[e]-
    ANY    ///< Undirected relationship: -[e]-
};

/**
 * @brief Represents an edge/relationship pattern (e.g., -[e:ACTED_IN {roles: [...]}]->).
 */
struct PatternEdge {
    std::string variable;                             ///< Optional variable name.
    std::shared_ptr<LabelExpression> label_expr;       ///< Optional label expression.
    EdgeDirection direction;                          ///< Direction of the relationship.
    std::map<std::string, property_type_t> properties; ///< Inline property map filter or payload.
    std::vector<PropertyFilter> property_filters;     ///< Pushed down property filters.
    bool is_variable_length = false;                  ///< True if variable-length hops repetition is used.
    uint64_t min_hops = 1;                            ///< Minimum number of repetitions.
    uint64_t max_hops = 1;                            ///< Maximum number of repetitions.
};

/**
 * @brief Represents a full traversal path pattern (e.g., node1 -> edge -> node2).
 */
struct PathPattern {
    std::vector<PatternNode> nodes; ///< Nodes along the path.
    std::vector<PatternEdge> edges; ///< Connecting edges.
};


/**
 * @brief Represents a single MATCH or OPTIONAL MATCH statement.
 */
struct MatchStatement {
    bool is_optional = false; ///< True if this is an OPTIONAL MATCH clause.
    PathPattern pattern;      ///< Path pattern to match.
};

/**
 * @brief Represents a projected expression inside the RETURN clause (e.g. p.name AS client_name).
 */
struct ReturnItem {
    std::unique_ptr<Expression> expr; ///< Projection expression.
    std::optional<std::string> alias;  ///< Optional alias name (AS alias).
};

/**
 * @brief Specifies sorting requirements in the ORDER BY clause.
 */
struct SortSpec {
    std::unique_ptr<Expression> expr; ///< Expression to sort by.
    bool ascending = true;             ///< True for ascending order, false for descending.
};

/**
 * @brief Represents database write/modification operations (INSERT, SET, REMOVE, DELETE).
 */
struct WriteOp {
    enum class Type { 
        INSERT,    ///< Create new nodes/relationships.
        SET,       ///< Update node/relationship property.
        REMOVE,    ///< Delete property.
        DELETE_OP  ///< Remove nodes/relationships from the graph.
    } type;

    // INSERT details
    PathPattern insert_pattern; ///< Pattern indicating nodes/relationships to be created.

    // SET details
    std::string set_var;                  ///< Target variable name to update.
    std::string set_prop;                 ///< Property key to update.
    std::unique_ptr<Expression> set_expr; ///< Expression evaluating to the new property value.

    // REMOVE details
    std::string remove_var;   ///< Variable to delete a property from.
    std::string remove_prop;  ///< Property key to remove.

    // DELETE details
    std::string delete_var;   ///< Variable pointing to the node/relationship to delete.
    bool detach = false;      ///< True if associated relationships should be deleted implicitly (RageDB behavior).
};

struct SchemaOperation {
    enum class Op {
        CREATE_NODE_TYPE,
        DROP_NODE_TYPE,
        CREATE_REL_TYPE,
        DROP_REL_TYPE,
        ALTER_NODE_TYPE,
        ALTER_REL_TYPE
    } op;

    std::string name;
    
    // For CREATE (properties list: name and datatype string, e.g. {"name", "string"})
    std::vector<std::pair<std::string, std::string>> properties;

    // For ALTER
    enum class AlterOp {
        ADD,
        DROP
    } alter_op;
    std::string alter_property_name;
    std::string alter_property_type; // For ALTER ADD
};

enum class QueryKind {
    SINGLE,
    UNION,
    UNION_ALL,
    INTERSECT,
    INTERSECT_ALL
};

/**
 * @brief Main root query AST node containing parsed MATCHes, WHERE conditions, write statements, and projections.
 * 
 * Supports both single GQL queries and recursive set operations (UNION / INTERSECT).
 */
struct GqlQuery {
    QueryKind kind = QueryKind::SINGLE;

    // For set operations:
    std::unique_ptr<GqlQuery> left;
    std::unique_ptr<GqlQuery> right;

    // For single query:
    std::vector<MatchStatement> matches;     ///< List of matching path patterns.
    std::unique_ptr<Expression> where_expr;  ///< Global WHERE filter expression.
    std::vector<WriteOp> writes;             ///< Sequence of write/mutation operations.
    std::vector<ReturnItem> returns;         ///< Projected RETURN clause items.
    bool distinct = false;                   ///< True if distinct results are required.
    std::vector<SortSpec> order_by;          ///< Sequence of sort specifications.
    std::optional<uint64_t> limit;           ///< Optional maximum number of rows to return.

    // DDL schema controls
    std::optional<SchemaOperation> schema_op;
};

} // namespace ragedb::gql

#endif // RAGEDB_GQLAST_H
