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
#include <set>
#include "../graph/PropertyType.h"
#include "../graph/Operation.h"
#include "../graph/Direction.h"
#include "../graph/paths/Path.h"

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
    AGGREGATION,      ///< GQL Aggregate function (e.g. COUNT, SUM, AVG, MIN, MAX)
    EXISTS,           ///< Exists subquery expression (e.g. EXISTS { MATCH ... })
    IS_NULL_CHECK     ///< Null check expression (e.g. x IS NULL)
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
    CONCAT,                  ///< String concatenation (||)
    EQ, NE, LT, LE, GT, GE,  ///< Comparison operators (=, !=, <, <=, >, >=)
    STARTS_WITH, ENDS_WITH,  ///< String comparisons
    CONTAINS,
    IS, AS                   ///< Keywords used in label specification and projections
};

/**
 * @brief Base struct for all GQL expression nodes.
 */
struct Expression {
    ExpressionKind kind;
    virtual ~Expression() = default;
    virtual std::unique_ptr<Expression> clone() const = 0;
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
    std::unique_ptr<Expression> clone() const override {
        return std::make_unique<LiteralExpr>(value);
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
    std::unique_ptr<Expression> clone() const override {
        return std::make_unique<VariableExpr>(name);
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
    std::unique_ptr<Expression> clone() const override {
        return std::make_unique<PropertyLookupExpr>(variable, property);
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
    std::unique_ptr<Expression> clone() const override {
        return std::make_unique<UnaryOpExpr>(op, expr ? expr->clone() : nullptr);
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
    std::unique_ptr<Expression> clone() const override {
        return std::make_unique<BinaryOpExpr>(op, left ? left->clone() : nullptr, right ? right->clone() : nullptr);
    }
};

/**
 * @brief Represents a null check expression (e.g. value IS NULL, value IS NOT NULL).
 */
struct IsNullExpr : public Expression {
    std::unique_ptr<Expression> expr;
    bool is_not; // true for IS NOT NULL, false for IS NULL
    IsNullExpr(std::unique_ptr<Expression> e, bool not_val) {
        kind = ExpressionKind::IS_NULL_CHECK;
        expr = std::move(e);
        is_not = not_val;
    }
    std::unique_ptr<Expression> clone() const override {
        return std::make_unique<IsNullExpr>(expr ? expr->clone() : nullptr, is_not);
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
    std::unique_ptr<Expression> clone() const override {
        return std::make_unique<AggregateExpr>(fn_kind, expr ? expr->clone() : nullptr);
    }
};


enum class LabelExprKind {
    LITERAL,
    NOT,
    AND,
    OR,
    WILDCARD
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
    std::shared_ptr<Expression> where_expr;           ///< Inline WHERE filter expression.
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
    std::shared_ptr<Expression> where_expr;           ///< Inline WHERE filter expression.
    std::shared_ptr<Expression> cost_expr;            ///< COST expression for Cheapest path.
    std::optional<uint64_t> max_cardinality_limit;    ///< Optional max cardinality constraint limit.
};

/**
 * @brief Represents a full traversal path pattern (e.g., node1 -> edge -> node2).
 */
struct PathPattern {
    std::vector<PatternNode> nodes; ///< Nodes along the path.
    std::vector<PatternEdge> edges; ///< Connecting edges.
    bool is_questioned = false;     ///< True if the path pattern is followed by ?
};


enum class MatchMode {
    DIFFERENT_EDGES,
    REPEATABLE_ELEMENTS
};

enum class PathMode {
    TRAIL,
    ACYCLIC,
    SIMPLE,
    WALK
};

/**
 * @brief Represents a single MATCH or OPTIONAL MATCH statement.
 */
struct MatchStatement {
    int id = -1;
    bool is_optional = false; ///< True if this is an OPTIONAL MATCH clause.
    int optional_group_id = -1; ///< Groups patterns belonging to the same OPTIONAL MATCH statement.
    MatchMode match_mode = MatchMode::DIFFERENT_EDGES; ///< GQL Match Mode (default is DIFFERENT EDGES).
    PathMode path_mode = PathMode::TRAIL;            ///< GQL Path Mode (default is TRAIL).
    PathPattern pattern;      ///< Path pattern to match.

    std::string path_variable;                              ///< Optional variable name to bind the entire matched path.
    ShortestPathKind shortest_path_kind = ShortestPathKind::NONE; ///< The shortest path selection mode (e.g. ALL, ANY, K).
    uint64_t shortest_path_k = 0;                           ///< The parameter 'k' specifying the path count for K / K_GROUP.

    bool is_khop = false;
    bool khop_count_only = false;
    bool algebraic_path_count = false;
    uint16_t path_count_hops = 0;
    std::string path_count_target_var;
    std::vector<std::string> path_count_rel_types;
    EdgeDirection path_count_dir = EdgeDirection::RIGHT;

    // FTS parameters
    bool is_search = false;
    std::string search_var;
    std::string search_type;
    std::vector<std::string> search_properties;
    std::string search_string;
    std::map<std::string, std::string> search_options;
    std::string yield_var;
    std::string yield_score_var;
};

/**
 * @brief Represents an EXISTS subquery expression (e.g., EXISTS { MATCH ... WHERE ... }).
 */
struct ExistsExpr : public Expression {
    std::vector<MatchStatement> matches; ///< Nested match statements.
    std::unique_ptr<Expression> where_expr; ///< Optional nested where filter.
    std::string target_variable; ///< Target variable for checking existence.
    
    ExistsExpr(std::vector<MatchStatement> m, std::unique_ptr<Expression> w) {
        kind = ExpressionKind::EXISTS;
        matches = std::move(m);
        where_expr = std::move(w);
    }
    std::unique_ptr<Expression> clone() const override {
        auto copy = std::make_unique<ExistsExpr>(matches, where_expr ? where_expr->clone() : nullptr);
        copy->target_variable = target_variable;
        return copy;
    }
};

/**
 * @brief Represents a projected expression inside the RETURN clause (e.g. p.name AS client_name).
 */
struct ReturnItem {
    std::unique_ptr<Expression> expr; ///< Projection expression.
    std::optional<std::string> alias;  ///< Optional alias name (AS alias).
    ReturnItem clone() const {
        return ReturnItem{ expr ? expr->clone() : nullptr, alias };
    }
};

/**
 * @brief Specifies sorting requirements in the ORDER BY clause.
 */
struct SortSpec {
    std::unique_ptr<Expression> expr; ///< Expression to sort by.
    bool ascending = true;             ///< True for ascending order, false for descending.
    SortSpec clone() const {
        return SortSpec{ expr ? expr->clone() : nullptr, ascending };
    }
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

    WriteOp clone() const {
        WriteOp copy;
        copy.type = type;
        copy.insert_pattern = insert_pattern;
        copy.set_var = set_var;
        copy.set_prop = set_prop;
        copy.set_expr = set_expr ? set_expr->clone() : nullptr;
        copy.remove_var = remove_var;
        copy.remove_prop = remove_prop;
        copy.delete_var = delete_var;
        copy.detach = detach;
        return copy;
    }
};

struct SchemaOperation {
    enum class Op {
        CREATE_NODE_TYPE,
        DROP_NODE_TYPE,
        CREATE_REL_TYPE,
        DROP_REL_TYPE,
        ALTER_NODE_TYPE,
        ALTER_REL_TYPE,
        CREATE_INDEX,
        CREATE_FULLTEXT_INDEX,
        DROP_INDEX,
        SHOW_INDEXES,
        CREATE_VIEW,
        DROP_VIEW,
        CREATE_CONSTRAINT,
        DROP_CONSTRAINT
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

    // For virtual views and constraints
    std::string query_string;
};

struct PlanNode {
    std::string operator_name;
    std::string detail;
    std::string variables;
    std::string key;
    std::optional<int64_t> estimated_rows;
    std::optional<int64_t> actual_rows;
    std::optional<double> time_ms;
    std::vector<std::shared_ptr<PlanNode>> children;
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

    // Optimization tracking fields
    std::set<std::string> outer_vars;
    bool has_unnested_subquery = false;
    bool no_op = false;
    bool skip_semantic = false;
    uint64_t count_multiplication_factor = 1;

    // Explain & Profile
    bool explain = false;
    bool profile = false;
    std::shared_ptr<PlanNode> plan_root;
    std::map<std::string, std::shared_ptr<PlanNode>> plan_nodes;

    // Cache tracking
    bool plan_cache_hit = false;
    bool clear_cache = false;

    GqlQuery clone() const {
        GqlQuery copy;
        copy.kind = kind;
        if (left) copy.left = std::make_unique<GqlQuery>(left->clone());
        if (right) copy.right = std::make_unique<GqlQuery>(right->clone());
        copy.matches = matches;
        if (where_expr) copy.where_expr = where_expr->clone();
        
        copy.writes.reserve(writes.size());
        for (const auto& w : writes) {
            copy.writes.push_back(w.clone());
        }
        
        copy.returns.reserve(returns.size());
        for (const auto& r : returns) {
            copy.returns.push_back(r.clone());
        }
        
        copy.distinct = distinct;
        
        copy.order_by.reserve(order_by.size());
        for (const auto& s : order_by) {
            copy.order_by.push_back(s.clone());
        }
        
        copy.limit = limit;
        copy.schema_op = schema_op;
        copy.outer_vars = outer_vars;
        copy.has_unnested_subquery = has_unnested_subquery;
        copy.no_op = no_op;
        copy.skip_semantic = skip_semantic;
        copy.count_multiplication_factor = count_multiplication_factor;
        copy.explain = explain;
        copy.profile = profile;
        copy.plan_cache_hit = plan_cache_hit;
        copy.clear_cache = clear_cache;
        // plan_root and plan_nodes are generated fresh during execution
        return copy;
    }
};

} // namespace ragedb::gql

#endif // RAGEDB_GQLAST_H
