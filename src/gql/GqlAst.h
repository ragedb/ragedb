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

namespace ragedb::gql {

enum class ExpressionKind {
    LITERAL,
    VARIABLE,
    PROPERTY_LOOKUP,
    UNARY_OP,
    BINARY_OP
};

enum class UnaryOpKind {
    NOT,
    NEG
};

enum class BinaryOpKind {
    AND, OR,
    ADD, SUB, MUL, DIV,
    EQ, NE, LT, LE, GT, GE,
    IS, AS
};

struct Expression {
    ExpressionKind kind;
    virtual ~Expression() = default;
};

struct LiteralExpr : public Expression {
    property_type_t value;
    explicit LiteralExpr(property_type_t val) {
        kind = ExpressionKind::LITERAL;
        value = std::move(val);
    }
};

struct VariableExpr : public Expression {
    std::string name;
    explicit VariableExpr(std::string n) {
        kind = ExpressionKind::VARIABLE;
        name = std::move(n);
    }
};

struct PropertyLookupExpr : public Expression {
    std::string variable;
    std::string property;
    PropertyLookupExpr(std::string var, std::string prop) {
        kind = ExpressionKind::PROPERTY_LOOKUP;
        variable = std::move(var);
        property = std::move(prop);
    }
};

struct UnaryOpExpr : public Expression {
    UnaryOpKind op;
    std::unique_ptr<Expression> expr;
    UnaryOpExpr(UnaryOpKind o, std::unique_ptr<Expression> e) {
        kind = ExpressionKind::UNARY_OP;
        op = o;
        expr = std::move(e);
    }
};

struct BinaryOpExpr : public Expression {
    BinaryOpKind op;
    std::unique_ptr<Expression> left;
    std::unique_ptr<Expression> right;
    BinaryOpExpr(BinaryOpKind o, std::unique_ptr<Expression> l, std::unique_ptr<Expression> r) {
        kind = ExpressionKind::BINARY_OP;
        op = o;
        left = std::move(l);
        right = std::move(r);
    }
};

struct PatternNode {
    std::string variable;
    std::string label;
    std::map<std::string, property_type_t> properties;
};

enum class EdgeDirection {
    RIGHT, // -[e]->
    LEFT,  // <-[e]-
    ANY    // -[e]-
};

struct PatternEdge {
    std::string variable;
    std::string label;
    EdgeDirection direction;
    std::map<std::string, property_type_t> properties;
};

struct PathPattern {
    std::vector<PatternNode> nodes;
    std::vector<PatternEdge> edges;
};

struct MatchStatement {
    bool is_optional = false;
    PathPattern pattern;
};

struct ReturnItem {
    std::unique_ptr<Expression> expr;
    std::optional<std::string> alias;
};

struct SortSpec {
    std::unique_ptr<Expression> expr;
    bool ascending = true;
};

struct WriteOp {
    enum class Type { INSERT, SET, REMOVE, DELETE_OP } type;

    // For INSERT
    PathPattern insert_pattern;

    // For SET
    std::string set_var;
    std::string set_prop;
    std::unique_ptr<Expression> set_expr;

    // For REMOVE
    std::string remove_var;
    std::string remove_prop;

    // For DELETE
    std::string delete_var;
    bool detach = false;
};

struct GqlQuery {
    std::vector<MatchStatement> matches;
    std::unique_ptr<Expression> where_expr;
    std::vector<WriteOp> writes;
    std::vector<ReturnItem> returns;
    bool distinct = false;
    std::vector<SortSpec> order_by;
    std::optional<uint64_t> limit;
};

} // namespace ragedb::gql

#endif // RAGEDB_GQLAST_H
