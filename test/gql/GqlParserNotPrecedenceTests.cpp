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

// Boolean NOT precedence. In ISO GQL (and SQL/Cypher) NOT is LOOSER than comparisons and predicates but
// TIGHTER than AND: comparison > NOT > AND > OR. So `NOT a = b` is `NOT (a = b)`, not `(NOT a) = b`, and
// `NOT x IS NULL` is `NOT (x IS NULL)`. The parser once placed NOT at the tight unary-arithmetic level,
// which inverted these -- `NOT n.age > 5` became `(NOT n.age) > 5`, applying NOT to a number.

#include <catch2/catch.hpp>
#include "../../src/gql/GqlParser.h"

using namespace ragedb;
using namespace ragedb::gql;

namespace {
// Parse `RETURN <expr>` and return the expression tree root.
const Expression* parse_expr_tree(GqlQuery& holder, const std::string& expr) {
    holder = GqlParser::parse("MATCH (n:N) RETURN " + expr + " AS r");
    return holder.returns[0].expr.get();
}
}  // namespace

TEST_CASE("NOT binds looser than a comparison, so NOT wraps the whole predicate", "[gql_parser]") {
    SECTION("NOT a = b parses as NOT (a = b)") {
        GqlQuery q;
        const Expression* e = parse_expr_tree(q, "a = b");   // sanity: bare comparison is EQ at the root
        REQUIRE(e->kind == ExpressionKind::BINARY_OP);

        e = parse_expr_tree(q, "NOT a = b");
        REQUIRE(e->kind == ExpressionKind::UNARY_OP);
        REQUIRE(static_cast<const UnaryOpExpr*>(e)->op == UnaryOpKind::NOT);
        const Expression* inner = static_cast<const UnaryOpExpr*>(e)->expr.get();
        REQUIRE(inner->kind == ExpressionKind::BINARY_OP);
        REQUIRE(static_cast<const BinaryOpExpr*>(inner)->op == BinaryOpKind::EQ);
    }

    SECTION("NOT n.age > 5 parses as NOT (n.age > 5), not (NOT n.age) > 5") {
        GqlQuery q;
        const Expression* e = parse_expr_tree(q, "NOT n.age > 5");
        REQUIRE(e->kind == ExpressionKind::UNARY_OP);
        REQUIRE(static_cast<const UnaryOpExpr*>(e)->op == UnaryOpKind::NOT);
        REQUIRE(static_cast<const UnaryOpExpr*>(e)->expr->kind == ExpressionKind::BINARY_OP);
        REQUIRE(static_cast<const BinaryOpExpr*>(static_cast<const UnaryOpExpr*>(e)->expr.get())->op
                == BinaryOpKind::GT);
    }

    SECTION("NOT n.x IS NULL parses as NOT (n.x IS NULL)") {
        GqlQuery q;
        const Expression* e = parse_expr_tree(q, "NOT n.x IS NULL");
        REQUIRE(e->kind == ExpressionKind::UNARY_OP);
        REQUIRE(static_cast<const UnaryOpExpr*>(e)->op == UnaryOpKind::NOT);
        REQUIRE(static_cast<const UnaryOpExpr*>(e)->expr->kind == ExpressionKind::IS_NULL_CHECK);
    }
}

TEST_CASE("NOT still binds tighter than AND/OR", "[gql_parser]") {
    SECTION("NOT a AND b is (NOT a) AND b") {
        GqlQuery q;
        const Expression* e = parse_expr_tree(q, "NOT a AND b");
        REQUIRE(e->kind == ExpressionKind::BINARY_OP);
        const auto* and_e = static_cast<const BinaryOpExpr*>(e);
        REQUIRE(and_e->op == BinaryOpKind::AND);
        REQUIRE(and_e->left->kind == ExpressionKind::UNARY_OP);
        REQUIRE(static_cast<const UnaryOpExpr*>(and_e->left.get())->op == UnaryOpKind::NOT);
    }

    SECTION("a AND NOT b = c is a AND (NOT (b = c))") {
        GqlQuery q;
        const Expression* e = parse_expr_tree(q, "a AND NOT b = c");
        REQUIRE(e->kind == ExpressionKind::BINARY_OP);
        const auto* and_e = static_cast<const BinaryOpExpr*>(e);
        REQUIRE(and_e->op == BinaryOpKind::AND);
        REQUIRE(and_e->right->kind == ExpressionKind::UNARY_OP);          // NOT (b = c)
        const auto* not_e = static_cast<const UnaryOpExpr*>(and_e->right.get());
        REQUIRE(not_e->op == UnaryOpKind::NOT);
        REQUIRE(not_e->expr->kind == ExpressionKind::BINARY_OP);
        REQUIRE(static_cast<const BinaryOpExpr*>(not_e->expr.get())->op == BinaryOpKind::EQ);
    }
}

TEST_CASE("arithmetic, concat and comparison precedence interactions", "[gql_parser]") {
    GqlQuery q;
    SECTION("unary minus binds tighter than *: -a * b is (-a) * b") {
        const Expression* e = parse_expr_tree(q, "-a * b");
        REQUIRE(e->kind == ExpressionKind::BINARY_OP);
        const auto* mul = static_cast<const BinaryOpExpr*>(e);
        REQUIRE(mul->op == BinaryOpKind::MUL);
        REQUIRE(mul->left->kind == ExpressionKind::UNARY_OP);
        REQUIRE(static_cast<const UnaryOpExpr*>(mul->left.get())->op == UnaryOpKind::NEG);
    }
    SECTION("concat shares +/- precedence and is left-associative: a + b || c is (a + b) || c") {
        const Expression* e = parse_expr_tree(q, "a + b || c");
        REQUIRE(e->kind == ExpressionKind::BINARY_OP);
        const auto* concat = static_cast<const BinaryOpExpr*>(e);
        REQUIRE(concat->op == BinaryOpKind::CONCAT);
        REQUIRE(concat->left->kind == ExpressionKind::BINARY_OP);
        REQUIRE(static_cast<const BinaryOpExpr*>(concat->left.get())->op == BinaryOpKind::ADD);
    }
    SECTION("arithmetic binds tighter than comparison: a + b = c is (a + b) = c") {
        const Expression* e = parse_expr_tree(q, "a + b = c");
        REQUIRE(e->kind == ExpressionKind::BINARY_OP);
        const auto* eq = static_cast<const BinaryOpExpr*>(e);
        REQUIRE(eq->op == BinaryOpKind::EQ);
        REQUIRE(eq->left->kind == ExpressionKind::BINARY_OP);
        REQUIRE(static_cast<const BinaryOpExpr*>(eq->left.get())->op == BinaryOpKind::ADD);
    }
    SECTION("IN is a comparison, so a IN [1,2] AND b groups the IN under the AND") {
        const Expression* e = parse_expr_tree(q, "a IN [1, 2] AND b");
        REQUIRE(e->kind == ExpressionKind::BINARY_OP);
        const auto* and_e = static_cast<const BinaryOpExpr*>(e);
        REQUIRE(and_e->op == BinaryOpKind::AND);
        // `a IN [1, 2]` desugars to (a = 1 OR a = 2), so the AND's left arm is that OR.
        REQUIRE(and_e->left->kind == ExpressionKind::BINARY_OP);
        REQUIRE(static_cast<const BinaryOpExpr*>(and_e->left.get())->op == BinaryOpKind::OR);
    }
}
