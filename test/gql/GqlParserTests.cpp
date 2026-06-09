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

#include <catch2/catch.hpp>
#include "../../src/gql/GqlParser.h"

using namespace ragedb;
using namespace ragedb::gql;

TEST_CASE("GQL Parser builds AST", "[gql_parser]") {
    std::string query = "MATCH (p:Person) RETURN p.name";
    auto q = GqlParser::parse(query);

    REQUIRE(q.matches.size() == 1);
    REQUIRE(q.matches[0].pattern.nodes.size() == 1);
    REQUIRE(q.matches[0].pattern.nodes[0].variable == "p");
    REQUIRE(q.matches[0].pattern.nodes[0].label == "Person");
    REQUIRE(q.returns.size() == 1);
}

TEST_CASE("GQL Parser supports IS keyword for label specification", "[gql_parser]") {
    std::string query = "MATCH (p IS Person)-[e IS ACTED_IN]->(m IS Movie) RETURN p.name";
    auto q = GqlParser::parse(query);

    REQUIRE(q.matches.size() == 1);
    REQUIRE(q.matches[0].pattern.nodes.size() == 2);
    REQUIRE(q.matches[0].pattern.nodes[0].variable == "p");
    REQUIRE(q.matches[0].pattern.nodes[0].label == "Person");
    REQUIRE(q.matches[0].pattern.edges.size() == 1);
    REQUIRE(q.matches[0].pattern.edges[0].variable == "e");
    REQUIRE(q.matches[0].pattern.edges[0].label == "ACTED_IN");
    REQUIRE(q.matches[0].pattern.nodes[1].variable == "m");
    REQUIRE(q.matches[0].pattern.nodes[1].label == "Movie");
    REQUIRE(q.returns.size() == 1);
}

TEST_CASE("GQL Parser parses write statements", "[gql_parser]") {
    SECTION("INSERT statement") {
        std::string query = "INSERT (p:Person {name: 'Charlie', age: 25, key: 'charlie'})";
        auto q = GqlParser::parse(query);

        REQUIRE(q.writes.size() == 1);
        REQUIRE(q.writes[0].type == WriteOp::Type::INSERT);
        REQUIRE(q.writes[0].insert_pattern.nodes.size() == 1);
        REQUIRE(q.writes[0].insert_pattern.nodes[0].variable == "p");
        REQUIRE(q.writes[0].insert_pattern.nodes[0].label == "Person");
        REQUIRE(q.writes[0].insert_pattern.nodes[0].properties.count("name") == 1);
    }

    SECTION("SET statement") {
        std::string query = "MATCH (p:Person) SET p.age = 30";
        auto q = GqlParser::parse(query);

        REQUIRE(q.writes.size() == 1);
        REQUIRE(q.writes[0].type == WriteOp::Type::SET);
        REQUIRE(q.writes[0].set_var == "p");
        REQUIRE(q.writes[0].set_prop == "age");
        REQUIRE(q.writes[0].set_expr != nullptr);
    }

    SECTION("REMOVE statement") {
        std::string query = "MATCH (p:Person) REMOVE p.age";
        auto q = GqlParser::parse(query);

        REQUIRE(q.writes.size() == 1);
        REQUIRE(q.writes[0].type == WriteOp::Type::REMOVE);
        REQUIRE(q.writes[0].remove_var == "p");
        REQUIRE(q.writes[0].remove_prop == "age");
    }

    SECTION("DELETE statement") {
        std::string query = "MATCH (p:Person) DELETE p";
        auto q = GqlParser::parse(query);

        REQUIRE(q.writes.size() == 1);
        REQUIRE(q.writes[0].type == WriteOp::Type::DELETE_OP);
        REQUIRE(q.writes[0].delete_var == "p");
        REQUIRE(q.writes[0].detach == false);
    }

    SECTION("DETACH DELETE statement") {
        std::string query = "MATCH (p:Person) DETACH DELETE p";
        auto q = GqlParser::parse(query);

        REQUIRE(q.writes.size() == 1);
        REQUIRE(q.writes[0].type == WriteOp::Type::DELETE_OP);
        REQUIRE(q.writes[0].delete_var == "p");
        REQUIRE(q.writes[0].detach == true);
    }
}

TEST_CASE("GQL Parser parses aggregate expressions", "[gql_parser]") {
    SECTION("COUNT(*)") {
        std::string query = "MATCH (p:Person) RETURN count(*)";
        auto q = GqlParser::parse(query);

        REQUIRE(q.returns.size() == 1);
        auto* expr = q.returns[0].expr.get();
        REQUIRE(expr->kind == ExpressionKind::AGGREGATION);
        auto* agg = static_cast<const AggregateExpr*>(expr);
        REQUIRE(agg->fn_kind == AggregateKind::COUNT);
        REQUIRE(agg->expr == nullptr);
    }

    SECTION("SUM(p.age)") {
        std::string query = "MATCH (p:Person) RETURN SUM(p.age)";
        auto q = GqlParser::parse(query);

        REQUIRE(q.returns.size() == 1);
        auto* expr = q.returns[0].expr.get();
        REQUIRE(expr->kind == ExpressionKind::AGGREGATION);
        auto* agg = static_cast<const AggregateExpr*>(expr);
        REQUIRE(agg->fn_kind == AggregateKind::SUM);
        REQUIRE(agg->expr != nullptr);
        REQUIRE(agg->expr->kind == ExpressionKind::PROPERTY_LOOKUP);
        auto* pl = static_cast<const PropertyLookupExpr*>(agg->expr.get());
        REQUIRE(pl->variable == "p");
        REQUIRE(pl->property == "age");
    }

    SECTION("Case-insensitive aggregate functions") {
        std::string query = "MATCH (p:Person) RETURN cOuNt(p.name), avg(p.age), mIn(p.age), MAX(p.age)";
        auto q = GqlParser::parse(query);

        REQUIRE(q.returns.size() == 4);

        {
            auto* agg = static_cast<const AggregateExpr*>(q.returns[0].expr.get());
            REQUIRE(agg->fn_kind == AggregateKind::COUNT);
        }
        {
            auto* agg = static_cast<const AggregateExpr*>(q.returns[1].expr.get());
            REQUIRE(agg->fn_kind == AggregateKind::AVG);
        }
        {
            auto* agg = static_cast<const AggregateExpr*>(q.returns[2].expr.get());
            REQUIRE(agg->fn_kind == AggregateKind::MIN);
        }
        {
            auto* agg = static_cast<const AggregateExpr*>(q.returns[3].expr.get());
            REQUIRE(agg->fn_kind == AggregateKind::MAX);
        }
    }
}

TEST_CASE("GQL Parser throws exceptions on syntax errors", "[gql_parser]") {
    SECTION("Unterminated MATCH node pattern") {
        REQUIRE_THROWS_AS(GqlParser::parse("MATCH (p:Person"), std::runtime_error);
    }

    SECTION("Missing arithmetic right operand") {
        REQUIRE_THROWS_AS(GqlParser::parse("MATCH (p) RETURN p.age +"), std::runtime_error);
    }

    SECTION("Empty RETURN clause") {
        REQUIRE_THROWS_AS(GqlParser::parse("MATCH (p) RETURN "), std::runtime_error);
    }

    SECTION("Unmatched open parenthesis in expression") {
        REQUIRE_THROWS_AS(GqlParser::parse("MATCH (p) RETURN (p.age"), std::runtime_error);
    }
}

TEST_CASE("GQL Parser parses logical and comparison expression precedence", "[gql_parser]") {
    // NOT binds tighter than AND, which binds tighter than OR.
    // "NOT (p.age > 20) AND p.name = 'Alice' OR p.age = 30"
    // parses as: ((NOT (p.age > 20)) AND (p.name = 'Alice')) OR (p.age = 30)
    std::string query = "MATCH (p) WHERE NOT (p.age > 20) AND p.name = 'Alice' OR p.age = 30 RETURN p";
    auto q = GqlParser::parse(query);

    REQUIRE(q.where_expr != nullptr);
    REQUIRE(q.where_expr->kind == ExpressionKind::BINARY_OP);
    auto* or_expr = static_cast<const BinaryOpExpr*>(q.where_expr.get());
    REQUIRE(or_expr->op == BinaryOpKind::OR);

    // Left child of OR is AND expression
    REQUIRE(or_expr->left->kind == ExpressionKind::BINARY_OP);
    auto* and_expr = static_cast<const BinaryOpExpr*>(or_expr->left.get());
    REQUIRE(and_expr->op == BinaryOpKind::AND);

    // Right child of OR is comparison p.age = 30
    REQUIRE(or_expr->right->kind == ExpressionKind::BINARY_OP);
    auto* right_eq = static_cast<const BinaryOpExpr*>(or_expr->right.get());
    REQUIRE(right_eq->op == BinaryOpKind::EQ);

    // Left child of AND is NOT expression
    REQUIRE(and_expr->left->kind == ExpressionKind::UNARY_OP);
    auto* not_expr = static_cast<const UnaryOpExpr*>(and_expr->left.get());
    REQUIRE(not_expr->op == UnaryOpKind::NOT);
}

TEST_CASE("GQL Parser parses arithmetic expressions and unary NEG", "[gql_parser]") {
    SECTION("Arithmetic operator precedence") {
        // "1 + 2 * 3" parses as: 1 + (2 * 3)
        std::string query = "MATCH (p) RETURN 1 + 2 * 3";
        auto q = GqlParser::parse(query);

        REQUIRE(q.returns.size() == 1);
        auto* expr = q.returns[0].expr.get();
        REQUIRE(expr->kind == ExpressionKind::BINARY_OP);
        auto* plus = static_cast<const BinaryOpExpr*>(expr);
        REQUIRE(plus->op == BinaryOpKind::ADD);
        REQUIRE(plus->left->kind == ExpressionKind::LITERAL);
        REQUIRE(plus->right->kind == ExpressionKind::BINARY_OP);
        auto* mul = static_cast<const BinaryOpExpr*>(plus->right.get());
        REQUIRE(mul->op == BinaryOpKind::MUL);
    }

    SECTION("Unary NEG expression") {
        std::string query = "MATCH (p) RETURN -p.age";
        auto q = GqlParser::parse(query);

        REQUIRE(q.returns.size() == 1);
        auto* expr = q.returns[0].expr.get();
        REQUIRE(expr->kind == ExpressionKind::UNARY_OP);
        auto* neg = static_cast<const UnaryOpExpr*>(expr);
        REQUIRE(neg->op == UnaryOpKind::NEG);
    }
}

TEST_CASE("GQL Parser parses ORDER BY sort specs", "[gql_parser]") {
    std::string query = "MATCH (p) RETURN p ORDER BY p.age ASCENDING, p.name DESC";
    auto q = GqlParser::parse(query);

    REQUIRE(q.order_by.size() == 2);
    REQUIRE(q.order_by[0].ascending == true);
    REQUIRE(q.order_by[1].ascending == false);
}

TEST_CASE("GQL Parser parses Set Operations", "[gql_parser]") {
    SECTION("UNION and UNION ALL") {
        std::string query = "MATCH (p:Person) RETURN p.name UNION MATCH (m:Movie) RETURN m.title";
        auto q = GqlParser::parse(query);

        REQUIRE(q.kind == QueryKind::UNION);
        REQUIRE(q.left != nullptr);
        REQUIRE(q.right != nullptr);
        REQUIRE(q.left->kind == QueryKind::SINGLE);
        REQUIRE(q.right->kind == QueryKind::SINGLE);

        std::string query_all = "MATCH (p:Person) RETURN p.name UNION ALL MATCH (m:Movie) RETURN m.title";
        auto q_all = GqlParser::parse(query_all);
        REQUIRE(q_all.kind == QueryKind::UNION_ALL);
    }

    SECTION("INTERSECT and INTERSECT ALL") {
        std::string query = "MATCH (p:Person) RETURN p.name INTERSECT MATCH (m:Movie) RETURN m.title";
        auto q = GqlParser::parse(query);

        REQUIRE(q.kind == QueryKind::INTERSECT);
        REQUIRE(q.left != nullptr);
        REQUIRE(q.right != nullptr);
        REQUIRE(q.left->kind == QueryKind::SINGLE);
        REQUIRE(q.right->kind == QueryKind::SINGLE);

        std::string query_all = "MATCH (p:Person) RETURN p.name INTERSECT ALL MATCH (m:Movie) RETURN m.title";
        auto q_all = GqlParser::parse(query_all);
        REQUIRE(q_all.kind == QueryKind::INTERSECT_ALL);
    }

    SECTION("Precedence of UNION vs INTERSECT") {
        // "Q1 UNION Q2 INTERSECT Q3" parses as: Q1 UNION (Q2 INTERSECT Q3) because INTERSECT binds tighter.
        std::string query = "MATCH (a) RETURN a UNION MATCH (b) RETURN b INTERSECT MATCH (c) RETURN c";
        auto q = GqlParser::parse(query);

        REQUIRE(q.kind == QueryKind::UNION);
        REQUIRE(q.left->kind == QueryKind::SINGLE);
        REQUIRE(q.right->kind == QueryKind::INTERSECT);
        REQUIRE(q.right->left->kind == QueryKind::SINGLE);
        REQUIRE(q.right->right->kind == QueryKind::SINGLE);
    }

    SECTION("Top-level ORDER BY and LIMIT on Set operations") {
        std::string query = "MATCH (p:Person) RETURN p.name UNION MATCH (m:Movie) RETURN m.title ORDER BY p.name LIMIT 5";
        auto q = GqlParser::parse(query);

        REQUIRE(q.kind == QueryKind::UNION);
        REQUIRE(q.order_by.size() == 1);
        REQUIRE(q.limit.has_value());
        REQUIRE(*q.limit == 5);

        // Subqueries should NOT have order_by or limit
        REQUIRE(q.left->order_by.empty());
        REQUIRE(!q.left->limit.has_value());
        REQUIRE(q.right->order_by.empty());
        REQUIRE(!q.right->limit.has_value());
    }
}



