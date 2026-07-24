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
    REQUIRE(q.matches[0].pattern.nodes[0].label_expr->name == "Person");
    REQUIRE(q.returns.size() == 1);
}

TEST_CASE("GQL Parser supports IS keyword for label specification", "[gql_parser]") {
    std::string query = "MATCH (p IS Person)-[e IS ACTED_IN]->(m IS Movie) RETURN p.name";
    auto q = GqlParser::parse(query);

    REQUIRE(q.matches.size() == 1);
    REQUIRE(q.matches[0].pattern.nodes.size() == 2);
    REQUIRE(q.matches[0].pattern.nodes[0].variable == "p");
    REQUIRE(q.matches[0].pattern.nodes[0].label_expr->name == "Person");
    REQUIRE(q.matches[0].pattern.edges.size() == 1);
    REQUIRE(q.matches[0].pattern.edges[0].variable == "e");
    REQUIRE(q.matches[0].pattern.edges[0].label_expr->name == "ACTED_IN");
    REQUIRE(q.matches[0].pattern.nodes[1].variable == "m");
    REQUIRE(q.matches[0].pattern.nodes[1].label_expr->name == "Movie");
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
        REQUIRE(q.writes[0].insert_pattern.nodes[0].label_expr->name == "Person");
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

TEST_CASE("GQL Parser parses label algebra and repetitions", "[gql_parser]") {
    SECTION("Label algebra OR/AND/NOT") {
        std::string query = "MATCH (p:Person|Employee) RETURN p";
        auto q = GqlParser::parse(query);
        REQUIRE(q.matches.size() == 1);
        auto expr = q.matches[0].pattern.nodes[0].label_expr;
        REQUIRE(expr != nullptr);
        REQUIRE(expr->kind == LabelExprKind::OR);
        REQUIRE(expr->left->name == "Person");
        REQUIRE(expr->right->name == "Employee");
    }

    SECTION("Label algebra AND/NOT and parentheses") {
        std::string query = "MATCH (p:(Person|Employee)&!Customer) RETURN p";
        auto q = GqlParser::parse(query);
        REQUIRE(q.matches.size() == 1);
        auto expr = q.matches[0].pattern.nodes[0].label_expr;
        REQUIRE(expr != nullptr);
        REQUIRE(expr->kind == LabelExprKind::AND);
        REQUIRE(expr->left->kind == LabelExprKind::OR);
        REQUIRE(expr->right->kind == LabelExprKind::NOT);
        REQUIRE(expr->right->expr->name == "Customer");
    }

    SECTION("Path repetitions range") {
        std::string query = "MATCH (p)-[:FRIEND*1..3]->(m) RETURN p";
        auto q = GqlParser::parse(query);
        REQUIRE(q.matches.size() == 1);
        auto edge = q.matches[0].pattern.edges[0];
        REQUIRE(edge.is_variable_length);
        REQUIRE(edge.min_hops == 1);
        REQUIRE(edge.max_hops == 3);
    }

    SECTION("Path repetitions unbounded") {
        std::string query = "MATCH (p)-[:FRIEND*]->(m) RETURN p";
        auto q = GqlParser::parse(query);
        REQUIRE(q.matches.size() == 1);
        auto edge = q.matches[0].pattern.edges[0];
        REQUIRE(edge.is_variable_length);
        REQUIRE(edge.min_hops == 1);
        REQUIRE(edge.max_hops == std::numeric_limits<uint64_t>::max());
    }

    SECTION("INSERT rejects complex label expressions") {
        std::string query = "INSERT (p:Person|Employee)";
        REQUIRE_THROWS_AS(GqlParser::parse(query), std::runtime_error);
    }
}

TEST_CASE("GQL Parser parses null checks, string operators and concatenation", "[gql_parser]") {
    SECTION("IS NULL and IS NOT NULL") {
        std::string query = "MATCH (p) WHERE p.age IS NULL AND p.name IS NOT NULL RETURN p";
        auto q = GqlParser::parse(query);
        REQUIRE(q.where_expr != nullptr);
        REQUIRE(q.where_expr->kind == ExpressionKind::BINARY_OP);
        auto* and_expr = static_cast<const BinaryOpExpr*>(q.where_expr.get());
        REQUIRE(and_expr->op == BinaryOpKind::AND);
        
        REQUIRE(and_expr->left->kind == ExpressionKind::IS_NULL_CHECK);
        auto* is_null = static_cast<const IsNullExpr*>(and_expr->left.get());
        REQUIRE(is_null->is_not == false);
        
        REQUIRE(and_expr->right->kind == ExpressionKind::IS_NULL_CHECK);
        auto* is_not_null = static_cast<const IsNullExpr*>(and_expr->right.get());
        REQUIRE(is_not_null->is_not == true);
    }

    SECTION("STARTS WITH, ENDS WITH, CONTAINS") {
        std::string query = "MATCH (p) WHERE p.name STARTS WITH 'Al' AND p.name ENDS WITH 'ce' OR p.name CONTAINS 'o' RETURN p";
        auto q = GqlParser::parse(query);
        REQUIRE(q.where_expr != nullptr);
        REQUIRE(q.where_expr->kind == ExpressionKind::BINARY_OP);
        auto* or_expr = static_cast<const BinaryOpExpr*>(q.where_expr.get());
        REQUIRE(or_expr->op == BinaryOpKind::OR);

        // left of OR is AND
        auto* and_expr = static_cast<const BinaryOpExpr*>(or_expr->left.get());
        REQUIRE(and_expr->op == BinaryOpKind::AND);

        auto* starts_expr = static_cast<const BinaryOpExpr*>(and_expr->left.get());
        REQUIRE(starts_expr->op == BinaryOpKind::STARTS_WITH);

        auto* ends_expr = static_cast<const BinaryOpExpr*>(and_expr->right.get());
        REQUIRE(ends_expr->op == BinaryOpKind::ENDS_WITH);

        auto* contains_expr = static_cast<const BinaryOpExpr*>(or_expr->right.get());
        REQUIRE(contains_expr->op == BinaryOpKind::CONTAINS);
    }

    SECTION("String concatenation operator ||") {
        std::string query = "MATCH (p) RETURN 'A' || 'B' || 'C'";
        auto q = GqlParser::parse(query);
        REQUIRE(q.returns.size() == 1);
        auto* expr = q.returns[0].expr.get();
        REQUIRE(expr->kind == ExpressionKind::BINARY_OP);
        auto* concat2 = static_cast<const BinaryOpExpr*>(expr);
        REQUIRE(concat2->op == BinaryOpKind::CONCAT);
        REQUIRE(concat2->left->kind == ExpressionKind::BINARY_OP);
        auto* concat1 = static_cast<const BinaryOpExpr*>(concat2->left.get());
        REQUIRE(concat1->op == BinaryOpKind::CONCAT);
    }
}

TEST_CASE("NEXT projection items and reserved-word identifiers", "[gql_parser]") {
    SECTION("non-variable RETURN items before NEXT require an alias") {
        REQUIRE_THROWS_WITH(
            GqlParser::parse("MATCH (p:Person) RETURN p.name NEXT MATCH (q:Person) RETURN q"),
            Catch::Contains("aliased"));
    }

    SECTION("aliased expression and plain variable projection items parse") {
        auto q = GqlParser::parse("MATCH (p:Person) RETURN p, p.name AS name NEXT RETURN name");
        REQUIRE(q.with_segments.size() == 1);
        REQUIRE(q.with_segments[0]->returns.size() == 2);
        REQUIRE(q.with_segments[0]->returns[1].alias == "name");
    }

    SECTION("reserved words are usable as property names and aliases") {
        auto q1 = GqlParser::parse("MATCH (n:Doc) RETURN n.with");
        REQUIRE(q1.returns.size() == 1);

        auto q2 = GqlParser::parse("MATCH (n:Doc) RETURN n.name AS with");
        REQUIRE(q2.returns[0].alias == "with");

        auto q3 = GqlParser::parse("MATCH (n:Doc) RETURN n.with AS with NEXT RETURN with");
        REQUIRE(q3.with_segments.size() == 1);
        REQUIRE(q3.with_segments[0]->returns[0].alias == "with");
    }
}

TEST_CASE("ORDER BY resolves RETURN aliases into sort keys", "[gql_parser]") {
    SECTION("aggregate alias becomes the aggregate expression") {
        auto q = GqlParser::parse(
            "MATCH (t:Tag)<-[:HAS_TAG]-(post) RETURN t.name AS name, count(DISTINCT post) AS cnt "
            "ORDER BY cnt DESC, name ASC LIMIT 10");
        REQUIRE(q.order_by.size() == 2);
        REQUIRE(q.order_by[0].expr->kind == ExpressionKind::AGGREGATION);
        auto* agg = static_cast<const AggregateExpr*>(q.order_by[0].expr.get());
        REQUIRE(agg->fn_kind == AggregateKind::COUNT);
        REQUIRE(agg->distinct);
        REQUIRE(q.order_by[1].expr->kind == ExpressionKind::PROPERTY_LOOKUP);
    }

    SECTION("alias inside an arithmetic sort key is substituted") {
        auto q = GqlParser::parse(
            "MATCH (p:Person) RETURN p.age AS a ORDER BY a + 1 DESC");
        REQUIRE(q.order_by[0].expr->kind == ExpressionKind::BINARY_OP);
        auto* bin = static_cast<const BinaryOpExpr*>(q.order_by[0].expr.get());
        REQUIRE(bin->left->kind == ExpressionKind::PROPERTY_LOOKUP);
    }

    SECTION("alias inside a postfix IS-predicate sort key is substituted, not only IS LABELED") {
        // The substitution must descend into every postfix-IS arm; when it skipped IS NULL / IS DIRECTED /
        // IS SOURCE the alias stayed an unresolved variable and the sort key evaluated as null on every row.
        auto q = GqlParser::parse("MATCH (p:Person) RETURN p.age AS a ORDER BY a IS NULL DESC");
        REQUIRE(q.order_by[0].expr->kind == ExpressionKind::IS_NULL_CHECK);
        const auto* inner = static_cast<const IsNullExpr*>(q.order_by[0].expr.get())->expr.get();
        REQUIRE(inner->kind == ExpressionKind::PROPERTY_LOOKUP);
    }

    SECTION("intermediate NEXT segment ORDER BY resolves that segment's aliases") {
        auto q = GqlParser::parse(
            "MATCH (p:Person)<-[:HAS_CREATOR]-(m) RETURN p, count(m) AS cnt ORDER BY cnt DESC LIMIT 3 "
            "NEXT RETURN p.name");
        REQUIRE(q.with_segments.size() == 1);
        REQUIRE(q.with_segments[0]->order_by.size() == 1);
        REQUIRE(q.with_segments[0]->order_by[0].expr->kind == ExpressionKind::AGGREGATION);
    }

    SECTION("non-alias variables in ORDER BY are untouched") {
        auto q = GqlParser::parse("MATCH (p:Person) RETURN p.name AS n ORDER BY p");
        REQUIRE(q.order_by[0].expr->kind == ExpressionKind::VARIABLE);
    }
}

TEST_CASE("EXISTS accepts an openCypher-style bare pattern subquery", "[gql_parser]") {
    auto q = GqlParser::parse(
        "MATCH (a:Person) WHERE EXISTS { (a)-[:KNOWS]->(b:Person) } RETURN a.name");
    REQUIRE(q.where_expr != nullptr);

    auto q2 = GqlParser::parse(
        "MATCH (m:Message)<-[:REPLY_OF]-(c) MATCH (c)-[:HAS_CREATOR]->(x:Person) "
        "WHERE EXISTS { (x)-[:KNOWS]-(y:Person {id: 1}) } RETURN x.id");
    REQUIRE(q2.matches.size() == 2);
}

TEST_CASE("GQL interleaved MATCH ... WHERE ... MATCH within a segment", "[gql_parser]") {
    SECTION("single segment: MATCH WHERE MATCH RETURN") {
        auto q = GqlParser::parse(
            "MATCH (a:Person)-[:KNOWS]-(f:Person) WHERE f.id <> 1 "
            "MATCH (f)-[:HAS_CREATOR]->(post:Post) RETURN a.id");
        REQUIRE(q.matches.size() == 2);
        REQUIRE(q.where_expr != nullptr);
    }
    SECTION("across a NEXT boundary (IC5 shape)") {
        auto q = GqlParser::parse(
            "MATCH (p:Person {id: 1})-[:KNOWS]-(f:Person) WHERE f.id <> 1 "
            "RETURN DISTINCT f "
            "NEXT "
            "MATCH (forum:Forum)-[hm:HAS_MEMBER]->(f) WHERE hm.joinDate >= 100 "
            "MATCH (forum)-[:CONTAINER_OF]->(post:Post)-[:HAS_CREATOR]->(f) "
            "RETURN forum.id AS fid, count(DISTINCT post) AS cnt "
            "ORDER BY count(DISTINCT post) DESC LIMIT 20");
        REQUIRE(q.with_segments.size() == 1);
        REQUIRE(q.matches.size() == 2); // final segment carries both MATCHes
    }
}

TEST_CASE("GQL IN-list membership desugars to an OR chain", "[gql_parser]") {
    auto q = GqlParser::parse(
        "MATCH (c:Country) WHERE c.name IN ['China', 'Germany'] RETURN c.name");
    REQUIRE(q.where_expr != nullptr);
    // x IN [a, b] -> (x = a) OR (x = b)
    REQUIRE(q.where_expr->kind == ExpressionKind::BINARY_OP);
    auto* top = static_cast<BinaryOpExpr*>(q.where_expr.get());
    REQUIRE(top->op == BinaryOpKind::OR);
    REQUIRE(top->left->kind == ExpressionKind::BINARY_OP);
    REQUIRE(static_cast<BinaryOpExpr*>(top->left.get())->op == BinaryOpKind::EQ);

    SECTION("empty list is always false") {
        auto q2 = GqlParser::parse("MATCH (c:Country) WHERE c.name IN [] RETURN c.name");
        REQUIRE(q2.where_expr->kind == ExpressionKind::LITERAL);
    }
    SECTION("multi-hop EXISTS with inner IN-list WHERE (IC3 shape)") {
        REQUIRE_NOTHROW(GqlParser::parse(
            "MATCH (p:Person {id: 1})-[:KNOWS]-(f:Person) WHERE f.id <> 1 "
            "RETURN DISTINCT f "
            "NEXT "
            "FILTER NOT EXISTS { MATCH (f)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(h:Country) "
            "WHERE h.name IN ['China', 'Germany'] } "
            "MATCH (f)<-[:HAS_CREATOR]-(m)-[:IS_LOCATED_IN]->(c:Country) "
            "RETURN f.id AS pid, count(DISTINCT c) AS cnt"));
    }
}


TEST_CASE("path mode is accepted after the path variable, not only before it", "[gql_parser]") {
    SECTION("p = TRAIL (...) sets TRAIL (FinBench CR1 shape)") {
        auto q = GqlParser::parse(
            "MATCH p = TRAIL (a:Account)-[:transfer]->{1,3}(b:Account) RETURN p");
        REQUIRE(q.matches[0].path_mode == PathMode::TRAIL);
        REQUIRE(q.matches[0].path_variable == "p");
    }
    SECTION("p = ACYCLIC (...) sets ACYCLIC (FinBench CR5 shape)") {
        auto q = GqlParser::parse(
            "MATCH p = ACYCLIC (a:Account)-[:transfer]->{1,3}(b:Account) RETURN p");
        REQUIRE(q.matches[0].path_mode == PathMode::ACYCLIC);
    }
    SECTION("the pre-assignment form TRAIL p = (...) still works") {
        auto q = GqlParser::parse(
            "MATCH TRAIL p = (a:Account)-[:transfer]->{1,3}(b:Account) RETURN p");
        REQUIRE(q.matches[0].path_mode == PathMode::TRAIL);
    }
}
