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

// Projection-level expression parsing: scalar functions (length/zoned_datetime), CASE, the COUNT { } and
// EXISTS subquery-count constructs, collect_list, and IN-list membership -- across the IC1/IC3/IC4/IC10/IC13
// shapes that exercise them. Split out of GqlParserTests.cpp to keep it under the file-size target.

#include <catch2/catch.hpp>
#include "../../src/gql/GqlParser.h"

using namespace ragedb;
using namespace ragedb::gql;

TEST_CASE("GQL scalar functions and CASE expressions (length/CASE/zoned_datetime)", "[gql_parser]") {
    SECTION("length(p) is a scalar function call") {
        auto q = GqlParser::parse(
            "MATCH p = ANY SHORTEST (a)-[:KNOWS]-{1,3}(f) RETURN length(p) AS d");
        REQUIRE(q.returns.size() == 1);
        REQUIRE(q.returns[0].expr->kind == ExpressionKind::FUNCTION_CALL);
        auto* fc = static_cast<FunctionCallExpr*>(q.returns[0].expr.get());
        REQUIRE(fc->name == "length"); // lowercased
        REQUIRE(fc->args.size() == 1);
    }
    SECTION("full pure-GQL IC1 shape parses (ANY SHORTEST + length + ORDER BY alias)") {
        REQUIRE_NOTHROW(GqlParser::parse(
            "MATCH (a:Person {id: 4398046519825}), (f:Person {firstName: 'John'}) "
            "MATCH p = ANY SHORTEST (a)-[:KNOWS]-{1,3}(f) "
            "RETURN length(p) AS dist, f.lastName AS lname, f.id AS pid "
            "ORDER BY dist, lname, pid LIMIT 20"));
    }
    SECTION("full pure-GQL IC13 shape parses (unbounded + length)") {
        REQUIRE_NOTHROW(GqlParser::parse(
            "MATCH (a:Person {id: 1}), (b:Person {id: 2}) "
            "MATCH pth = ANY SHORTEST (a)-[:KNOWS]-+(b) "
            "RETURN length(pth) AS hops"));
    }
    SECTION("searched CASE WHEN ... THEN ... ELSE ... END") {
        auto q = GqlParser::parse(
            "MATCH (c:Country) RETURN CASE WHEN c.name = 'China' THEN 1 ELSE 0 END AS x");
        REQUIRE(q.returns[0].expr->kind == ExpressionKind::CASE_WHEN);
        auto* ce = static_cast<CaseExpr*>(q.returns[0].expr.get());
        REQUIRE(ce->branches.size() == 1);
        REQUIRE(ce->else_expr != nullptr);
    }
    SECTION("CASE with multiple WHEN branches and no ELSE") {
        auto q = GqlParser::parse(
            "MATCH (p:Person) RETURN CASE WHEN p.age < 18 THEN 'minor' "
            "WHEN p.age < 65 THEN 'adult' END AS bucket");
        auto* ce = static_cast<CaseExpr*>(q.returns[0].expr.get());
        REQUIRE(ce->branches.size() == 2);
        REQUIRE(ce->else_expr == nullptr);
    }
    SECTION("aggregated CASE: sum(CASE WHEN ... THEN 1 ELSE 0 END)") {
        REQUIRE_NOTHROW(GqlParser::parse(
            "MATCH (f:Person)-[:IS_LOCATED_IN]->(c:Country) "
            "RETURN f, sum(CASE WHEN c.name = 'China' THEN 1 ELSE 0 END) AS xc, "
            "sum(CASE WHEN c.name = 'Germany' THEN 1 ELSE 0 END) AS yc"));
    }
    SECTION("zoned_datetime() scalar function in a WHERE comparison") {
        auto q = GqlParser::parse(
            "MATCH (m:Message) WHERE m.creationDate >= zoned_datetime('2010-01-01') "
            "AND m.creationDate < zoned_datetime('2014-02-09') RETURN m");
        REQUIRE(q.where_expr != nullptr);
    }
    SECTION("full pure-GQL IC3 shape with CASE + zoned_datetime parses") {
        REQUIRE_NOTHROW(GqlParser::parse(
            "MATCH TRAIL (p:Person {id: 1})-[:KNOWS]-{1,2}(f:Person) WHERE f.id <> 1 "
            "RETURN DISTINCT f "
            "NEXT "
            "FILTER NOT EXISTS { (f)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(:Country {name: 'China'}) } "
            "MATCH (f)<-[:HAS_CREATOR]-(m:Message)-[:IS_LOCATED_IN]->(c:Country) "
            "WHERE c.name IN ['China', 'Germany'] "
            "  AND m.creationDate >= zoned_datetime('2010-01-01') "
            "  AND m.creationDate < zoned_datetime('2014-02-09') "
            "RETURN f, "
            "  sum(CASE WHEN c.name = 'China' THEN 1 ELSE 0 END) AS countryXCount, "
            "  sum(CASE WHEN c.name = 'Germany' THEN 1 ELSE 0 END) AS countryYCount "
            "NEXT "
            "FILTER countryXCount > 0 AND countryYCount > 0 "
            "RETURN f.id AS personId, countryXCount, countryYCount "
            "ORDER BY countryXCount + countryYCount DESC, personId ASC "
            "LIMIT 20"));
    }
    SECTION("COUNT { } subquery-count with a bare pattern") {
        auto q = GqlParser::parse(
            "MATCH (foaf:Person) RETURN foaf.id AS id, "
            "COUNT { (foaf)<-[:HAS_CREATOR]-(:Post) } AS total");
        REQUIRE(q.returns.size() == 2);
        REQUIRE(q.returns[1].expr->kind == ExpressionKind::SIZE_OP); // COUNT{} reuses SizeExpr
    }
    SECTION("COUNT { MATCH ... WHERE EXISTS { ... } } (IC10 shape via LET)") {
        REQUIRE_NOTHROW(GqlParser::parse(
            "MATCH (p:Person {id: 1})-[:KNOWS]-(:Person)-[:KNOWS]-(foaf:Person) WHERE foaf.id <> 1 "
            "RETURN DISTINCT p, foaf "
            "NEXT "
            "FILTER NOT EXISTS { (p)-[:KNOWS]-(foaf) } "
            "LET total = COUNT { (foaf)<-[:HAS_CREATOR]-(:Post) } "
            "LET common = COUNT { MATCH (foaf)<-[:HAS_CREATOR]-(post:Post) "
            "                     WHERE EXISTS { (post)-[:HAS_TAG]->(:Tag)<-[:HAS_INTEREST]-(p) } } "
            "RETURN foaf.id AS personId, 2 * common - total AS commonInterestScore "
            "ORDER BY commonInterestScore DESC, personId ASC LIMIT 10"));
    }
    SECTION("COUNT { } whose WHERE is an EXISTS keeps the nested subquery (IC10 common)") {
        // The correlated precompute reduces a SizeExpr over its sub-rows and recurses into a WHERE that
        // nests a further subquery; lock that the parser yields exactly that SIZE_OP -> EXISTS shape.
        auto q = GqlParser::parse(
            "MATCH (p:Person) RETURN COUNT { (p)-[:KNOWS]->(x:Person) "
            "WHERE EXISTS { (x)-[:KNOWS]->(y:Person) } } AS c");
        REQUIRE(q.returns[0].expr->kind == ExpressionKind::SIZE_OP);
        auto* se = static_cast<SizeExpr*>(q.returns[0].expr.get());
        REQUIRE(se->where_expr != nullptr);
        REQUIRE(se->where_expr->kind == ExpressionKind::EXISTS);
    }
    SECTION("collect_list(DISTINCT x) is a COLLECT aggregate") {
        auto q = GqlParser::parse("MATCH (t:Tag) RETURN collect_list(DISTINCT t) AS tags");
        REQUIRE(q.returns[0].expr->kind == ExpressionKind::AGGREGATION);
        auto* agg = static_cast<AggregateExpr*>(q.returns[0].expr.get());
        REQUIRE(agg->fn_kind == AggregateKind::COLLECT);
        REQUIRE(agg->distinct);
    }
    SECTION("x IN <listExpr> (non-literal) is an InExpr membership test") {
        auto q = GqlParser::parse("MATCH (t:Tag) WHERE NOT (t IN before) RETURN t.name");
        // NOT ( t IN before ) -> UNARY_OP(NOT, IN_LIST)
        REQUIRE(q.where_expr->kind == ExpressionKind::UNARY_OP);
        auto* un = static_cast<UnaryOpExpr*>(q.where_expr.get());
        REQUIRE(un->expr->kind == ExpressionKind::IN_LIST);
    }
    SECTION("x IN [literal] still desugars to OR (not an InExpr)") {
        auto q = GqlParser::parse("MATCH (c:Country) WHERE c.name IN ['A', 'B'] RETURN c.name");
        REQUIRE(q.where_expr->kind == ExpressionKind::BINARY_OP); // OR chain, not IN_LIST
    }
    SECTION("full pure-GQL IC4 shape parses (collect_list + IN list-value)") {
        REQUIRE_NOTHROW(GqlParser::parse(
            "MATCH (:Person {id: 1})-[:KNOWS]-(:Person)<-[:HAS_CREATOR]-(pre:Post)-[:HAS_TAG]->(tb:Tag) "
            "WHERE pre.creationDate < zoned_datetime('2011-01-01') "
            "RETURN collect_list(DISTINCT tb) AS before "
            "NEXT "
            "MATCH (:Person {id: 1})-[:KNOWS]-(:Person)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(t:Tag) "
            "WHERE post.creationDate >= zoned_datetime('2011-01-01') "
            "  AND post.creationDate < zoned_datetime('2012-01-01') "
            "  AND NOT (t IN before) "
            "RETURN t.name AS tagName, count(DISTINCT post) AS postCount "
            "ORDER BY postCount DESC, tagName ASC LIMIT 10"));
    }
}
