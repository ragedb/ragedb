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

// The ISO linear-query lowering of the parser: how RETURN/NEXT/FILTER/LET and a standalone ORDER BY/LIMIT
// sort-page break a query into pipeline segments (with_segments) and where each clause lands. Split out of
// GqlParserTests.cpp, which had grown past the file-size ceiling.

#include <catch2/catch.hpp>
#include "../../src/gql/GqlParser.h"

using namespace ragedb;
using namespace ragedb::gql;

TEST_CASE("GQL ISO linear-query NEXT/FILTER lowering", "[gql_parser]") {
    SECTION("RETURN ... NEXT is a projection boundary that feeds the next statement") {
        auto next_q = GqlParser::parse(
            "MATCH (a:Person) RETURN a.id AS x NEXT MATCH (b:Person) WHERE b.id = x RETURN b.name");
        REQUIRE(next_q.with_segments.size() == 1);
        REQUIRE(next_q.with_segments[0]->returns.size() == 1);
        REQUIRE(next_q.with_segments[0]->returns[0].alias == std::string("x"));
        REQUIRE(next_q.matches.size() == 1); // the post-NEXT MATCH is the final segment
        REQUIRE(next_q.returns.size() == 1);
    }
    SECTION("a NEXT segment of only primitive query statements needs no result statement (BI Q1)") {
        auto q = GqlParser::parse(
            "MATCH (m:Message) RETURN count(m) AS totalInt "
            "NEXT "
            "LET total = CAST(totalInt AS FLOAT) "
            "NEXT "
            "MATCH (m:Message) RETURN total, count(m) AS c");
        // The LET-only segment closes with a passthrough carrying the piped column and the new one.
        REQUIRE(q.with_segments.size() == 2);
        REQUIRE(q.with_segments[1]->let_bindings.size() == 1);
        REQUIRE(q.with_segments[1]->returns.size() == 2);
        REQUIRE(q.with_segments[1]->returns[0].alias == "totalInt");
        REQUIRE(q.with_segments[1]->returns[1].alias == "total");
        REQUIRE(q.matches.size() == 1);
    }
    SECTION("a FILTER-only NEXT segment forwards the surviving rows") {
        auto q = GqlParser::parse(
            "MATCH (p:Person) RETURN p.age AS age NEXT FILTER age > 3 NEXT RETURN age");
        REQUIRE(q.with_segments.size() == 2);
        REQUIRE(q.with_segments[1]->where_expr != nullptr);
        REQUIRE(q.with_segments[1]->returns.size() == 1);
        REQUIRE(q.with_segments[1]->returns[0].alias == "age");
    }
    SECTION("a query with no result statement and no NEXT is still rejected") {
        REQUIRE_THROWS_WITH(GqlParser::parse("MATCH (p:Person)"),
                            Catch::Contains("RETURN clause"));
        REQUIRE_THROWS_WITH(GqlParser::parse("MATCH (p:Person) LET x = 1"),
                            Catch::Contains("RETURN clause"));
        // The last linear query statement has to produce a result, NEXT or not.
        REQUIRE_THROWS_WITH(GqlParser::parse("MATCH (p:Person) RETURN p.age AS a NEXT FILTER a > 3"),
                            Catch::Contains("RETURN clause"));
    }
    SECTION("RETURN DISTINCT ... NEXT carries the distinct projection forward") {
        auto q = GqlParser::parse(
            "MATCH (p:Person {id: 1})-[:KNOWS]-(f:Person) WHERE f.id <> 1 "
            "RETURN DISTINCT f "
            "NEXT "
            "MATCH (forum:Forum)-[:HAS_MEMBER]->(f) "
            "MATCH (forum)-[:CONTAINER_OF]->(post:Post)-[:HAS_CREATOR]->(f) "
            "RETURN forum.id AS forumId, count(DISTINCT post) AS postCount "
            "ORDER BY postCount DESC, forumId ASC LIMIT 20");
        REQUIRE(q.with_segments.size() == 1);
        REQUIRE(q.with_segments[0]->distinct == true);
        REQUIRE(q.matches.size() == 2);
    }
    SECTION("FILTER lowers to a segment predicate like WHERE") {
        auto q = GqlParser::parse(
            "MATCH (p:Person {id: 1})-[:KNOWS]-(f:Person) "
            "RETURN DISTINCT f "
            "NEXT "
            "FILTER NOT EXISTS { (f)-[:IS_LOCATED_IN]->(:City) } "
            "MATCH (f)<-[:HAS_CREATOR]-(m:Message)-[:IS_LOCATED_IN]->(c:Country) "
            "WHERE c.name IN ['China', 'Germany'] "
            "RETURN f.id AS pid, count(DISTINCT c) AS cnt");
        REQUIRE(q.with_segments.size() == 1);
        REQUIRE(q.where_expr != nullptr); // FILTER + WHERE combined onto the final segment
    }
    SECTION("intermediate RETURN keeps ORDER BY/LIMIT on its own segment") {
        auto q = GqlParser::parse(
            "MATCH (p:Person {id: 1})<-[:HAS_CREATOR]-(m:Message) "
            "RETURN m ORDER BY m.creationDate DESC LIMIT 20 "
            "NEXT "
            "RETURN m.creationDate AS ms");
        REQUIRE(q.with_segments.size() == 1);
        REQUIRE(q.with_segments[0]->limit.has_value());
        REQUIRE(q.with_segments[0]->limit.value() == 20);
        REQUIRE(q.with_segments[0]->order_by.size() == 1);
    }
    SECTION("openCypher WITH is rejected (pure GQL dialect)") {
        REQUIRE_THROWS_WITH(
            GqlParser::parse("MATCH (a:Person) WITH a AS x RETURN x.name"),
            Catch::Contains("WITH is not GQL"));
    }
    SECTION("an unimplemented function is a hard error, not a silent NULL") {
        // These are still not implemented -- openCypher spellings (toInteger/toString/labels/type) and
        // the Cypher-only shortestPath() function -- and used to parse, typecheck as ANY and evaluate to
        // NULL, so a query calling them returned plausible-looking wrong answers instead of failing.
        // (abs/upper/coalesce and friends ARE implemented now, so they are no longer here.)
        for (const auto& call : { "toInteger(p.age)", "toString(p.age)", "labels(p)",
                                  "keys(p)", "head(p.name)", "shortestPath(p)" }) {
            INFO("call: " << call);
            REQUIRE_THROWS_WITH(
                GqlParser::parse(std::string("MATCH (p:Person) RETURN ") + call + " AS v"),
                Catch::Contains("Unknown function"));
        }
    }
    SECTION("the implemented scalar functions still parse") {
        REQUIRE_NOTHROW(GqlParser::parse(
            "MATCH p = ANY SHORTEST (a:Person)-[:KNOWS]-{1,3}(b:Person) RETURN length(p) AS d"));
        REQUIRE_NOTHROW(GqlParser::parse(
            "MATCH (m:Post) FILTER m.creationDate >= zoned_datetime('2011-07-01') RETURN count(m) AS n"));
    }
    SECTION("openCypher collect() is rejected in favour of collect_list()") {
        REQUIRE_THROWS_WITH(
            GqlParser::parse("MATCH (a:Person) RETURN collect(a.name) AS names"),
            Catch::Contains("collect is not GQL"));
        // The ISO GQL general set function still parses, DISTINCT and all.
        auto q = GqlParser::parse("MATCH (a:Person) RETURN collect_list(DISTINCT a.name) AS names");
        REQUIRE(q.returns.size() == 1);
        REQUIRE(q.returns[0].expr->kind == ExpressionKind::AGGREGATION);
        const auto* agg = static_cast<const AggregateExpr*>(q.returns[0].expr.get());
        REQUIRE(agg->fn_kind == AggregateKind::COLLECT);
        REQUIRE(agg->distinct);
    }
    SECTION("standalone ORDER BY/LIMIT sort-page then RETURN pushes top-K to producer (IC2/IS2)") {
        auto q = GqlParser::parse(
            "MATCH (p:Person {id: 1})<-[:HAS_CREATOR]-(m:Message) "
            "RETURN m.creationDate AS ms, m.id AS mid "
            "NEXT "
            "ORDER BY ms DESC, mid ASC LIMIT 20 "
            "RETURN ms");
        REQUIRE(q.with_segments.size() == 1);
        // The ORDER BY/LIMIT is pushed onto the producing segment (which has the MATCH) so its
        // streaming top-K bounds the sort; the final segment carries neither.
        REQUIRE(q.with_segments[0]->order_by.size() == 2);
        REQUIRE(q.with_segments[0]->limit.value() == 20);
        // The producer's ORDER BY alias `ms` was resolved to its projected expression (m.creationDate).
        REQUIRE(q.with_segments[0]->order_by[0].expr->kind == ExpressionKind::PROPERTY_LOOKUP);
        REQUIRE(q.order_by.empty());
        REQUIRE(!q.limit.has_value());
    }
    SECTION("standalone ORDER BY/LIMIT sort-page then MATCH (IS5 top-1 then expand)") {
        auto q = GqlParser::parse(
            "MATCH (p:Person {id: 1})<-[:HAS_CREATOR]-(m:Message) "
            "RETURN m "
            "NEXT "
            "ORDER BY m.creationDate DESC LIMIT 1 "
            "MATCH (m)-[:HAS_CREATOR]->(creator:Person) "
            "RETURN creator.id AS creatorId");
        // Two pipeline segments: the RETURN m projection and the sort/page passthrough of m.
        REQUIRE(q.with_segments.size() == 2);
        REQUIRE(q.with_segments[1]->limit.value() == 1);
        REQUIRE(q.with_segments[1]->order_by.size() == 1);
        REQUIRE(q.with_segments[1]->returns.size() == 1); // passthrough of m
        REQUIRE(q.matches.size() == 1);                    // the post-sort MATCH
    }
    SECTION("standalone ORDER BY/LIMIT after this segment's own MATCH sorts before the RETURN") {
        auto q = GqlParser::parse(
            "MATCH (p:Person) "
            "ORDER BY p.age DESC LIMIT 10 "
            "RETURN p.name AS name");
        // The sort/page closes the segment: it must take effect on the matched rows, not on the
        // projection, so the MATCH plus a passthrough of `p` becomes the producing segment.
        REQUIRE(q.with_segments.size() == 1);
        REQUIRE(q.with_segments[0]->matches.size() == 1);
        REQUIRE(q.with_segments[0]->order_by.size() == 1);
        REQUIRE(q.with_segments[0]->limit.value() == 10);
        REQUIRE(q.with_segments[0]->returns.size() == 1);
        REQUIRE(q.with_segments[0]->returns[0].alias == "p");
        REQUIRE(q.matches.empty());
        REQUIRE(q.order_by.empty());
        REQUIRE(!q.limit.has_value());
    }
    SECTION("standalone ORDER BY/LIMIT sorts on a LET binding of the same segment") {
        auto q = GqlParser::parse(
            "MATCH (p:Person) "
            "LET score = p.likes / 2 "
            "ORDER BY score DESC LIMIT 5 "
            "RETURN p.id AS personId");
        REQUIRE(q.with_segments.size() == 1);
        REQUIRE(q.with_segments[0]->let_bindings.size() == 1);
        REQUIRE(q.with_segments[0]->order_by.size() == 1);
        REQUIRE(q.with_segments[0]->limit.value() == 5);
        // The passthrough carries both the pattern variable and the LET column forward.
        REQUIRE(q.with_segments[0]->returns.size() == 2);
        REQUIRE(q.with_segments[0]->returns[0].alias == "p");
        REQUIRE(q.with_segments[0]->returns[1].alias == "score");
        REQUIRE(q.returns.size() == 1);
        REQUIRE(q.returns[0].alias == "personId");
    }
    SECTION("standalone ORDER BY/LIMIT on piped rows with a LET closes the segment (BI Q13 tail)") {
        auto q = GqlParser::parse(
            "MATCH (z:Person) RETURN z AS zombie, count(z) AS likes "
            "NEXT "
            "LET zombieScore = likes / 2 "
            "ORDER BY zombieScore DESC, zombie.id ASC LIMIT 100 "
            "RETURN zombie.id AS zombieId, likes");
        // Producer projection, then the LET+sort/page segment, then the final re-projection. The
        // ordering cannot be pushed into the producer: its key is this segment's LET column.
        REQUIRE(q.with_segments.size() == 2);
        REQUIRE(q.with_segments[0]->order_by.empty());
        REQUIRE(q.with_segments[1]->let_bindings.size() == 1);
        REQUIRE(q.with_segments[1]->order_by.size() == 2);
        REQUIRE(q.with_segments[1]->limit.value() == 100);
        REQUIRE(q.with_segments[1]->returns.size() == 3); // zombie, likes, zombieScore
        REQUIRE(q.returns.size() == 2);
    }
    SECTION("a page before an aggregating RETURN bounds the rows fed to the aggregate") {
        auto q = GqlParser::parse(
            "MATCH (p:Person) ORDER BY p.age DESC LIMIT 10 RETURN count(p) AS c");
        // The LIMIT belongs to the producing segment, not to the one-row aggregate result.
        REQUIRE(q.with_segments.size() == 1);
        REQUIRE(q.with_segments[0]->limit.value() == 10);
        REQUIRE(!q.limit.has_value());
        REQUIRE(q.returns.size() == 1);
        REQUIRE(q.returns[0].expr->kind == ExpressionKind::AGGREGATION);
    }
    SECTION("LET binds a computed column usable by a later FILTER/RETURN") {
        auto q = GqlParser::parse(
            "MATCH (p:Person) LET fullName = p.firstName || ' ' || p.lastName "
            "FILTER fullName <> '' RETURN fullName");
        REQUIRE(q.let_bindings.size() == 1);
        REQUIRE(q.let_bindings[0].alias.has_value());
        REQUIRE(q.let_bindings[0].alias.value() == "fullName");
        REQUIRE(q.where_expr != nullptr); // FILTER lowered to a predicate
    }
    SECTION("multiple LET bindings, comma-separated") {
        auto q = GqlParser::parse("MATCH (p:Person) LET a = p.x, b = p.y RETURN a, b");
        REQUIRE(q.let_bindings.size() == 2);
    }
    SECTION("full pure-GQL IC5 shape parses (RETURN DISTINCT ... NEXT ... aggregate)") {
        REQUIRE_NOTHROW(GqlParser::parse(
            "MATCH TRAIL (p:Person {id: 4398046519825})-[:KNOWS]-{1,2}(f:Person) "
            "WHERE f.id <> 4398046519825 "
            "RETURN DISTINCT f "
            "NEXT "
            "MATCH (forum:Forum)-[hm:HAS_MEMBER]->(f) "
            "WHERE hm.joinDate >= 100 "
            "MATCH (forum)-[:CONTAINER_OF]->(post:Post)-[:HAS_CREATOR]->(f) "
            "RETURN forum.id AS forumId, count(DISTINCT post) AS postCount "
            "ORDER BY postCount DESC, forumId ASC LIMIT 20"));
    }
    SECTION("full pure-GQL IC3 shape parses (FILTER + NOT EXISTS + IN + multi-NEXT)") {
        REQUIRE_NOTHROW(GqlParser::parse(
            "MATCH TRAIL (p:Person {id: 1})-[:KNOWS]-{1,2}(f:Person) WHERE f.id <> 1 "
            "RETURN DISTINCT f "
            "NEXT "
            "FILTER NOT EXISTS { (f)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(:Country {name: 'China'}) } "
            "  AND NOT EXISTS { (f)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(:Country {name: 'Germany'}) } "
            "MATCH (f)<-[:HAS_CREATOR]-(m:Message)-[:IS_LOCATED_IN]->(c:Country) "
            "WHERE c.name IN ['China', 'Germany'] "
            "RETURN f.id AS personId, count(DISTINCT c) AS cnt "
            "ORDER BY cnt DESC, personId ASC LIMIT 20"));
    }
}
