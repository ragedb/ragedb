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

// The CALL procedure family: fts.search and algo.propagate (which parse into search / propagate match
// statements with YIELD columns) and the CALL (imports) { subquery } scoped-subquery segment. Split out of
// GqlParserTests.cpp to keep it under the file-size target.

#include <catch2/catch.hpp>
#include "../../src/gql/GqlParser.h"

using namespace ragedb;
using namespace ragedb::gql;

TEST_CASE("CALL fts.search(...) YIELD translates to a search match statement", "[gql_parser]") {
    // spb fts shape: a full-text search prefix binding a node variable, then a follow-on MATCH uses it.
    auto q = GqlParser::parse(
        "CALL fts.search('CreativeWork', 'title', 'policy') YIELD node AS w "
        "MATCH (w)-[:about]->(x) RETURN w");
    REQUIRE(q.matches.size() >= 2);
    REQUIRE(q.matches[0].is_search);
    REQUIRE(q.matches[0].search_type == "CreativeWork");
    REQUIRE(q.matches[0].search_properties.size() == 1);
    REQUIRE(q.matches[0].search_properties[0] == "title");
    REQUIRE(q.matches[0].search_string == "policy");
    REQUIRE(q.matches[0].yield_var == "w");
    REQUIRE(q.matches[1].is_search == false);   // the follow-on MATCH
}

TEST_CASE("CALL algo.propagate(...) YIELD parses into a propagate match statement", "[gql_parser]") {
    // finbench CR8 shape: a value-propagating first-claim BFS over correlated seed/value lists, then
    // a follow-on RETURN reads the three yielded columns (node, value, depth).
    auto q = GqlParser::parse(
        "CALL algo.propagate(seeds, vals, ['transfer', 'withdraw'], 'out', 3, 'amount', 'asc', 10000) "
        "YIELD node AS dst, value AS inflow, depth AS dist "
        "RETURN dst.id AS dstId");
    REQUIRE(q.matches.size() >= 1);
    REQUIRE(q.matches[0].is_propagate);
    REQUIRE(q.matches[0].is_search == false);
    REQUIRE(q.matches[0].propagate_args.size() == 8);
    REQUIRE(q.matches[0].yield_var == "dst");
    REQUIRE(q.matches[0].yield_score_var == "inflow");
    REQUIRE(q.matches[0].yield_depth_var == "dist");
}

TEST_CASE("CALL (imports) { subquery } parses a scoped subquery segment", "[gql_parser]") {
    // bi q4 shape: import an outer variable into a nested UNION ALL subquery whose rows feed the segment.
    auto q = GqlParser::parse(
        "CALL (topForums) {"
        "  FOR f IN topForums MATCH (f)-[:CONTAINER_OF]->(p:Post) RETURN p AS person, count(p) AS c"
        "  UNION ALL"
        "  FOR f IN topForums MATCH (x:Person)<-[:HAS_MEMBER]-(f) RETURN x AS person, 0 AS c"
        "} "
        "RETURN person, sum(c) AS total ORDER BY total DESC");
    REQUIRE(q.call_import_vars.size() == 1);
    REQUIRE(q.call_import_vars[0] == "topForums");
    REQUIRE(q.call_subquery != nullptr);
    REQUIRE(q.call_subquery->kind == QueryKind::UNION_ALL);
    REQUIRE(q.returns.size() == 2);
}

TEST_CASE("CALL (imports) { subquery } accepts a single-branch body and multiple imports", "[gql_parser]") {
    auto q = GqlParser::parse(
        "CALL (xs, ys) { FOR x IN xs MATCH (p:Person) RETURN p AS person } RETURN person");
    REQUIRE(q.call_import_vars.size() == 2);
    REQUIRE(q.call_import_vars[0] == "xs");
    REQUIRE(q.call_import_vars[1] == "ys");
    REQUIRE(q.call_subquery != nullptr);
    REQUIRE(q.call_subquery->kind == QueryKind::SINGLE);
}

TEST_CASE("CALL algo.propagate YIELD columns are order-independent and default to their names", "[gql_parser]") {
    auto q = GqlParser::parse(
        "CALL algo.propagate(s, v, ['t'], 'out', 2, 'amount', 'desc', 0) "
        "YIELD depth, node, value "
        "RETURN node");
    REQUIRE(q.matches[0].is_propagate);
    REQUIRE(q.matches[0].yield_var == "node");
    REQUIRE(q.matches[0].yield_score_var == "value");
    REQUIRE(q.matches[0].yield_depth_var == "depth");
}
