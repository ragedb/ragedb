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

// Variable-length edge quantifier bounds. ragedb accepts two spellings with DIFFERENT lower-bound
// defaults: the Cypher in-bracket form `-[:R*]->` is 1..inf, while the ISO GQL postfix form `-[:R]->*` is
// 0..inf. An omitted lower bound defaults to 1 in-bracket (`*..3` = 1..3) but to 0 postfix (`->{,3}` = 0..3).
// A one-off here silently changes which paths match, so the bounds are pinned directly on the parsed edge.

#include <catch2/catch.hpp>
#include <limits>
#include "../../src/gql/GqlParser.h"

using namespace ragedb;
using namespace ragedb::gql;

namespace {
constexpr uint64_t INF = std::numeric_limits<uint64_t>::max();

const PatternEdge& first_edge(GqlQuery& q, const std::string& pattern) {
    q = GqlParser::parse("MATCH " + pattern + " RETURN a");
    for (const auto& m : q.matches) {
        if (!m.pattern.edges.empty()) return m.pattern.edges[0];
    }
    FAIL("pattern produced no edge: " << pattern);
    return q.matches[0].pattern.edges[0];  // unreachable
}
}  // namespace

TEST_CASE("Cypher in-bracket variable-length bounds (lower defaults to 1)", "[gql_parser]") {
    GqlQuery q;
    SECTION("*1..3 is 1..3") {
        const auto& e = first_edge(q, "(a)-[:R*1..3]->(b)");
        REQUIRE(e.is_variable_length); REQUIRE(e.min_hops == 1); REQUIRE(e.max_hops == 3);
    }
    SECTION("*2.. is 2..inf") {
        const auto& e = first_edge(q, "(a)-[:R*2..]->(b)");
        REQUIRE(e.min_hops == 2); REQUIRE(e.max_hops == INF);
    }
    SECTION("*..3 is 1..3 -- omitted lower defaults to 1 in the Cypher form") {
        const auto& e = first_edge(q, "(a)-[:R*..3]->(b)");
        REQUIRE(e.min_hops == 1); REQUIRE(e.max_hops == 3);
    }
    SECTION("bare * is 1..inf") {
        const auto& e = first_edge(q, "(a)-[:R*]->(b)");
        REQUIRE(e.min_hops == 1); REQUIRE(e.max_hops == INF);
    }
    SECTION("*3 is exactly 3") {
        const auto& e = first_edge(q, "(a)-[:R*3]->(b)");
        REQUIRE(e.min_hops == 3); REQUIRE(e.max_hops == 3);
    }
}

TEST_CASE("ISO GQL postfix quantifier bounds (lower defaults to 0)", "[gql_parser]") {
    GqlQuery q;
    SECTION("postfix * is 0..inf (zero or more), unlike the in-bracket form") {
        const auto& e = first_edge(q, "(a)-[:R]->*(b)");
        REQUIRE(e.is_variable_length); REQUIRE(e.min_hops == 0); REQUIRE(e.max_hops == INF);
    }
    SECTION("postfix + is 1..inf") {
        const auto& e = first_edge(q, "(a)-[:R]->+(b)");
        REQUIRE(e.min_hops == 1); REQUIRE(e.max_hops == INF);
    }
    SECTION("{1,3} is 1..3") {
        const auto& e = first_edge(q, "(a)-[:R]->{1,3}(b)");
        REQUIRE(e.min_hops == 1); REQUIRE(e.max_hops == 3);
    }
    SECTION("{2,} is 2..inf") {
        const auto& e = first_edge(q, "(a)-[:R]->{2,}(b)");
        REQUIRE(e.min_hops == 2); REQUIRE(e.max_hops == INF);
    }
    SECTION("{,3} is 0..3 -- omitted lower defaults to 0 in the postfix form") {
        const auto& e = first_edge(q, "(a)-[:R]->{,3}(b)");
        REQUIRE(e.min_hops == 0); REQUIRE(e.max_hops == 3);
    }
    SECTION("{3} is exactly 3") {
        const auto& e = first_edge(q, "(a)-[:R]->{3}(b)");
        REQUIRE(e.min_hops == 3); REQUIRE(e.max_hops == 3);
    }
}
