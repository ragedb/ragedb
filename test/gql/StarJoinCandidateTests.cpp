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

// find_star_join_candidate decides whether a run of MATCH statements forms a star: two or more of
// them sharing exactly one central variable, each branch otherwise independent. Getting it wrong is
// not cosmetic -- it selects the factorized execution path, which builds the branches separately and
// joins on the centre. Recognising a star that is not one (branches that also share a non-central
// variable, or that anchor to an already-bound incoming variable) would join on the wrong key and
// drop or duplicate rows. This is a pure function over the parsed AST, so it is pinned directly.

#include <catch2/catch.hpp>
#include "../../src/gql/GqlParser.h"
#include "../../src/gql/executor/StarJoinRewriter.h"

using namespace ragedb;
using namespace ragedb::gql;

namespace {
std::vector<MatchStatement> matches_of(const std::string& text) {
    return GqlParser::parse(text).matches;
}
}  // namespace

TEST_CASE("a star join is two branches sharing exactly one central variable", "[gql_honeycomb]") {
    auto m = matches_of("MATCH (a)-[:R]->(b) MATCH (a)-[:S]->(c) RETURN a");
    auto cand = find_star_join_candidate(m, 0, {});
    REQUIRE(cand.has_value());
    REQUIRE(cand->central_var == "a");
    REQUIRE(cand->match_indices.size() == 2);
    REQUIRE(cand->match_indices[0] == 0);
    REQUIRE(cand->match_indices[1] == 1);
}

TEST_CASE("find_star_join_candidate declines the non-star shapes", "[gql_honeycomb]") {
    SECTION("fewer than two remaining matches cannot form a star") {
        auto m = matches_of("MATCH (a)-[:R]->(b) RETURN a");
        REQUIRE_FALSE(find_star_join_candidate(m, 0, {}).has_value());
    }

    SECTION("a variable shared by only one branch is not a centre") {
        // b appears in a single match, so it can never gather two branches; a appears once too.
        auto m = matches_of("MATCH (a)-[:R]->(b) MATCH (c)-[:S]->(d) RETURN a");
        REQUIRE_FALSE(find_star_join_candidate(m, 0, {}).has_value());
    }

    SECTION("branches that also share a non-central variable are not independent") {
        // Both matches bind a and b: joining on a alone would double-count the shared b.
        auto m = matches_of("MATCH (a)-[:R]->(b) MATCH (a)-[:S]->(b) RETURN a");
        REQUIRE_FALSE(find_star_join_candidate(m, 0, {}).has_value());
    }

    SECTION("a branch anchored to an incoming variable is not a free star arm") {
        // b is already bound upstream, so this arm is a probe against incoming rows, not an
        // independent branch the factorized path may build on its own.
        auto m = matches_of("MATCH (a)-[:R]->(b) MATCH (a)-[:S]->(c) RETURN a");
        REQUIRE_FALSE(find_star_join_candidate(m, 0, {"b"}).has_value());
    }

    SECTION("a synthetic EXISTS variable is never chosen as the centre") {
        auto m = matches_of("MATCH (a)-[:R]->(b) MATCH (a)-[:S]->(c) RETURN a");
        // Rename the natural centre to the reserved unnested-subquery prefix; nothing else gathers two
        // branches, so the star must dissolve.
        m[0].pattern.nodes[0].variable = "_exists_0";
        m[1].pattern.nodes[0].variable = "_exists_0";
        REQUIRE_FALSE(find_star_join_candidate(m, 0, {}).has_value());
    }
}

TEST_CASE("find_star_join_candidate scans only from match_idx onward", "[gql_honeycomb]") {
    // The first match is treated as already executed; the star is sought among the rest. Here the two
    // b-sharing matches at indices 1 and 2 form the star, and their reported indices are absolute.
    auto m = matches_of("MATCH (z:Z) MATCH (b)-[:R]->(p) MATCH (b)-[:S]->(q) RETURN b");
    auto cand = find_star_join_candidate(m, 1, {});
    REQUIRE(cand.has_value());
    REQUIRE(cand->central_var == "b");
    REQUIRE(cand->match_indices.size() == 2);
    REQUIRE(cand->match_indices[0] == 1);
    REQUIRE(cand->match_indices[1] == 2);
}
