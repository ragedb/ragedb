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

// EqualityJoinEliminator collapses two isomorphic self-join arms whose targets are equated in the
// WHERE (MATCH (a)-[:R]->(b), (a)-[:R]->(c) WHERE b = c). Unlike a bare mandatory-join strip, the
// equality pins b and c to the same node, so the cross product collapses to its diagonal -- exactly one
// binding per R-neighbour, multiplicity 1. Erasing the duplicate arm therefore preserves the result
// under every semantics (bag, count, DISTINCT). The pass had only a timing benchmark; these pin that it
// collapses when the equality makes it sound and leaves a plain self-join alone when it does not.

#include <catch2/catch.hpp>
#include "../../../src/gql/GqlParser.h"
#include "../../../src/gql/GqlOptimizer.h"
#include "../../../src/gql/GqlVirtualCatalog.h"

using namespace ragedb;
using namespace ragedb::gql;

namespace {
GqlQuery optimized(const std::string& text) {
    GqlVirtualCatalog::local().clear();
    auto query = GqlParser::parse(text);
    GqlOptimizer::optimize(query);
    return query;
}
}  // namespace

TEST_CASE("equality join elimination collapses an equated self-join", "[gql_optimizer]") {
    SECTION("bag projection: WHERE b = c collapses the two arms to one") {
        auto q = optimized("MATCH (a:Person)-[:KNOWS]->(b:Person) MATCH (a:Person)-[:KNOWS]->(c:Person) "
                           "WHERE b = c RETURN a.name");
        REQUIRE(q.matches.size() == 1);
    }
    SECTION("count(*): the collapse is still sound (diagonal = single traversal)") {
        auto q = optimized("MATCH (a:Person)-[:KNOWS]->(b:Person) MATCH (a:Person)-[:KNOWS]->(c:Person) "
                           "WHERE b = c RETURN count(*) AS n");
        REQUIRE(q.matches.size() == 1);
    }
    SECTION("DISTINCT: collapses as well") {
        auto q = optimized("MATCH (a:Person)-[:KNOWS]->(b:Person) MATCH (a:Person)-[:KNOWS]->(c:Person) "
                           "WHERE b = c RETURN DISTINCT a.name");
        REQUIRE(q.matches.size() == 1);
    }
}

TEST_CASE("equality join elimination leaves a genuine self-join in place", "[gql_optimizer]") {
    SECTION("no equality: the two arms are an independent bag self-join, not collapsed") {
        auto q = optimized("MATCH (a:Person)-[:KNOWS]->(b:Person) MATCH (a:Person)-[:KNOWS]->(c:Person) "
                           "RETURN a.name");
        REQUIRE(q.matches.size() == 2);
    }
    SECTION("an inequality does not equate the targets") {
        auto q = optimized("MATCH (a:Person)-[:KNOWS]->(b:Person) MATCH (a:Person)-[:KNOWS]->(c:Person) "
                           "WHERE b <> c RETURN a.name");
        REQUIRE(q.matches.size() == 2);
    }
}
