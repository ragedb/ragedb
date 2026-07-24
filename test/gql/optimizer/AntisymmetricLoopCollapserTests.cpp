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

// AntisymmetricLoopCollapser folds an antisymmetric 2-cycle (a)-[:R]->(b) plus (b)-[:R]->(a): antisymmetry
// forces a == b. The rewrite is result-changing, so it is gated tightly on registered algebraic traits and
// on the edges being totally unconstrained. An irreflexive relation makes the forced self-loop impossible,
// so the query matches nothing (no_op); a reflexive one makes it trivially true, so both edges drop; with
// neither trait one self-loop is kept as the residual constraint. These pin the three firing outcomes and
// the guards that must decline. The pass is exercised in isolation because it only runs when traits are
// registered, which the full pipeline's other passes do not set up here.

#include <catch2/catch.hpp>
#include "../../../src/gql/GqlParser.h"
#include "../../../src/gql/GqlVirtualCatalog.h"
#include "../../../src/gql/optimizer/AntisymmetricLoopCollapser.h"

using namespace ragedb;
using namespace ragedb::gql;

namespace {
size_t edge_count(const GqlQuery& q) {
    size_t e = 0;
    for (const auto& m : q.matches) e += m.pattern.edges.size();
    return e;
}
// Register PRECEDES with the given traits, parse, run only the collapser, return the rewritten query.
GqlQuery collapsed(const std::string& q, const std::unordered_set<std::string>& traits) {
    GqlVirtualCatalog::local().clear();
    if (!traits.empty()) GqlVirtualCatalog::local().set_relationship_algebraic_properties("PRECEDES", traits);
    GqlQuery query = GqlParser::parse(q);
    AntisymmetricLoopCollapser::antisymmetric_loop_pass(query);
    GqlVirtualCatalog::local().clear();
    return query;
}
const std::string kCycle =
    "MATCH (a:N)-[:PRECEDES]->(b:N) MATCH (b:N)-[:PRECEDES]->(a:N) RETURN a";
}  // namespace

TEST_CASE("antisymmetric 2-cycle collapse fires per the relation's traits", "[gql_optimizer]") {
    SECTION("irreflexive: the forced self-loop is impossible, so the query matches nothing") {
        auto q = collapsed(kCycle, {"antisymmetric", "irreflexive"});
        REQUIRE(q.no_op);
    }

    SECTION("reflexive: the forced self-loop is trivially true, so both edges drop") {
        auto q = collapsed(kCycle, {"antisymmetric", "reflexive"});
        REQUIRE_FALSE(q.no_op);
        REQUIRE(edge_count(q) == 0);
    }

    SECTION("antisymmetric alone: one self-loop is kept as the residual constraint") {
        auto q = collapsed(kCycle, {"antisymmetric"});
        REQUIRE_FALSE(q.no_op);
        REQUIRE(edge_count(q) == 1);
    }
}

TEST_CASE("antisymmetric 2-cycle collapse declines when its preconditions are unmet", "[gql_optimizer]") {
    SECTION("no algebraic trait registered: the pass cannot fire") {
        auto q = collapsed(kCycle, {});
        REQUIRE_FALSE(q.no_op);
        REQUIRE(edge_count(q) == 2);
    }

    SECTION("a constrained edge (bound variable) is never collapsed") {
        // Collapse deletes an edge; if the edge bound a variable or carried predicates they would be lost.
        auto q = collapsed(
            "MATCH (a:N)-[e:PRECEDES]->(b:N) MATCH (b:N)-[:PRECEDES]->(a:N) RETURN a", {"antisymmetric"});
        REQUIRE(edge_count(q) == 2);
    }

    SECTION("two edges that do not form a 2-cycle are left alone") {
        auto q = collapsed(
            "MATCH (a:N)-[:PRECEDES]->(b:N) MATCH (b:N)-[:PRECEDES]->(c:N) RETURN a", {"antisymmetric"});
        REQUIRE_FALSE(q.no_op);
        REQUIRE(edge_count(q) == 2);
    }
}
