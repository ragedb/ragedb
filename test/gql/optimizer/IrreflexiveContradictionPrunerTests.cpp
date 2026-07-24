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

// IrreflexiveContradictionPruner marks a query no_op when it forces a DIRECT self-loop on an irreflexive
// relationship -- (a)-[:R]->(a), or (a)-[:R]->(b) where a and b are proven equal by the WHERE. Since a
// false positive silently returns an EMPTY result, the guards matter: a variable-length edge can reach the
// same node via a longer path (satisfiable), anonymous endpoints are distinct nodes, an equality under an
// OR is not guaranteed, and the relation must actually carry the irreflexive trait. These pin the firing
// self-loop cases and every guard that must decline. Exercised in isolation because the pass only runs when
// algebraic traits are registered.

#include <catch2/catch.hpp>
#include "../../../src/gql/GqlParser.h"
#include "../../../src/gql/GqlVirtualCatalog.h"
#include "../../../src/gql/optimizer/IrreflexiveContradictionPruner.h"

using namespace ragedb;
using namespace ragedb::gql;

namespace {
// Register PRECEDES with the given traits, parse, run only the pruner, return whether it marked no_op.
bool prunes(const std::string& q, const std::unordered_set<std::string>& traits) {
    GqlVirtualCatalog::local().clear();
    if (!traits.empty()) GqlVirtualCatalog::local().set_relationship_algebraic_properties("PRECEDES", traits);
    GqlQuery query = GqlParser::parse(q);
    IrreflexiveContradictionPruner::irreflexive_contradiction_pass(query);
    bool no_op = query.no_op;
    GqlVirtualCatalog::local().clear();
    return no_op;
}
}  // namespace

TEST_CASE("irreflexive self-loop is pruned to no_op", "[gql_optimizer]") {
    SECTION("a same-variable self-loop is impossible") {
        REQUIRE(prunes("MATCH (a:N)-[:PRECEDES]->(a) RETURN a", {"irreflexive"}));
    }
    SECTION("endpoints proven equal by a WHERE equality") {
        REQUIRE(prunes("MATCH (a:N)-[:PRECEDES]->(b:N) WHERE a = b RETURN a", {"irreflexive"}));
    }
    SECTION("endpoints proven equal by matching id properties (unique-key convention)") {
        REQUIRE(prunes("MATCH (a:N)-[:PRECEDES]->(b:N) WHERE a.id = b.id RETURN a", {"irreflexive"}));
    }
    SECTION("equality is followed transitively (a = b AND b = c makes a and c equal)") {
        REQUIRE(prunes("MATCH (a:N)-[:PRECEDES]->(c:N) WHERE a = b AND b = c RETURN a", {"irreflexive"}));
    }
}

TEST_CASE("irreflexive pruner declines when the self-loop is not forced", "[gql_optimizer]") {
    SECTION("distinct endpoints with no equality are satisfiable") {
        REQUIRE_FALSE(prunes("MATCH (a:N)-[:PRECEDES]->(b:N) RETURN a", {"irreflexive"}));
    }
    SECTION("a variable-length edge can return to the same node via a longer path") {
        REQUIRE_FALSE(prunes("MATCH (a:N)-[:PRECEDES*2..2]->(a) RETURN a", {"irreflexive"}));
    }
    SECTION("an equality under an OR is not guaranteed, so it cannot force the loop") {
        REQUIRE_FALSE(prunes("MATCH (a:N)-[:PRECEDES]->(b:N) WHERE a = b OR a.name = 'x' RETURN a", {"irreflexive"}));
    }
    SECTION("a relation without the irreflexive trait permits the self-loop") {
        REQUIRE_FALSE(prunes("MATCH (a:N)-[:PRECEDES]->(a) RETURN a", {"reflexive"}));
    }
    SECTION("no algebraic trait registered: the pass cannot fire") {
        REQUIRE_FALSE(prunes("MATCH (a:N)-[:PRECEDES]->(a) RETURN a", {}));
    }
}
