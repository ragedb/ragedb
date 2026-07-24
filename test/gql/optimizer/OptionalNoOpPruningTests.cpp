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

// A whole class of no_op / contradiction pruners short-circuit a query to empty when a pattern is proven
// impossible (an unreachable edge, an empty attribute range, an unsatisfiable domain formula). That is
// correct only for a REQUIRED pattern. An OPTIONAL pattern that cannot match null-extends the anchor rows
// instead -- the query is NOT empty. Several passes iterated query.matches without an is_optional guard and
// wrongly marked the whole query no_op (a silent empty-result wrong-answer). These full-pipeline cases pin
// that an impossible OPTIONAL pattern is NEVER pruned to no_op, while the required forms still are, across
// SchemaReachabilityPruner, EdgeContradictionPruner, ContradictionPruner and DomainConstraintReasoner.

#include <catch2/catch.hpp>
#include "../../../src/gql/GqlParser.h"
#include "../../../src/gql/GqlOptimizer.h"
#include "../../../src/gql/GqlVirtualCatalog.h"

using namespace ragedb;
using namespace ragedb::gql;

namespace {
bool optimizes_to_noop(const std::string& q, bool register_schema = false) {
    GqlVirtualCatalog::local().clear();
    if (register_schema) GqlVirtualCatalog::local().add_allowed_relationship("N", "GOOD", "M");
    GqlQuery query = GqlParser::parse(q);
    GqlOptimizer::optimize(query);
    bool no_op = query.no_op;
    GqlVirtualCatalog::local().clear();
    return no_op;
}

bool optimizes_to_noop_disjoint(const std::string& q) {
    GqlVirtualCatalog::local().clear();
    GqlVirtualCatalog::local().add_disjoint_labels("X", "Y");   // X and Y are disjoint concepts
    GqlQuery query = GqlParser::parse(q);
    GqlOptimizer::optimize(query);
    bool no_op = query.no_op;
    GqlVirtualCatalog::local().clear();
    return no_op;
}

bool optimizes_to_noop_taxonomy(const std::string& q) {
    GqlVirtualCatalog::local().clear();
    // A value hierarchy on Cat.code: 'a' -SUBCAT-> 'b' -SUBCAT-> 'c', so 'a' reaches 'c' only in two hops.
    GqlVirtualCatalog::local().add_constraint("h1", "MATCH (x:Cat {code: 'a'})-[:SUBCAT]->(y:Cat {code: 'b'}) RETURN x");
    GqlVirtualCatalog::local().add_constraint("h2", "MATCH (x:Cat {code: 'b'})-[:SUBCAT]->(y:Cat {code: 'c'}) RETURN x");
    GqlQuery query = GqlParser::parse(q);
    GqlOptimizer::optimize(query);
    bool no_op = query.no_op;
    GqlVirtualCatalog::local().clear();
    return no_op;
}
}  // namespace

TEST_CASE("an impossible REQUIRED pattern is still pruned to no_op", "[gql_optimizer]") {
    SECTION("a schema-unreachable required edge empties the query") {
        REQUIRE(optimizes_to_noop("MATCH (a:N)-[:BADREL]->(b:M) RETURN a", /*schema=*/true));
    }
    SECTION("an empty attribute range on a required node empties the query") {
        REQUIRE(optimizes_to_noop("MATCH (a:N)-[:R]->(b:M WHERE b.age > 5 AND b.age < 2) RETURN a"));
    }
    SECTION("an empty attribute range on a required edge empties the query") {
        REQUIRE(optimizes_to_noop("MATCH (a:N)-[r:KNOWS WHERE r.weight > 5 AND r.weight < 2]->(b:M) RETURN a"));
    }
    SECTION("a required anchor contradiction still fires when an optional match is also present") {
        REQUIRE(optimizes_to_noop(
            "MATCH (a:N WHERE a.age > 5 AND a.age < 2) OPTIONAL MATCH (a)-[:R]->(b:M) RETURN a"));
    }
    SECTION("a required variable-length edge between disjoint labels empties the query") {
        REQUIRE(optimizes_to_noop_disjoint("MATCH (a:X)-[:R*1..2]->(b:Y) RETURN a"));
    }
    SECTION("a required edge whose value hierarchy is unreachable in one hop empties the query") {
        REQUIRE(optimizes_to_noop_taxonomy("MATCH (x:Cat {code: 'a'})-[:SUBCAT]->(y:Cat {code: 'c'}) RETURN x"));
    }
}

TEST_CASE("an impossible OPTIONAL pattern null-extends and is never pruned to no_op", "[gql_optimizer]") {
    // In every case the optional pattern cannot match, so each anchor survives with null bindings -- a
    // non-empty result. Marking the query no_op here was a silent wrong-answer.
    SECTION("a schema-unreachable optional edge") {
        REQUIRE_FALSE(optimizes_to_noop(
            "MATCH (a:N) OPTIONAL MATCH (a)-[:BADREL]->(b:M) RETURN a", /*schema=*/true));
    }
    SECTION("an empty attribute range on an optional node") {
        REQUIRE_FALSE(optimizes_to_noop(
            "MATCH (a:N) OPTIONAL MATCH (a)-[:R]->(b:M WHERE b.age > 5 AND b.age < 2) RETURN a"));
    }
    SECTION("an empty attribute range on an optional edge") {
        REQUIRE_FALSE(optimizes_to_noop(
            "MATCH (a:N) OPTIONAL MATCH (a)-[r:KNOWS WHERE r.weight > 5 AND r.weight < 2]->(b:M) RETURN a"));
    }
    SECTION("an optional variable-length edge between disjoint labels") {
        REQUIRE_FALSE(optimizes_to_noop_disjoint("MATCH (n:N) OPTIONAL MATCH (a:X)-[:R*1..2]->(b:Y) RETURN n"));
    }
    SECTION("an optional edge whose value hierarchy is unreachable in one hop") {
        REQUIRE_FALSE(optimizes_to_noop_taxonomy(
            "MATCH (n:N) OPTIONAL MATCH (x:Cat {code: 'a'})-[:SUBCAT]->(y:Cat {code: 'c'}) RETURN n"));
    }
}
