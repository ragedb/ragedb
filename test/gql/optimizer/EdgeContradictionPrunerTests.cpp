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

// EdgeContradictionPruner marks a query no_op when a REQUIRED edge's attribute range is empty (e.g.
// weight > 5 AND weight < 2) -- that edge can never match, so the query is empty. A false positive silently
// returns an empty result, so the crucial boundary is the OPTIONAL match: an optional edge that cannot
// match does NOT empty the query, it null-extends the anchor rows. These pin the firing required-edge cases
// and the optional-edge case that must decline (the regression the fix locks in).

#include <catch2/catch.hpp>
#include "../../../src/gql/GqlParser.h"
#include "../../../src/gql/GqlVirtualCatalog.h"
#include "../../../src/gql/optimizer/EdgeContradictionPruner.h"

using namespace ragedb;
using namespace ragedb::gql;

namespace {
bool prunes(const std::string& q) {
    GqlVirtualCatalog::local().clear();   // no catalog constraints: exercise the internal-contradiction check
    GqlQuery query = GqlParser::parse(q);
    EdgeContradictionPruner::edge_contradiction_pruning_pass(query);
    bool no_op = query.no_op;
    GqlVirtualCatalog::local().clear();
    return no_op;
}
}  // namespace

TEST_CASE("edge contradiction on a required edge prunes to no_op", "[gql_optimizer]") {
    SECTION("an inline edge WHERE with an empty range empties the query") {
        REQUIRE(prunes("MATCH (a:N)-[r:KNOWS WHERE r.weight > 5 AND r.weight < 2]->(b:N) RETURN a"));
    }
    SECTION("a query-level WHERE with an empty range on a required edge empties the query") {
        REQUIRE(prunes("MATCH (a:N)-[r:KNOWS]->(b:N) WHERE r.weight > 5 AND r.weight < 2 RETURN a"));
    }
    SECTION("a required-edge contradiction still fires when an optional match is also present") {
        REQUIRE(prunes(
            "MATCH (a:N)-[r:KNOWS WHERE r.weight > 5 AND r.weight < 2]->(b:N) "
            "OPTIONAL MATCH (a)-[s:LIKES]->(c:N) RETURN a"));
    }
}

TEST_CASE("edge contradiction pruner declines when the edge cannot empty the query", "[gql_optimizer]") {
    SECTION("an OPTIONAL edge's inline contradiction null-extends, it does not empty the query") {
        // The optional match finds no edge satisfying the impossible range, so every anchor survives with a
        // null edge -- a non-empty result. Marking no_op here was a wrong-answer bug.
        REQUIRE_FALSE(prunes(
            "MATCH (a:N) OPTIONAL MATCH (a)-[r:KNOWS WHERE r.weight > 5 AND r.weight < 2]->(b:N) RETURN a"));
    }
    SECTION("an OPTIONAL edge is not treated as a required contradiction for a post-filter either") {
        // A query-level contradiction is the query-level pruner's concern, and the executor's post-filter
        // still empties this; this pass conservatively declines rather than reason about an optional edge.
        REQUIRE_FALSE(prunes(
            "MATCH (a:N) OPTIONAL MATCH (a)-[r:KNOWS]->(b:N) WHERE r.weight > 5 AND r.weight < 2 RETURN a"));
    }
    SECTION("a satisfiable edge range is left alone") {
        REQUIRE_FALSE(prunes("MATCH (a:N)-[r:KNOWS WHERE r.weight > 5 AND r.weight < 20]->(b:N) RETURN a"));
    }
}
