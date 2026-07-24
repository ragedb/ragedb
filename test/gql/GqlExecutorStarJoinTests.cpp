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

// The candidate check is pinned in StarJoinCandidateTests; this exercises the executor half --
// execute_match_chain_factorized -- against a real graph. A star query joins independent branches on
// their shared centre, and the hazard is a branch that joins on the wrong key: the branches would
// cross-multiply across centres instead of pairing within each centre. That is silent -- the answer is
// just a larger or smaller set of plausible rows -- so the guard is an exact row-set assertion on a
// graph where the two failure modes give visibly different counts.

#include <catch2/catch.hpp>
#include <Graph.h>
#include "../../src/gql/GqlParser.h"
#include "../../src/gql/GqlOptimizer.h"
#include "../../src/gql/GqlExecutor.h"

using namespace ragedb;
using namespace ragedb::gql;

namespace {
struct StopGuard {
    Graph& g;
    ~StopGuard() { g.Stop().get(); }
};
}  // namespace

// Own tag, deliberately not [gql_honeycomb]: the per-tag verification runs one tag per process, and
// this builds a graph. Sharing the graph-heavy honeycomb tag (whose WCOJ cases construct and tear down
// 1000-node graphs) tips the process over the known seastar multi-graph-teardown limit and it SIGSEGVs
// during a later section -- an artifact of running too many graph lifecycles in one process, not of the
// query, which runs clean in isolation.
TEST_CASE("a star query pairs branches within each centre, not across centres", "[gql_star_exec]") {
    auto graph = Graph("gql_test_star_join_exec");
    graph.Start().get();
    graph.Clear();
    StopGuard guard{graph};

    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().RelationshipTypeInsertPeered("KNOWS").get();
    graph.shard.local().RelationshipTypeInsertPeered("LIKES").get();

    auto person = [&](const std::string& name) {
        return graph.shard.local().NodeAddPeered("Person", name, "{\"name\": \"" + name + "\"}").get();
    };
    // Two independent stars. hub_a: 2 friends x 1 liked = 2 rows. hub_b: 1 friend x 2 liked = 2 rows.
    // Correct total is 4. A branch joined on the wrong key would cross the hubs -- (3 friends) x
    // (3 liked) = 9 -- so the count alone separates right from wrong.
    uint64_t hub_a = person("HubA");
    uint64_t hub_b = person("HubB");
    uint64_t f1 = person("F1");
    uint64_t f2 = person("F2");
    uint64_t f3 = person("F3");
    uint64_t i1 = person("I1");
    uint64_t i2 = person("I2");
    uint64_t i3 = person("I3");

    graph.shard.local().RelationshipAddPeered("KNOWS", hub_a, f1, "{}").get();
    graph.shard.local().RelationshipAddPeered("KNOWS", hub_a, f2, "{}").get();
    graph.shard.local().RelationshipAddPeered("LIKES", hub_a, i1, "{}").get();
    graph.shard.local().RelationshipAddPeered("KNOWS", hub_b, f3, "{}").get();
    graph.shard.local().RelationshipAddPeered("LIKES", hub_b, i2, "{}").get();
    graph.shard.local().RelationshipAddPeered("LIKES", hub_b, i3, "{}").get();

    auto run = [&](const std::string& q) {
        auto query = GqlParser::parse(q);
        GqlOptimizer::optimize(query);
        return GqlExecutor::execute(graph, std::move(query)).get();
    };

    SECTION("two branches sharing the centre yield the per-centre cross product") {
        std::string res = run(
            "MATCH (h:Person)-[:KNOWS]->(f:Person) "
            "MATCH (h)-[:LIKES]->(m:Person) "
            "RETURN h.name AS hn, f.name AS fn, m.name AS mn");
        INFO("result: " << res);

        // hub_a: (F1,I1),(F2,I1). hub_b: (F3,I2),(F3,I3). Exactly four rows, all sharing their hub.
        REQUIRE(res.find("\"hn\": \"HubA\", \"fn\": \"F1\", \"mn\": \"I1\"") != std::string::npos);
        REQUIRE(res.find("\"hn\": \"HubA\", \"fn\": \"F2\", \"mn\": \"I1\"") != std::string::npos);
        REQUIRE(res.find("\"hn\": \"HubB\", \"fn\": \"F3\", \"mn\": \"I2\"") != std::string::npos);
        REQUIRE(res.find("\"hn\": \"HubB\", \"fn\": \"F3\", \"mn\": \"I3\"") != std::string::npos);

        // No row crosses a hub: HubA never pairs with I2/I3, HubB never pairs with F1/F2.
        REQUIRE(res.find("\"hn\": \"HubA\", \"fn\": \"F1\", \"mn\": \"I2\"") == std::string::npos);
        REQUIRE(res.find("\"hn\": \"HubB\", \"fn\": \"F1\"") == std::string::npos);

        // Count check via count(*) so a stray extra row cannot hide among the four.
        std::string cnt = run(
            "MATCH (h:Person)-[:KNOWS]->(f:Person) "
            "MATCH (h)-[:LIKES]->(m:Person) "
            "RETURN count(*) AS n");
        INFO("count: " << cnt);
        REQUIRE(cnt.find("\"n\": 4") != std::string::npos);
    }

    // A third branch with a REPEATED relationship type (two KNOWS arms) is deliberately not asserted
    // here: it makes the subsumption pruner erase one arm as a projection-dead equivalent, which is
    // unsound for count(*) -- it drops the arm's row multiplication and undercounts. That is a separate
    // pre-existing pruner bug tracked on its own; a clean three-branch star with distinct rel types can
    // be added once it is fixed.
}
