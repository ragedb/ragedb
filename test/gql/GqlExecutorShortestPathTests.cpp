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

#include <catch2/catch.hpp>
#include <Graph.h>
#include "../../src/gql/GqlParser.h"
#include "../../src/gql/GqlOptimizer.h"
#include "../../src/gql/GqlExecutor.h"
#include "../../src/gql/executor/PathTraverser.h"

using namespace ragedb;
using namespace ragedb::gql;

struct GraphStopGuard {
    Graph& g;
    bool stopped = false;
    explicit GraphStopGuard(Graph& graph) : g(graph) {}
    ~GraphStopGuard() {
        if (!stopped) {
            try {
                g.Stop().get();
            } catch (...) {}
            stopped = true;
        }
    }
    void stop() {
        if (!stopped) {
            g.Stop().get();
            stopped = true;
        }
    }
};

TEST_CASE("GQL Execution Shortest Path Tests", "[gql_executor_shortest_path]") {
    auto graph = Graph("gql_test_shortest_path");
    graph.Start().get();
    graph.Clear();
    GraphStopGuard guard(graph);
    
    // Setup schemas
    graph.shard.local().NodeTypeInsertPeered("City").get();
    graph.shard.local().NodePropertyTypeAddPeered("City", "name", "string").get();
    graph.shard.local().RelationshipTypeInsertPeered("Links").get();

    // Create test nodes
    uint64_t zenith = graph.shard.local().NodeAddPeered("City", "Zenith", "{\"name\": \"Zenith\"}").get();
    uint64_t arcadia = graph.shard.local().NodeAddPeered("City", "Arcadia", "{\"name\": \"Arcadia\"}").get();
    uint64_t verona = graph.shard.local().NodeAddPeered("City", "Verona", "{\"name\": \"Verona\"}").get();
    uint64_t nebula = graph.shard.local().NodeAddPeered("City", "Nebula", "{\"name\": \"Nebula\"}").get();
    uint64_t mirage = graph.shard.local().NodeAddPeered("City", "Mirage", "{\"name\": \"Mirage\"}").get();
    uint64_t lunaria = graph.shard.local().NodeAddPeered("City", "Lunaria", "{\"name\": \"Lunaria\"}").get();
    uint64_t solara = graph.shard.local().NodeAddPeered("City", "Solara", "{\"name\": \"Solara\"}").get();
    uint64_t eldoria = graph.shard.local().NodeAddPeered("City", "Eldoria", "{\"name\": \"Eldoria\"}").get();
    uint64_t nexis = graph.shard.local().NodeAddPeered("City", "Nexis", "{\"name\": \"Nexis\"}").get();

    // Create relationships
    graph.shard.local().RelationshipAddPeered("Links", arcadia, zenith, "{}").get();
    graph.shard.local().RelationshipAddPeered("Links", arcadia, verona, "{}").get();
    graph.shard.local().RelationshipAddPeered("Links", arcadia, solara, "{}").get();
    graph.shard.local().RelationshipAddPeered("Links", mirage, arcadia, "{}").get();
    graph.shard.local().RelationshipAddPeered("Links", nebula, verona, "{}").get();
    graph.shard.local().RelationshipAddPeered("Links", mirage, nebula, "{}").get();
    graph.shard.local().RelationshipAddPeered("Links", verona, mirage, "{}").get();
    graph.shard.local().RelationshipAddPeered("Links", mirage, eldoria, "{}").get();
    graph.shard.local().RelationshipAddPeered("Links", solara, eldoria, "{}").get();
    graph.shard.local().RelationshipAddPeered("Links", lunaria, solara, "{}").get();

    SECTION("ANY SHORTEST path query") {
        // Arcadia->Eldoria has two shortest paths (via Solara and via Mirage). ANY returns one of them.
        std::string query_str = "MATCH p = ANY SHORTEST (a)-[:Links]-{1,10}(b) WHERE a.name = 'Arcadia' AND b.name = 'Eldoria' RETURN p";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        std::string results = GqlExecutor::execute(graph, std::move(query)).get();
        REQUIRE(results.find("Arcadia") != std::string::npos);
        REQUIRE(results.find("Eldoria") != std::string::npos);
        REQUIRE((results.find("Solara") != std::string::npos || results.find("Mirage") != std::string::npos));
    }

    SECTION("ANY SHORTEST with an unbounded quantifier terminates (IC13 OOM)") {
        // `{1,}` has no upper bound. A path-enumerating BFS fans out by the average degree each level
        // and exhausts the heap before reaching the destination; the ANY search must instead keep one
        // path per node so the frontier stays bounded and it returns the single 2-hop shortest path.
        std::string query_str = "MATCH p = ANY SHORTEST (a)-[:Links]-{1,}(b) WHERE a.name = 'Arcadia' AND b.name = 'Eldoria' RETURN p";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        std::string results = GqlExecutor::execute(graph, std::move(query)).get();
        REQUIRE(results.find("Arcadia") != std::string::npos);
        REQUIRE(results.find("Eldoria") != std::string::npos);
        REQUIRE((results.find("Solara") != std::string::npos || results.find("Mirage") != std::string::npos));
    }

    SECTION("ALL SHORTEST paths query") {
        std::string query_str = "MATCH p = ALL SHORTEST (a)-[:Links]-+(b) WHERE a.name = 'Arcadia' AND b.name = 'Eldoria' RETURN p";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        std::string results = GqlExecutor::execute(graph, std::move(query)).get();
        REQUIRE(results.find("Solara") != std::string::npos);
        REQUIRE(results.find("Mirage") != std::string::npos);
    }

    SECTION("SHORTEST k paths query") {
        std::string query_str = "MATCH p = SHORTEST 3 (a)-[:Links]-{1,10}(b) WHERE a.name = 'Arcadia' AND b.name = 'Eldoria' RETURN p";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        std::string results = GqlExecutor::execute(graph, std::move(query)).get();
        REQUIRE(results.find("Arcadia") != std::string::npos);
    }

    SECTION("SHORTEST k GROUP paths query") {
        std::string query_str = "MATCH p = SHORTEST 3 GROUP (a)-[:Links]-{1,10}(b) WHERE a.name = 'Arcadia' AND b.name = 'Eldoria' RETURN p";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        std::string results = GqlExecutor::execute(graph, std::move(query)).get();
        REQUIRE(results.find("Arcadia") != std::string::npos);
    }

    // One bound start against an unbound endpoint runs a separate search per candidate pair -- the
    // shape that made a person-to-every-Person-of-a-first-name query exhaust the heap. The searches
    // now run with a bounded number in flight, and neither the rows nor their order may depend on
    // that bound.
    SECTION("many-endpoint search is invariant under the concurrency bound") {
        std::string query_str = "MATCH p = ANY SHORTEST (a)-[:Links]-{1,10}(b) WHERE a.name = 'Arcadia' RETURN b.name AS name, length(p) AS d";

        const size_t saved_concurrency = gql_shortest_path_concurrency;
        std::string serial;
        std::string bounded;
        try {
            gql_shortest_path_concurrency = 1;
            auto serial_query = GqlParser::parse(query_str);
            GqlOptimizer::optimize(serial_query);
            serial = GqlExecutor::execute(graph, std::move(serial_query)).get();

            gql_shortest_path_concurrency = 4;
            auto bounded_query = GqlParser::parse(query_str);
            GqlOptimizer::optimize(bounded_query);
            bounded = GqlExecutor::execute(graph, std::move(bounded_query)).get();
        } catch (...) {
            gql_shortest_path_concurrency = saved_concurrency;
            throw;
        }
        gql_shortest_path_concurrency = saved_concurrency;

        REQUIRE(serial == bounded);

        // Arcadia reaches every city except the isolated Nexis, so each reachable endpoint yields a
        // row and the unreachable one yields none.
        REQUIRE(serial.find("Eldoria") != std::string::npos);
        REQUIRE(serial.find("Nebula") != std::string::npos);
        REQUIRE(serial.find("Lunaria") != std::string::npos);
        REQUIRE(serial.find("Nexis") == std::string::npos);
    }

    guard.stop();
}

// `ANY SHORTEST ... COST r.w` must select the minimum-weight path (Dijkstra), not the fewest-hops path.
// The direct edge is one hop but expensive; the two-hop detour is cheaper, so the weighted search must
// return the two-hop path. (Previously ANY ignored COST and returned the one-hop path.)
TEST_CASE("GQL weighted ANY SHORTEST COST picks the cheapest path, not fewest hops",
          "[gql_executor_shortest_path]") {
    auto graph = Graph("gql_test_weighted_shortest_path");
    graph.Start().get();
    graph.Clear();
    GraphStopGuard guard(graph);

    graph.shard.local().NodeTypeInsertPeered("N").get();
    graph.shard.local().NodePropertyTypeAddPeered("N", "id", "integer").get();
    graph.shard.local().RelationshipTypeInsertPeered("R").get();
    graph.shard.local().RelationshipPropertyTypeAddPeered("R", "w", "double").get();

    uint64_t a = graph.shard.local().NodeAddPeered("N", "a", "{\"id\": 1}").get();
    uint64_t b = graph.shard.local().NodeAddPeered("N", "b", "{\"id\": 2}").get();
    uint64_t d = graph.shard.local().NodeAddPeered("N", "d", "{\"id\": 4}").get();
    graph.shard.local().RelationshipAddPeered("R", a, d, "{\"w\": 10.0}").get();  // 1 hop, cost 10
    graph.shard.local().RelationshipAddPeered("R", a, b, "{\"w\": 1.0}").get();   // }
    graph.shard.local().RelationshipAddPeered("R", b, d, "{\"w\": 1.0}").get();   // } 2 hops, cost 2

    SECTION("cheapest weighted path is chosen over fewer hops") {
        std::string q = "MATCH p = ANY SHORTEST (src:N {id: 1})-[r:R]->{1,5}(dst:N {id: 4}) COST r.w "
                        "RETURN length(p) AS hops";
        auto query = GqlParser::parse(q);
        GqlOptimizer::optimize(query);
        std::string res = GqlExecutor::execute(graph, std::move(query)).get();
        INFO("res: " << res);
        REQUIRE(res.find("\"hops\": 2") != std::string::npos);   // a->b->d (cost 2), not a->d (cost 10)
    }

    guard.stop();
}
