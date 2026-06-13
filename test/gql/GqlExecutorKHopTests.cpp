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

using namespace ragedb;
using namespace ragedb::gql;

struct KHopGraphStopGuard {
    Graph& g;
    bool stopped = false;
    explicit KHopGraphStopGuard(Graph& graph) : g(graph) {}
    ~KHopGraphStopGuard() {
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

TEST_CASE("GQL Execution KHOP Tests", "[gql_executor_khop]") {
    auto graph = Graph("gql_test_khop");
    graph.Start().get();
    graph.Clear();
    KHopGraphStopGuard guard(graph);

    // Setup schemas
    graph.shard.local().NodeTypeInsertPeered("City").get();
    graph.shard.local().NodePropertyTypeAddPeered("City", "name", "string").get();
    graph.shard.local().NodePropertyTypeAddPeered("City", "population", "integer").get();
    graph.shard.local().RelationshipTypeInsertPeered("Links").get();

    // Create test nodes
    uint64_t zenith = graph.shard.local().NodeAddPeered("City", "Zenith", "{\"name\": \"Zenith\", \"population\": 100}").get();
    uint64_t arcadia = graph.shard.local().NodeAddPeered("City", "Arcadia", "{\"name\": \"Arcadia\", \"population\": 200}").get();
    uint64_t verona = graph.shard.local().NodeAddPeered("City", "Verona", "{\"name\": \"Verona\", \"population\": 300}").get();
    uint64_t nebula = graph.shard.local().NodeAddPeered("City", "Nebula", "{\"name\": \"Nebula\", \"population\": 400}").get();
    uint64_t mirage = graph.shard.local().NodeAddPeered("City", "Mirage", "{\"name\": \"Mirage\", \"population\": 500}").get();

    // Create relationships:
    // Arcadia -> Zenith (1-hop)
    // Arcadia -> Verona (1-hop)
    // Verona -> Nebula (1-hop from Verona, 2-hop from Arcadia)
    // Nebula -> Mirage (1-hop from Nebula, 3-hop from Arcadia)
    graph.shard.local().RelationshipAddPeered("Links", arcadia, zenith, "{}").get();
    graph.shard.local().RelationshipAddPeered("Links", arcadia, verona, "{}").get();
    graph.shard.local().RelationshipAddPeered("Links", verona, nebula, "{}").get();
    graph.shard.local().RelationshipAddPeered("Links", nebula, mirage, "{}").get();

    SECTION("Exact hop distance query") {
        std::string query_str = "MATCH KHOP (a)-[:Links]->{2, 2}(b) WHERE a.name = 'Arcadia' RETURN b.name ORDER BY b.name";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        std::string results = GqlExecutor::execute(graph, std::move(query)).get();
        // 2 hops from Arcadia going OUT is Nebula.
        REQUIRE(results.find("Nebula") != std::string::npos);
        REQUIRE(results.find("Zenith") == std::string::npos);
        REQUIRE(results.find("Verona") == std::string::npos);
        REQUIRE(results.find("Mirage") == std::string::npos);
    }

    SECTION("Hop range query") {
        std::string query_str = "MATCH KHOP (a)-[:Links]->{1,3}(b) WHERE a.name = 'Arcadia' RETURN b.name ORDER BY b.name";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        std::string results = GqlExecutor::execute(graph, std::move(query)).get();
        // 1 to 3 hops from Arcadia OUT is Zenith, Verona, Nebula, Mirage.
        REQUIRE(results.find("Zenith") != std::string::npos);
        REQUIRE(results.find("Verona") != std::string::npos);
        REQUIRE(results.find("Nebula") != std::string::npos);
        REQUIRE(results.find("Mirage") != std::string::npos);
    }

    SECTION("Count-only optimized traversal (no filters)") {
        std::string query_str = "MATCH KHOP (a)-[:Links]->{1,3}(b) WHERE a.name = 'Arcadia' RETURN count(b)";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        
        // Assert the optimizer set khop_count_only = true
        REQUIRE(query.matches[0].khop_count_only == true);

        std::string results = GqlExecutor::execute(graph, std::move(query)).get();
        // Total neighbors is 4 (Zenith, Verona, Nebula, Mirage)
        REQUIRE(results.find("4") != std::string::npos);
    }

    SECTION("Count-only optimized traversal with filters") {
        std::string query_str = "MATCH KHOP (a)-[:Links]->{1,3}(b) WHERE a.name = 'Arcadia' AND b.population > 250 RETURN count(b)";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);

        // Even with filters, it should still set khop_count_only = true because b is only inside count(b)
        REQUIRE(query.matches[0].khop_count_only == true);

        std::string results = GqlExecutor::execute(graph, std::move(query)).get();
        // Verona (300), Nebula (400), Mirage (500) have population > 250. Zenith (100) and Arcadia (200) do not.
        // So count is 3.
        REQUIRE(results.find("3") != std::string::npos);
        REQUIRE(results.find("4") == std::string::npos);
    }

    SECTION("K-Hop standard traversal with filters") {
        std::string query_str = "MATCH KHOP (a)-[:Links]->{1,3}(b) WHERE a.name = 'Arcadia' AND b.population > 350 RETURN b.name ORDER BY b.name";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);

        // khop_count_only should be false since b is returned directly
        REQUIRE(query.matches[0].khop_count_only == false);

        std::string results = GqlExecutor::execute(graph, std::move(query)).get();
        // Nebula (400) and Mirage (500) match.
        REQUIRE(results.find("Nebula") != std::string::npos);
        REQUIRE(results.find("Mirage") != std::string::npos);
        REQUIRE(results.find("Verona") == std::string::npos);
    }

    SECTION("OPTIONAL MATCH KHOP query with no match") {
        // From Mirage, outgoing Links has 0 neighbors
        std::string query_str = "OPTIONAL MATCH KHOP (a)-[:Links]->{1,2}(b) WHERE a.name = 'Mirage' RETURN a.name, b.name";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        std::string results = GqlExecutor::execute(graph, std::move(query)).get();
        REQUIRE(results.find("Mirage") != std::string::npos);
    }

    guard.stop();
}
