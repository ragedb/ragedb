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
        std::string query_str = "MATCH p = ANY SHORTEST (a)-[:Links]-{1,10}(b) WHERE a.name = 'Arcadia' AND b.name = 'Eldoria' RETURN p";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        std::string results = GqlExecutor::execute(graph, std::move(query)).get();
        REQUIRE(results.find("Arcadia") != std::string::npos);
        REQUIRE(results.find("Solara") != std::string::npos);
        REQUIRE(results.find("Eldoria") != std::string::npos);
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

    guard.stop();
}
