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

TEST_CASE("GQL Execution Correlated Subquery Tests", "[gql_executor_subquery]") {
    auto graph = Graph("gql_test_subquery");
    graph.Start().get();
    graph.Clear();
    GraphStopGuard guard(graph);
    
    // Setup schemas
    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().RelationshipTypeInsertPeered("FRIEND").get();

    // Create test nodes
    uint64_t id1 = graph.shard.local().NodeAddPeered("Person", "alice", "{\"name\": \"Alice\"}").get();
    uint64_t id2 = graph.shard.local().NodeAddPeered("Person", "bob", "{\"name\": \"Bob\"}").get();
    uint64_t id3 = graph.shard.local().NodeAddPeered("Person", "charlie", "{\"name\": \"Charlie\"}").get();

    // Alice is friends with Bob. Charlie has no friends.
    graph.shard.local().RelationshipAddPeered("FRIEND", id1, id2, "{}").get();

    SECTION("Correlated EXISTS subquery") {
        std::string query_str = "MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:FRIEND]->(f) } RETURN p.name";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        std::string results = GqlExecutor::execute(graph, std::move(query)).get();
        REQUIRE(results.find("Alice") != std::string::npos);
        REQUIRE(results.find("Bob") == std::string::npos);
        REQUIRE(results.find("Charlie") == std::string::npos);
    }

    SECTION("Correlated EXISTS subquery with filter") {
        std::string query_str = "MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:FRIEND]->(f) WHERE f.name = 'Bob' } RETURN p.name";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        std::string results = GqlExecutor::execute(graph, std::move(query)).get();
        REQUIRE(results.find("Alice") != std::string::npos);
        REQUIRE(results.find("Bob") == std::string::npos);
        REQUIRE(results.find("Charlie") == std::string::npos);
    }

    SECTION("Correlated NOT EXISTS subquery") {
        std::string query_str = "MATCH (p:Person) WHERE NOT EXISTS { MATCH (p)-[:FRIEND]->(f) } RETURN p.name";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        std::string results = GqlExecutor::execute(graph, std::move(query)).get();
        REQUIRE(results.find("Alice") == std::string::npos);
        REQUIRE(results.find("Bob") != std::string::npos);
        REQUIRE(results.find("Charlie") != std::string::npos);
    }

    guard.stop();
}
