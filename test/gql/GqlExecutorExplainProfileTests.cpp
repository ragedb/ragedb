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
 * See the License for the Schindler-like and limitations under the License.
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

TEST_CASE("GQL Execution EXPLAIN and PROFILE Tests", "[gql_explain_profile]") {
    auto graph = Graph("gql_test_explain_profile");
    graph.Start().get();
    graph.Clear();
    GraphStopGuard guard(graph);
    
    // Setup schemas
    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "age", "integer").get();
    graph.shard.local().RelationshipTypeInsertPeered("FRIEND").get();

    // Create test nodes
    uint64_t id1 = graph.shard.local().NodeAddPeered("Person", "alice", "{\"name\": \"Alice\", \"age\": 30}").get();
    uint64_t id2 = graph.shard.local().NodeAddPeered("Person", "bob", "{\"name\": \"Bob\", \"age\": 35}").get();
    graph.shard.local().RelationshipAddPeered("FRIEND", id1, id2, "{}").get();

    SECTION("EXPLAIN query plan") {
        std::string query_str = "EXPLAIN MATCH (p:Person)-[:FRIEND]->(friend) RETURN p.name, friend.name";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        
        REQUIRE(query.explain);
        REQUIRE_FALSE(query.profile);

        std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();

        // Should return explanation JSON
        REQUIRE(results_json.find("ProduceResults") != std::string::npos);
        REQUIRE(results_json.find("Scan / Traverse") != std::string::npos);
        REQUIRE(results_json.find("Operator") != std::string::npos);
        REQUIRE(results_json.find("Details") != std::string::npos);
        REQUIRE(results_json.find("Outputs") != std::string::npos);
        // Explain should not contain actual rows or time_ms fields in rows
        REQUIRE(results_json.find("Actual Rows") == std::string::npos);
    }

    SECTION("PROFILE query plan") {
        std::string query_str = "PROFILE MATCH (p:Person)-[:FRIEND]->(friend) RETURN p.name, friend.name";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        
        REQUIRE(query.profile);
        REQUIRE_FALSE(query.explain);

        std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();

        // Should return profile JSON including execution stats
        REQUIRE(results_json.find("ProduceResults") != std::string::npos);
        REQUIRE(results_json.find("Scan / Traverse") != std::string::npos);
        REQUIRE(results_json.find("Operator") != std::string::npos);
        REQUIRE(results_json.find("Details") != std::string::npos);
        REQUIRE(results_json.find("Outputs") != std::string::npos);
        REQUIRE(results_json.find("Actual Rows") != std::string::npos);
        REQUIRE(results_json.find("Time (ms)") != std::string::npos);
    }

    SECTION("EXPLAIN schema operation query plan") {
        std::string query_str = "EXPLAIN CREATE NODE TYPE Student (name STRING)";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        
        REQUIRE(query.explain);
        
        std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
        REQUIRE(results_json.find("SchemaOperation") != std::string::npos);
        REQUIRE(results_json.find("Student") != std::string::npos);
    }

    guard.stop();
}
