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

TEST_CASE("GQL Execution Read Tests", "[gql_executor_read]") {
    auto graph = Graph("gql_test");
    graph.Start().get();
    graph.Clear();
    GraphStopGuard guard(graph);
    
    // Setup schemas
    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "age", "integer").get();

    // Create test nodes
    uint64_t id1 = graph.shard.local().NodeAddPeered("Person", "alice", "{\"name\": \"Alice\", \"age\": 30}").get();
    uint64_t id2 = graph.shard.local().NodeAddPeered("Person", "bob", "{\"name\": \"Bob\", \"age\": 35}").get();

    REQUIRE(id1 > 0);
    REQUIRE(id2 > 0);

    SECTION("Simple match return properties") {
        std::string query_str = "MATCH (p:Person) RETURN p.name, p.age";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        
        std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
        
        REQUIRE(results_json.find("Alice") != std::string::npos);
        REQUIRE(results_json.find("Bob") != std::string::npos);
        REQUIRE(results_json.find("30") != std::string::npos);
        REQUIRE(results_json.find("35") != std::string::npos);
    }

    SECTION("Filtered match return properties") {
        std::string query_str = "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name, p.age";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        
        std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
        
        REQUIRE(results_json.find("Alice") != std::string::npos);
        REQUIRE(results_json.find("Bob") == std::string::npos);
    }

    SECTION("Match with LIMIT pushdown") {
        std::string query_str = "MATCH (p:Person) RETURN p.name LIMIT 1";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        
        std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
        
        bool has_alice = results_json.find("Alice") != std::string::npos;
        bool has_bob = results_json.find("Bob") != std::string::npos;
        REQUIRE((has_alice || has_bob));
        REQUIRE_FALSE((has_alice && has_bob));
    }

    SECTION("Match with range and inequality pushdown") {
        {
            std::string query_str = "MATCH (p:Person) WHERE p.age > 32 RETURN p.name";
            auto query = GqlParser::parse(query_str);
            GqlOptimizer::optimize(query);
            std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
            REQUIRE(results_json.find("Bob") != std::string::npos);
            REQUIRE(results_json.find("Alice") == std::string::npos);
        }
        {
            std::string query_str = "MATCH (p:Person) WHERE p.age < 32 RETURN p.name";
            auto query = GqlParser::parse(query_str);
            GqlOptimizer::optimize(query);
            std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
            REQUIRE(results_json.find("Alice") != std::string::npos);
            REQUIRE(results_json.find("Bob") == std::string::npos);
        }
        {
            std::string query_str = "MATCH (p:Person) WHERE p.age != 30 RETURN p.name";
            auto query = GqlParser::parse(query_str);
            GqlOptimizer::optimize(query);
            std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
            REQUIRE(results_json.find("Bob") != std::string::npos);
            REQUIRE(results_json.find("Alice") == std::string::npos);
        }
    }

    SECTION("Top-K push down query") {
        std::string query_str = "MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age DESC LIMIT 1";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
        REQUIRE(results_json.find("Bob") != std::string::npos);
        REQUIRE(results_json.find("35") != std::string::npos);
        REQUIRE(results_json.find("Alice") == std::string::npos);
    }

    guard.stop();
}

TEST_CASE("GQL Execution Label Algebra and Repetition Tests", "[gql_executor_complex]") {
    auto graph = Graph("gql_test_complex");
    graph.Start().get();
    graph.Clear();
    GraphStopGuard guard(graph);
    
    // Setup schemas
    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "age", "integer").get();
    graph.shard.local().NodeTypeInsertPeered("Employee").get();
    graph.shard.local().NodePropertyTypeAddPeered("Employee", "name", "string").get();
    graph.shard.local().NodeTypeInsertPeered("Robot").get();
    graph.shard.local().NodePropertyTypeAddPeered("Robot", "name", "string").get();

    graph.shard.local().RelationshipTypeInsertPeered("FRIEND").get();
    graph.shard.local().RelationshipTypeInsertPeered("KNOWS").get();

    // Create test nodes
    uint64_t id1 = graph.shard.local().NodeAddPeered("Person", "alice", "{\"name\": \"Alice\", \"age\": 30}").get();
    uint64_t id2 = graph.shard.local().NodeAddPeered("Employee", "bob", "{\"name\": \"Bob\"}").get();
    uint64_t id3 = graph.shard.local().NodeAddPeered("Robot", "charlie", "{\"name\": \"Charlie\"}").get();

    // Create relationships
    graph.shard.local().RelationshipAddPeered("FRIEND", id1, id2, "{}").get();
    graph.shard.local().RelationshipAddPeered("KNOWS", id2, id3, "{}").get();

    SECTION("Label algebra OR type query") {
        std::string query_str = "MATCH (p:Person|Employee) RETURN p.name";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        std::string results = GqlExecutor::execute(graph, std::move(query)).get();
        REQUIRE(results.find("Alice") != std::string::npos);
        REQUIRE(results.find("Bob") != std::string::npos);
        REQUIRE(results.find("Charlie") == std::string::npos);
    }

    SECTION("Label algebra NOT type query") {
        std::string query_str = "MATCH (p:!Robot) RETURN p.name";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        std::string results = GqlExecutor::execute(graph, std::move(query)).get();
        REQUIRE(results.find("Alice") != std::string::npos);
        REQUIRE(results.find("Bob") != std::string::npos);
        REQUIRE(results.find("Charlie") == std::string::npos);
    }

    SECTION("Repetitions 1..2 hops query") {
        std::string query_str = "MATCH (p)-[:FRIEND|KNOWS*1..2]->(m) WHERE p.name = 'Alice' RETURN m.name";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        std::string results = GqlExecutor::execute(graph, std::move(query)).get();
        REQUIRE(results.find("Bob") != std::string::npos);
        REQUIRE(results.find("Charlie") != std::string::npos);
    }

    SECTION("Repetitions 1..2 hops query with LIMIT pushdown") {
        std::string query_str = "MATCH (p)-[:FRIEND|KNOWS*1..2]->(m) WHERE p.name = 'Alice' RETURN m.name LIMIT 1";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        std::string results = GqlExecutor::execute(graph, std::move(query)).get();
        bool has_bob = results.find("Bob") != std::string::npos;
        bool has_charlie = results.find("Charlie") != std::string::npos;
        REQUIRE((has_bob || has_charlie));
        REQUIRE_FALSE((has_bob && has_charlie));
    }

    SECTION("Projection pruning query") {
        std::string query_str = "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.age";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
        REQUIRE(results_json.find("30") != std::string::npos);
        REQUIRE(results_json.find("Alice") == std::string::npos);
    }

    SECTION("Relationship count degree optimization query") {
        std::string query_str = "MATCH (p:Person)-[:FRIEND]->(f) RETURN p.name, count(f)";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        REQUIRE(query.matches[0].pattern.nodes.size() == 1);
        REQUIRE(query.matches[0].pattern.edges.empty());

        std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
        REQUIRE(results_json.find("Alice") != std::string::npos);
        REQUIRE(results_json.find("1") != std::string::npos);
    }

    SECTION("Aggregate key dependency optimization query") {
        // Query groups by both node variable 'p' and its property 'p.name'.
        // The optimization should prune 'p.name' from grouping keys, grouping only by 'p',
        // but still projecting 'p.name' successfully in the output.
        std::string query_str = "MATCH (p:Person) RETURN p, p.name, count(*)";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
        REQUIRE(results_json.find("Alice") != std::string::npos);
    }

    SECTION("Join query utilizing accumulator hash join") {
        // MATCH uses two path patterns sharing variable 'b'.
        // This will trigger natural_join of both pattern results, invoking
        // join_flat_rows_variable to join them on the shared variable 'b'.
        std::string query_str = "MATCH (a)-[:FRIEND]->(b) MATCH (b)-[:KNOWS]->(c) RETURN a.name, b.name, c.name";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        std::string results = GqlExecutor::execute(graph, std::move(query)).get();
        REQUIRE(results.find("Alice") != std::string::npos);
        REQUIRE(results.find("Bob") != std::string::npos);
        REQUIRE(results.find("Charlie") != std::string::npos);
    }

    SECTION("Cartesian product join query") {
        // Query matches two disjoint patterns with no shared variables.
        // This should trigger the Cartesian product fallback in join_flat_rows_variable.
        std::string query_str = "MATCH (a:Person) MATCH (b:Robot) RETURN a.name, b.name";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        std::string results = GqlExecutor::execute(graph, std::move(query)).get();
        REQUIRE(results.find("Alice") != std::string::npos);
        REQUIRE(results.find("Charlie") != std::string::npos);
    }

    guard.stop();
}
