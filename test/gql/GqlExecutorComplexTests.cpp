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

    SECTION("Star join utilizing factorization rewriter") {
        // Set up FRIEND edge between Bob and Charlie's friend
        uint64_t id_dan = graph.shard.local().NodeAddPeered("Person", "dan", "{\"name\": \"Dan\"}").get();
        graph.shard.local().RelationshipAddPeered("FRIEND", id2, id_dan, "{}").get();

        std::string query_str = "MATCH (a)-[:FRIEND]->(b) MATCH (b)-[:KNOWS]->(c) MATCH (b)-[:FRIEND]->(d) RETURN a.name, b.name, c.name, d.name";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        std::string results = GqlExecutor::execute(graph, std::move(query)).get();
        REQUIRE(results.find("Alice") != std::string::npos);
        REQUIRE(results.find("Bob") != std::string::npos);
        REQUIRE(results.find("Charlie") != std::string::npos);
        REQUIRE(results.find("Dan") != std::string::npos);
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

    SECTION("Order by push down query") {
        // We will insert some more nodes to have enough to sort.
        uint64_t id_dan = graph.shard.local().NodeAddPeered("Person", "dan", "{\"name\": \"Dan\", \"age\": 25}").get();
        uint64_t id_eve = graph.shard.local().NodeAddPeered("Person", "eve", "{\"name\": \"Eve\", \"age\": 35}").get();
        uint64_t id_fred = graph.shard.local().NodeAddPeered("Person", "fred", "{\"name\": \"Fred\", \"age\": 20}").get();

        // 1. Sort by property ascending
        {
            std::string query_str = "MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age ASC";
            auto query = GqlParser::parse(query_str);
            GqlOptimizer::optimize(query);
            std::string results = GqlExecutor::execute(graph, std::move(query)).get();
            
            size_t pos_fred = results.find("Fred");
            size_t pos_dan = results.find("Dan");
            size_t pos_alice = results.find("Alice");
            size_t pos_eve = results.find("Eve");
            
            REQUIRE(pos_fred != std::string::npos);
            REQUIRE(pos_dan != std::string::npos);
            REQUIRE(pos_alice != std::string::npos);
            REQUIRE(pos_eve != std::string::npos);
            
            REQUIRE(pos_fred < pos_dan);
            REQUIRE(pos_dan < pos_alice);
            REQUIRE(pos_alice < pos_eve);
        }

        // 2. Sort by property descending
        {
            std::string query_str = "MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age DESC";
            auto query = GqlParser::parse(query_str);
            GqlOptimizer::optimize(query);
            std::string results = GqlExecutor::execute(graph, std::move(query)).get();
            
            size_t pos_eve = results.find("Eve");
            size_t pos_alice = results.find("Alice");
            size_t pos_dan = results.find("Dan");
            size_t pos_fred = results.find("Fred");
            
            REQUIRE(pos_fred != std::string::npos);
            REQUIRE(pos_dan != std::string::npos);
            REQUIRE(pos_alice != std::string::npos);
            REQUIRE(pos_eve != std::string::npos);
            
            REQUIRE(pos_eve < pos_alice);
            REQUIRE(pos_alice < pos_dan);
            REQUIRE(pos_dan < pos_fred);
        }

        // 3. Sort by node variable (internal ID) ascending
        {
            std::string query_str = "MATCH (p:Person) RETURN p.name ORDER BY p ASC";
            auto query = GqlParser::parse(query_str);
            GqlOptimizer::optimize(query);
            std::string results = GqlExecutor::execute(graph, std::move(query)).get();
            
            std::vector<std::pair<uint64_t, std::string>> nodes = {
                {id1, "Alice"},
                {id_dan, "Dan"},
                {id_eve, "Eve"},
                {id_fred, "Fred"}
            };
            std::sort(nodes.begin(), nodes.end());

            size_t pos0 = results.find(nodes[0].second);
            size_t pos1 = results.find(nodes[1].second);
            size_t pos2 = results.find(nodes[2].second);
            size_t pos3 = results.find(nodes[3].second);
            
            REQUIRE(pos0 != std::string::npos);
            REQUIRE(pos1 != std::string::npos);
            REQUIRE(pos2 != std::string::npos);
            REQUIRE(pos3 != std::string::npos);
            
            REQUIRE(pos0 < pos1);
            REQUIRE(pos1 < pos2);
            REQUIRE(pos2 < pos3);
        }
    }

    guard.stop();
}
