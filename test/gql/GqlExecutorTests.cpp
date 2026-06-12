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

TEST_CASE("GQL Execution Full-Text Search Tests", "[gql_executor_fts]") {
    auto graph = Graph("gql_test_fts");
    graph.Start().get();
    graph.Clear();
    GraphStopGuard guard(graph);
    
    // Setup schemas
    graph.shard.local().NodeTypeInsertPeered("Product").get();
    graph.shard.local().NodePropertyTypeAddPeered("Product", "name", "string").get();
    graph.shard.local().NodePropertyTypeAddPeered("Product", "description", "string").get();

    // Create FTS Index
    GqlExecutor::execute(graph, GqlParser::parse("CREATE FULLTEXT INDEX Product.description")).get();

    // Create test nodes
    graph.shard.local().NodeAddPeered("Product", "p1", "{\"name\": \"Database Book\", \"description\": \"A great book about relational databases and SQL.\"}").get();
    graph.shard.local().NodeAddPeered("Product", "p2", "{\"name\": \"Graph DB Book\", \"description\": \"An introduction to graph databases and graph traversal algorithms.\"}").get();
    graph.shard.local().NodeAddPeered("Product", "p3", "{\"name\": \"Novel\", \"description\": \"A science fiction novel about agentic artificial intelligence and space travel.\"}").get();

    SECTION("FTS Exact term match") {
        std::string query_str = "MATCH p IN SEARCH Product.description FOR 'databases' YIELD p, score RETURN p.name, score";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        std::string results = GqlExecutor::execute(graph, std::move(query)).get();
        
        REQUIRE(results.find("Database Book") != std::string::npos);
        REQUIRE(results.find("Graph DB Book") != std::string::npos);
        REQUIRE(results.find("Novel") == std::string::npos);
    }

    SECTION("FTS Fuzzy match") {
        std::string query_str = "MATCH p IN SEARCH Product.description FOR 'databas~' OPTIONS { fuzzy: 'true' } YIELD p, score RETURN p.name, score";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        std::string results = GqlExecutor::execute(graph, std::move(query)).get();
        
        REQUIRE(results.find("Database Book") != std::string::npos);
        REQUIRE(results.find("Graph DB Book") != std::string::npos);
        REQUIRE(results.find("Novel") == std::string::npos);
    }

    SECTION("FTS No match") {
        std::string query_str = "MATCH p IN SEARCH Product.description FOR 'cooking' YIELD p, score RETURN p.name, score";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        std::string results = GqlExecutor::execute(graph, std::move(query)).get();
        
        REQUIRE(results == "[]");
    }

    guard.stop();
}
