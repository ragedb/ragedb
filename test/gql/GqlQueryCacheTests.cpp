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
#include "../src/gql/GqlQueryCache.h"
#include "../src/gql/GqlExecutor.h"
#include "../src/gql/GqlParser.h"
#include "../src/graph/Graph.h"

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

// Helper guard to clean up cache before/after tests
struct CacheCleanGuard {
    CacheCleanGuard() {
        GqlQueryCache::local().clear();
    }
    ~CacheCleanGuard() {
        GqlQueryCache::local().clear();
    }
};

TEST_CASE("GQL Query Cache LRU Logic", "[gql_cache]") {
    CacheCleanGuard guard;

    SECTION("Basic put and get") {
        auto query = GqlParser::parse("MATCH (p:Person) RETURN p.name");
        GqlQueryCache::local().put("MATCH (p:Person) RETURN p.name", query);
        
        REQUIRE(GqlQueryCache::local().size() == 1);
        auto retrieved = GqlQueryCache::local().get("MATCH (p:Person) RETURN p.name");
        REQUIRE(retrieved.has_value());
        REQUIRE(retrieved->kind == QueryKind::SINGLE);
    }

    SECTION("Capacity and Eviction (evicts oldest first)") {
        // Create cache with custom capacity of 3 to test eviction easily
        GqlQueryCache small_cache(3);
        
        auto q1 = GqlParser::parse("MATCH (p1) RETURN p1");
        auto q2 = GqlParser::parse("MATCH (p2) RETURN p2");
        auto q3 = GqlParser::parse("MATCH (p3) RETURN p3");
        auto q4 = GqlParser::parse("MATCH (p4) RETURN p4");

        small_cache.put("q1", q1);
        small_cache.put("q2", q2);
        small_cache.put("q3", q3);
        
        REQUIRE(small_cache.size() == 3);
        
        // Access q1 to make it most recently used
        small_cache.get("q1");

        // Put q4, which should trigger eviction. 
        // q2 should be evicted because q1 was accessed, and q3 is newer than q2.
        small_cache.put("q4", q4);

        REQUIRE(small_cache.size() == 3);
        REQUIRE_FALSE(small_cache.get("q2").has_value()); // Evicted!
        REQUIRE(small_cache.get("q1").has_value());
        REQUIRE(small_cache.get("q3").has_value());
        REQUIRE(small_cache.get("q4").has_value());
    }
}

TEST_CASE("GQL Query Cache Integration in Executor", "[gql_cache_integration]") {
    auto graph = Graph("gql_test_cache");
    graph.Start().get();
    graph.Clear();
    GraphStopGuard graph_guard(graph);
    
    // Create node type for test queries
    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();

    CacheCleanGuard cache_guard;

    SECTION("EXPLAIN plan Cache HIT/MISS verification") {
        std::string query_str = "EXPLAIN MATCH (p:Person) RETURN p.name";
        
        // First execution -> MISS
        std::string res1 = GqlExecutor::execute(graph, query_str).get();
        REQUIRE(res1.find("\"Cache\": \"MISS\"") != std::string::npos);
        REQUIRE(res1.find("\"Cache\": \"HIT\"") == std::string::npos);

        // Second execution -> HIT
        std::string res2 = GqlExecutor::execute(graph, query_str).get();
        REQUIRE(res2.find("\"Cache\": \"HIT\"") != std::string::npos);
        REQUIRE(res2.find("\"Cache\": \"MISS\"") == std::string::npos);
    }

    SECTION("PROFILE plan Cache HIT/MISS verification") {
        std::string query_str = "PROFILE MATCH (p:Person) RETURN p.name";

        // First execution -> MISS
        std::string res1 = GqlExecutor::execute(graph, query_str).get();
        REQUIRE(res1.find("\"Cache\": \"MISS\"") != std::string::npos);
        
        // Second execution -> HIT
        std::string res2 = GqlExecutor::execute(graph, query_str).get();
        REQUIRE(res2.find("\"Cache\": \"HIT\"") != std::string::npos);
    }

    SECTION("Schema change invalidates cache on all shards") {
        std::string query_str = "EXPLAIN MATCH (p:Person) RETURN p.name";
        
        // Populate cache
        GqlExecutor::execute(graph, query_str).get();
        std::string res_before = GqlExecutor::execute(graph, query_str).get();
        REQUIRE(res_before.find("\"Cache\": \"HIT\"") != std::string::npos);

        // Run Schema Operation -> Invalidates cache
        std::string create_schema = "CREATE NODE TYPE Student (name STRING)";
        GqlExecutor::execute(graph, create_schema).get();

        // Execution after schema change -> MISS
        std::string res_after = GqlExecutor::execute(graph, query_str).get();
        REQUIRE(res_after.find("\"Cache\": \"MISS\"") != std::string::npos);
    }

    SECTION("CALL CLEAR CACHE resets cache on all shards") {
        std::string query_str = "EXPLAIN MATCH (p:Person) RETURN p.name";
        
        // Populate cache
        GqlExecutor::execute(graph, query_str).get();
        std::string res_before = GqlExecutor::execute(graph, query_str).get();
        REQUIRE(res_before.find("\"Cache\": \"HIT\"") != std::string::npos);

        // Run CALL CLEAR CACHE -> Invalidates cache
        std::string clear_res = GqlExecutor::execute(graph, "CALL CLEAR CACHE").get();
        REQUIRE(clear_res.find("cache cleared") != std::string::npos);

        // Execution after clear cache -> MISS
        std::string res_after = GqlExecutor::execute(graph, query_str).get();
        REQUIRE(res_after.find("\"Cache\": \"MISS\"") != std::string::npos);
    }
}
