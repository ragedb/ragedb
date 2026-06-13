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
