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
#include "../../src/gql/GqlTypechecker.h"

using namespace ragedb;
using namespace ragedb::gql;

struct PatternGraphStopGuard {
    Graph& g;
    bool stopped = false;
    explicit PatternGraphStopGuard(Graph& graph) : g(graph) {}
    ~PatternGraphStopGuard() {
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

TEST_CASE("GQL Execution Advanced Pattern Matching Tests", "[gql_executor_patterns]") {
    auto graph = Graph("gql_test_patterns");
    graph.Start().get();
    graph.Clear();
    PatternGraphStopGuard guard(graph);

    // Setup schemas
    graph.shard.local().NodeTypeInsertPeered("City").get();
    graph.shard.local().NodePropertyTypeAddPeered("City", "name", "string").get();
    graph.shard.local().NodePropertyTypeAddPeered("City", "population", "integer").get();
    
    graph.shard.local().RelationshipTypeInsertPeered("Links").get();
    graph.shard.local().RelationshipPropertyTypeAddPeered("Links", "since", "integer").get();

    // Create test nodes
    uint64_t zenith = graph.shard.local().NodeAddPeered("City", "Zenith", "{\"name\": \"Zenith\", \"population\": 100}").get();
    uint64_t arcadia = graph.shard.local().NodeAddPeered("City", "Arcadia", "{\"name\": \"Arcadia\", \"population\": 200}").get();
    uint64_t verona = graph.shard.local().NodeAddPeered("City", "Verona", "{\"name\": \"Verona\", \"population\": 300}").get();
    uint64_t nebula = graph.shard.local().NodeAddPeered("City", "Nebula", "{\"name\": \"Nebula\", \"population\": 400}").get();

    // Create relationships
    graph.shard.local().RelationshipAddPeered("Links", arcadia, zenith, "{\"since\": 2020}").get();
    graph.shard.local().RelationshipAddPeered("Links", arcadia, verona, "{\"since\": 2026}").get();
    graph.shard.local().RelationshipAddPeered("Links", verona, nebula, "{\"since\": 2026}").get();

    // Setup Road schema for Cheapest path tests
    graph.shard.local().RelationshipTypeInsertPeered("Road").get();
    graph.shard.local().RelationshipPropertyTypeAddPeered("Road", "weight", "double").get();
    graph.shard.local().RelationshipPropertyTypeAddPeered("Road", "cost", "integer").get();

    // Create Road relationships
    graph.shard.local().RelationshipAddPeered("Road", arcadia, verona, "{\"weight\": 1.0, \"cost\": 10}").get();
    graph.shard.local().RelationshipAddPeered("Road", verona, nebula, "{\"weight\": 1.0, \"cost\": 10}").get();
    graph.shard.local().RelationshipAddPeered("Road", arcadia, nebula, "{\"weight\": 5.0, \"cost\": 50}").get();

    SECTION("Phase 1: Quantified Paths - Exact / Range / Wildcard") {
        // 1. Exact hops: {2}
        {
            std::string query_str = "MATCH (a)-[:Links]->{2}(b) WHERE a.name = 'Arcadia' RETURN b.name";
            auto query = GqlParser::parse(query_str);
            GqlTypechecker::typecheck(graph, query);
            GqlOptimizer::optimize(query);
            std::string results = GqlExecutor::execute(graph, std::move(query)).get();
            // 2 hops from Arcadia is Nebula
            REQUIRE(results.find("Nebula") != std::string::npos);
            REQUIRE(results.find("Zenith") == std::string::npos);
            REQUIRE(results.find("Verona") == std::string::npos);
        }

        // 2. Open min range: {2,}
        {
            std::string query_str = "MATCH (a)-[:Links]->{2,}(b) WHERE a.name = 'Arcadia' RETURN b.name";
            auto query = GqlParser::parse(query_str);
            GqlTypechecker::typecheck(graph, query);
            GqlOptimizer::optimize(query);
            std::string results = GqlExecutor::execute(graph, std::move(query)).get();
            REQUIRE(results.find("Nebula") != std::string::npos);
            REQUIRE(results.find("Verona") == std::string::npos);
        }

        // 3. Open max range: {,1}
        {
            std::string query_str = "MATCH (a)-[:Links]->{,1}(b) WHERE a.name = 'Arcadia' RETURN b.name ORDER BY b.name";
            auto query = GqlParser::parse(query_str);
            GqlTypechecker::typecheck(graph, query);
            GqlOptimizer::optimize(query);
            std::string results = GqlExecutor::execute(graph, std::move(query)).get();
            // 0-hop (Arcadia) and 1-hop (Zenith, Verona)
            REQUIRE(results.find("Arcadia") != std::string::npos);
            REQUIRE(results.find("Zenith") != std::string::npos);
            REQUIRE(results.find("Verona") != std::string::npos);
            REQUIRE(results.find("Nebula") == std::string::npos);
        }

        // 4. Wildcard *: 0 or more hops
        {
            std::string query_str = "MATCH (a)-[:Links]->*(b) WHERE a.name = 'Arcadia' RETURN b.name ORDER BY b.name";
            auto query = GqlParser::parse(query_str);
            GqlTypechecker::typecheck(graph, query);
            GqlOptimizer::optimize(query);
            std::string results = GqlExecutor::execute(graph, std::move(query)).get();
            REQUIRE(results.find("Arcadia") != std::string::npos);
            REQUIRE(results.find("Zenith") != std::string::npos);
            REQUIRE(results.find("Verona") != std::string::npos);
            REQUIRE(results.find("Nebula") != std::string::npos);
        }
    }

    SECTION("Phase 2: Comma-Separated Path Patterns (Graph Patterns)") {
        // Multi pattern match
        std::string query_str = "MATCH (a)-[:Links]->(b), (b)-[:Links]->(c) WHERE a.name = 'Arcadia' RETURN c.name";
        auto query = GqlParser::parse(query_str);
        GqlTypechecker::typecheck(graph, query);
        GqlOptimizer::optimize(query);
        std::string results = GqlExecutor::execute(graph, std::move(query)).get();
        REQUIRE(results.find("Nebula") != std::string::npos);
    }

    SECTION("Phase 2: Grouped Optional MATCH") {
        // Success case
        {
            std::string query_str = "OPTIONAL MATCH (a)-[:Links]->(b), (b)-[:Links]->(c) WHERE a.name = 'Arcadia' RETURN c.name";
            auto query = GqlParser::parse(query_str);
            GqlTypechecker::typecheck(graph, query);
            GqlOptimizer::optimize(query);
            std::string results = GqlExecutor::execute(graph, std::move(query)).get();
            REQUIRE(results.find("Nebula") != std::string::npos);
        }

        // Fail case (whole group should fail, yielding NULL for b and c)
        {
            std::string query_str = "OPTIONAL MATCH (a)-[:Links]->(b), (b)-[:Links]->(c) WHERE a.name = 'Verona' RETURN b.name, c.name";
            auto query = GqlParser::parse(query_str);
            GqlTypechecker::typecheck(graph, query);
            GqlOptimizer::optimize(query);
            std::string results = GqlExecutor::execute(graph, std::move(query)).get();
            // Since Verona has a neighbor Nebula but Nebula has no OUT links, the second pattern fails.
            // The whole group should fail to match Verona. b and c must be NULL.
            REQUIRE(results.find("null") != std::string::npos);
        }
    }

    SECTION("Phase 3: Inline WHERE Filters in Node and Edge Patterns") {
        // 1. Node inline filter
        {
            std::string query_str = "MATCH (a:City WHERE a.name = 'Arcadia')-[:Links]->(b:City WHERE b.population > 250) RETURN b.name";
            auto query = GqlParser::parse(query_str);
            GqlTypechecker::typecheck(graph, query);
            GqlOptimizer::optimize(query);
            std::string results = GqlExecutor::execute(graph, std::move(query)).get();
            // Verona has population 300 (> 250), Zenith has 100 (<= 250)
            REQUIRE(results.find("Verona") != std::string::npos);
            REQUIRE(results.find("Zenith") == std::string::npos);
        }

        // 2. Edge inline filter
        {
            std::string query_str = "MATCH (a)-[e:Links WHERE e.since = 2026]->(b) WHERE a.name = 'Arcadia' RETURN b.name";
            auto query = GqlParser::parse(query_str);
            GqlTypechecker::typecheck(graph, query);
            GqlOptimizer::optimize(query);
            std::string results = GqlExecutor::execute(graph, std::move(query)).get();
            // Arcadia -Links {since: 2026}-> Verona, Arcadia -Links {since: 2020}-> Zenith
            REQUIRE(results.find("Verona") != std::string::npos);
            REQUIRE(results.find("Zenith") == std::string::npos);
        }
    }

    SECTION("Phase 4: Questioned Paths (Optional Path Patterns)") {
        std::string query_str = "MATCH ((a)-[:Links]->(b))? WHERE a.name = 'Arcadia' RETURN b.name ORDER BY b.name";
        auto query = GqlParser::parse(query_str);
        GqlTypechecker::typecheck(graph, query);
        GqlOptimizer::optimize(query);
        std::string results = GqlExecutor::execute(graph, std::move(query)).get();
        // Should yield Arcadia (0-hop) and Zenith, Verona (1-hop)
        REQUIRE(results.find("Arcadia") != std::string::npos);
        REQUIRE(results.find("Zenith") != std::string::npos);
        REQUIRE(results.find("Verona") != std::string::npos);
        REQUIRE(results.find("Nebula") == std::string::npos);
    }

    SECTION("Phase 5: Cheapest Paths (Weighted Shortest Path)") {
        // Find cheapest path using weight property (Arcadia -> Verona -> Nebula has total weight 2.0, direct is 5.0)
        // So the cheapest path should contain Verona.
        {
            std::string query_str = "MATCH p = CHEAPEST PATH ((a)-[e:Road]->(b)) COST e.weight WHERE a.name = 'Arcadia' AND b.name = 'Nebula' RETURN p";
            auto query = GqlParser::parse(query_str);
            GqlTypechecker::typecheck(graph, query);
            GqlOptimizer::optimize(query);
            std::string results = GqlExecutor::execute(graph, std::move(query)).get();
            // Should find the 2-hop path (containing Verona)
            REQUIRE(results.find("Verona") != std::string::npos);
        }
    }
}
