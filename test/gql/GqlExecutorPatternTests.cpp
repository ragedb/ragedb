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
    std::cout << "=== RUNNING TEST CASE GQL Execution Advanced Pattern Matching Tests ===" << std::endl;
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

    SECTION("Phase 7: Questioned Paths (Singleton Variable Binding)") {
        // 1. When match exists, binds to singleton neighbors
        {
            std::string query_str = "MATCH ((a)-[:Links]->(b))? WHERE a.name = 'Arcadia' RETURN b.name ORDER BY b.name";
            auto query = GqlParser::parse(query_str);
            GqlTypechecker::typecheck(graph, query);
            GqlOptimizer::optimize(query);
            std::string results = GqlExecutor::execute(graph, std::move(query)).get();
            // Should yield Zenith and Verona, but NOT Arcadia (since it is a singleton optional match, not a 0-hop)
            REQUIRE(results.find("Zenith") != std::string::npos);
            REQUIRE(results.find("Verona") != std::string::npos);
            REQUIRE(results.find("Arcadia") == std::string::npos);
        }

        // 2. When match does not exist, binds to null
        {
            std::string query_str = "MATCH ((a)-[:Links]->(b))? WHERE a.name = 'Nebula' RETURN b.name";
            auto query = GqlParser::parse(query_str);
            GqlTypechecker::typecheck(graph, query);
            GqlOptimizer::optimize(query);
            std::string results = GqlExecutor::execute(graph, std::move(query)).get();
            std::cout << "DEBUG QUESTIONED PATH: results=" << results << std::endl;
            // Nebula has no outgoing links, so b should bind to null
            REQUIRE(results.find("null") != std::string::npos);
        }
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

    SECTION("Phase 6: Arbitrary Cost Expressions in Cheapest Path") {
        // 1. Inline edge COST expression combining two properties
        {
            std::string query_str = "MATCH p = CHEAPEST PATH ((a)-[e:Road COST e.weight + e.cost]->(b)) WHERE a.name = 'Arcadia' AND b.name = 'Nebula' RETURN p";
            auto query = GqlParser::parse(query_str);
            GqlTypechecker::typecheck(graph, query);
            GqlOptimizer::optimize(query);
            std::string results = GqlExecutor::execute(graph, std::move(query)).get();
            // Should find the 2-hop path (containing Verona, cost 22 vs 55)
            REQUIRE(results.find("Verona") != std::string::npos);
        }

        // 2. Inline edge COST invalid combination with WHERE
        {
            std::string query_str = "MATCH p = CHEAPEST PATH ((a)-[e:Road WHERE e.cost > 0 COST e.weight]->(b)) WHERE a.name = 'Arcadia' AND b.name = 'Nebula' RETURN p";
            REQUIRE_THROWS_AS(GqlParser::parse(query_str), std::runtime_error);
        }
    }

    SECTION("Phase 8: Match Modes and Path Modes") {
        // Add a cycle: Nebula -> Arcadia
        graph.shard.local().RelationshipAddPeered("Links", nebula, arcadia, "{\"since\": 2026}").get();

        // 1. Path Mode: ACYCLIC (no repeated nodes)
        {
            std::string query_str = "MATCH ACYCLIC (a)-[:Links]->*(b) WHERE a.name = 'Arcadia' RETURN b.name ORDER BY b.name";
            auto query = GqlParser::parse(query_str);
            GqlTypechecker::typecheck(graph, query);
            GqlOptimizer::optimize(query);
            std::string results = GqlExecutor::execute(graph, std::move(query)).get();
            // Expected nodes: Arcadia, Verona, Nebula, Zenith
            REQUIRE(results.find("Arcadia") != std::string::npos);
            REQUIRE(results.find("Zenith") != std::string::npos);
            REQUIRE(results.find("Verona") != std::string::npos);
            REQUIRE(results.find("Nebula") != std::string::npos);
        }

        // 2. Path Mode: SIMPLE (no repeated nodes except first and last can be same)
        {
            {
                std::string query_str = "MATCH ACYCLIC (a)-[:Links]->{3}(b) WHERE a.name = 'Arcadia' RETURN b.name";
                auto query = GqlParser::parse(query_str);
                GqlTypechecker::typecheck(graph, query);
                GqlOptimizer::optimize(query);
                std::string results = GqlExecutor::execute(graph, std::move(query)).get();
                // Under ACYCLIC, 3 hops from Arcadia must repeat Arcadia, so it fails, returning empty.
                REQUIRE(results.find("Arcadia") == std::string::npos);
            }
            {
                std::string query_str = "MATCH SIMPLE (a)-[:Links]->{3}(b) WHERE a.name = 'Arcadia' RETURN b.name";
                auto query = GqlParser::parse(query_str);
                GqlTypechecker::typecheck(graph, query);
                GqlOptimizer::optimize(query);
                std::string results = GqlExecutor::execute(graph, std::move(query)).get();
                // Under SIMPLE, Arcadia -> Verona -> Nebula -> Arcadia is allowed because first & last are same.
                REQUIRE(results.find("Arcadia") != std::string::npos);
            }
        }

        // 3. Path Mode: TRAIL (no repeated relationships)
        {
            {
                std::string query_str = "MATCH TRAIL (a)-[:Links]->{4}(b) WHERE a.name = 'Arcadia' RETURN b.name";
                auto query = GqlParser::parse(query_str);
                GqlTypechecker::typecheck(graph, query);
                GqlOptimizer::optimize(query);
                std::string results = GqlExecutor::execute(graph, std::move(query)).get();
                REQUIRE(results.find("Verona") == std::string::npos);
            }
            {
                std::string query_str = "MATCH WALK (a)-[:Links]->{4}(b) WHERE a.name = 'Arcadia' RETURN b.name";
                auto query = GqlParser::parse(query_str);
                GqlTypechecker::typecheck(graph, query);
                GqlOptimizer::optimize(query);
                std::string results = GqlExecutor::execute(graph, std::move(query)).get();
                // 4 hops: Arcadia -> Verona -> Nebula -> Arcadia -> Verona. So b is Verona.
                REQUIRE(results.find("Verona") != std::string::npos);
            }
        }

        // 4. Match Mode: DIFFERENT EDGES (no relationship ID bound to more than one variable)
        {
            {
                std::string query_str = "MATCH DIFFERENT EDGES (a)-[e1:Links]->(b), (c)-[e2:Links]->(d) WHERE a.name = 'Arcadia' AND c.name = 'Arcadia' AND b.name = 'Verona' AND d.name = 'Verona' RETURN b.name, d.name";
                auto query = GqlParser::parse(query_str);
                GqlTypechecker::typecheck(graph, query);
                GqlOptimizer::optimize(query);
                std::string results = GqlExecutor::execute(graph, std::move(query)).get();
                // Under DIFFERENT EDGES, this should be empty/null (not found)
                REQUIRE(results.find("Verona") == std::string::npos);
            }
            {
                std::string query_str = "MATCH REPEATABLE ELEMENTS (a)-[e1:Links]->(b), (c)-[e2:Links]->(d) WHERE a.name = 'Arcadia' AND c.name = 'Arcadia' AND b.name = 'Verona' AND d.name = 'Verona' RETURN b.name, d.name";
                auto query = GqlParser::parse(query_str);
                GqlTypechecker::typecheck(graph, query);
                GqlOptimizer::optimize(query);
                std::string results = GqlExecutor::execute(graph, std::move(query)).get();
                // Under REPEATABLE ELEMENTS, e1 and e2 can be the same, so we get Verona.
                REQUIRE(results.find("Verona") != std::string::npos);
            }
        }
    }

    SECTION("Phase 9: Advanced Cheapest Path Selectors") {
        // Setup Road Zenith relations to create multiple paths of equal minimal cost
        graph.shard.local().RelationshipAddPeered("Road", arcadia, zenith, "{\"weight\": 1.0, \"cost\": 10}").get();
        graph.shard.local().RelationshipAddPeered("Road", zenith, nebula, "{\"weight\": 1.0, \"cost\": 10}").get();

        // 1. ALL CHEAPEST paths
        {
            std::string query_str = "MATCH p = ALL CHEAPEST PATH ((a)-[e:Road]->(b)) COST e.weight WHERE a.name = 'Arcadia' AND b.name = 'Nebula' RETURN p";
            auto query = GqlParser::parse(query_str);
            GqlTypechecker::typecheck(graph, query);
            GqlOptimizer::optimize(query);
            std::string results = GqlExecutor::execute(graph, std::move(query)).get();
            REQUIRE(results.find("Verona") != std::string::npos);
            REQUIRE(results.find("Zenith") != std::string::npos);
        }

        // 2. ANY CHEAPEST paths
        {
            std::string query_str = "MATCH p = ANY CHEAPEST PATH ((a)-[e:Road]->(b)) COST e.weight WHERE a.name = 'Arcadia' AND b.name = 'Nebula' RETURN p";
            auto query = GqlParser::parse(query_str);
            GqlTypechecker::typecheck(graph, query);
            GqlOptimizer::optimize(query);
            std::string results = GqlExecutor::execute(graph, std::move(query)).get();
            REQUIRE((results.find("Verona") != std::string::npos || results.find("Zenith") != std::string::npos));
        }

        // 3. CHEAPEST k paths
        {
            std::string query_str = "MATCH p = CHEAPEST 3 PATHS ((a)-[e:Road]->(b)) COST e.weight WHERE a.name = 'Arcadia' AND b.name = 'Nebula' RETURN p";
            auto query = GqlParser::parse(query_str);
            GqlTypechecker::typecheck(graph, query);
            GqlOptimizer::optimize(query);
            std::string results = GqlExecutor::execute(graph, std::move(query)).get();
            REQUIRE(results.find("Zenith") != std::string::npos);
            REQUIRE(results.find("Verona") != std::string::npos);
        }
    }

    SECTION("Phase 10: Repetitions on Parenthesized Path Groups") {
        // 1. Single-edge quantifier repetition: ((a)-[:Links]->(b)){2}
        {
            std::cout << "=== RUNNING PHASE 10 TEST 1 ===" << std::endl;
            std::string query_str = "MATCH ((a)-[:Links]->(b)){2} WHERE a.name = 'Arcadia' RETURN b.name";
            auto query = GqlParser::parse(query_str);
            GqlTypechecker::typecheck(graph, query);
            GqlOptimizer::optimize(query);
            std::string results = GqlExecutor::execute(graph, std::move(query)).get();
            std::cout << "PHASE 10 TEST 1 RESULTS: " << results << std::endl;
            REQUIRE(results.find("Nebula") != std::string::npos);
        }

        // 2. Multi-edge exact repetition: ((a)-[:Links]->(b)-[:Road]->(c)){1}
        {
            std::cout << "=== RUNNING PHASE 10 TEST 2 ===" << std::endl;
            std::string query_str = "MATCH ((a)-[:Links]->(b)-[:Road]->(c)){1} WHERE a.name = 'Arcadia' RETURN c.name";
            auto query = GqlParser::parse(query_str);
            GqlTypechecker::typecheck(graph, query);
            GqlOptimizer::optimize(query);
            std::string results = GqlExecutor::execute(graph, std::move(query)).get();
            std::cout << "PHASE 10 TEST 2 RESULTS: " << results << std::endl;
            REQUIRE(results.find("Nebula") != std::string::npos);
        }

        // 3. Single-edge range repetition: ((a)-[:Links]->(b)){1,2}
        {
            std::cout << "=== RUNNING PHASE 10 TEST 3 ===" << std::endl;
            std::string query_str = "MATCH ((a)-[:Links]->(b)){1,2} WHERE a.name = 'Arcadia' RETURN b.name ORDER BY b.name";
            auto query = GqlParser::parse(query_str);
            GqlTypechecker::typecheck(graph, query);
            GqlOptimizer::optimize(query);
            std::string results = GqlExecutor::execute(graph, std::move(query)).get();
            std::cout << "PHASE 10 TEST 3 RESULTS: " << results << std::endl;
            REQUIRE(results.find("Zenith") != std::string::npos);
            REQUIRE(results.find("Verona") != std::string::npos);
            REQUIRE(results.find("Nebula") != std::string::npos);
        }
    }

    SECTION("Phase 11: Wildcard & Negated Label Expressions") {
        // 1. Wildcard % (matches any label)
        {
            std::string query_str = "MATCH (a:%)-[e:%]->(b) WHERE a.name = 'Arcadia' RETURN b.name ORDER BY b.name";
            auto query = GqlParser::parse(query_str);
            GqlTypechecker::typecheck(graph, query);
            GqlOptimizer::optimize(query);
            std::string results = GqlExecutor::execute(graph, std::move(query)).get();
            REQUIRE(results.find("Zenith") != std::string::npos);
            REQUIRE(results.find("Verona") != std::string::npos);
            REQUIRE(results.find("Nebula") != std::string::npos);
        }

        // 2. Negated Wildcard !% (matches no label)
        {
            std::string query_str = "MATCH (a:!%) WHERE a.name = 'Arcadia' RETURN a.name";
            auto query = GqlParser::parse(query_str);
            GqlTypechecker::typecheck(graph, query);
            GqlOptimizer::optimize(query);
            std::string results = GqlExecutor::execute(graph, std::move(query)).get();
            REQUIRE(results.find("Arcadia") == std::string::npos);
        }
    }
}

