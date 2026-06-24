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
#include "../../src/gql/GqlVirtualCatalog.h"
#include <iostream>
#include <chrono>

using namespace ragedb;
using namespace ragedb::gql;

struct PerformanceGraphStopGuard {
    Graph& g;
    bool stopped = false;
    explicit PerformanceGraphStopGuard(Graph& graph) : g(graph) {}
    ~PerformanceGraphStopGuard() {
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

TEST_CASE("GQL Semantic Query Optimizer Performance Benchmarks", "[gql_optimizer_perf]") {
    auto graph = Graph("gql_test_optimizer_perf");
    graph.Start().get();
    graph.Clear();
    PerformanceGraphStopGuard guard(graph);
    
    // Setup schema
    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "age", "integer").get();

    graph.shard.local().NodeTypeInsertPeered("FriendNode").get();
    graph.shard.local().NodePropertyTypeAddPeered("FriendNode", "age", "integer").get();

    graph.shard.local().NodeTypeInsertPeered("PosetNode").get();
    graph.shard.local().NodePropertyTypeAddPeered("PosetNode", "age", "integer").get();

    graph.shard.local().NodeTypeInsertPeered("Shipment").get();
    graph.shard.local().NodePropertyTypeAddPeered("Shipment", "name", "string").get();

    graph.shard.local().NodeTypeInsertPeered("Location").get();
    graph.shard.local().NodePropertyTypeAddPeered("Location", "name", "string").get();

    graph.shard.local().RelationshipTypeInsertPeered("SHIPPED_FROM").get();
    graph.shard.local().RelationshipTypeInsertPeered("FRIEND").get();

    // Register active constraints
    GqlVirtualCatalog::local().clear();
    GqlVirtualCatalog::local().add_constraint("PositiveAge", "MATCH (p:Person) WHERE p.age < 0 RETURN p");
    GqlVirtualCatalog::local().add_constraint("MandatoryShippedFrom", "MATCH (s:Shipment) WHERE NOT EXISTS { MATCH (s)-[:SHIPPED_FROM]->(l:Location) } RETURN s");

    // Insert dummy data
    // 1. Shipment and Location data (1,000 nodes and 1,000 edges)
    for (int i = 0; i < 1000; ++i) {
        std::string loc_name = "Location" + std::to_string(i);
        uint64_t loc_id = graph.shard.local().NodeAddPeered("Location", loc_name, "{\"name\": \"" + loc_name + "\"}").get();
        
        std::string ship_name = "Shipment" + std::to_string(i);
        uint64_t ship_id = graph.shard.local().NodeAddPeered("Shipment", ship_name, "{\"name\": \"" + ship_name + "\"}").get();
        
        graph.shard.local().RelationshipAddPeered("SHIPPED_FROM", ship_id, loc_id, "{}").get();
    }

    // 2. Person nodes (5 nodes)
    for (int i = 0; i < 5; ++i) {
        std::string name = "Person" + std::to_string(i);
        graph.shard.local().NodeAddPeered("Person", name, "{\"name\": \"" + name + "\", \"age\": " + std::to_string(20 + i) + "}").get();
    }

    // 3. FriendNode nodes (2,000 nodes)
    for (int i = 0; i < 2000; ++i) {
        std::string name = "Friend" + std::to_string(i);
        graph.shard.local().NodeAddPeered("FriendNode", name, "{\"age\": " + std::to_string(20 + (i % 50)) + "}").get();
    }

    // 4. FRIEND edges (10,000 edges from Person to FriendNode)
    for (int i = 0; i < 5; ++i) {
        uint64_t p_id = graph.shard.local().NodeGetPeered("Person", "Person" + std::to_string(i)).get().getId();
        for (int j = 0; j < 2000; ++j) {
            uint64_t f_id = graph.shard.local().NodeGetPeered("FriendNode", "Friend" + std::to_string(j)).get().getId();
            graph.shard.local().RelationshipAddPeered("FRIEND", p_id, f_id, "{}").get();
        }
    }

    // 5. PosetNode (15 nodes for Phase 3 constraint cycle benchmark)
    for (int i = 0; i < 15; ++i) {
        std::string name = "PosetNode" + std::to_string(i);
        graph.shard.local().NodeAddPeered("PosetNode", name, "{\"age\": " + std::to_string(10 + i) + "}").get();
    }

    auto run_bench = [&](const std::string& name, const std::string& query, const std::string& unopt_query) {
        const int iterations = 50;

        // 1. Cold Semantic (Cache Miss)
        auto start_cold = std::chrono::steady_clock::now();
        for (int i = 0; i < iterations; ++i) {
            GqlExecutor::execute(graph, "CALL CLEAR CACHE").get();
            GqlExecutor::execute(graph, query).get();
        }
        auto end_cold = std::chrono::steady_clock::now();
        double time_cold = std::chrono::duration<double, std::milli>(end_cold - start_cold).count() / iterations;

        // 2. Hot Semantic (Cache Hit)
        GqlExecutor::execute(graph, query).get(); // warm up
        auto start_hot = std::chrono::steady_clock::now();
        for (int i = 0; i < iterations; ++i) {
            GqlExecutor::execute(graph, query).get();
        }
        auto end_hot = std::chrono::steady_clock::now();
        double time_hot = std::chrono::duration<double, std::milli>(end_hot - start_hot).count() / iterations;

        // 3. Unoptimized (Semantic OFF)
        auto start_unopt = std::chrono::steady_clock::now();
        for (int i = 0; i < iterations; ++i) {
            GqlExecutor::execute(graph, unopt_query).get();
        }
        auto end_unopt = std::chrono::steady_clock::now();
        double time_unopt = std::chrono::duration<double, std::milli>(end_unopt - start_unopt).count() / iterations;

        std::cout << "| " << name << " | " 
                  << time_cold << " ms | " 
                  << time_hot << " ms | " 
                  << time_unopt << " ms |\n";
    };

    std::cout << "\n=========================================\n";
    std::cout << "   SEMANTIC OPTIMIZER PERFORMANCE BENCH   \n";
    std::cout << "=========================================\n";
    std::cout << "| Optimization Pass | Cold (Cache Miss) | Hot (Cache Hit) | Unoptimized (Bypassed) |\n";
    std::cout << "|---|---|---|---|\n";
    run_bench("Phase 1: Contradiction Pruning", "MATCH (x:Person) WHERE x.age = -5 RETURN x.name", "NO_SEMANTIC MATCH (x:Person) WHERE x.age = -5 RETURN x.name");
    run_bench("Phase 2: Join Elimination", "MATCH (s:Shipment)-[:SHIPPED_FROM]->(l:Location) RETURN s.name", "NO_SEMANTIC MATCH (s:Shipment)-[:SHIPPED_FROM]->(l:Location) RETURN s.name");
    run_bench("Phase 3: Relational Cycle Pruning", "MATCH (a:PosetNode), (b:PosetNode), (c:PosetNode) WHERE a.age < b.age AND b.age <= c.age AND c.age < a.age RETURN a.age", "NO_SEMANTIC MATCH (a:PosetNode), (b:PosetNode), (c:PosetNode) WHERE a.age < b.age AND b.age <= c.age AND c.age < a.age RETURN a.age");
    run_bench("Phase 4: Algebraic Sum Rewrite", "MATCH (p:Person)-[:FRIEND]->(f:FriendNode) RETURN p.name, sum(p.age * f.age)", "NO_SEMANTIC MATCH (p:Person)-[:FRIEND]->(f:FriendNode) RETURN p.name, sum(p.age * f.age)");
    std::cout << "=========================================\n\n";

    GqlVirtualCatalog::local().clear();
    guard.stop();
}
