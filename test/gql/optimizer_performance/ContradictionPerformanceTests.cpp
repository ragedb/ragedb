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

#include "PerformanceHelper.h"
#include "../../src/gql/GqlVirtualCatalog.h"

using namespace ragedb;
using namespace ragedb::gql;

TEST_CASE("Contradiction Performance Benchmarks", "[gql_optimizer_perf]") {
    auto graph = Graph("gql_test_contradiction_perf");
    graph.Start().get();
    graph.Clear();
    PerformanceGraphStopGuard guard(graph);

    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "age", "integer").get();

    graph.shard.local().NodeTypeInsertPeered("PosetNode").get();
    graph.shard.local().NodePropertyTypeAddPeered("PosetNode", "age", "integer").get();

    GqlVirtualCatalog::local().clear();
    GqlVirtualCatalog::local().add_constraint("PositiveAge", "MATCH (p:Person) WHERE p.age < 0 RETURN p");

    // Dummy data
    for (int i = 0; i < 5; ++i) {
        std::string name = "Person" + std::to_string(i);
        graph.shard.local().NodeAddPeered("Person", name, "{\"name\": \"" + name + "\", \"age\": " + std::to_string(20 + i) + "}").get();
    }
    for (int i = 0; i < 15; ++i) {
        std::string name = "PosetNode" + std::to_string(i);
        graph.shard.local().NodeAddPeered("PosetNode", name, "{\"age\": " + std::to_string(10 + i) + "}").get();
    }

    std::cout << "\n=========================================\n";
    std::cout << "   CONTRADICTION PERFORMANCE BENCH\n";
    std::cout << "=========================================\n";
    run_bench(graph, "Phase 1: Contradiction Pruning", "MATCH (x:Person) WHERE x.age = -5 RETURN x.name", "NO_SEMANTIC MATCH (x:Person) WHERE x.age = -5 RETURN x.name");
    run_bench(graph, "Phase 3: Relational Cycle Pruning", "MATCH (a:PosetNode), (b:PosetNode), (c:PosetNode) WHERE a.age < b.age AND b.age <= c.age AND c.age < a.age RETURN a.age", "NO_SEMANTIC MATCH (a:PosetNode), (b:PosetNode), (c:PosetNode) WHERE a.age < b.age AND b.age <= c.age AND c.age < a.age RETURN a.age");
    std::cout << "=========================================\n";

    GqlVirtualCatalog::local().clear();
}
