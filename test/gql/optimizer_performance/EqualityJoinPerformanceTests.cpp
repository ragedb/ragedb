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

TEST_CASE("Equality Join Performance Benchmarks", "[gql_optimizer_perf]") {
    auto graph = Graph("gql_test_equality_join_perf");
    graph.Start().get();
    graph.Clear();
    PerformanceGraphStopGuard guard(graph);

    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "age", "integer").get();
    graph.shard.local().NodeTypeInsertPeered("Category").get();
    graph.shard.local().NodePropertyTypeAddPeered("Category", "name", "string").get();
    
    graph.shard.local().RelationshipTypeInsertPeered("FRIEND").get();
    graph.shard.local().RelationshipPropertyTypeAddPeered("FRIEND", "weight", "integer").get();
    graph.shard.local().RelationshipTypeInsertPeered("SUB_CATEGORY").get();

    // Setup graph nodes: 1000 Person nodes
    std::vector<uint64_t> person_ids;
    for (int i = 0; i < 1000; ++i) {
        std::string name = "Person" + std::to_string(i);
        uint64_t id = graph.shard.local().NodeAddPeered("Person", name, "{\"name\": \"" + name + "\", \"age\": " + std::to_string(20 + (i % 50)) + "}").get();
        person_ids.push_back(id);
    }
    
    // Connect Person nodes with FRIEND relationships
    for (int i = 0; i < 1000; ++i) {
        uint64_t src = person_ids[i];
        uint64_t tgt = person_ids[(i + 1) % 1000];
        graph.shard.local().RelationshipAddPeered("FRIEND", src, tgt, "{\"weight\": 10}").get();
    }

    std::cout << "\n=========================================\n";
    std::cout << "   EQUALITY JOIN PERFORMANCE BENCH\n";
    std::cout << "=========================================\n";

    run_bench(
        graph,
        "Phase 19: Equality Join Elimination",
        "MATCH (a:Person)-[:FRIEND]->(b:Person) MATCH (a)-[:FRIEND]->(c:Person) WHERE b = c RETURN a.name, b.name",
        "NO_SEMANTIC MATCH (a:Person)-[:FRIEND]->(b:Person) MATCH (a)-[:FRIEND]->(c:Person) WHERE b = c RETURN a.name, b.name",
        2
    );

    std::cout << "=========================================\n";
}
