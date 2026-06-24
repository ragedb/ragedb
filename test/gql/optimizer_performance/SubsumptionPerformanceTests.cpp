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

TEST_CASE("Subsumption Performance Benchmarks", "[gql_optimizer_perf]") {
    auto graph = Graph("gql_test_subsumption_perf");
    graph.Start().get();
    graph.Clear();
    PerformanceGraphStopGuard guard(graph);

    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().NodeTypeInsertPeered("SubsumptionNode").get();
    graph.shard.local().NodePropertyTypeAddPeered("SubsumptionNode", "age", "integer").get();
    graph.shard.local().RelationshipTypeInsertPeered("SUB_FRIEND").get();

    for (int i = 0; i < 5; ++i) {
        std::string name = "Person" + std::to_string(i);
        graph.shard.local().NodeAddPeered("Person", name, "{\"name\": \"" + name + "\"}").get();
    }
    for (int i = 0; i < 20; ++i) {
        std::string name = "SubNode" + std::to_string(i);
        graph.shard.local().NodeAddPeered("SubsumptionNode", name, "{\"age\": " + std::to_string(15 + (i % 20)) + "}").get();
    }
    for (int i = 0; i < 5; ++i) {
        uint64_t p_id = graph.shard.local().NodeGetPeered("Person", "Person" + std::to_string(i)).get().getId();
        for (int j = 0; j < 20; ++j) {
            uint64_t s_id = graph.shard.local().NodeGetPeered("SubsumptionNode", "SubNode" + std::to_string(j)).get().getId();
            graph.shard.local().RelationshipAddPeered("SUB_FRIEND", p_id, s_id, "{}").get();
        }
    }

    std::cout << "\n=========================================\n";
    std::cout << "   SUBSUMPTION PERFORMANCE BENCH\n";
    std::cout << "=========================================\n";
    run_bench(graph, "Phase 6: Subsumption Pruning", "MATCH (p:Person)-[:SUB_FRIEND]->(b:SubsumptionNode), (p)-[:SUB_FRIEND]->(c:SubsumptionNode) WHERE b.age > 21 AND c.age > 18 RETURN p.name", "NO_SEMANTIC MATCH (p:Person)-[:SUB_FRIEND]->(b:SubsumptionNode), (p)-[:SUB_FRIEND]->(c:SubsumptionNode) WHERE b.age > 21 AND c.age > 18 RETURN p.name");
    std::cout << "=========================================\n";
}
