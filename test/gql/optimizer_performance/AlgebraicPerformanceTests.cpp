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

TEST_CASE("Algebraic Performance Benchmarks", "[gql_optimizer_perf]") {
    auto graph = Graph("gql_test_algebraic_perf");
    graph.Start().get();
    graph.Clear();
    PerformanceGraphStopGuard guard(graph);

    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "age", "integer").get();
    graph.shard.local().NodeTypeInsertPeered("FriendNode").get();
    graph.shard.local().NodePropertyTypeAddPeered("FriendNode", "age", "integer").get();
    graph.shard.local().RelationshipTypeInsertPeered("FRIEND").get();

    for (int i = 0; i < 5; ++i) {
        std::string name = "Person" + std::to_string(i);
        graph.shard.local().NodeAddPeered("Person", name, "{\"name\": \"" + name + "\", \"age\": " + std::to_string(20 + i) + "}").get();
    }
    for (int i = 0; i < 2000; ++i) {
        std::string name = "Friend" + std::to_string(i);
        graph.shard.local().NodeAddPeered("FriendNode", name, "{\"age\": " + std::to_string(20 + (i % 50)) + "}").get();
    }
    for (int i = 0; i < 5; ++i) {
        uint64_t p_id = graph.shard.local().NodeGetPeered("Person", "Person" + std::to_string(i)).get().getId();
        for (int j = 0; j < 2000; ++j) {
            uint64_t f_id = graph.shard.local().NodeGetPeered("FriendNode", "Friend" + std::to_string(j)).get().getId();
            graph.shard.local().RelationshipAddPeered("FRIEND", p_id, f_id, "{}").get();
        }
    }
    for (int i = 0; i < 2000; ++i) {
        uint64_t f1_id = graph.shard.local().NodeGetPeered("FriendNode", "Friend" + std::to_string(i)).get().getId();
        uint64_t f2_id = graph.shard.local().NodeGetPeered("FriendNode", "Friend" + std::to_string((i + 1) % 2000)).get().getId();
        graph.shard.local().RelationshipAddPeered("FRIEND", f1_id, f2_id, "{}").get();
    }

    std::cout << "\n=========================================\n";
    std::cout << "   ALGEBRAIC PERFORMANCE BENCH\n";
    std::cout << "=========================================\n";
    run_bench(graph, "Phase 4: Algebraic Sum Rewrite", "MATCH (p:Person)-[:FRIEND]->(f:FriendNode) RETURN p.name, sum(p.age * f.age)", "NO_SEMANTIC MATCH (p:Person)-[:FRIEND]->(f:FriendNode) RETURN p.name, sum(p.age * f.age)");
    run_bench(graph, "Phase 4.5: Algebraic Path Count Rewrite", "MATCH (p:Person)-[:FRIEND]->(a)-[:FRIEND]->(b)-[:FRIEND]->(f:FriendNode) RETURN p.name, count(f)", "NO_SEMANTIC MATCH (p:Person)-[:FRIEND]->(a)-[:FRIEND]->(b)-[:FRIEND]->(f:FriendNode) RETURN p.name, count(f)");
    std::cout << "=========================================\n";
}
