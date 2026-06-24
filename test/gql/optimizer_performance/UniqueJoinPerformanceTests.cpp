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

TEST_CASE("Unique Join Performance Benchmarks", "[gql_optimizer_perf]") {
    auto graph = Graph("gql_test_unique_join_perf");
    graph.Start().get();
    graph.Clear();
    PerformanceGraphStopGuard guard(graph);

    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().NodeTypeInsertPeered("FriendNode").get();
    graph.shard.local().NodePropertyTypeAddPeered("FriendNode", "age", "integer").get();
    graph.shard.local().NodeTypeInsertPeered("Category").get();
    graph.shard.local().NodeTypeInsertPeered("TargetNode").get();
    
    graph.shard.local().RelationshipTypeInsertPeered("FRIEND").get();
    graph.shard.local().RelationshipTypeInsertPeered("UNIQUE_REL").get();

    // Setup graph nodes
    for (int i = 0; i < 5; ++i) {
        std::string name = "Person" + std::to_string(i);
        graph.shard.local().NodeAddPeered("Person", name, "{\"name\": \"" + name + "\"}").get();
    }
    for (int i = 0; i < 1000; ++i) {
        std::string name = "Friend" + std::to_string(i);
        graph.shard.local().NodeAddPeered("FriendNode", name, "{\"age\": " + std::to_string(20 + (i % 50)) + "}").get();
    }
    
    // Connect relationships
    for (int i = 0; i < 5; ++i) {
        uint64_t p_id = graph.shard.local().NodeGetPeered("Person", "Person" + std::to_string(i)).get().getId();
        for (int j = 0; j < 1000; ++j) {
            uint64_t f_id = graph.shard.local().NodeGetPeered("FriendNode", "Friend" + std::to_string(j)).get().getId();
            graph.shard.local().RelationshipAddPeered("FRIEND", p_id, f_id, "{}").get();
        }
    }

    std::cout << "\n=========================================\n";
    std::cout << "   UNIQUE JOIN PERFORMANCE BENCH\n";
    std::cout << "=========================================\n";

    GqlVirtualCatalog::local().clear();
    GqlVirtualCatalog::local().add_constraint("UniqueConstraint", 
        "MATCH (s:Person)-[r:UNIQUE_REL]->(t1:TargetNode) MATCH (s)-[:UNIQUE_REL]->(t2:TargetNode) WHERE t1 != t2 RETURN s");
    run_bench(
        graph,
        "Phase 14: Unique Join Elimination",
        "OPTIONAL MATCH (p:Person)-[:UNIQUE_REL]->(t:TargetNode) RETURN p.name",
        "NO_SEMANTIC OPTIONAL MATCH (p:Person)-[:UNIQUE_REL]->(t:TargetNode) RETURN p.name",
        20
    );

    std::cout << "=========================================\n";
    GqlVirtualCatalog::local().clear();
}
