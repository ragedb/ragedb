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

TEST_CASE("Phases 11 to 15 Performance Benchmarks", "[gql_optimizer_perf]") {
    auto graph = Graph("gql_test_phases_11_15_perf");
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
    std::cout << "   PHASES 11 TO 15 PERFORMANCE BENCH\n";
    std::cout << "=========================================\n";

    // 1. Phase 11: Schema Path Unsatisfiability Pruning
    GqlVirtualCatalog::local().clear();
    GqlVirtualCatalog::local().add_allowed_relationship("Person", "FRIEND", "Person");
    run_bench(
        graph,
        "Phase 11: Schema Unsatisfiability",
        "MATCH (p:Person)-[:FRIEND]->(c:Category) RETURN p.name",
        "NO_SEMANTIC MATCH (p:Person)-[:FRIEND]->(c:Category) RETURN p.name",
        20
    );

    // 2. Phase 12: Optional Match Promotion
    run_bench(
        graph,
        "Phase 12: Optional Match Promotion",
        "OPTIONAL MATCH (p:Person)-[:FRIEND]->(f:FriendNode) WHERE f.age > 45 RETURN p.name, f.age",
        "NO_SEMANTIC OPTIONAL MATCH (p:Person)-[:FRIEND]->(f:FriendNode) WHERE f.age > 45 RETURN p.name, f.age",
        20
    );

    // 3. Phase 13: Degree-Constraint Pruning
    run_bench(
        graph,
        "Phase 13: Degree-Constraint Pruning",
        "MATCH (p:Person) WHERE size((p)-[:FRIEND]->()) > 500 RETURN p.name",
        "NO_SEMANTIC MATCH (p:Person) WHERE size((p)-[:FRIEND]->()) > 500 RETURN p.name",
        20
    );

    // 4. Phase 14: Unique Join Elimination
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

    // 5. Phase 15: Limit Pushdown
    run_bench(
        graph,
        "Phase 15: Limit Pushdown",
        "MATCH (p:Person)-[:FRIEND]->(f:FriendNode) RETURN p.name, f.age LIMIT 5",
        "NO_SEMANTIC MATCH (p:Person)-[:FRIEND]->(f:FriendNode) RETURN p.name, f.age LIMIT 5",
        20
    );

    std::cout << "=========================================\n";
    GqlVirtualCatalog::local().clear();
}
