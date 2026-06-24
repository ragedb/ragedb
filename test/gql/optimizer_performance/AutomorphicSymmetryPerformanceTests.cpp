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

using namespace ragedb;
using namespace ragedb::gql;

TEST_CASE("Automorphic Graph Symmetry Deduplication Performance Benchmarks", "[gql_optimizer_perf]") {
    auto graph = Graph("gql_test_symmetry_perf");
    graph.Start().get();
    graph.Clear();
    PerformanceGraphStopGuard guard(graph);

    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().RelationshipTypeInsertPeered("FRIEND").get();

    // Insert 150 Person nodes
    std::vector<uint64_t> nodes;
    for (int i = 0; i < 150; ++i) {
        std::string name = "Person" + std::to_string(i);
        uint64_t id = graph.shard.local().NodeAddPeered("Person", name, "{}").get();
        nodes.push_back(id);
    }

    // Connect them in directed cycles of length 3 (triangles) to generate traversals
    // For each i: i -> i+1, i+1 -> i+2, i+2 -> i
    for (int i = 0; i < 150; ++i) {
        int n1 = i;
        int n2 = (i + 1) % 150;
        int n3 = (i + 2) % 150;

        graph.shard.local().RelationshipAddPeered("FRIEND", nodes[n1], nodes[n2], "{}").get();
        graph.shard.local().RelationshipAddPeered("FRIEND", nodes[n2], nodes[n3], "{}").get();
        graph.shard.local().RelationshipAddPeered("FRIEND", nodes[n3], nodes[n1], "{}").get();
    }

    std::cout << "\n=========================================\n";
    std::cout << "   AUTOMORPHIC SYMMETRY PERFORMANCE BENCH\n";
    std::cout << "=========================================\n";
    run_bench(
        graph,
        "Phase 10: Automorphic Symmetry",
        "MATCH (a:Person)-[:FRIEND]->(b:Person)-[:FRIEND]->(c:Person)-[:FRIEND]->(a) RETURN count(*)",
        "NO_SEMANTIC MATCH (a:Person)-[:FRIEND]->(b:Person)-[:FRIEND]->(c:Person)-[:FRIEND]->(a) RETURN count(*)"
    );
    std::cout << "=========================================\n";
}
