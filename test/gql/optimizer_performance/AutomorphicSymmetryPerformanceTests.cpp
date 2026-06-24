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

    // Insert 1 clique of 16 Person nodes.
    // This creates a dense and predictable topology with a safe number of directed triangles:
    // 1 clique * (16 * 15 * 14) cycles = 3,360 directed cycle paths of length 3.
    // This runs fast and stays well within Seastar memory limits (bad_alloc) while still
    // showing a substantial speedup from early pruning (visiting 560 states vs 3,360 states).
    for (int c = 0; c < 1; ++c) {
        std::vector<uint64_t> clique_nodes;
        for (int i = 0; i < 16; ++i) {
            std::string name = "Person_c" + std::to_string(c) + "_" + std::to_string(i);
            uint64_t id = graph.shard.local().NodeAddPeered("Person", name, "{}").get();
            clique_nodes.push_back(id);
        }
        // Fully connect the clique with directed edges
        for (int i = 0; i < 16; ++i) {
            for (int j = 0; j < 16; ++j) {
                if (i != j) {
                    graph.shard.local().RelationshipAddPeered("FRIEND", clique_nodes[i], clique_nodes[j], "{}").get();
                }
            }
        }
    }

    // Force disable Honeycomb and LFTJ globally for the duration of this benchmark
    // to compare standard traversal with pruning vs standard traversal without pruning.
    GqlExecutor::force_disable_honeycomb = true;
    GqlExecutor::force_disable_lftj = true;

    std::cout << "\n=========================================\n";
    std::cout << "   AUTOMORPHIC SYMMETRY PERFORMANCE BENCH\n";
    std::cout << "=========================================\n";
    std::string opt_res = GqlExecutor::execute(graph, "MATCH (a:Person)-[:FRIEND]->(b:Person)-[:FRIEND]->(c:Person)-[:FRIEND]->(a) RETURN count(*)").get();
    std::string unopt_res = GqlExecutor::execute(graph, "NO_SEMANTIC MATCH (a:Person)-[:FRIEND]->(b:Person)-[:FRIEND]->(c:Person)-[:FRIEND]->(a) RETURN count(*)").get();
    std::cout << "Optimized result: " << opt_res << std::endl;
    std::cout << "Unoptimized result: " << unopt_res << std::endl;

    // Use 20 iterations to get clean, stable timings now that the graph size is safe.
    run_bench(
        graph,
        "Phase 10: Automorphic Symmetry",
        "MATCH (a:Person)-[:FRIEND]->(b:Person)-[:FRIEND]->(c:Person)-[:FRIEND]->(a) RETURN count(*)",
        "NO_SEMANTIC MATCH (a:Person)-[:FRIEND]->(b:Person)-[:FRIEND]->(c:Person)-[:FRIEND]->(a) RETURN count(*)",
        20
    );
    std::cout << "=========================================\n";

    // Restore executor flags
    GqlExecutor::force_disable_honeycomb = false;
    GqlExecutor::force_disable_lftj = false;
}
