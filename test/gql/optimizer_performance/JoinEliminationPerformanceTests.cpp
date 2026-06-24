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

TEST_CASE("Join Elimination Performance Benchmarks", "[gql_optimizer_perf]") {
    auto graph = Graph("gql_test_join_elim_perf");
    graph.Start().get();
    graph.Clear();
    PerformanceGraphStopGuard guard(graph);

    graph.shard.local().NodeTypeInsertPeered("Shipment").get();
    graph.shard.local().NodePropertyTypeAddPeered("Shipment", "name", "string").get();
    graph.shard.local().NodeTypeInsertPeered("Location").get();
    graph.shard.local().NodePropertyTypeAddPeered("Location", "name", "string").get();
    graph.shard.local().RelationshipTypeInsertPeered("SHIPPED_FROM").get();

    GqlVirtualCatalog::local().clear();
    GqlVirtualCatalog::local().add_constraint("MandatoryShippedFrom", "MATCH (s:Shipment) WHERE NOT EXISTS { MATCH (s)-[:SHIPPED_FROM]->(l:Location) } RETURN s");

    for (int i = 0; i < 1000; ++i) {
        std::string loc_name = "Location" + std::to_string(i);
        uint64_t loc_id = graph.shard.local().NodeAddPeered("Location", loc_name, "{\"name\": \"" + loc_name + "\"}").get();
        std::string ship_name = "Shipment" + std::to_string(i);
        uint64_t ship_id = graph.shard.local().NodeAddPeered("Shipment", ship_name, "{\"name\": \"" + ship_name + "\"}").get();
        graph.shard.local().RelationshipAddPeered("SHIPPED_FROM", ship_id, loc_id, "{}").get();
    }

    std::cout << "\n=========================================\n";
    std::cout << "   JOIN ELIMINATION PERFORMANCE BENCH\n";
    std::cout << "=========================================\n";
    run_bench(graph, "Phase 2: Join Elimination", "MATCH (s:Shipment)-[:SHIPPED_FROM]->(l:Location) RETURN s.name", "NO_SEMANTIC MATCH (s:Shipment)-[:SHIPPED_FROM]->(l:Location) RETURN s.name");
    std::cout << "=========================================\n";

    GqlVirtualCatalog::local().clear();
}
