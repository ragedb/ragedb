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

TEST_CASE("Transitive Reachability Performance Benchmarks", "[gql_optimizer_perf]") {
    auto graph = Graph("gql_test_reachability_perf");
    graph.Start().get();
    graph.Clear();
    PerformanceGraphStopGuard guard(graph);

    graph.shard.local().NodeTypeInsertPeered("Category").get();
    graph.shard.local().NodePropertyTypeAddPeered("Category", "name", "string").get();
    graph.shard.local().RelationshipTypeInsertPeered("SUBCLASS_OF").get();

    for (int i = 0; i < 1000; ++i) {
        std::string name = "Category" + std::to_string(i);
        graph.shard.local().NodeAddPeered("Category", name, "{\"name\": \"" + name + "\"}").get();
    }
    uint64_t root_id = graph.shard.local().NodeAddPeered("Category", "Books", "{\"name\": \"Books\"}").get();
    uint64_t parent_id = root_id;
    for (int i = 0; i < 10; ++i) {
        std::string name = "SubCat" + std::to_string(i);
        uint64_t sub_id = graph.shard.local().NodeAddPeered("Category", name, "{\"name\": \"" + name + "\"}").get();
        graph.shard.local().RelationshipAddPeered("SUBCLASS_OF", sub_id, parent_id, "{}").get();
        parent_id = sub_id;
    }
    uint64_t tools_id = graph.shard.local().NodeAddPeered("Category", "Tools", "{\"name\": \"Tools\"}").get();
    uint64_t gardening_id = graph.shard.local().NodeAddPeered("Category", "Gardening_Tools", "{\"name\": \"Gardening_Tools\"}").get();
    graph.shard.local().RelationshipAddPeered("SUBCLASS_OF", gardening_id, tools_id, "{}").get();

    GqlVirtualCatalog::local().clear();
    GqlVirtualCatalog::local().add_constraint(
        "SciFiToBooks",
        "MATCH (c:Category {name: 'SciFi_Books'})-[:SUBCLASS_OF]->(p:Category {name: 'Books'}) RETURN c"
    );
    GqlVirtualCatalog::local().add_constraint(
        "SpaceOperaToSciFi",
        "MATCH (c:Category {name: 'SpaceOpera'})-[:SUBCLASS_OF]->(p:Category {name: 'SciFi_Books'}) RETURN c"
    );
    GqlVirtualCatalog::local().add_constraint(
        "GardeningToTools",
        "MATCH (c:Category {name: 'Gardening_Tools'})-[:SUBCLASS_OF]->(p:Category {name: 'Tools'}) RETURN c"
    );

    std::cout << "\n=========================================\n";
    std::cout << "   TRANSITIVE REACHABILITY PERFORMANCE BENCH\n";
    std::cout << "=========================================\n";
    run_bench(graph, "Phase 8: Transitive DAG Reachability", "MATCH (c1:Category)-[:SUBCLASS_OF*]->(c2:Category) WHERE c1.name = 'Gardening_Tools' AND c2.name = 'Books' RETURN count(*)", "NO_SEMANTIC MATCH (c1:Category)-[:SUBCLASS_OF*]->(c2:Category) WHERE c1.name = 'Gardening_Tools' AND c2.name = 'Books' RETURN count(*)");
    std::cout << "=========================================\n";

    GqlVirtualCatalog::local().clear();
}
