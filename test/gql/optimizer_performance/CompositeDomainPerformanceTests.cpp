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

TEST_CASE("Composite Domain Performance Benchmarks", "[gql_optimizer_perf]") {
    auto graph = Graph("gql_test_composite_perf");
    graph.Start().get();
    graph.Clear();
    PerformanceGraphStopGuard guard(graph);

    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "age", "integer").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "status", "string").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "is_student", "boolean").get();

    for (int i = 0; i < 5; ++i) {
        std::string name = "Person" + std::to_string(i);
        graph.shard.local().NodeAddPeered("Person", name, "{\"name\": \"" + name + "\", \"age\": " + std::to_string(20 + i) + ", \"status\": \"adult\", \"is_student\": false}").get();
    }

    GqlVirtualCatalog::local().clear();
    GqlVirtualCatalog::local().add_constraint("CompositePersonConstraint", "MATCH (p:Person) WHERE p.age < 18 OR p.status = 'minor' OR p.is_student = true RETURN p");

    std::cout << "\n=========================================\n";
    std::cout << "   COMPOSITE DOMAIN PERFORMANCE BENCH\n";
    std::cout << "=========================================\n";
    run_bench(graph, "Phase 7: Composite Domain Constraint", "MATCH (p:Person) WHERE p.age = 15 RETURN p.name", "NO_SEMANTIC MATCH (p:Person) WHERE p.age = 15 RETURN p.name");
    std::cout << "=========================================\n";

    GqlVirtualCatalog::local().clear();
}
