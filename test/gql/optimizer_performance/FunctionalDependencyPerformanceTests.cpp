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

TEST_CASE("Functional Dependency Performance Benchmarks", "[gql_optimizer_perf]") {
    auto graph = Graph("gql_test_fd_perf");
    graph.Start().get();
    graph.Clear();
    PerformanceGraphStopGuard guard(graph);

    graph.shard.local().NodeTypeInsertPeered("CityNode").get();
    graph.shard.local().NodePropertyTypeAddPeered("CityNode", "zip_code", "string").get();
    graph.shard.local().NodePropertyTypeAddPeered("CityNode", "state_name", "string").get();

    GqlVirtualCatalog::local().clear();
    GqlVirtualCatalog::local().add_constraint(
        "CityZipToState",
        "MATCH (u:CityNode) MATCH (v:CityNode) WHERE u.zip_code = v.zip_code AND u.state_name != v.state_name RETURN u"
    );

    // Insert 2000 nodes with 100 unique zip codes and corresponding states
    for (int i = 0; i < 2000; ++i) {
        std::string name = "City" + std::to_string(i);
        std::string zip = std::to_string(10000 + (i % 100));
        std::string state = "State" + std::to_string(i % 100);
        graph.shard.local().NodeAddPeered(
            "CityNode", name,
            "{\"zip_code\": \"" + zip + "\", \"state_name\": \"" + state + "\"}"
        ).get();
    }

    std::cout << "\n=========================================\n";
    std::cout << "   FUNCTIONAL DEPENDENCY PERFORMANCE BENCH\n";
    std::cout << "=========================================\n";
    run_bench(
        graph,
        "Phase 9: Functional Dependency",
        "MATCH (b:CityNode) RETURN b.zip_code, count(b.state_name)",
        "NO_SEMANTIC MATCH (b:CityNode) RETURN b.zip_code, count(b.state_name)"
    );
    std::cout << "=========================================\n";

    GqlVirtualCatalog::local().clear();
}
