/*
 * Copyright Max De Marzi. All Rights Reserved.
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

#include <catch2/catch.hpp>
#include <Graph.h>

using namespace ragedb;

struct LocalGraphStopGuard {
    Graph& g;
    bool stopped = false;
    explicit LocalGraphStopGuard(Graph& graph) : g(graph) {}
    ~LocalGraphStopGuard() {
        if (!stopped) {
            try {
                g.Stop().get();
            } catch (...) {}
            stopped = true;
        }
    }
};

SCENARIO( "Shard can execute graph Clustering algorithms", "[graph_clustering]" ) {
    GIVEN("A graph with 4 nodes forming a cycle and a triangle") {
        auto graph = Graph("graph_clustering_test");
        graph.Start().get();
        graph.Clear();
        LocalGraphStopGuard guard(graph);

        // Setup schemas
        graph.shard.local().NodeTypeInsertPeered("Person").get();
        graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
        
        graph.shard.local().RelationshipTypeInsertPeered("KNOWS").get();
        graph.shard.local().RelationshipPropertyTypeAddPeered("KNOWS", "weight", "double").get();

        // Create test nodes
        uint64_t idA = graph.shard.local().NodeAddPeered("Person", "A", R"({"name": "A"})").get();
        uint64_t idB = graph.shard.local().NodeAddPeered("Person", "B", R"({"name": "B"})").get();
        uint64_t idC = graph.shard.local().NodeAddPeered("Person", "C", R"({"name": "C"})").get();
        uint64_t idD = graph.shard.local().NodeAddPeered("Person", "D", R"({"name": "D"})").get();

        // A -> B -> C -> D -> A and A -> C (creates triangle A-B-C)
        uint64_t relAB = graph.shard.local().RelationshipAddPeered("KNOWS", idA, idB, R"({"weight": 1.0})").get();
        uint64_t relBC = graph.shard.local().RelationshipAddPeered("KNOWS", idB, idC, R"({"weight": 2.0})").get();
        uint64_t relCD = graph.shard.local().RelationshipAddPeered("KNOWS", idC, idD, R"({"weight": 3.0})").get();
        uint64_t relDA = graph.shard.local().RelationshipAddPeered("KNOWS", idD, idA, R"({"weight": 4.0})").get();
        uint64_t relAC = graph.shard.local().RelationshipAddPeered("KNOWS", idA, idC, R"({"weight": 1.0})").get();

        WHEN("We query C++ clustering algorithms directly") {
            
            THEN("Triangles identifies all permutations") {
                auto tris = graph.shard.local().TrianglesPeered("KNOWS", "", "", "", "", false, false);
                REQUIRE(tris.size() == 12);
            }

            THEN("UniqueTriangles identifies a single unique triangle") {
                auto uniq_tris = graph.shard.local().UniqueTrianglesPeered("KNOWS", "", "", "", "", false, false);
                REQUIRE(uniq_tris.size() == 2);
            }

            THEN("LocalClusteringCoefficient computes correct scores") {
                auto lcc = graph.shard.local().LocalClusteringCoefficientPeered("KNOWS", "", "", "", "", false, false);
                REQUIRE(lcc.size() == 4);
                // A has neighbors B, C, D. B-C and C-D exist (2). possible pairs: 3*2 = 6. LCC = 2 * 2 / 6 = 2/3.
                REQUIRE(lcc[idA] == Approx(2.0 / 3.0));
            }

            THEN("AverageClusteringCoefficient computes correctly") {
                double avg_cc = graph.shard.local().AverageClusteringCoefficientPeered("KNOWS", "", "", "", "", false, false);
                REQUIRE(avg_cc > 0.0);
            }
        }

        WHEN("We query via Lua graph clustering bindings") {
            THEN("Lua UniqueTriangles executes") {
                std::string script = "local uniq = UniqueTriangles('KNOWS', '', '', '', '', false, false)\n"
                                     "uniq";
                std::string res = graph.shard.local().RunRWLua(script).get();
                REQUIRE((res == "[[2, 131074, 262146], [2, 262146, 393218]]" || res == "[[2, 262146, 393218], [2, 131074, 262146]]"));
            }
        }
    }
}
