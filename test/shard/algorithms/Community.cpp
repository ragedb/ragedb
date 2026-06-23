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

SCENARIO( "Shard can execute graph Community detection algorithms", "[graph_community]" ) {
    GIVEN("A graph with 4 nodes forming a cycle") {
        auto graph = Graph("graph_community_test");
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

        // A -> B -> C -> D -> A
        uint64_t relAB = graph.shard.local().RelationshipAddPeered("KNOWS", idA, idB, R"({"weight": 1.0})").get();
        uint64_t relBC = graph.shard.local().RelationshipAddPeered("KNOWS", idB, idC, R"({"weight": 2.0})").get();
        uint64_t relCD = graph.shard.local().RelationshipAddPeered("KNOWS", idC, idD, R"({"weight": 3.0})").get();
        uint64_t relDA = graph.shard.local().RelationshipAddPeered("KNOWS", idD, idA, R"({"weight": 4.0})").get();

        WHEN("We query C++ community detection algorithms directly") {
            
            THEN("Louvain partitioning runs") {
                auto louvain = graph.shard.local().LouvainPeered("KNOWS", "", "", "", "", false, false);
                REQUIRE(louvain.size() == 4);
            }

            THEN("Leiden partitioning runs") {
                auto leiden = graph.shard.local().LeidenPeered("KNOWS", "", "", "", "", false, false);
                REQUIRE(leiden.size() == 4);
            }

            THEN("Label Propagation runs") {
                auto lpa = graph.shard.local().LabelPropagationPeered("KNOWS", "", "", "", "", false, false);
                REQUIRE(lpa.size() == 4);
            }

            THEN("Infomap runs") {
                auto infomap = graph.shard.local().InfomapPeered("KNOWS", "", "", "", "", false, false);
                REQUIRE(infomap.size() == 4);
            }
        }

        WHEN("We query via Lua graph community bindings") {
            
            THEN("Lua Leiden executes") {
                std::string script = "local res = Leiden('KNOWS', '', '', '', '', false, false)\n"
                                     "local count = 0\n"
                                     "for k, v in pairs(res) do count = count + 1 end\n"
                                     "count";
                std::string res = graph.shard.local().RunRWLua(script).get();
                REQUIRE(res == "4");
            }
        }
    }
}
