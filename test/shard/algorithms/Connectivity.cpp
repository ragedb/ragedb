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

SCENARIO( "Shard can execute graph Connectivity algorithms", "[graph_connectivity]" ) {
    GIVEN("A graph with 4 nodes forming a cycle") {
        auto graph = Graph("graph_connectivity_test");
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

        REQUIRE(idA > 0);
        REQUIRE(idB > 0);
        REQUIRE(idC > 0);
        REQUIRE(idD > 0);

        WHEN("We query C++ connectivity algorithms directly") {
            
            THEN("WeaklyConnectedComponents identifies a single component") {
                auto wcc = graph.shard.local().WeaklyConnectedComponentsPeered("KNOWS", "", "", "", "", true, false);
                REQUIRE(wcc.size() == 4);
                REQUIRE(wcc[idA] == wcc[idB]);
                REQUIRE(wcc[idB] == wcc[idC]);
                REQUIRE(wcc[idC] == wcc[idD]);
            }

            THEN("IsConnected returns true") {
                bool is_conn = graph.shard.local().IsConnectedPeered("KNOWS", "", "", "", "", true, false);
                REQUIRE(is_conn == true);
            }

            THEN("Reachable computes correct reachability pairs") {
                auto reach = graph.shard.local().ReachablePeered("KNOWS", "", "", "", "", true, false);
                // Cycle of 4: each node can reach the other 3
                REQUIRE(reach.size() == 12);
            }
        }

        WHEN("We query via Lua graph algorithm bindings") {
            
            THEN("Lua WeaklyConnectedComponents runs") {
                std::string script = "local wcc = WeaklyConnectedComponents('KNOWS', '', '', '', '', false, false)\n"
                                     "local first_val = nil\n"
                                     "local all_same = true\n"
                                     "for k, v in pairs(wcc) do\n"
                                     "  if first_val == nil then first_val = v\n"
                                     "  elseif v ~= first_val then all_same = false end\n"
                                     "end\n"
                                     "all_same";
                std::string res = graph.shard.local().RunRWLua(script).get();
                REQUIRE(res == "true");
            }

            THEN("Lua IsConnected returns true") {
                std::string script = "local res = IsConnected('KNOWS', '', '', '', '', false, false)\n"
                                     "res";
                std::string res = graph.shard.local().RunRWLua(script).get();
                REQUIRE(res == "true");
            }
        }
    }
}
