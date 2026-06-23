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
#include <algorithm>

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

SCENARIO( "Shard can compute KHops", "[khop]" ) {
    GIVEN("A graph with nodes and relationships") {
        auto graph = Graph("khop_test");
        graph.Start().get();
        graph.Clear();
        LocalGraphStopGuard guard(graph);

        // Setup schemas
        graph.shard.local().NodeTypeInsertPeered("Person").get();
        graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
        
        graph.shard.local().RelationshipTypeInsertPeered("KNOWS").get();
        graph.shard.local().RelationshipPropertyTypeAddPeered("KNOWS", "weight", "double").get();

        graph.shard.local().RelationshipTypeInsertPeered("LOVES").get();
        graph.shard.local().RelationshipPropertyTypeAddPeered("LOVES", "weight", "double").get();

        // Create test nodes
        uint64_t idA = graph.shard.local().NodeAddPeered("Person", "A", R"({"name": "A"})").get();
        uint64_t idB = graph.shard.local().NodeAddPeered("Person", "B", R"({"name": "B"})").get();
        uint64_t idC = graph.shard.local().NodeAddPeered("Person", "C", R"({"name": "C"})").get();
        uint64_t idD = graph.shard.local().NodeAddPeered("Person", "D", R"({"name": "D"})").get();
        uint64_t idE = graph.shard.local().NodeAddPeered("Person", "E", R"({"name": "E"})").get();
        uint64_t idF = graph.shard.local().NodeAddPeered("Person", "F", R"({"name": "F"})").get();

        // Create relationships
        // A -[KNOWS {weight: 5.0}]-> B
        // A -[KNOWS {weight: 1.0}]-> C
        // C -[KNOWS {weight: 1.5}]-> B
        // B -[LOVES {weight: 2.0}]-> D
        // D -[KNOWS {weight: 1.0}]-> F
        // E is isolated
        uint64_t relAB = graph.shard.local().RelationshipAddPeered("KNOWS", idA, idB, R"({"weight": 5.0})").get();
        uint64_t relAC = graph.shard.local().RelationshipAddPeered("KNOWS", idA, idC, R"({"weight": 1.0})").get();
        uint64_t relCB = graph.shard.local().RelationshipAddPeered("KNOWS", idC, idB, R"({"weight": 1.5})").get();
        uint64_t relBD = graph.shard.local().RelationshipAddPeered("LOVES", idB, idD, R"({"weight": 2.0})").get();
        uint64_t relDF = graph.shard.local().RelationshipAddPeered("KNOWS", idD, idF, R"({"weight": 1.0})").get();

        REQUIRE(idA > 0);
        REQUIRE(idB > 0);
        REQUIRE(idC > 0);
        REQUIRE(idD > 0);
        REQUIRE(idE > 0);
        REQUIRE(idF > 0);
        REQUIRE(relAB > 0);
        REQUIRE(relAC > 0);
        REQUIRE(relCB > 0);
        REQUIRE(relBD > 0);
        REQUIRE(relDF > 0);

        WHEN("We query KHopIdsPeered") {
            THEN("KHopIdsPeered from A with OUT direction up to 1 hop returns B, C") {
                auto ids = graph.shard.local().KHopIdsPeered(idA, 1, Direction::OUT).get();
                REQUIRE(ids.size() == 2);
                REQUIRE(std::find(ids.begin(), ids.end(), idB) != ids.end());
                REQUIRE(std::find(ids.begin(), ids.end(), idC) != ids.end());
            }

            THEN("KHopIdsPeered from A with OUT direction up to 2 hops returns B, C, D") {
                auto ids = graph.shard.local().KHopIdsPeered(idA, 2, Direction::OUT).get();
                REQUIRE(ids.size() == 3);
                REQUIRE(std::find(ids.begin(), ids.end(), idB) != ids.end());
                REQUIRE(std::find(ids.begin(), ids.end(), idC) != ids.end());
                REQUIRE(std::find(ids.begin(), ids.end(), idD) != ids.end());
            }

            THEN("KHopIdsPeered from A with OUT direction up to 3 hops returns B, C, D, F") {
                auto ids = graph.shard.local().KHopIdsPeered(idA, 3, Direction::OUT).get();
                REQUIRE(ids.size() == 4);
                REQUIRE(std::find(ids.begin(), ids.end(), idB) != ids.end());
                REQUIRE(std::find(ids.begin(), ids.end(), idC) != ids.end());
                REQUIRE(std::find(ids.begin(), ids.end(), idD) != ids.end());
                REQUIRE(std::find(ids.begin(), ids.end(), idF) != ids.end());
            }

            THEN("KHopIdsPeered from A with OUT direction and KNOWS type up to 3 hops returns B, C") {
                auto ids = graph.shard.local().KHopIdsPeered(idA, 3, Direction::OUT, "KNOWS").get();
                REQUIRE(ids.size() == 2);
                REQUIRE(std::find(ids.begin(), ids.end(), idB) != ids.end());
                REQUIRE(std::find(ids.begin(), ids.end(), idC) != ids.end());
            }

            THEN("KHopIdsPeered from A with OUT direction and multiple types up to 3 hops returns B, C, D, F") {
                auto ids = graph.shard.local().KHopIdsPeered(idA, 3, Direction::OUT, std::vector<std::string>{"KNOWS", "LOVES"}).get();
                REQUIRE(ids.size() == 4);
            }
        }

        WHEN("We query KHopNodesPeered") {
            THEN("KHopNodesPeered from A with OUT direction up to 1 hop returns B, C nodes") {
                auto nodes = graph.shard.local().KHopNodesPeered(idA, 1, Direction::OUT).get();
                REQUIRE(nodes.size() == 2);
                REQUIRE((nodes[0].getKey() == "B" || nodes[0].getKey() == "C"));
                REQUIRE((nodes[1].getKey() == "B" || nodes[1].getKey() == "C"));
            }
        }

        WHEN("We query KHopCountPeered") {
            THEN("KHopCountPeered from A with OUT direction up to 1 hop returns 2") {
                auto count = graph.shard.local().KHopCountPeered(idA, 1, Direction::OUT).get();
                REQUIRE(count == 2);
            }

            THEN("KHopCountPeered from A with OUT direction up to 2 hops returns 3") {
                auto count = graph.shard.local().KHopCountPeered(idA, 2, Direction::OUT).get();
                REQUIRE(count == 3);
            }

            THEN("KHopCountPeered from A with OUT direction up to 3 hops returns 4") {
                auto count = graph.shard.local().KHopCountPeered(idA, 3, Direction::OUT).get();
                REQUIRE(count == 4);
            }

            THEN("KHopCountPeered from A with OUT direction and KNOWS type up to 3 hops returns 2") {
                auto count = graph.shard.local().KHopCountPeered(idA, 3, Direction::OUT, "KNOWS").get();
                REQUIRE(count == 2);
            }
        }

        WHEN("We query KHopCountsPeered") {
            THEN("KHopCountsPeered from A with OUT direction up to 3 hops returns counts at each hop") {
                auto counts = graph.shard.local().KHopCountsPeered(idA, 3, Direction::OUT).get();
                REQUIRE(counts.size() == 3);
                REQUIRE(counts[0] == 2); // B, C
                REQUIRE(counts[1] == 3); // B, C, D
                REQUIRE(counts[2] == 4); // B, C, D, F
            }

            THEN("KHopCountsPeered from D with BOTH direction up to 3 hops returns counts at each hop") {
                auto counts = graph.shard.local().KHopCountsPeered(idD, 3, Direction::BOTH).get();
                REQUIRE(counts.size() == 3);
                REQUIRE(counts[0] == 2); // B, F
                REQUIRE(counts[1] == 4); // A, B, C, F
                REQUIRE(counts[2] == 4); // A, B, C, F
            }
        }

        WHEN("We query via Lua wrappers") {
            THEN("Lua execution integrates KHopIds correctly") {
                std::string script = "local ids = KHopIds('Person', 'A', 2, Direction.OUT)\n"
                                     "local res = #ids\n"
                                     "res";
                std::string res = graph.shard.local().RunRWLua(script).get();
                REQUIRE(res == "3");
            }

            THEN("Lua execution integrates KHopNodes correctly") {
                std::string script = "local nodes = KHopNodes('Person', 'A', 1, Direction.OUT)\n"
                                     "local res = #nodes\n"
                                     "res";
                std::string res = graph.shard.local().RunRWLua(script).get();
                REQUIRE(res == "2");
            }

            THEN("Lua execution integrates KHopCount correctly") {
                std::string script = "local count = KHopCount('Person', 'A', 3, Direction.OUT)\n"
                                     "script_res = count\n"
                                     "script_res";
                std::string res = graph.shard.local().RunRWLua(script).get();
                REQUIRE(res == "4");
            }

            THEN("Lua execution integrates KHopCounts correctly") {
                std::string script = "local counts = KHopCounts('Person', 'A', 3, Direction.OUT)\n"
                                     "local res = counts[1] .. ',' .. counts[2] .. ',' .. counts[3]\n"
                                     "res";
                std::string res = graph.shard.local().RunRWLua(script).get();
                REQUIRE(res == "\"2,3,4\"");
            }
        }
    }
}
