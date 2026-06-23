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

SCENARIO( "Shard can execute graph Centrality algorithms", "[graph_centrality]" ) {
    GIVEN("A graph with 4 nodes forming a cycle") {
        auto graph = Graph("graph_centrality_test");
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

        WHEN("We query C++ centrality algorithms directly") {
            
            THEN("PageRank computes correct values") {
                auto pr = graph.shard.local().PageRankPeered("KNOWS", "", "", "", "weight", true, true, 0.85, 20, 1e-6);
                REQUIRE(pr.size() == 4);
                REQUIRE(pr[idA] > 0.0);
                REQUIRE(pr[idB] > 0.0);
                REQUIRE(pr[idC] > 0.0);
                REQUIRE(pr[idD] > 0.0);
            }

            THEN("Eigenvector Centrality computes values") {
                auto ec = graph.shard.local().EigenvectorCentralityPeered("KNOWS", "", "", "", "weight", true, true, 20, 1e-6);
                REQUIRE(ec.size() == 4);
                REQUIRE(ec[idA] > 0.0);
            }

            THEN("Betweenness Centrality computes values") {
                auto bc = graph.shard.local().BetweennessCentralityPeered("KNOWS", "", "", "", "", true, false);
                REQUIRE(bc.size() == 4);
            }

            THEN("Degree Centrality computes correct values") {
                auto dc = graph.shard.local().DegreeCentralityPeered("KNOWS", "", "", "", "", false, false, Direction::BOTH);
                REQUIRE(dc.size() == 4);
                // Undirected degree of A is 2 (connected to B, D)
                REQUIRE(dc[idA] == Approx(2.0 / 3.0));
            }
        }

        WHEN("We query via Lua graph algorithm bindings") {
            
            THEN("Lua PageRank executes and returns correct types") {
                std::string script = "local pr = PageRank('KNOWS', '', '', '', 'weight', true, true, 0.85, 20, 1e-6)\n"
                                     "local sum = 0.0\n"
                                     "for k, v in pairs(pr) do sum = sum + v end\n"
                                     "sum > 0.99 and sum < 1.01";
                std::string res = graph.shard.local().RunRWLua(script).get();
                REQUIRE(res == "true");
            }

            THEN("Lua DegreeCentrality executes") {
                std::string script = "local dc = DegreeCentrality('KNOWS', '', '', '', '', false, false, Direction.BOTH)\n"
                                     "local score_A = dc[tostring(" + std::to_string(idA) + ")] or dc[" + std::to_string(idA) + "]\n"
                                     "score_A > 0.6";
                std::string res = graph.shard.local().RunRWLua(script).get();
                REQUIRE(res == "true");
            }
        }
    }
}
