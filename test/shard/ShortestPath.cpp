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

SCENARIO( "Shard can compute Shortest Path", "[shortest_path]" ) {
    GIVEN("A graph with nodes and relationships") {
        auto graph = Graph("shortest_path_test");
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

        WHEN("We query ShortestPathPeered (unweighted BFS)") {
            THEN("Path to self has length 0 and contains start node") {
                auto path_opt = graph.shard.local().ShortestPathPeered(idA, idA, Direction::BOTH, {}).get();
                REQUIRE(path_opt.has_value());
                REQUIRE(path_opt->length() == 0);
                REQUIRE(path_opt->GetStartNode().getId() == idA);
                REQUIRE(path_opt->GetEndNode().getId() == idA);
                REQUIRE(path_opt->GetNodes().size() == 1);
                REQUIRE(path_opt->GetRelationships().empty());
            }

            THEN("Path from A to B directly is found because it has fewer hops (1 hop)") {
                auto path_opt = graph.shard.local().ShortestPathPeered(idA, idB, Direction::OUT, {}).get();
                REQUIRE(path_opt.has_value());
                REQUIRE(path_opt->length() == 1);
                auto nodes = path_opt->GetNodes();
                REQUIRE(nodes.size() == 2);
                REQUIRE(nodes[0].getId() == idA);
                REQUIRE(nodes[1].getId() == idB);
                auto rels = path_opt->GetRelationships();
                REQUIRE(rels.size() == 1);
                REQUIRE(rels[0].getId() == relAB);
            }

            THEN("Path from A to D is found (2 hops: A -> B -> D)") {
                auto path_opt = graph.shard.local().ShortestPathPeered(idA, idD, Direction::OUT, {}).get();
                REQUIRE(path_opt.has_value());
                REQUIRE(path_opt->length() == 2);
                auto nodes = path_opt->GetNodes();
                REQUIRE(nodes.size() == 3);
                REQUIRE(nodes[0].getId() == idA);
                REQUIRE(nodes[1].getId() == idB);
                REQUIRE(nodes[2].getId() == idD);
            }

            THEN("Direction constraints are respected") {
                // A -> B -> D exists, but in reverse (D to A) with OUT should not exist
                auto path_outgoing = graph.shard.local().ShortestPathPeered(idD, idA, Direction::OUT, {}).get();
                REQUIRE_FALSE(path_outgoing.has_value());

                // With BOTH direction, D to A should exist
                auto path_both = graph.shard.local().ShortestPathPeered(idD, idA, Direction::BOTH, {}).get();
                REQUIRE(path_both.has_value());
                REQUIRE(path_both->length() == 2);
            }

            THEN("Relationship type filtering is respected") {
                // Shortest path A to D only allowing LOVES does not exist (since A -> B is KNOWS)
                auto path_loves = graph.shard.local().ShortestPathPeered(idA, idD, Direction::OUT, {"LOVES"}).get();
                REQUIRE_FALSE(path_loves.has_value());

                // Shortest path A to D allowing KNOWS and LOVES does exist
                auto path_both_types = graph.shard.local().ShortestPathPeered(idA, idD, Direction::OUT, {"KNOWS", "LOVES"}).get();
                REQUIRE(path_both_types.has_value());
                REQUIRE(path_both_types->length() == 2);
            }

            THEN("Unreachable nodes return nullopt") {
                auto path_isolated = graph.shard.local().ShortestPathPeered(idA, idE, Direction::BOTH, {}).get();
                REQUIRE_FALSE(path_isolated.has_value());
            }

            THEN("Path from A to D with max_hops=1 is not found") {
                auto path_opt = graph.shard.local().ShortestPathPeered(idA, idD, Direction::OUT, {}, 1).get();
                REQUIRE_FALSE(path_opt.has_value());
            }

            THEN("Path from A to D with max_hops=2 is found") {
                auto path_opt = graph.shard.local().ShortestPathPeered(idA, idD, Direction::OUT, {}, 2).get();
                REQUIRE(path_opt.has_value());
                REQUIRE(path_opt->length() == 2);
            }

            THEN("Longer path from A to F is found (3 hops: A -> B -> D -> F)") {
                auto path_opt = graph.shard.local().ShortestPathPeered(idA, idF, Direction::OUT, {}).get();
                REQUIRE(path_opt.has_value());
                REQUIRE(path_opt->length() == 3);
                auto nodes = path_opt->GetNodes();
                REQUIRE(nodes.size() == 4);
                REQUIRE(nodes[0].getId() == idA);
                REQUIRE(nodes[1].getId() == idB);
                REQUIRE(nodes[2].getId() == idD);
                REQUIRE(nodes[3].getId() == idF);
                auto rels = path_opt->GetRelationships();
                REQUIRE(rels.size() == 3);
                REQUIRE(rels[0].getId() == relAB);
                REQUIRE(rels[1].getId() == relBD);
                REQUIRE(rels[2].getId() == relDF);
            }
        }

        WHEN("We query ShortestWeightedPathPeered (Dijkstra)") {
            THEN("Weighted path to self has length 0, weight 0.0") {
                auto path_opt = graph.shard.local().ShortestWeightedPathPeered(idA, idA, Direction::BOTH, {}).get();
                REQUIRE(path_opt.has_value());
                REQUIRE(path_opt->length() == 0);
                REQUIRE(path_opt->weight() == 0.0);
                REQUIRE(path_opt->GetStartNode().getId() == idA);
                REQUIRE(path_opt->GetNodes().size() == 1);
            }

            THEN("Path from A to B prefers A -> C -> B (weight 2.5) over A -> B (weight 5.0)") {
                auto path_opt = graph.shard.local().ShortestWeightedPathPeered(idA, idB, Direction::OUT, {}).get();
                REQUIRE(path_opt.has_value());
                REQUIRE(path_opt->length() == 2); // 2 hops: A -> C -> B
                REQUIRE(path_opt->weight() == 2.5);
                auto nodes = path_opt->GetNodes();
                REQUIRE(nodes.size() == 3);
                REQUIRE(nodes[0].getId() == idA);
                REQUIRE(nodes[1].getId() == idC);
                REQUIRE(nodes[2].getId() == idB);
                auto rels = path_opt->GetRelationships();
                REQUIRE(rels.size() == 2);
                REQUIRE(rels[0].getId() == relAC);
                REQUIRE(rels[1].getId() == relCB);
            }

            THEN("Path from A to D is A -> C -> B -> D (weight 4.5)") {
                auto path_opt = graph.shard.local().ShortestWeightedPathPeered(idA, idD, Direction::OUT, {}).get();
                REQUIRE(path_opt.has_value());
                REQUIRE(path_opt->length() == 3);
                REQUIRE(path_opt->weight() == 4.5);
                auto nodes = path_opt->GetNodes();
                REQUIRE(nodes.size() == 4);
                REQUIRE(nodes[0].getId() == idA);
                REQUIRE(nodes[1].getId() == idC);
                REQUIRE(nodes[2].getId() == idB);
                REQUIRE(nodes[3].getId() == idD);
            }

            THEN("Direction constraints are respected") {
                auto path_outgoing = graph.shard.local().ShortestWeightedPathPeered(idD, idA, Direction::OUT, {}).get();
                REQUIRE_FALSE(path_outgoing.has_value());

                auto path_both = graph.shard.local().ShortestWeightedPathPeered(idD, idA, Direction::BOTH, {}).get();
                REQUIRE(path_both.has_value());
                REQUIRE(path_both->weight() == 4.5);
            }

            THEN("Relationship type filtering is respected") {
                auto path_loves = graph.shard.local().ShortestWeightedPathPeered(idA, idD, Direction::OUT, {"LOVES"}).get();
                REQUIRE_FALSE(path_loves.has_value());

                auto path_both_types = graph.shard.local().ShortestWeightedPathPeered(idA, idD, Direction::OUT, {"KNOWS", "LOVES"}).get();
                REQUIRE(path_both_types.has_value());
                REQUIRE(path_both_types->weight() == 4.5);
            }

            THEN("Unreachable nodes return nullopt") {
                auto path_isolated = graph.shard.local().ShortestWeightedPathPeered(idA, idE, Direction::BOTH, {}).get();
                REQUIRE_FALSE(path_isolated.has_value());
            }
        }

        WHEN("We query via Lua wrappers") {
            THEN("ShortestPathViaLua returns the expected path") {
                auto path_opt = graph.shard.local().ShortestPathViaLua(idA, idB);
                REQUIRE(path_opt.has_value());
                REQUIRE(path_opt->length() == 1);

                auto path_by_key = graph.shard.local().ShortestPathViaLua("Person", "A", "Person", "B");
                REQUIRE(path_by_key.has_value());
                REQUIRE(path_by_key->length() == 1);
            }

            THEN("ShortestWeightedPathViaLua returns the expected weighted path") {
                auto path_opt = graph.shard.local().ShortestWeightedPathViaLua(idA, idB);
                REQUIRE(path_opt.has_value());
                REQUIRE(path_opt->weight() == 2.5);

                auto path_by_key = graph.shard.local().ShortestWeightedPathViaLua("Person", "A", "Person", "B");
                REQUIRE(path_by_key.has_value());
                REQUIRE(path_by_key->weight() == 2.5);
            }

            THEN("Lua execution integrates ShortestPath correctly") {
                std::string script = "local p = ShortestPath('Person', 'A', 'Person', 'B')\n"
                                     "local res = -1\n"
                                     "if p then res = p:length() end\n"
                                     "res";
                std::string res = graph.shard.local().RunRWLua(script).get();
                REQUIRE(res == "1");
            }

            THEN("Lua ShortestPath respects max_hops parameter") {
                std::string script = "local p = ShortestPath('Person', 'A', 'Person', 'D', Direction.OUT, 1)\n"
                                     "local res = 0\n"
                                     "if p then res = 1 end\n"
                                     "res";
                std::string res = graph.shard.local().RunRWLua(script).get();
                REQUIRE(res == "0");

                std::string script2 = "local p = ShortestPath('Person', 'A', 'Person', 'D', Direction.OUT, 2)\n"
                                      "local res = 0\n"
                                      "if p then res = 1 end\n"
                                      "res";
                std::string res2 = graph.shard.local().RunRWLua(script2).get();
                REQUIRE(res2 == "1");
            }

            THEN("Lua execution integrates ShortestWeightedPath correctly") {
                std::string script = "local p = ShortestWeightedPath('Person', 'A', 'Person', 'B')\n"
                                     "local res = -1.0\n"
                                     "if p then res = p:weight() end\n"
                                     "res";
                std::string res = graph.shard.local().RunRWLua(script).get();
                REQUIRE(res == "2.5");
            }
        }

    }
}

