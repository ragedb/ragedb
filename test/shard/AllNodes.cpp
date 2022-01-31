/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

SCENARIO( "Shard can handle All Nodes", "[node]" ) {

    GIVEN( "A shard with a bunch of nodes" ) {
        ragedb::Shard shard(4);
        shard.NodeTypeInsert("Node", 1);
        shard.NodeTypeInsert("User", 2);

        uint64_t empty = shard.NodeAddEmpty(1, "empty");
        uint64_t existing = shard.NodeAdd(1,  "existing", R"({ "name":"max" })");

        uint64_t three = shard.NodeAddEmpty(1,  "three");
        uint64_t four = shard.NodeAddEmpty(1,  "four");
        uint64_t five = shard.NodeAddEmpty(1,  "five");
        uint64_t six = shard.NodeAddEmpty(1,  "six");

        REQUIRE( empty == 1024 );
        REQUIRE( existing == 67109888 );
        REQUIRE( three == 134218752 );
        REQUIRE( four == 201327616 );
        REQUIRE( five == 268436480 );
        REQUIRE( six == 335545344 );

        WHEN( "more nodes are added" ) {
            shard.NodeAddEmpty(2, "one");
            shard.NodeAddEmpty(2, "two");

            THEN( "the shard should get the right node counts" ) {
                auto counts = shard.NodeCounts();
                REQUIRE(counts.at(1) == 6);
                REQUIRE(counts.at(2) == 2);

                REQUIRE(shard.NodeCount(1) == 6);
                REQUIRE(shard.NodeCount(2) == 2);

                REQUIRE(shard.NodeCount("Node") == 6);
                REQUIRE(shard.NodeCount("User") == 2);

                REQUIRE(shard.NodeCount(99) == 0);
                REQUIRE(shard.NodeCount("Wrong") == 0);
            }
        }

        WHEN( "more nodes are added" ) {
            shard.NodeAddEmpty(2, "one");
            shard.NodeAddEmpty(2, "two");
            THEN( "the shard should get zero nodes if the wrong type is asked for" ) {
                std::vector<ragedb::Node> it = shard.AllNodes("Wrong");
                REQUIRE(it.empty());
            }
        }

        WHEN( "more nodes are added" ) {
            shard.NodeAddEmpty(2, "one");
            shard.NodeAddEmpty(2, "two");

            THEN( "the shard should get all the node ids" ) {
                auto it = shard.NodeCounts();
                uint64_t counter = 0;
                for( auto [k, v] : it) {
                    counter += v;
                }

                REQUIRE(counter == 8);
            }
        }

        WHEN( "more nodes are added" ) {
            shard.NodeAddEmpty(2, "one");
            shard.NodeAddEmpty(2, "two");

            THEN( "the shard should get all the nodes" ) {
                std::vector<ragedb::Node> it = shard.AllNodes();
                REQUIRE(it.size() == 8);
            }
        }

        WHEN( "more nodes of a different type are added" ) {
            shard.NodeAddEmpty(2, "one");
            shard.NodeAddEmpty(2, "two");

            THEN( "the shard should get all the node ids by type" ) {

                REQUIRE(shard.NodeCount("User") == 2);
                REQUIRE(shard.NodeCount("Node") == 6);
            }
        }


        WHEN( "more nodes of a different type are added" ) {
            shard.NodeAddEmpty(2, "one");
            shard.NodeAddEmpty(2, "two");

            THEN( "the shard should get all the nodes by type" ) {
                std::vector<ragedb::Node> it = shard.AllNodes("User");
                REQUIRE(it.size() == 2);

                it = shard.AllNodes("Node");
                REQUIRE(it.size() == 6);

                std::vector<uint64_t> it2 = shard.AllNodeIds();
                REQUIRE(it2.size() == 8);

                it2 = shard.AllNodeIds("User");
                REQUIRE(it2.size() == 2);

                it2 = shard.AllNodeIds("User", 1, 2);
                REQUIRE(it2.size() == 1);

                it2 = shard.AllNodeIds(2, 1, 2);
                REQUIRE(it2.size() == 1);

                it2 = shard.AllNodeIds( static_cast<uint64_t>(2) , 3);
                REQUIRE(it2.size() == 3);
            }
        }
    }
}
