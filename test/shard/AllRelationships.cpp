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
#include "../../src/graph/Shard.h"

SCENARIO( "Shard can handle All Relationships", "[relationship]" ) {

    GIVEN("A shard with an empty node and an existing node with properties") {
        ragedb::Shard shard(4);
        shard.NodeTypeInsert("Node", 1);
        shard.NodeTypeInsert("User", 2);
        uint64_t empty = shard.NodeAddEmpty(1, "empty");
        uint64_t existing = shard.NodeAdd(1, "existing", R"({ "name":"max", "email":"maxdemarzi@example.com" })");

        REQUIRE( empty == 1024 );
        REQUIRE( existing == 67109888 );

        shard.RelationshipTypeInsert("KNOWS", 1);
        shard.RelationshipTypeInsert("LIKES", 2);
        shard.RelationshipTypeInsert("FRIENDS", 3);
        shard.RelationshipTypeInsert("BLOCKS", 4);

        WHEN("an empty relationship is added") {
            shard.RelationshipAddEmptySameShard(1, "Node", "empty", "Node", "existing");

            THEN( "the shard should get zero relationship ids if the wrong type is asked for" ) {
                REQUIRE(shard.AllRelationshipIdCounts("Wrong") == 0);
            }
        }

        WHEN("an empty relationship is added") {
            shard.RelationshipAddEmptySameShard(1, "Node", "empty", "Node", "existing");

            THEN( "the shard should get zero relationships if the wrong type is asked for" ) {
                std::vector<ragedb::Relationship> it = shard.AllRelationships("Wrong");
                REQUIRE(it.empty());
            }
        }

        WHEN("a bunch of empty relationships are added") {
            shard.RelationshipAddEmptySameShard(1, "Node", "empty", "Node", "existing");
            shard.RelationshipAddEmptySameShard(2, "Node", "empty", "Node", "existing");
            shard.RelationshipAddEmptySameShard(3, "Node", "empty", "Node", "existing");
            shard.RelationshipAddEmptySameShard(3, "Node", "empty", "Node", "existing");

            THEN( "the shard should get the right relationships counts" ) {
                auto counts = shard.AllRelationshipIdCounts();
                REQUIRE(counts.at(1) == 1);
                REQUIRE(counts.at(2) == 1);
                REQUIRE(counts.at(3) == 2);

                REQUIRE(shard.AllRelationshipIdCounts(1) == 1);
                REQUIRE(shard.AllRelationshipIdCounts(2) == 1);
                REQUIRE(shard.AllRelationshipIdCounts(3) == 2);

                REQUIRE(shard.AllRelationshipIdCounts("KNOWS") == 1);
                REQUIRE(shard.AllRelationshipIdCounts("LIKES") == 1);
                REQUIRE(shard.AllRelationshipIdCounts("FRIENDS") == 2);

                REQUIRE(shard.AllRelationshipIdCounts(99) == 0);
                REQUIRE(shard.AllRelationshipIdCounts("Wrong") == 0);
            }
        }

        WHEN("a bunch of empty relationships are added") {
            shard.RelationshipAddEmptySameShard(1, "Node", "empty", "Node", "existing");
            shard.RelationshipAddEmptySameShard(2, "Node", "empty", "Node", "existing");
            shard.RelationshipAddEmptySameShard(3, "Node", "empty", "Node", "existing");
            shard.RelationshipAddEmptySameShard(4, "Node", "empty", "Node", "existing");

            THEN( "the shard should get all the relationships" ) {
                uint64_t counter = 0;
                for (auto [k,v] : shard.AllRelationshipIdCounts()) {
                    counter += v;
                }
                REQUIRE(counter == 4);
            }
        }

        WHEN("a bunch of empty relationships are added") {
            shard.RelationshipAddEmptySameShard(1, "Node", "empty", "Node", "existing");
            shard.RelationshipAddEmptySameShard(2, "Node", "empty", "Node", "existing");
            shard.RelationshipAddEmptySameShard(3, "Node", "empty", "Node", "existing");
            shard.RelationshipAddEmptySameShard(4, "Node", "empty", "Node", "existing");

            THEN( "the shard should get all the relationships" ) {
                std::vector<ragedb::Relationship> it = shard.AllRelationships();
                REQUIRE(it.size() == 4);
            }
        }

        WHEN("an empty relationship is added after deleting one") {
            shard.RelationshipAddEmptySameShard(1, "Node", "empty", "Node", "existing");
            shard.RelationshipAddEmptySameShard(2, "Node", "empty", "Node", "existing");
            uint64_t added = shard.RelationshipAddEmptySameShard(3, "Node", "empty", "Node", "existing");
            shard.RelationshipAddEmptySameShard(4, "Node", "empty", "Node", "existing");

            THEN("the shard keeps the id") {
                REQUIRE(added > 1);

                std::pair <uint16_t, uint64_t> rel_type_incoming_node_id = shard.RelationshipRemoveGetIncoming(added);
                bool deleted = shard.RelationshipRemoveIncoming(rel_type_incoming_node_id.first, added, rel_type_incoming_node_id.second);
                REQUIRE (deleted);

                REQUIRE(shard.AllRelationships().size() == 3);
            }
        }

        WHEN("an empty relationship is added after deleting one") {
            shard.RelationshipAddEmptySameShard(1, "Node", "empty", "Node", "existing");
            shard.RelationshipAddEmptySameShard(2, "Node", "empty", "Node", "existing");
            uint64_t added = shard.RelationshipAddEmptySameShard(3, "Node", "empty", "Node", "existing");
            shard.RelationshipAddEmptySameShard(4, "Node", "empty", "Node", "existing");

            THEN("the shard keeps it") {
                REQUIRE(added > 1);

                std::pair <uint16_t, uint64_t> rel_type_incoming_node_id = shard.RelationshipRemoveGetIncoming(added);
                bool deleted = shard.RelationshipRemoveIncoming(rel_type_incoming_node_id.first, added, rel_type_incoming_node_id.second);
                REQUIRE (deleted);

                std::vector<ragedb::Relationship> it = shard.AllRelationships();
                REQUIRE(it.size() == 3);
            }
        }

        WHEN("more relationships of a different type are added") {
            shard.RelationshipAddEmptySameShard(1, "Node", "empty", "Node", "existing");
            shard.RelationshipAddEmptySameShard(1, "Node", "empty", "Node", "existing");
            shard.RelationshipAddEmptySameShard(3, "Node", "empty", "Node", "existing");
            shard.RelationshipAddEmptySameShard(3, "Node", "empty", "Node", "existing");
            shard.RelationshipAddEmptySameShard(3, "Node", "empty", "Node", "existing");

            THEN("the shard keeps the ids") {
                REQUIRE(shard.AllRelationshipIdCounts("KNOWS") == 2);
                REQUIRE(shard.AllRelationshipIdCounts("FRIENDS") == 3);
                REQUIRE(shard.AllRelationshipIdCounts(1) == 2);
                REQUIRE(shard.AllRelationshipIdCounts(3) == 3);
            }
        }

        WHEN("more relationships of a different type are added") {
            shard.RelationshipAddEmptySameShard(1, "Node", "empty", "Node", "existing");
            shard.RelationshipAddEmptySameShard(1, "Node", "empty", "Node", "existing");
            shard.RelationshipAddEmptySameShard(3, "Node", "empty", "Node", "existing");
            shard.RelationshipAddEmptySameShard(3, "Node", "empty", "Node", "existing");
            shard.RelationshipAddEmptySameShard(3, "Node", "empty", "Node", "existing");

            THEN("the shard keeps them") {
                std::vector<ragedb::Relationship> it = shard.AllRelationships("KNOWS");
                REQUIRE(it.size() == 2);

                it = shard.AllRelationships("FRIENDS");
                REQUIRE(it.size() == 3);

                std::vector<uint64_t> it2 = shard.AllRelationshipIds();
                REQUIRE(it2.size() == 5);

                it2 = shard.AllRelationshipIds("FRIENDS");
                REQUIRE(it2.size() == 3);

                it2 = shard.AllRelationshipIds("FRIENDS", 1, 2);
                REQUIRE(it2.size() == 2);

                it2 = shard.AllRelationshipIds(3, 1, 2);
                REQUIRE(it2.size() == 2);

                it2 = shard.AllRelationshipIds(static_cast<uint64_t>(2),static_cast<uint64_t>(3));
                REQUIRE(it2.size() == 3);
            }
        }

        WHEN("more relationships of a different type are added") {
            shard.RelationshipAddEmptySameShard(1, "Node", "empty", "Node", "existing");
            shard.RelationshipAddEmptySameShard(1, "Node", "empty", "Node", "existing");
            shard.RelationshipAddEmptySameShard(3, "Node", "empty", "Node", "existing");
            shard.RelationshipAddEmptySameShard(3, "Node", "empty", "Node", "existing");
            shard.RelationshipAddEmptySameShard(3, "Node", "empty", "Node", "existing");

            THEN("the shard keeps them") {
                std::vector<ragedb::Relationship> it = shard.AllRelationships(static_cast<uint16_t>(1));
                REQUIRE(it.size() == 2);

                it = shard.AllRelationships(static_cast<uint64_t>(3), 0, 100);
                REQUIRE(it.size() == 3);
            }
        }
    }
}