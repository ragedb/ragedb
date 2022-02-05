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


SCENARIO("Shard can handle Nodes", "[node]") {
    // integer_type = 2, string_type = 4

    GIVEN("A shard with an empty node and an existing node with properties") {
        ragedb::Shard shard(4);
        shard.NodeTypeInsert("Node", 1);
        shard.NodeTypeInsert("User", 2);
        shard.NodePropertyTypeAdd(1, "name", 4);
        shard.NodePropertyTypeAdd(1, "email", 4);
        shard.NodePropertyTypeAdd(1, "strength", 3);
        shard.NodePropertyTypeAdd(1, "color", 4);
        shard.NodePropertyTypeAdd(1, "expired", 1);
        shard.NodePropertyTypeAdd(1, "size", 2);

        REQUIRE(shard.NodeGetTypeId(1) == 0);
        REQUIRE(shard.NodeGetKey(1).empty());

        uint64_t empty = shard.NodeAddEmpty(1, "empty");
        uint64_t existing = shard.NodeAdd(1, "existing", R"({ "name":"max", "email":"maxdemarzi@example.com" })");

        REQUIRE(empty == 1024);
        REQUIRE(existing == 67109888);

        WHEN("we print a new node") {
            uint64_t added = shard.NodeAdd(1, "new", R"({ "strength": 0.8, "color": "blue", "expired": false, "size": 9 })");
            std::stringstream out;
            out << shard.NodeGet(added);
            THEN("we get the correct output") {
                REQUIRE(out.str() == "{ \"id\": 134218752, \"type\": \"Node\", \"key\": \"new\", \"properties\": { \"color\": \"blue\", \"expired\": false, \"size\": 9, \"strength\": 0.8 } }");
            }
        }

        WHEN("an empty node is added") {
            uint64_t added = shard.NodeAddEmpty(1, "added");

            THEN("the shard keeps it") {
                REQUIRE("Node" == shard.NodeGetType(added));
                ragedb::Node addedNode = shard.NodeGet(added);
                REQUIRE(addedNode.getId() > 0);
                REQUIRE("added" == addedNode.getKey());

                REQUIRE(shard.NodeGetTypeId(added) == addedNode.getTypeId());
                REQUIRE(shard.NodeGetKey(added) == addedNode.getKey());
            }
        }

        WHEN("a node with properties is added") {
            uint64_t added = shard.NodeAdd(1, "withProperties",
                                          R"({ "name":"max de marzi", "email":"maxdemarzi@gmail.com" })");

            THEN("the shard keeps it with properties") {
                auto NodeLabel = shard.NodeGetType(added);
                REQUIRE("Node" == NodeLabel);
                ragedb::Node addedNode = shard.NodeGet(added);
                REQUIRE(addedNode.getTypeId() > 0);
                REQUIRE("Node" == shard.NodeGetType(added));
                REQUIRE("withProperties" == shard.NodeGetKey(added));
                REQUIRE("max de marzi" == get<std::string>(shard.NodePropertyGet(added, "name")));
                REQUIRE("maxdemarzi@gmail.com" == get<std::string>(shard.NodePropertyGet(added, "email")));
            }
        }

        WHEN("a node with properties is added, removed and readded") {
            shard.NodeAdd(1, "withProperties",
                          R"({ "name":"max de marzi", "email":"maxdemarzi@gmail.com" })");
            shard.NodeRemove("Node", "withProperties");
            uint64_t added = shard.NodeAdd(1, "withProperties",
                                          R"({ "name":"max de marzi", "email":"maxdemarzi@gmail.com" })");

            THEN("the shard keeps it with properties") {
                auto NodeLabel = shard.NodeGetType(added);
                REQUIRE("Node" == NodeLabel);
                ragedb::Node addedNode = shard.NodeGet("Node", "withProperties");
                REQUIRE(addedNode.getTypeId() > 0);
                REQUIRE("Node" == shard.NodeGetType(added));
                REQUIRE("withProperties" == shard.NodeGetKey(added));
                REQUIRE("max de marzi" == get<std::string>(shard.NodePropertyGet(added, "name")));
                REQUIRE("maxdemarzi@gmail.com" == get<std::string>(shard.NodePropertyGet(added, "email")));
            }
        }

        WHEN("a node is removed by label and key") {
            uint64_t added = shard.NodeAddEmpty(1, "remove_me_by_label_and_key");
            ragedb::Node addedNode = shard.NodeGet(added);
            REQUIRE("Node" == shard.NodeGetType(added));

            bool removed = shard.NodeRemove("Node", "remove_me_by_label_and_key");
            THEN("the shard removes it") {
                REQUIRE(removed);
                auto removedNode = shard.NodeGet(added);
                REQUIRE(removedNode.getId() == 0);
            }
        }

        WHEN("a node is removed by id") {
            uint64_t added = shard.NodeAddEmpty(1, "remove_me_by_id");
            ragedb::Node addedNode = shard.NodeGet(added);
            REQUIRE("Node" == shard.NodeGetType(added));

            bool removed = shard.NodeRemove(added);
            THEN("the shard removes it") {
                REQUIRE(removed);
                auto removedNode = shard.NodeGet(added);
                REQUIRE(removedNode.getId() == 0);
            }
        }

        WHEN("node zero is removed by label/key") {
            bool removed = shard.NodeRemove("", "");
            THEN("the shard does not remove it") {
                REQUIRE(!removed);
                auto removedNode = shard.NodeGet("", "");
                REQUIRE(removedNode.getId() == 0);
            }
        }

        WHEN("node zero is removed by id") {
            bool removed = shard.NodeRemove(0);
            THEN("the shard does not remove it") {
                REQUIRE(!removed);
                auto removedNode = shard.NodeGet(0);
                REQUIRE(removedNode.getId() == 0);
            }
        }

        WHEN("a node is removed by id and re-added") {
            uint64_t added = shard.NodeAddEmpty(1, "remove_me_by_id");
            ragedb::Node addedNode = shard.NodeGet(added);
            REQUIRE("Node" == shard.NodeGetType(added));

            bool removed = shard.NodeRemove(added);
            THEN("the shard removes it and re-adds it") {
                REQUIRE(removed);
                auto removedNode = shard.NodeGet(added);
                REQUIRE(removedNode.getId() == 0);

                uint64_t added2 = shard.NodeAddEmpty(1, "remove_me_by_id");
                ragedb::Node addedNode2 = shard.NodeGet(added2);
                REQUIRE(added == added2);
                REQUIRE(addedNode.getId() == addedNode2.getId());
            }
        }

        WHEN("a node with relationships is removed by id") {
            uint64_t added = shard.NodeAddEmpty(1, "remove_me_by_id");
            ragedb::Node addedNode = shard.NodeGet(added);
            REQUIRE("Node" == shard.NodeGetType(added));

            shard.RelationshipTypeInsert("KNOWS", 1);
            uint64_t addedRelId = shard.RelationshipAddEmptySameShard(1, added, existing);
            uint64_t addedRelId2 = shard.RelationshipAddEmptySameShard(1, existing, added);

            ragedb::Relationship addedRel = shard.RelationshipGet(addedRelId);
            ragedb::Relationship addedRel2 = shard.RelationshipGet(addedRelId2);

            REQUIRE(addedRel.getId() > 0);
            REQUIRE(addedRel2.getId() > 0);

            bool removed = shard.NodeRemove(added);
            THEN("the shard removes it") {
                REQUIRE(removed);
                auto removedNode = shard.NodeGet(added);
                REQUIRE(removedNode.getId() == 0);

                ragedb::Relationship deletedRel = shard.RelationshipGet(addedRelId);
                REQUIRE(deletedRel.getId() == 0);

                uint64_t degree = shard.NodeGetDegree(added);
                REQUIRE(degree == 0);
            }
        }
    }
}