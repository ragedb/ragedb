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

SCENARIO( "Shard can handle Relationships", "[relationship]" ) {

    GIVEN("A shard with an empty node and an existing node with properties") {
        ragedb::Shard shard(4);
        shard.NodeTypeInsert("Node", 1);
        shard.NodeTypeInsert("User", 2);
        uint64_t empty = shard.NodeAddEmpty(1, "empty");
        uint64_t existing = shard.NodeAdd(1, "existing", R"({ "name":"max", "email":"maxdemarzi@example.com" })");

        shard.RelationshipTypeInsert("KNOWS", 1);
        shard.RelationshipPropertyTypeAdd(1, "active", 1);
        shard.RelationshipPropertyTypeAdd(1, "weight", 3);
        shard.RelationshipPropertyTypeAdd(1, "tag", 4);

        REQUIRE(empty == 1024);
        REQUIRE(existing == 67109888);
        REQUIRE(shard.RelationshipGetStartingNodeId(1) == 0);
        REQUIRE(shard.RelationshipGetEndingNodeId(1) == 0);

        shard.RelationshipTypeInsert("KNOWS", 1);

//        WHEN("we print a new relationship") {
//            uint64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing", R"({ "strength": 0.8, "color": "blue", "expired": false, "size": 9 })");
//            std::stringstream out;
//            out << shard.RelationshipGet(added);
//            THEN("we get the correct output") {
//                REQUIRE(out.str() == "{ \"id\": 256, \"type_id\": 1, \"starting_node_id\": 256, \"ending_node_id\": 512, \"properties\": { \"color\": \"blue\", \"expired\": false, \"size\": 9, \"strength\": 0.8 } }");
//            }
//        }

        WHEN("an empty relationship is added") {
            uint64_t added = shard.RelationshipAddEmptySameShard(1, "Node", "empty", "Node", "existing");

            THEN("the shard keeps it") {
                REQUIRE(added == 1024);
                ragedb::Relationship added_relationship = shard.RelationshipGet(added);
                REQUIRE(added_relationship.getId() == added);
                REQUIRE(added_relationship.getTypeId() == 1);
                REQUIRE(added_relationship.getStartingNodeId() == empty);
                REQUIRE(added_relationship.getEndingNodeId() == existing);

                REQUIRE(shard.RelationshipGetType(added) == "KNOWS");
                REQUIRE(shard.RelationshipGetType(added) == shard.RelationshipTypeGetType(added_relationship.getTypeId()));
                REQUIRE(shard.RelationshipGetTypeId(added) == added_relationship.getTypeId());

                REQUIRE(shard.RelationshipGetStartingNodeId(added) == added_relationship.getStartingNodeId());
                REQUIRE(shard.RelationshipGetEndingNodeId(added) == added_relationship.getEndingNodeId());

            }
        }

        WHEN("a relationship with properties is added") {

            uint64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing", R"({ "active":true, "weight":1.0, "tag":"college" })");

            THEN("the shard keeps it") {
                REQUIRE(added == 1024);
                ragedb::Relationship added_relationship = shard.RelationshipGet(added);
                REQUIRE(added_relationship.getId() == added);
                REQUIRE(added_relationship.getTypeId() == 1);
                REQUIRE(added_relationship.getStartingNodeId() == empty);
                REQUIRE(added_relationship.getEndingNodeId() == existing);
                REQUIRE( "college" == std::any_cast<std::string>(shard.RelationshipPropertyGet(added, "tag")));
                REQUIRE(!added_relationship.getProperties().empty());
            }
        }

        WHEN("an empty relationship is added after deleting one") {
            uint64_t added = shard.RelationshipAddEmptySameShard(1, "Node", "empty", "Node", "existing");

            THEN("the shard keeps it") {
                REQUIRE(added == 1024);
                std::pair <uint16_t, uint64_t> rel_type_incoming_node_id = shard.RelationshipRemoveGetIncoming(added);
                bool deleted = shard.RelationshipRemoveIncoming(rel_type_incoming_node_id.first, added, rel_type_incoming_node_id.second);
                REQUIRE (deleted);
                added = shard.RelationshipAddEmptySameShard(1, "Node", "empty", "Node", "existing");
                REQUIRE(added == 1024);
                ragedb::Relationship added_relationship = shard.RelationshipGet(added);
                REQUIRE(added_relationship.getId() == added);
                REQUIRE(added_relationship.getTypeId() == 1);
                REQUIRE(added_relationship.getStartingNodeId() == empty);
                REQUIRE(added_relationship.getEndingNodeId() == existing);
            }
        }

        WHEN("a relationship is added after deleting one") {
            uint64_t added = shard.RelationshipAddEmptySameShard(1, "Node", "empty", "Node", "existing");

            THEN("the shard keeps it") {
                REQUIRE(added == 1024);
                std::pair <uint16_t, uint64_t> rel_type_incoming_node_id = shard.RelationshipRemoveGetIncoming(added);
                bool deleted = shard.RelationshipRemoveIncoming(rel_type_incoming_node_id.first, added, rel_type_incoming_node_id.second);
                REQUIRE (deleted);
                added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing", R"({ "active":true, "weight":1.0, "tag":"college" })");
                REQUIRE(added == 1024);
                ragedb::Relationship added_relationship = shard.RelationshipGet(added);
                REQUIRE(added_relationship.getId() == added);
                REQUIRE(added_relationship.getTypeId() == 1);
                REQUIRE(added_relationship.getStartingNodeId() == empty);
                REQUIRE(added_relationship.getEndingNodeId() == existing);
                REQUIRE( "college" == std::any_cast<std::string>(shard.RelationshipPropertyGet(added, "tag")));
                REQUIRE(!added_relationship.getProperties().empty());

            }
        }

        WHEN("an invalid relationship is added") {
            uint64_t added = shard.RelationshipAddEmptySameShard(1, "Node", "not_there", "Node", "existing");

            THEN("the shard ignores it") {
                REQUIRE(added == 0);
                ragedb::Relationship added_relationship = shard.RelationshipGet(0);
                REQUIRE(added_relationship.getId() == 0);
                REQUIRE(added_relationship.getTypeId() == 0);
                REQUIRE(added_relationship.getStartingNodeId() == 0);
                REQUIRE(added_relationship.getEndingNodeId() == 0);
            }
        }

        WHEN("an invalid relationship with properties is added") {
            uint64_t added = shard.RelationshipAddSameShard(1, "Node", "not_there", "Node", "existing", R"({ "active":true, "weight":1.0, "tag":"college" })");

            THEN("the shard ignores it") {
                REQUIRE(added == 0);
                ragedb::Relationship added_relationship = shard.RelationshipGet(0);
                REQUIRE(added_relationship.getId() == 0);
                REQUIRE(added_relationship.getTypeId() == 0);
                REQUIRE(added_relationship.getStartingNodeId() == 0);
                REQUIRE(added_relationship.getEndingNodeId() == 0);
                REQUIRE(!shard.RelationshipPropertyGet(added, "tag").has_value());
                REQUIRE(added_relationship.getProperties().empty());
            }
        }

        WHEN("two relationships to the same nodes are added") {
            uint64_t added = shard.RelationshipAddEmptySameShard(1, "Node", "empty", "Node", "existing");
            uint64_t added2 = shard.RelationshipAddEmptySameShard(1, "Node", "empty", "Node", "existing");

            THEN("the shard keeps them") {
                REQUIRE(added == 1024);
                ragedb::Relationship added_relationship = shard.RelationshipGet(added);
                REQUIRE(added_relationship.getId() == added);
                REQUIRE(added_relationship.getTypeId() == 1);
                REQUIRE(added_relationship.getStartingNodeId() == empty);
                REQUIRE(added_relationship.getEndingNodeId() == existing);

                REQUIRE(added2 == 67109888);
                ragedb::Relationship added_relationship2 = shard.RelationshipGet(added2);
                REQUIRE(added_relationship2.getId() == added2);
                REQUIRE(added_relationship2.getTypeId() == 1);
                REQUIRE(added_relationship2.getStartingNodeId() == empty);
                REQUIRE(added_relationship2.getEndingNodeId() == existing);
            }
        }

        WHEN("two relationships with properties to the same nodes are added") {
            uint64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing", R"({ "active":true, "weight":1.0, "tag":"college" })");
            uint64_t added2 = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing", R"({ "active":true, "weight":2.0, "tag":"college" })");

            THEN("the shard keeps them") {
                REQUIRE(added == 1024);
                ragedb::Relationship added_relationship = shard.RelationshipGet(added);
                REQUIRE(added_relationship.getId() == added);
                REQUIRE(added_relationship.getTypeId() == 1);
                REQUIRE(added_relationship.getStartingNodeId() == empty);
                REQUIRE(added_relationship.getEndingNodeId() == existing);
                REQUIRE(std::any_cast<double>(shard.RelationshipPropertyGet(added, "weight")) == 1.0);

                REQUIRE(added2 == 67109888);
                ragedb::Relationship added_relationship2 = shard.RelationshipGet(added2);
                REQUIRE(added_relationship2.getId() == added2);
                REQUIRE(added_relationship2.getTypeId() == 1);
                REQUIRE(added_relationship2.getStartingNodeId() == empty);
                REQUIRE(added_relationship2.getEndingNodeId() == existing);
                REQUIRE(std::any_cast<double>(shard.RelationshipPropertyGet(added2, "weight")) == 2.0);
            }
        }
    }
}