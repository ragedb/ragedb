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

SCENARIO( "Shard can handle Relationship Types", "[relationship_types]" ) {

    GIVEN("An empty shard") {
        ragedb::Shard shard(4);

        WHEN("we get the relationship types") {
            THEN("it should be empty") {
                std::set<std::string> types = shard.RelationshipTypesGet();
                REQUIRE(types.empty());
            }
        }

        WHEN("we get the count of relationship types") {
            THEN("it should be zero") {
                uint64_t count = shard.RelationshipTypesGetCount();
                REQUIRE(count == 0);
            }
        }

        WHEN("we get the type id and type") {
            THEN("it should have the empty id and type") {
                uint16_t type_id = shard.RelationshipTypeGetTypeId("NOT_THERE");
                std::string type = shard.RelationshipTypeGetType(99);
                REQUIRE(type_id == 0);
                REQUIRE(type.empty());
            }
        }

        WHEN("we add a relationship type") {
            shard.RelationshipTypeInsert("LOVES", 1);

            THEN("it should be one") {
                uint64_t count = shard.RelationshipTypesGetCount();
                REQUIRE(count == 1);
            }

            THEN("it should be in the set") {
                std::set<std::string> types = shard.RelationshipTypesGet();
                REQUIRE(types.size() == 1);
                REQUIRE(types == std::set<std::string>({"LOVES"}));
            }

            THEN("it should have the correct id and type") {
                uint16_t type_id = shard.RelationshipTypeGetTypeId("LOVES");
                std::string type = shard.RelationshipTypeGetType(1);
                REQUIRE(type_id == 1);
                REQUIRE(type == "LOVES");
            }

            THEN("it should be unique and retain original id") {
                shard.RelationshipTypeInsert("LOVES", 2);
                uint16_t type_id = shard.RelationshipTypeGetTypeId("LOVES");
                std::string type = shard.RelationshipTypeGetType(1);
                std::string invalid = shard.RelationshipTypeGetType(2);
                REQUIRE(type_id == 1);
                REQUIRE(type == "LOVES");
                REQUIRE(invalid.empty());
            }
        }

        WHEN("we add two relationship types") {
            shard.RelationshipTypeInsert("LOVES", 1);
            shard.RelationshipTypeInsert("HATES", 2);

            THEN("it should be two") {
                uint64_t count = shard.RelationshipTypesGetCount();
                REQUIRE(count == 2);
            }
        }

    }

    GIVEN("A shard with three nodes") {
        ragedb::Shard shard(4);
        shard.NodeTypeInsert("Node", 1);
        shard.NodeTypeInsert("User", 2);
        uint64_t node1id = shard.NodeAddEmpty(1, "one");
        uint64_t node2id = shard.NodeAddEmpty(1, "two");
        uint64_t node3id = shard.NodeAddEmpty(1, "three");

        WHEN("we get the count of an invalid relationship type") {
            THEN("it should be zero") {
                uint64_t count = shard.RelationshipTypesGetCount("DOES_NOT_EXIST");
                REQUIRE(count == 0);
            }
        }

        WHEN("we get the count of an invalid relationship type id") {
            THEN("it should be zero") {
                uint64_t count = shard.RelationshipTypesGetCount(99);
                REQUIRE(count == 0);
            }
        }

        WHEN("add a relationship") {
            shard.RelationshipTypeInsert("LOVES", 1);
            shard.RelationshipAddEmptySameShard(1, node1id, node2id);

            THEN("request the count of the relationship type") {
                uint64_t count_by_id = shard.RelationshipTypesGetCount(1);
                uint64_t count_by_type = shard.RelationshipTypesGetCount("LOVES");
                REQUIRE(count_by_id == 1);
                REQUIRE(count_by_type == 1);
            }
        }

        WHEN("add new relationships") {
            shard.RelationshipTypeInsert("LOVES", 1);
            shard.RelationshipTypeInsert("HATES", 2);
            shard.RelationshipAddEmptySameShard(1, node1id, node2id);
            shard.RelationshipAddEmptySameShard(1, node2id, node3id);
            shard.RelationshipAddEmptySameShard(2, node3id, node1id);

            THEN("request the count of the relationship types") {
                uint64_t likes_count = shard.RelationshipTypesGetCount("LOVES");
                uint64_t likes_count_by_id = shard.RelationshipTypesGetCount(1);
                uint64_t hates_count = shard.RelationshipTypesGetCount("HATES");
                uint64_t hates_count_by_id = shard.RelationshipTypesGetCount(2);
                REQUIRE(likes_count == 2);
                REQUIRE(likes_count_by_id == 2);
                REQUIRE(hates_count == 1);
                REQUIRE(hates_count_by_id == 1);
            }
        }
    }
}