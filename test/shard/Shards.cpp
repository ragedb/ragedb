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

SCENARIO( "Shard can reserve and clear", "[links]" ) {
    GIVEN("An empty shard") {
        ragedb::Shard shard(4);
        shard.NodeTypeInsert("Node", 1);
        shard.NodeTypeInsert("User", 2);
        shard.NodeTypeInsert("Person", 3);

        WHEN("we add some nodes and relationships") {
            uint64_t node1id = shard.NodeAddEmpty(1, "one");
            uint64_t node3id = shard.NodeAddEmpty(2, "two");
            uint64_t node2id = shard.NodeAddEmpty(3, "three");

            shard.RelationshipTypeInsert("LOVES", 1);
            shard.RelationshipTypeInsert("HATES", 2);

            shard.RelationshipAddEmptySameShard(1, node1id, node2id);
            shard.RelationshipAddEmptySameShard(1, node2id, node3id);
            shard.RelationshipAddEmptySameShard(2, node3id, node1id);

            THEN("clear the shard") {
                uint64_t likes_count = shard.RelationshipTypesGetCount("LOVES");
                uint64_t node_types_count = shard.NodeTypesGetCount();
                REQUIRE(likes_count == 2);
                REQUIRE(node_types_count == 3);
                shard.Clear();
                likes_count = shard.RelationshipTypesGetCount("LOVES");
                node_types_count = shard.NodeTypesGetCount();
                REQUIRE(likes_count == 0);
                REQUIRE(node_types_count == 0);
            }
        }
    }
}