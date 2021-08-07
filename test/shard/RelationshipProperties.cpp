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

SCENARIO( "Shard can handle Relationship Properties", "[relationship,properties]" ) {

    GIVEN("A shard with an empty node and an existing node with properties") {
        ragedb::Shard shard(4);
        shard.NodeTypeInsert("Node", 1);
        shard.NodeTypeInsert("User", 2);
        uint64_t empty = shard.NodeAddEmpty(1, "empty");
        uint64_t existing = shard.NodeAdd(1, "existing", R"({ "name":"max", "age":99, "weight":230.5 })");

        shard.RelationshipTypeInsert("KNOWS", 1);
        shard.RelationshipPropertyTypeAdd(1, "active", 1);
        shard.RelationshipPropertyTypeAdd(1, "new", 1);
        shard.RelationshipPropertyTypeAdd(1, "age", 2);
        shard.RelationshipPropertyTypeAdd(1, "number", 2);
        shard.RelationshipPropertyTypeAdd(1, "weight", 3);
        shard.RelationshipPropertyTypeAdd(1, "tag", 4);
        shard.RelationshipPropertyTypeAdd(1, "name", 4);


        REQUIRE( empty == 1024 );
        REQUIRE( existing == 67109888 );

        shard.RelationshipTypeInsert("KNOWS", 1);

        WHEN("a relationship with properties is added") {

            uint64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                                           R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

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

        WHEN("a relationship with invalid properties is added") {

            uint64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                                           R"({ "active":true, "weight":1.0, "tag":"college", "invalid:3 })");

            THEN("the shard does not keep it") {
                REQUIRE(added == 1024);
                // TODO: So we should not allow it, HTTP check is done.  and Check in Lua
            }
        }

        WHEN("a string property is requested by id") {
            uint64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                                           R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

            THEN("the shard gets it") {
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

        WHEN("an integer property is requested by id") {
            uint64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                                           R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

            THEN("the shard gets it") {
                REQUIRE(added == 1024);
                ragedb::Relationship added_relationship = shard.RelationshipGet(added);
                REQUIRE(added_relationship.getId() == added);
                REQUIRE(added_relationship.getTypeId() == 1);
                REQUIRE(added_relationship.getStartingNodeId() == empty);
                REQUIRE(added_relationship.getEndingNodeId() == existing);
                REQUIRE( 3 == std::any_cast<int64_t>(shard.RelationshipPropertyGet(added, "number")));
                REQUIRE(!added_relationship.getProperties().empty());
            }

        }

        WHEN("a double property is requested by id") {
            uint64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                                           R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

            THEN("the shard gets it") {
                REQUIRE(added == 1024);
                ragedb::Relationship added_relationship = shard.RelationshipGet(added);
                REQUIRE(added_relationship.getId() == added);
                REQUIRE(added_relationship.getTypeId() == 1);
                REQUIRE(added_relationship.getStartingNodeId() == empty);
                REQUIRE(added_relationship.getEndingNodeId() == existing);
                REQUIRE( 1.0 == std::any_cast<double>(shard.RelationshipPropertyGet(added, "weight")));
                REQUIRE(!added_relationship.getProperties().empty());
            }
        }

        WHEN("a boolean property is requested by id") {
            uint64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                                           R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

            THEN("the shard gets it") {
                REQUIRE(added == 1024);
                ragedb::Relationship added_relationship = shard.RelationshipGet(added);
                REQUIRE(added_relationship.getId() == added);
                REQUIRE(added_relationship.getTypeId() == 1);
                REQUIRE(added_relationship.getStartingNodeId() == empty);
                REQUIRE(added_relationship.getEndingNodeId() == existing);
                REQUIRE(std::any_cast<bool>(shard.RelationshipPropertyGet(added, "active")));
                REQUIRE(!added_relationship.getProperties().empty());
            }
        }

        WHEN("a string property is set by id") {
            uint64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                                           R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

            THEN("the shard sets it") {
                bool set = shard.RelationshipPropertySetFromJson(added, "name", "\"alex\"");
                REQUIRE(set);
                auto value =  std::any_cast<std::string>(shard.RelationshipPropertyGet(added, "name"));
                REQUIRE("alex" == value);
            }
        }

        WHEN("a string literal property is set by id") {
            uint64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                                           R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

            THEN("the shard sets it") {
                bool set = shard.RelationshipPropertySetFromJson(added, "name", "\"alex\"");
                REQUIRE(set);
                auto value = std::any_cast<std::string>(shard.RelationshipPropertyGet(added, "name"));
                REQUIRE("alex" == value);
            }
        }

        WHEN("an integer property is set by id") {
            uint64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                                           R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

            THEN("the shard sets it") {
                bool set = shard.RelationshipPropertySetFromJson(added, "age", "55");
                REQUIRE(set);
                auto value = std::any_cast<int64_t>(shard.RelationshipPropertyGet(added, "age"));
                REQUIRE(55 == value);
            }
        }

        WHEN("a double property is set by id") {
            uint64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                                           R"({ "active":true, "tag":"college", "number":3 })");

            THEN("the shard sets it") {
                bool set = shard.RelationshipPropertySetFromJson(added, "weight", "190.0");
                REQUIRE(set);
                auto value = std::any_cast<double>(shard.RelationshipPropertyGet(added, "weight"));
                REQUIRE(190.0 == value);
            }
        }

        WHEN("a boolean property is set by id") {
            uint64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                                           R"({ "active":true, "tag":"college", "number":3 })");

            THEN("the shard sets it") {
                bool set = shard.RelationshipPropertySetFromJson(added, "new", "true");
                REQUIRE(set);
                auto value = std::any_cast<bool>(shard.RelationshipPropertyGet(added, "new"));
                REQUIRE(value);
            }
        }

        WHEN("a string property is set by id to an invalid id") {
            uint64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                                           R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

            THEN("the shard does not set it") {
                bool set = shard.RelationshipPropertySetFromJson(100 + added, "name", "\"alex\"");
                REQUIRE(set == false);
                auto value = shard.RelationshipPropertyGet(100 + added, "name");
                REQUIRE(!value.has_value());
            }
        }

        WHEN("a string literal property is set by id to an invalid id") {
            uint64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                                           R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

            THEN("the shard does not set it") {
                bool set = shard.RelationshipPropertySetFromJson(100 + added, "name", "\"alex\"");
                REQUIRE(set == false);
                auto value = shard.RelationshipPropertyGet(100 + added, "name");
                REQUIRE(!value.has_value());
            }
        }

        WHEN("a integer property is set by id to an invalid id") {
            uint64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                                           R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

            THEN("the shard does not set it") {
                bool set = shard.RelationshipPropertySetFromJson(100 + added, "age", "55");
                REQUIRE(set == false);
                auto value = shard.RelationshipPropertyGet(100 + added, "age");
                REQUIRE(!value.has_value());
            }
        }

        WHEN("a double property is set by id to an invalid id") {
            uint64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                                           R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

            THEN("the shard does not set it") {
                bool set = shard.RelationshipPropertySetFromJson(100 + added, "weight", "190.0");
                REQUIRE(set == false);
                auto value = shard.RelationshipPropertyGet(100 + added, "weight");
                REQUIRE(!value.has_value());
            }
        }

        WHEN("a boolean property is set by id to an invalid id") {
            uint64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                                           R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

            THEN("the shard does not set it") {
                bool set = shard.RelationshipPropertySetFromJson(100 + added, "new", "true");
                REQUIRE(set == false);
                auto value = shard.RelationshipPropertyGet(100 + added, "new");
                REQUIRE(!value.has_value());
            }
        }

        WHEN("a string property is set by id to a new property") {
            uint64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                                           R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

            THEN("the shard does not set it") {
                bool set = shard.RelationshipPropertySetFromJson(added, "not_there", "\"alex\"");
                REQUIRE(!set);
                auto value = shard.RelationshipPropertyGet(added, "not_there");
                REQUIRE(!value.has_value());
            }
        }

        WHEN("a integer property is set by id to an new property") {
            uint64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                                           R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

            THEN("the shard does not set it") {
                bool set = shard.RelationshipPropertySetFromJson(added, "not_there", "55");
                REQUIRE(!set);
                auto value = shard.RelationshipPropertyGet(added, "not_there");
                REQUIRE(!value.has_value());
            }
        }

        WHEN("a double property is set by id to a new property") {
            uint64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                                           R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

            THEN("the shard does not set it") {
                bool set = shard.RelationshipPropertySetFromJson(added, "not_there", "190.0");
                REQUIRE(!set);
                auto value = shard.RelationshipPropertyGet(added, "not_there");
                REQUIRE(!value.has_value());
            }
        }

        WHEN("a boolean property is set by id to a new property") {
            uint64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                                           R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

            THEN("the shard does not set it") {
                bool set = shard.RelationshipPropertySetFromJson(added, "not_there", "true");
                REQUIRE(!set);
                auto value = shard.RelationshipPropertyGet(added, "not_there");
                REQUIRE(!value.has_value());
            }
        }

        WHEN("a string property is deleted by id") {
            uint64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                                           R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

            THEN("the shard deletes it") {
                bool set = shard.RelationshipPropertyDelete(added, "tag");
                REQUIRE(set);
                auto value = shard.RelationshipPropertyGet(added, "tag");
                REQUIRE(!value.has_value());
            }
        }

        WHEN("an integer property is deleted by id") {
            uint64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                                           R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

            THEN("the shard deletes it") {
                bool set = shard.RelationshipPropertyDelete(added, "number");
                REQUIRE(set);
                auto value = shard.RelationshipPropertyGet(added, "number");
                REQUIRE(!value.has_value());
            }
        }

        WHEN("a double property is deleted by id") {
            uint64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                                           R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

            THEN("the shard deletes it") {
                bool set = shard.RelationshipPropertyDelete(added, "weight");
                REQUIRE(set);
                auto value = shard.RelationshipPropertyGet(added, "weight");
                REQUIRE(!value.has_value());
            }
        }

        WHEN("a boolean property is deleted by id") {
            uint64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                                           R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

            THEN("the shard deletes it") {
                bool set = shard.RelationshipPropertyDelete(added, "active");
                REQUIRE(set);
                auto value = shard.RelationshipPropertyGet(added, "active");
                REQUIRE(!value.has_value());
            }
        }

        WHEN("a non-existing property is deleted by id") {
            uint64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                                           R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

            THEN("the shard ignores it") {
                bool set = shard.RelationshipPropertyDelete(added, "not_there");
                REQUIRE(set == false);
                auto value = shard.RelationshipPropertyGet(added, "not_there");
                REQUIRE(!value.has_value());
            }
        }

        WHEN("all properties are deleted by id") {
            uint64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                                           R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

            THEN("the shard deletes them") {
                bool set = shard.RelationshipPropertiesDelete(added+100);
                REQUIRE(!set);
                set = shard.RelationshipPropertiesDelete(added);
                REQUIRE(set);
                auto value = shard.RelationshipPropertyGet(added, "number");
                REQUIRE(!value.has_value());
            }
        }

//        WHEN("all properties are set by id") {
//            uint64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
//                                                           R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");
//
//            THEN("the shard sets them") {
//                std::map<std::string, std::any> properties;
//                properties.insert({"eyes", std::string("brown")});
//                properties.insert({"height", 5.11});
//
//                bool set = shard.RelationshipPropertiesSet(added, properties);
//                REQUIRE(set);
//                auto eyes = shard.RelationshipPropertyGetString(added, "eyes");
//                REQUIRE("brown" == eyes);
//                auto height = shard.RelationshipPropertyGetDouble(added, "height");
//                REQUIRE(5.11 == height);
//            }
//        }
    }
}