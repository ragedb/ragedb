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

SCENARIO( "Shard can handle Node Types", "[node_types]" ) {
    GIVEN("An empty shard") {
        ragedb::Shard shard(4);

        WHEN("we get the node types") {
            THEN("it should be empty") {
                std::set<std::string> types = shard.NodeTypesGet();
                REQUIRE(types.empty());
            }
        }

        WHEN("we get the count of node types") {
            THEN("it should be zero") {
                uint64_t count = shard.NodeTypesGetCount();
                REQUIRE(count == 0);
            }
        }

        WHEN("we get the type id and type") {
            THEN("it should have the empty id and type") {
                uint16_t type_id = shard.NodeTypeGetTypeId("NOT_THERE");
                std::string type = shard.NodeTypeGetType(99);
                REQUIRE(type_id == 0);
                REQUIRE(type.empty());
            }
        }

        WHEN("we add a node type") {
            shard.NodeTypeInsert("Person", 1);

            THEN("it should be one") {
                uint64_t count = shard.NodeTypesGetCount();
                REQUIRE(count == 1);
            }

            THEN("it should be in the set") {
                std::set<std::string> types = shard.NodeTypesGet();
                REQUIRE(types.size() == 1);
                REQUIRE(types == std::set<std::string>({"Person"}));
            }

            THEN("it should have the correct id and type") {
                uint16_t type_id = shard.NodeTypeGetTypeId("Person");
                std::string type = shard.NodeTypeGetType(1);
                REQUIRE(type_id == 1);
                REQUIRE(type == "Person");
            }

            THEN("it should be unique and retain original id") {
                shard.NodeTypeInsert("Person", 2);
                uint16_t type_id = shard.NodeTypeGetTypeId("Person");
                std::string type = shard.NodeTypeGetType(1);
                std::string invalid = shard.NodeTypeGetType(2);
                REQUIRE(type_id == 1);
                REQUIRE(type == "Person");
                REQUIRE(invalid.empty());
            }

            THEN("it should retain a map when asked for") {
              shard.NodePropertyTypeAdd(1, "name", 4);
              shard.NodePropertyTypeAdd(1, "age", 2);
              auto type_map = shard.NodeTypeGet("Person");
              REQUIRE(type_map.size() > 0);
              REQUIRE(type_map.at("name") == "string");
              REQUIRE(type_map.at("age") == "integer");
            }
        }

        WHEN("we add two node types") {
            shard.NodeTypeInsert("Person", 1);
            shard.NodeTypeInsert("User", 2);

            THEN("it should be two") {
                uint64_t count = shard.NodeTypesGetCount();
                REQUIRE(count == 2);
            }

            shard.DeleteNodeType("User");
            THEN("it should be one") {
              uint64_t count = shard.NodeTypesGetCount();
              REQUIRE(count == 1);
            }

        }
    }

    GIVEN("A shard with three nodes") {
        ragedb::Shard shard(4);
        shard.NodeTypeInsert("Node", 1);
        uint64_t node1id = shard.NodeAddEmpty(1, "one");
        uint64_t node2id = shard.NodeAddEmpty(1, "two");
        uint64_t node3id = shard.NodeAddEmpty(1, "three");

        WHEN("we get the count of an invalid node type") {
            THEN("it should be zero") {
                uint64_t count = shard.NodeTypesGetCount("DOES_NOT_EXIST");
                REQUIRE(count == 0);
            }
        }

        WHEN("add a node") {
            shard.NodeTypeInsert("Person", 2);
            shard.NodeAddEmpty( 2, "four");

            THEN("request the count of the node type") {
                uint64_t count_by_type = shard.NodeTypesGetCount("Person");
                  REQUIRE(count_by_type == 1);
            }
        }

        WHEN("add new nodes") {
            shard.NodeTypeInsert("User", 2);
            shard.NodeTypeInsert("Thing", 3);
            shard.NodeAddEmpty(2, "some user");
            shard.NodeAddEmpty(3, "some thing");
            shard.NodeAddEmpty(3, "some thing else");

            THEN("request the count of the node types") {
                uint64_t user_count = shard.NodeTypesGetCount("User");
                uint64_t thing_count = shard.NodeTypesGetCount("Thing");
                REQUIRE(user_count == 1);
                REQUIRE(thing_count == 2);
            }
        }

        WHEN("we try to insert a type that already exists ") {
            shard.NodeTypeInsert("Node", 1);

            THEN("it should ignore it") {
                uint64_t node_count = shard.NodeTypesGetCount("Node");
                REQUIRE(node_count == 3);

                REQUIRE(node1id == shard.NodeGetID("Node", "one"));
                REQUIRE(node2id == shard.NodeGetID("Node", "two"));
                REQUIRE(node3id == shard.NodeGetID("Node", "three"));
            }
        }

        WHEN("find count of nodes") {
            shard.NodeTypeInsert("Person", 2);
            shard.NodePropertyTypeAdd(2, "name", 4);
            shard.NodePropertyTypeAdd(2, "age", 2);
            shard.NodePropertyTypeAdd(2, "weight", 3);
            shard.NodePropertyTypeAdd(2, "active", 1);
            shard.NodePropertyTypeAdd(2, "vector", 6);
            shard.NodeAdd(2, "one", R"({ "name":"max", "age":99, "weight":230.5, "active":true, "vector":[1,2,3,4] })");
            shard.NodeAdd(2, "two", R"({ "name":"max", "age":99, "weight":230.5, "active":true, "vector":[1,2,3,4] })");
            shard.NodeAdd(2, "three", R"({ "name":"alex", "age":55, "weight":199, "active":false, "vector":[1,2] })");
            shard.NodeAdd(2, "four", R"({ "name":"alex", "age":55, "weight":199, "active":false, "vector":[3,4] })");

            THEN("find the count of the nodes") {
                REQUIRE(shard.FindNodeCount("Person", "age", ragedb::Operation::EQ, 55) == 2);
                REQUIRE(shard.FindNodeCount("Person", "age", ragedb::Operation::GT, 55) == 2);
                REQUIRE(shard.FindNodeCount("Person", "age", ragedb::Operation::GTE, 55) == 4);
                REQUIRE(shard.FindNodeCount("Person", "age", ragedb::Operation::LT, 55) == 0);
                REQUIRE(shard.FindNodeCount("Person", "age", ragedb::Operation::LTE, 55) == 2);
                REQUIRE(shard.FindNodeCount("Person", "age", ragedb::Operation::NEQ, 3) == 4);
                REQUIRE(shard.FindNodeCount("Person", "age", ragedb::Operation::IS_NULL, 0) == 0);
                REQUIRE(shard.FindNodeCount("Person", "age", ragedb::Operation::NOT_IS_NULL, 0) == 4);
                REQUIRE(shard.FindNodeCount("Person", "name", ragedb::Operation::STARTS_WITH, "a") == 2);
                REQUIRE(shard.FindNodeCount("Person", "name", ragedb::Operation::NOT_STARTS_WITH, "a") == 2);
                REQUIRE(shard.FindNodeCount("Person", "name", ragedb::Operation::ENDS_WITH, "x") == 4);
                REQUIRE(shard.FindNodeCount("Person", "name", ragedb::Operation::NOT_ENDS_WITH, "x") == 0);
                REQUIRE(shard.FindNodeCount("Person", "name", ragedb::Operation::CONTAINS, "a") == 4);
                REQUIRE(shard.FindNodeCount("Person", "name", ragedb::Operation::NOT_CONTAINS, "l") == 2);

                 REQUIRE(shard.FindNodeIds("Person", "age", ragedb::Operation::EQ, 55).size() == 2);
                REQUIRE(shard.FindNodeIds("Person", "age", ragedb::Operation::GT, 55).size() == 2);
                REQUIRE(shard.FindNodeIds("Person", "age", ragedb::Operation::GTE, 55).size() == 4);
                REQUIRE(shard.FindNodeIds("Person", "age", ragedb::Operation::LT, 55).size() == 0);
                REQUIRE(shard.FindNodeIds("Person", "age", ragedb::Operation::LTE, 55).size() == 2);
                REQUIRE(shard.FindNodeIds("Person", "age", ragedb::Operation::NEQ, 3).size() == 4);
                REQUIRE(shard.FindNodeIds("Person", "age", ragedb::Operation::IS_NULL, 0).size() == 0);
                REQUIRE(shard.FindNodeIds("Person", "age", ragedb::Operation::NOT_IS_NULL, 0).size() == 4);
                REQUIRE(shard.FindNodeIds("Person", "name", ragedb::Operation::STARTS_WITH, "a").size() == 2);
                REQUIRE(shard.FindNodeIds("Person", "name", ragedb::Operation::NOT_STARTS_WITH, "a").size() == 2);
                REQUIRE(shard.FindNodeIds("Person", "name", ragedb::Operation::ENDS_WITH, "x").size() == 4);
                REQUIRE(shard.FindNodeIds("Person", "name", ragedb::Operation::NOT_ENDS_WITH, "x").size() == 0);
                REQUIRE(shard.FindNodeIds("Person", "name", ragedb::Operation::CONTAINS, "a").size() == 4);
                REQUIRE(shard.FindNodeIds("Person", "name", ragedb::Operation::NOT_CONTAINS, "l").size() == 2);

                REQUIRE(shard.FindNodes("Person", "age", ragedb::Operation::EQ, 55).size() == 2);
                REQUIRE(shard.FindNodes("Person", "age", ragedb::Operation::GT, 55).size() == 2);
                REQUIRE(shard.FindNodes("Person", "age", ragedb::Operation::GTE, 55).size() == 4);
                REQUIRE(shard.FindNodes("Person", "age", ragedb::Operation::LT, 55).size() == 0);
                REQUIRE(shard.FindNodes("Person", "age", ragedb::Operation::LTE, 55).size() == 2);
                REQUIRE(shard.FindNodes("Person", "age", ragedb::Operation::NEQ, 3).size() == 4);
                REQUIRE(shard.FindNodes("Person", "age", ragedb::Operation::IS_NULL, 0).size() == 0);
                REQUIRE(shard.FindNodes("Person", "age", ragedb::Operation::NOT_IS_NULL, 0).size() == 4);
                REQUIRE(shard.FindNodes("Person", "name", ragedb::Operation::STARTS_WITH, "a").size() == 2);
                REQUIRE(shard.FindNodes("Person", "name", ragedb::Operation::NOT_STARTS_WITH, "a").size() == 2);
                REQUIRE(shard.FindNodes("Person", "name", ragedb::Operation::ENDS_WITH, "x").size() == 4);
                REQUIRE(shard.FindNodes("Person", "name", ragedb::Operation::NOT_ENDS_WITH, "x").size() == 0);
                REQUIRE(shard.FindNodes("Person", "name", ragedb::Operation::CONTAINS, "a").size() == 4);
                REQUIRE(shard.FindNodes("Person", "name", ragedb::Operation::NOT_CONTAINS, "l").size() == 2);
            }
        }

        WHEN("add and remove property types") {
          shard.NodeTypeInsert("Person", 2);
          shard.NodePropertyTypeAdd(2, "name", 4);
          shard.NodePropertyTypeAdd(2, "age", 2);
          shard.NodePropertyTypeAdd(2, "weight", 3);
          shard.NodePropertyTypeAdd(2, "active", 1);
          shard.NodePropertyTypeAdd(2, "vector", 6);

          THEN("add and remove property types") {
            std::string property_type = shard.NodePropertyTypeGet("Person", "name");
            REQUIRE(property_type == "string");
            property_type = shard.NodePropertyTypeGet("Person", "age");
            REQUIRE(property_type == "integer");
            property_type = shard.NodePropertyTypeGet("Person", "weight");
            REQUIRE(property_type == "double");
            property_type = shard.NodePropertyTypeGet("Person", "active");
            REQUIRE(property_type == "boolean");

            shard.NodePropertyTypeDelete(2, "name");
            property_type = shard.NodePropertyTypeGet("Person", "name");
            REQUIRE(property_type == "");
          }
        }
    }

}