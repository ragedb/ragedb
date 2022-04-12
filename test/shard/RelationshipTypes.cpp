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

            THEN("it should retain a map when asked for") {
              shard.RelationshipPropertyTypeAdd(1, "name", 4);
              shard.RelationshipPropertyTypeAdd(1, "age", 2);
              auto type_map = shard.RelationshipTypeGet("LOVES");
              REQUIRE(type_map.size() > 0);
              REQUIRE(type_map.at("name") == "string");
              REQUIRE(type_map.at("age") == "integer");
            }

        }

        WHEN("we add two relationship types") {
            shard.RelationshipTypeInsert("LOVES", 1);
            shard.RelationshipTypeInsert("HATES", 2);

            THEN("it should be two") {
                uint64_t count = shard.RelationshipTypesGetCount();
                REQUIRE(count == 2);
            }

            shard.DeleteRelationshipType("HATES");
            THEN("it should be one") {
              uint64_t count = shard.RelationshipTypesGetCount();
              REQUIRE(count == 1);
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

        WHEN("find count of relationships") {
          shard.RelationshipTypeInsert("LOVES", 1);
          shard.RelationshipTypeInsert("HATES", 2);
          shard.RelationshipPropertyTypeAdd(2, "name", 4);
          shard.RelationshipPropertyTypeAdd(2, "age", 2);
          shard.RelationshipPropertyTypeAdd(2, "weight", 3);
          shard.RelationshipPropertyTypeAdd(2, "active", 1);
          shard.RelationshipPropertyTypeAdd(2, "vector", 6);

          uint64_t id1 = shard.NodeAdd(2, "one", R"({ "name":"max", "age":99, "weight":230.5, "active":true, "vector":[1,2,3,4] })");
          uint64_t id2 = shard.NodeAdd(2, "two", R"({ "name":"max", "age":99, "weight":230.5, "active":true, "vector":[1,2,3,4] })");
          uint64_t id3 = shard.NodeAdd(2, "three", R"({ "name":"alex", "age":55, "weight":199, "active":false, "vector":[1,2] })");
          uint64_t id4 = shard.NodeAdd(2, "four", R"({ "name":"alex", "age":55, "weight":199, "active":false, "vector":[3,4] })");

          shard.RelationshipAddSameShard(2, id1, id2, R"({ "name":"max", "age":99, "weight":230.5, "active":true, "vector":[1,2,3,4] })");
          shard.RelationshipAddSameShard(2, id1, id3, R"({ "name":"max", "age":99, "weight":230.5, "active":true, "vector":[1,2,3,4] })");
          shard.RelationshipAddSameShard(2, id1, id4, R"({ "name":"alex", "age":55, "weight":199, "active":false, "vector":[1,2] })");
          shard.RelationshipAddSameShard(2, id2, id3, R"({ "name":"alex", "age":55, "weight":199, "active":false, "vector":[3,4] })");

          THEN("find the count of the relationships") {
            REQUIRE(shard.FindRelationshipCount(2, "age", ragedb::Operation::EQ, 55) == 2);
            REQUIRE(shard.FindRelationshipCount(2, "age", ragedb::Operation::GT, 55) == 2);
            REQUIRE(shard.FindRelationshipCount(2, "age", ragedb::Operation::GTE, 55) == 4);
            REQUIRE(shard.FindRelationshipCount(2, "age", ragedb::Operation::LT, 55) == 0);
            REQUIRE(shard.FindRelationshipCount(2, "age", ragedb::Operation::LTE, 55) == 2);
            REQUIRE(shard.FindRelationshipCount(2, "age", ragedb::Operation::NEQ, 3) == 4);
            REQUIRE(shard.FindRelationshipCount(2, "age", ragedb::Operation::IS_NULL, 0) == 0);
            REQUIRE(shard.FindRelationshipCount(2, "age", ragedb::Operation::NOT_IS_NULL, 0) == 4);
            REQUIRE(shard.FindRelationshipCount(2, "name", ragedb::Operation::STARTS_WITH, "a") == 2);
            REQUIRE(shard.FindRelationshipCount(2, "name", ragedb::Operation::NOT_STARTS_WITH, "a") == 2);
            REQUIRE(shard.FindRelationshipCount(2, "name", ragedb::Operation::ENDS_WITH, "x") == 4);
            REQUIRE(shard.FindRelationshipCount(2, "name", ragedb::Operation::NOT_ENDS_WITH, "x") == 0);
            REQUIRE(shard.FindRelationshipCount(2, "name", ragedb::Operation::CONTAINS, "a") == 4);
            REQUIRE(shard.FindRelationshipCount(2, "name", ragedb::Operation::NOT_CONTAINS, "l") == 2);

            REQUIRE(shard.FindRelationshipCount("HATES", "age", ragedb::Operation::EQ, 55) == 2);
            REQUIRE(shard.FindRelationshipCount("HATES", "age", ragedb::Operation::GT, 55) == 2);
            REQUIRE(shard.FindRelationshipCount("HATES", "age", ragedb::Operation::GTE, 55) == 4);
            REQUIRE(shard.FindRelationshipCount("HATES", "age", ragedb::Operation::LT, 55) == 0);
            REQUIRE(shard.FindRelationshipCount("HATES", "age", ragedb::Operation::LTE, 55) == 2);
            REQUIRE(shard.FindRelationshipCount("HATES", "age", ragedb::Operation::NEQ, 3) == 4);
            REQUIRE(shard.FindRelationshipCount("HATES", "age", ragedb::Operation::IS_NULL, 0) == 0);
            REQUIRE(shard.FindRelationshipCount("HATES", "age", ragedb::Operation::NOT_IS_NULL, 0) == 4);
            REQUIRE(shard.FindRelationshipCount("HATES", "name", ragedb::Operation::STARTS_WITH, "a") == 2);
            REQUIRE(shard.FindRelationshipCount("HATES", "name", ragedb::Operation::NOT_STARTS_WITH, "a") == 2);
            REQUIRE(shard.FindRelationshipCount("HATES", "name", ragedb::Operation::ENDS_WITH, "x") == 4);
            REQUIRE(shard.FindRelationshipCount("HATES", "name", ragedb::Operation::NOT_ENDS_WITH, "x") == 0);
            REQUIRE(shard.FindRelationshipCount("HATES", "name", ragedb::Operation::CONTAINS, "a") == 4);
            REQUIRE(shard.FindRelationshipCount("HATES", "name", ragedb::Operation::NOT_CONTAINS, "l") == 2);

            REQUIRE(shard.FindRelationshipIds(2, "age", ragedb::Operation::EQ, 55).size() == 2);
            REQUIRE(shard.FindRelationshipIds(2, "age", ragedb::Operation::GT, 55).size() == 2);
            REQUIRE(shard.FindRelationshipIds(2, "age", ragedb::Operation::GTE, 55).size() == 4);
            REQUIRE(shard.FindRelationshipIds(2, "age", ragedb::Operation::LT, 55).size() == 0);
            REQUIRE(shard.FindRelationshipIds(2, "age", ragedb::Operation::LTE, 55).size() == 2);
            REQUIRE(shard.FindRelationshipIds(2, "age", ragedb::Operation::NEQ, 3).size() == 4);
            REQUIRE(shard.FindRelationshipIds(2, "age", ragedb::Operation::IS_NULL, 0).size() == 0);
            REQUIRE(shard.FindRelationshipIds(2, "age", ragedb::Operation::NOT_IS_NULL, 0).size() == 4);
            REQUIRE(shard.FindRelationshipIds(2, "name", ragedb::Operation::STARTS_WITH, "a").size() == 2);
            REQUIRE(shard.FindRelationshipIds(2, "name", ragedb::Operation::NOT_STARTS_WITH, "a").size() == 2);
            REQUIRE(shard.FindRelationshipIds(2, "name", ragedb::Operation::ENDS_WITH, "x").size() == 4);
            REQUIRE(shard.FindRelationshipIds(2, "name", ragedb::Operation::NOT_ENDS_WITH, "x").size() == 0);
            REQUIRE(shard.FindRelationshipIds(2, "name", ragedb::Operation::CONTAINS, "a").size() == 4);
            REQUIRE(shard.FindRelationshipIds(2, "name", ragedb::Operation::NOT_CONTAINS, "l").size() == 2);

            REQUIRE(shard.FindRelationshipIds("HATES", "age", ragedb::Operation::EQ, 55).size() == 2);
            REQUIRE(shard.FindRelationshipIds("HATES", "age", ragedb::Operation::GT, 55).size() == 2);
            REQUIRE(shard.FindRelationshipIds("HATES", "age", ragedb::Operation::GTE, 55).size() == 4);
            REQUIRE(shard.FindRelationshipIds("HATES", "age", ragedb::Operation::LT, 55).size() == 0);
            REQUIRE(shard.FindRelationshipIds("HATES", "age", ragedb::Operation::LTE, 55).size() == 2);
            REQUIRE(shard.FindRelationshipIds("HATES", "age", ragedb::Operation::NEQ, 3).size() == 4);
            REQUIRE(shard.FindRelationshipIds("HATES", "age", ragedb::Operation::IS_NULL, 0).size() == 0);
            REQUIRE(shard.FindRelationshipIds("HATES", "age", ragedb::Operation::NOT_IS_NULL, 0).size() == 4);
            REQUIRE(shard.FindRelationshipIds("HATES", "name", ragedb::Operation::STARTS_WITH, "a").size() == 2);
            REQUIRE(shard.FindRelationshipIds("HATES", "name", ragedb::Operation::NOT_STARTS_WITH, "a").size() == 2);
            REQUIRE(shard.FindRelationshipIds("HATES", "name", ragedb::Operation::ENDS_WITH, "x").size() == 4);
            REQUIRE(shard.FindRelationshipIds("HATES", "name", ragedb::Operation::NOT_ENDS_WITH, "x").size() == 0);
            REQUIRE(shard.FindRelationshipIds("HATES", "name", ragedb::Operation::CONTAINS, "a").size() == 4);
            REQUIRE(shard.FindRelationshipIds("HATES", "name", ragedb::Operation::NOT_CONTAINS, "l").size() == 2);

            REQUIRE(shard.FindRelationships(2, "age", ragedb::Operation::EQ, 55).size() == 2);
            REQUIRE(shard.FindRelationships(2, "age", ragedb::Operation::GT, 55).size() == 2);
            REQUIRE(shard.FindRelationships(2, "age", ragedb::Operation::GTE, 55).size() == 4);
            REQUIRE(shard.FindRelationships(2, "age", ragedb::Operation::LT, 55).size() == 0);
            REQUIRE(shard.FindRelationships(2, "age", ragedb::Operation::LTE, 55).size() == 2);
            REQUIRE(shard.FindRelationships(2, "age", ragedb::Operation::NEQ, 3).size() == 4);
            REQUIRE(shard.FindRelationships(2, "age", ragedb::Operation::IS_NULL, 0).size() == 0);
            REQUIRE(shard.FindRelationships(2, "age", ragedb::Operation::NOT_IS_NULL, 0).size() == 4);
            REQUIRE(shard.FindRelationships(2, "name", ragedb::Operation::STARTS_WITH, "a").size() == 2);
            REQUIRE(shard.FindRelationships(2, "name", ragedb::Operation::NOT_STARTS_WITH, "a").size() == 2);
            REQUIRE(shard.FindRelationships(2, "name", ragedb::Operation::ENDS_WITH, "x").size() == 4);
            REQUIRE(shard.FindRelationships(2, "name", ragedb::Operation::NOT_ENDS_WITH, "x").size() == 0);
            REQUIRE(shard.FindRelationships(2, "name", ragedb::Operation::CONTAINS, "a").size() == 4);
            REQUIRE(shard.FindRelationships(2, "name", ragedb::Operation::NOT_CONTAINS, "l").size() == 2);

            REQUIRE(shard.FindRelationships("HATES", "age", ragedb::Operation::EQ, 55).size() == 2);
            REQUIRE(shard.FindRelationships("HATES", "age", ragedb::Operation::GT, 55).size() == 2);
            REQUIRE(shard.FindRelationships("HATES", "age", ragedb::Operation::GTE, 55).size() == 4);
            REQUIRE(shard.FindRelationships("HATES", "age", ragedb::Operation::LT, 55).size() == 0);
            REQUIRE(shard.FindRelationships("HATES", "age", ragedb::Operation::LTE, 55).size() == 2);
            REQUIRE(shard.FindRelationships("HATES", "age", ragedb::Operation::NEQ, 3).size() == 4);
            REQUIRE(shard.FindRelationships("HATES", "age", ragedb::Operation::IS_NULL, 0).size() == 0);
            REQUIRE(shard.FindRelationships("HATES", "age", ragedb::Operation::NOT_IS_NULL, 0).size() == 4);
            REQUIRE(shard.FindRelationships("HATES", "name", ragedb::Operation::STARTS_WITH, "a").size() == 2);
            REQUIRE(shard.FindRelationships("HATES", "name", ragedb::Operation::NOT_STARTS_WITH, "a").size() == 2);
            REQUIRE(shard.FindRelationships("HATES", "name", ragedb::Operation::ENDS_WITH, "x").size() == 4);
            REQUIRE(shard.FindRelationships("HATES", "name", ragedb::Operation::NOT_ENDS_WITH, "x").size() == 0);
            REQUIRE(shard.FindRelationships("HATES", "name", ragedb::Operation::CONTAINS, "a").size() == 4);
            REQUIRE(shard.FindRelationships("HATES", "name", ragedb::Operation::NOT_CONTAINS, "l").size() == 2);
          }
        }

        WHEN("add and remove property types") {
          shard.RelationshipTypeInsert("LOVES", 1);
          shard.RelationshipTypeInsert("HATES", 2);
          shard.RelationshipPropertyTypeAdd(2, "name", 4);
          shard.RelationshipPropertyTypeAdd(2, "age", 2);
          shard.RelationshipPropertyTypeAdd(2, "weight", 3);
          shard.RelationshipPropertyTypeAdd(2, "active", 1);
          shard.RelationshipPropertyTypeAdd(2, "vector", 6);

          THEN("add and remove property types") {
            std::string property_type = shard.RelationshipPropertyTypeGet("HATES", "name");
            REQUIRE(property_type == "string");
            property_type = shard.RelationshipPropertyTypeGet("HATES", "age");
            REQUIRE(property_type == "integer");
            property_type = shard.RelationshipPropertyTypeGet("HATES", "weight");
            REQUIRE(property_type == "double");
            property_type = shard.RelationshipPropertyTypeGet("HATES", "active");
            REQUIRE(property_type == "boolean");

            shard.RelationshipPropertyTypeDelete(2, "name");
            property_type = shard.RelationshipPropertyTypeGet("HATES", "name");
            REQUIRE(property_type == "");
          }
        }
    }
}