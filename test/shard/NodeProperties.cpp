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

SCENARIO( "Shard can handle Node Properties", "[node,properties]" ) {

    GIVEN("A shard with an empty node and an existing node with properties") {
        ragedb::Shard shard(4);
        shard.NodeTypeInsert("Node", 1);
        shard.NodeTypeInsert("User", 2);
        shard.NodePropertyTypeAdd(1, "name", 4);
        shard.NodePropertyTypeAdd(1, "age", 2);
        shard.NodePropertyTypeAdd(1, "weight", 3);
        shard.NodePropertyTypeAdd(1, "bald", 1);
        shard.NodePropertyTypeAdd(1, "active", 1);
        shard.NodePropertyTypeAdd(2, "name", 4);
        shard.NodePropertyTypeAdd(2, "age", 2);
        shard.NodePropertyTypeAdd(2, "weight", 3);
        shard.NodePropertyTypeAdd(2, "bald", 1);
        uint64_t empty = shard.NodeAddEmpty(1, "empty");
        uint64_t existing = shard.NodeAdd(1, "existing", R"({ "name":"max", "age":99, "weight":230.5, "bald":true, "nested":{ "inside":"yes" }, "vector":[1,2,3,4] })");

        REQUIRE( empty == 8 );
        REQUIRE( existing == 524296 );

        WHEN("a property is requested by label/key") {
            THEN("the shard gets it") {
                auto value = shard.NodeGetProperty("Node", "existing", "name");
                REQUIRE("max" == get<std::string>(value));
            }
        }

        WHEN("a string property is requested by label/key") {
            THEN("the shard gets it") {
                auto value = shard.NodeGetProperty("Node", "existing", "name");
                REQUIRE("max" == get<std::string>(value));
            }
        }

        WHEN("an integer property is requested by label/key") {
            THEN("the shard gets it") {
                auto value = shard.NodeGetProperty("Node", "existing", "age");
                REQUIRE(99 == get<int64_t>(value));
            }
        }

        WHEN("a double property is requested by label/key") {
            THEN("the shard gets it") {
                auto value = shard.NodeGetProperty("Node", "existing", "weight");
                REQUIRE(230.5 == get<double>(value));
            }
        }

        WHEN("a boolean property is requested by label/key") {
            THEN("the shard gets it") {
                auto value = shard.NodeGetProperty("Node", "existing", "bald");
                bool bald = get<bool>(value);
                REQUIRE(bald);
            }
        }

//        WHEN("an object property is requested by label/key") {
//            THEN("the shard gets it") {
//                auto value = shard.NodePropertyGetObject("Node", "existing", "nested");
//                REQUIRE(!value.empty());
//                REQUIRE(value.at("inside").has_value());
//                REQUIRE("yes" == get<std::string>(value.at("inside")));
//            }
//        }

        WHEN("a property is requested by id") {
            THEN("the shard gets it") {
                auto value = shard.NodeGetProperty(existing, "name");
                REQUIRE(value.index() == 4);
                REQUIRE("max" == get<std::string>(value));
            }
        }

        WHEN("a string property is requested by id") {
            THEN("the shard gets it") {
                auto value = shard.NodeGetProperty(existing, "name");
                REQUIRE("max" == get<std::string>(value));
            }
        }

        WHEN("an integer property is requested by id") {
            THEN("the shard gets it") {
                auto value = shard.NodeGetProperty(existing, "age");
                REQUIRE(99 == get<int64_t>(value));
            }
        }

        WHEN("a double property is requested by id") {
            THEN("the shard gets it") {
                auto value = shard.NodeGetProperty(existing, "weight");
                REQUIRE(230.5 == get<double>(value));
            }
        }

        WHEN("a boolean property is requested by id") {
            THEN("the shard gets it") {
                auto value = shard.NodeGetProperty("Node", "existing", "bald");
                REQUIRE(get<bool>(value));
            }
        }

        WHEN("a string property is set by label/key to a previously empty node") {
            THEN("the shard sets it") {
                bool set = shard.NodeSetPropertyFromJson("Node", "empty", "name", "\"alex\"");
                REQUIRE(set);
                auto value = shard.NodeGetProperty("Node", "empty", "name");
                REQUIRE("alex" == get<std::string>(value));
            }
        }

        WHEN("a string property is set by label/key") {
            THEN("the shard sets it") {
                bool set = shard.NodeSetPropertyFromJson("Node", "existing", "name", "\"alex\"");
                REQUIRE(set);
                auto value = shard.NodeGetProperty("Node", "existing", "name");
                REQUIRE("alex" == get<std::string>(value));
            }
        }

        WHEN("a string literal property is set by label/key") {
            THEN("the shard sets it") {
                bool set = shard.NodeSetPropertyFromJson("Node", "existing", "name", "\"alex\"");
                REQUIRE(set);
                auto value = shard.NodeGetProperty("Node", "existing", "name");
                REQUIRE("alex" == get<std::string>(value));
            }
        }

        WHEN("an integer property is set by label/key") {
            THEN("the shard sets it") {
                bool set = shard.NodeSetPropertyFromJson("Node", "existing", "age", "55");
                REQUIRE(set);
                auto value = shard.NodeGetProperty("Node", "existing", "age");
                REQUIRE(55 == get<int64_t>(value));
            }
        }

        WHEN("a double property is set by label/key") {
            THEN("the shard sets it") {
                bool set = shard.NodeSetPropertyFromJson("Node", "existing", "weight", "190.0");
                REQUIRE(set);
                auto value = shard.NodeGetProperty("Node", "existing", "weight");
                REQUIRE(190.0 == get<double>(value));
            }
        }

        WHEN("a boolean property is set by label/key") {
            THEN("the shard sets it") {
                bool set = shard.NodeSetPropertyFromJson("Node", "existing", "active", "true");
                REQUIRE(set);
                auto value = shard.NodeGetProperty("Node", "existing", "active");
                REQUIRE(get<bool>(value));
            }
        }

//        WHEN("an object property is set by label/key") {
//            THEN("the shard sets it") {
//                std::map<std::string, std::any> property {{"first_property", std::string("one")}, {"second_property", uint64_t(9)}};
//                bool set = shard.NodeSetProperty("Node", "existing", "properties", property);
//                REQUIRE(set);
//                auto value = shard.NodePropertyGetObject("Node", "existing", "properties");
//                REQUIRE(value.at("first_property").has_value());
//                REQUIRE(value.at("second_property").has_value());
//                REQUIRE(get<std::string>(value.at("first_property")) == "one");
//                REQUIRE(get<std::int64_t>(value.at("second_property")) == 9);
//            }
//        }

//        WHEN("an object property from JSON is set by label/key") {
//            THEN("the shard sets it") {
//                std::string property(R"({"first_property": "one", "second_property":9 })");
//                bool set = shard.NodeSetPropertyFromJson("Node", "existing", "properties", property);
//                REQUIRE(set);
//                auto value = shard.NodePropertyGetObject("Node", "existing", "properties");
//                REQUIRE(value.at("first_property").has_value());
//                REQUIRE(value.at("second_property").has_value());
//                REQUIRE(get<std::string>(value.at("first_property")) == "one");
//                REQUIRE(get<std::int64_t>(value.at("second_property")) == 9);
//            }
//        }

        WHEN("a string property is set by id") {
            THEN("the shard sets it") {
                bool set = shard.NodeSetPropertyFromJson(existing, "name", "\"alex\"");
                REQUIRE(set);
                auto value = shard.NodeGetProperty(existing, "name");
                REQUIRE("alex" == get<std::string>(value));
            }
        }

        WHEN("a string literal property is set by id") {
            THEN("the shard sets it") {
                bool set = shard.NodeSetPropertyFromJson(existing, "name", "\"alex\"");
                REQUIRE(set);
                auto value = shard.NodeGetProperty(existing, "name");
                REQUIRE("alex" == get<std::string>(value));
            }
        }

        WHEN("an integer property is set by id") {
            THEN("the shard sets it") {
                bool set = shard.NodeSetPropertyFromJson(existing, "age", "55");
                REQUIRE(set);
                auto value = shard.NodeGetProperty(existing, "age");
                REQUIRE(55 == get<int64_t>(value));
            }
        }

        WHEN("a double property is set by id") {
            THEN("the shard sets it") {
                bool set = shard.NodeSetPropertyFromJson(existing, "weight", "190.0");
                REQUIRE(set);
                auto value = shard.NodeGetProperty(existing, "weight");
                REQUIRE(190.0 == get<double>(value));
            }
        }

        WHEN("a boolean property is set by id") {
            THEN("the shard sets it") {
                bool set = shard.NodeSetPropertyFromJson(existing, "active", "true");
                REQUIRE(set);
                auto value = shard.NodeGetProperty(existing, "active");
                REQUIRE(get<bool>(value));
            }
        }

//        WHEN("an object property is set by id") {
//            THEN("the shard sets it") {
//                std::map<std::string, std::any> property {{"first_property", std::string("one")}, {"second_property", uint64_t(9)}};
//                bool set = shard.NodeSetProperty(existing, "properties", property);
//                REQUIRE(set);
//                auto value = shard.NodePropertyGetObject(existing, "properties");
//                REQUIRE(value.at("first_property").has_value());
//                REQUIRE(value.at("second_property").has_value());
//                REQUIRE(get<std::string>(value.at("first_property")) == "one");
//                REQUIRE(get<std::int64_t>(value.at("second_property")) == 9);
//            }
//        }

//        WHEN("an object property from JSON is set by id") {
//            THEN("the shard sets it") {
//                std::string property(R"({"first_property": "one", "second_property":9 })");
//                bool set = shard.NodeSetPropertyFromJson(existing, "properties", property);
//                REQUIRE(set);
//                auto value = shard.NodePropertyGetObject(existing, "properties");
//                REQUIRE(value.at("first_property").has_value());
//                REQUIRE(value.at("second_property").has_value());
//                REQUIRE(get<std::string>(value.at("first_property")) == "one");
//                REQUIRE(get<std::int64_t>(value.at("second_property")) == 9);
//            }
//        }

        WHEN("a string property is set by label/key to an invalid label") {
            THEN("the shard does not set it") {
                bool set = shard.NodeSetPropertyFromJson("NotThere", "existing", "name", "\"alex\"");
                REQUIRE(set == false);
                auto value = shard.NodeGetProperty("NotThere", "existing", "name");
                REQUIRE(value.index() == 0);
            }
        }

        WHEN("an integer property is set by label/key to an invalid label") {
            THEN("the shard does not set it") {
                bool set = shard.NodeSetPropertyFromJson("NotThere", "existing", "age", "55");
                REQUIRE(set == false);
                auto value = shard.NodeGetProperty("NotThere", "existing", "age");
                REQUIRE(value.index() == 0);
            }
        }

        WHEN("a double property is set by label/key to an invalid label") {
            THEN("the shard does not set it") {
                bool set = shard.NodeSetPropertyFromJson("NotThere", "existing", "weight", "190.0");
                REQUIRE(set == false);
                auto value = shard.NodeGetProperty("NotThere", "existing", "weight");
                REQUIRE(value.index() == 0);
            }
        }

        WHEN("a boolean property is set by label/key to an invalid label") {
            THEN("the shard does not set it") {
                bool set = shard.NodeSetPropertyFromJson("NotThere", "existing", "active", "true");
                REQUIRE(set == false);
                auto value = shard.NodeGetProperty("NotThere", "existing", "active");
                REQUIRE(value.index() == 0);
            }
        }

        WHEN("a string property is set by label/key to an invalid key") {
            THEN("the shard does not set it") {
                bool set = shard.NodeSetPropertyFromJson("Node", "not_existing", "name", "\"alex\"");
                REQUIRE(set == false);
                auto value = shard.NodeGetProperty("Node", "not_existing", "name");
                REQUIRE(value.index() == 0);
            }
        }

        WHEN("an integer property is set by label/key to an invalid key") {
            THEN("the shard does not set it") {
                bool set = shard.NodeSetPropertyFromJson("Node", "not_existing", "age", "55");
                REQUIRE(set == false);
                auto value = shard.NodeGetProperty("Node", "not_existing", "age");
                REQUIRE(value.index() == 0);
            }
        }

        WHEN("a double property is set by label/key to an invalid key") {
            THEN("the shard does not set it") {
                bool set = shard.NodeSetPropertyFromJson("Node", "not_existing", "weight", "190.0");
                REQUIRE(set == false);
                auto value = shard.NodeGetProperty("Node", "not_existing", "weight");
                REQUIRE(value.index() == 0);
            }
        }

        WHEN("a boolean property is set by label/key to an invalid key") {
            THEN("the shard does not set it") {
                bool set = shard.NodeSetPropertyFromJson("Node", "not_existing", "active", "true");
                REQUIRE(set == false);
                auto value = shard.NodeGetProperty("Node", "not_existing", "active");
                REQUIRE(value.index() == 0);
            }
        }

        WHEN("a string property is set by id to an invalid id") {
            THEN("the shard does not set it") {
                bool set = shard.NodeSetPropertyFromJson(100 + existing, "name", "alex");
                REQUIRE(set == false);
                auto value = shard.NodeGetProperty(100 + existing, "name");
                REQUIRE(value.index() == 0);
            }
        }

        WHEN("a integer property is set by id to an invalid id") {
            THEN("the shard does not set it") {
                bool set = shard.NodeSetPropertyFromJson(100 + existing, "age", "55");
                REQUIRE(set == false);
                auto value = shard.NodeGetProperty(100 + existing, "age");
                REQUIRE(value.index() == 0);
            }
        }

        WHEN("a double property is set by id to an invalid id") {
            THEN("the shard does not set it") {
                bool set = shard.NodeSetPropertyFromJson(100 + existing, "weight", "190.0");
                REQUIRE(set == false);
                auto value = shard.NodeGetProperty(100 + existing, "weight");
                REQUIRE(value.index() == 0);
            }
        }

        WHEN("a boolean property is set by id to an invalid id") {
            THEN("the shard does not set it") {
                bool set = shard.NodeSetPropertyFromJson(100 + existing, "active", "true");
                REQUIRE(set == false);
                auto value = shard.NodeGetProperty(100 + existing, "active");
                REQUIRE(value.index() == 0);
            }
        }

        WHEN("a string property is set by id to a new property that is not set") {
            THEN("the shard does not set it") {
                bool set = shard.NodeSetPropertyFromJson(existing, "not_there_string", "\"alex\"");
                REQUIRE(set == false);
                auto value = shard.NodeGetProperty(existing, "not_there_string");
                REQUIRE(value.index() == 0);
            }
        }

        WHEN("a integer property is set by id to a new property that is not set") {
            THEN("the shard does not set it") {
                bool set = shard.NodeSetPropertyFromJson(existing, "not_there_int", "55");
                REQUIRE(set == false);
                auto value = shard.NodeGetProperty(existing, "not_there_int");
                REQUIRE(value.index() == 0);
            }
        }

        WHEN("a double property is set by id to a new property that is not set") {
            THEN("the shard does not set it") {
                bool set = shard.NodeSetPropertyFromJson(existing, "not_there_double", "190.0");
                REQUIRE(set == false);
                auto value = shard.NodeGetProperty(existing, "not_there_double");
                REQUIRE(value.index() == 0);
            }
        }

        WHEN("a boolean property is set by id to a new property that is not there") {
            THEN("the shard does not set it") {
                bool set = shard.NodeSetPropertyFromJson(existing, "not_there_boolean", "true");
                REQUIRE(set == false);
                auto value = shard.NodeGetProperty(existing, "not_there_boolean");
                REQUIRE(value.index() == 0);
            }
        }

        WHEN("a string property is deleted by label/key") {
            THEN("the shard deletes it") {
                bool set = shard.NodeDeleteProperty("Node", "existing", "name");
                REQUIRE(set);
                auto value = shard.NodeGetProperty("Node", "existing", "name");
                REQUIRE(value.index() == 0);
            }
        }

        WHEN("an integer property is deleted by label/key") {
            THEN("the shard deletes it") {
                bool set = shard.NodeDeleteProperty("Node", "existing", "age");
                REQUIRE(set);
                auto value = shard.NodeGetProperty("Node", "existing", "age");
                REQUIRE(value.index() == 0);
            }
        }

        WHEN("a double property is deleted by label/key") {
            THEN("the shard deletes it") {
                bool set = shard.NodeDeleteProperty("Node", "existing", "weight");
                REQUIRE(set);
                auto value = shard.NodeGetProperty("Node", "existing", "weight");
                REQUIRE(value.index() == 0);
            }
        }

        WHEN("a string property is deleted by id") {
            THEN("the shard deletes it") {
                bool set = shard.NodeDeleteProperty(existing, "name");
                REQUIRE(set);
                auto value = shard.NodeGetProperty(existing, "name");
                REQUIRE(value.index() == 0);
            }
        }

        WHEN("an integer property is deleted by id") {
            THEN("the shard deletes it") {
                bool set = shard.NodeDeleteProperty(existing, "age");
                REQUIRE(set);
                auto value = shard.NodeGetProperty(existing, "age");
                REQUIRE(value.index() == 0);
            }
        }

        WHEN("a double property is deleted by id") {
            THEN("the shard deletes it") {
                bool set = shard.NodeDeleteProperty(existing, "weight");
                REQUIRE(set);
                auto value = shard.NodeGetProperty(existing, "weight");
                REQUIRE(value.index() == 0);
            }
        }

        WHEN("a non-existing property is deleted by label/key") {
            THEN("the shard deletes it") {
                bool set = shard.NodeDeleteProperty("Node", "existing", "not_there");
                REQUIRE(set == false);
                auto value = shard.NodeGetProperty("Node", "existing", "not_there");
                REQUIRE(value.index() == 0);
            }
        }

        WHEN("a non-existing property is deleted by id") {
            THEN("the shard ignores it") {
                bool set = shard.NodeDeleteProperty(existing, "not_there");
                REQUIRE(set == false);
                auto value = shard.NodeGetProperty(existing, "not_there");
                REQUIRE(value.index() == 0);
            }
        }

        WHEN("a double property is deleted by label/key to an invalid label") {
            THEN("the shard does not delete it") {
                bool set = shard.NodeDeleteProperty("NotThere", "existing", "weight");
                REQUIRE(set == false);
                auto value = shard.NodeGetProperty("NotThere", "existing", "weight");
                REQUIRE(value.index() == 0);
            }
        }

        WHEN("a string property is deleted by label/key to an invalid key") {
            THEN("the shard does not delete it") {
                bool set = shard.NodeDeleteProperty("Node", "not_existing", "name");
                REQUIRE(set == false);
                auto value = shard.NodeGetProperty("Node", "not_existing", "name");
                REQUIRE(value.index() == 0);
            }
        }

        WHEN("all properties are deleted by label/key") {
            THEN("the shard deletes them") {
                bool set = shard.NodeDeleteProperties("Node", "existing");
                REQUIRE(set);
                auto value = shard.NodeGetProperty("Node", "existing", "age");
                REQUIRE(value.index() == 0);
            }
        }

        WHEN("all properties are deleted by id") {
            THEN("the shard deletes them") {
                bool set = shard.NodeDeleteProperties(existing);
                REQUIRE(set);
                auto value = shard.NodeGetProperty(existing, "age");
                REQUIRE(value.index() == 0);
            }
        }

        WHEN("all properties are deleted by label/key to an invalid label") {
            THEN("the shard deletes them") {
                bool set = shard.NodeDeleteProperties("NotThere", "existing");
                REQUIRE(set == false);
                auto value = shard.NodeGetProperty("NotThere", "existing", "age");
                REQUIRE(value.index() == 0);
            }
        }

        WHEN("all properties are deleted by label/key to an invalid key") {
            THEN("the shard ignores them") {
                bool set = shard.NodeDeleteProperties("Node", "not_existing");
                REQUIRE(set == false);
                auto value = shard.NodeGetProperty("Node", "not_existing", "age");
                REQUIRE(value.index() == 0);
            }
        }

//        WHEN("all properties are set by label/key") {
//            THEN("the shard sets them") {
//                std::map<std::string, std::any> properties;
//
//                properties.insert({"eyes", std::string("brown")});
//                properties.insert({"height", 5.11});
//                bool set = shard.NodePropertiesSet("Node", "existing", properties);
//                REQUIRE(set);
//                auto eyes = shard.NodeGetProperty("Node", "existing", "eyes");
//                REQUIRE("brown" == get<std::string>(eyes));
//                auto height = shard.NodePropertyGetDouble("Node", "existing", "height");
//                REQUIRE(5.11 == height);
//            }
//        }

//        WHEN("all properties are set by id") {
//            THEN("the shard sets them") {
//                std::map<std::string, std::any> properties;
//                properties.insert({"eyes", std::string("brown")});
//                properties.insert({"height", 5.11});
//
//                bool set = shard.NodePropertiesSet(existing, properties);
//                REQUIRE(set);
//                auto eyes = shard.NodeGetProperty("Node", "existing", "eyes");
//                REQUIRE("brown" == get<std::string>(eyes));
//                auto height = shard.NodePropertyGetDouble("Node", "existing", "height");
//                REQUIRE(5.11 == height);
//
//            }
//        }
    }
}
