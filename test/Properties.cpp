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
#include "../src/graph/Properties.h"

SCENARIO( "Properties can be added", "[properties]" ) {
    GIVEN("Empty properties") {
        ragedb::Properties properties;
        properties.setPropertyType("valid", "boolean");
        properties.setPropertyType("number", "integer");

        WHEN("a boolean property is set") {
            THEN("we set true to item 1") {
                properties.setBooleanProperty("valid", 1, true);
                REQUIRE(properties.getBooleanProperty("valid", 1) == true);
            }
            THEN("we set true to item 5") {
                properties.setBooleanProperty("valid", 5, true);
                REQUIRE(properties.getBooleanProperty("valid", 5) == true);
            }
            THEN("we set false to item 3") {
                properties.setBooleanProperty("valid", 3, false);
                REQUIRE(properties.getBooleanProperty("valid", 3) == false);
            }
        }
        WHEN("an integer property is set") {
            THEN("we set 123 to item 1") {
                properties.setIntegerProperty("number", 1, 123);
                REQUIRE(properties.getIntegerProperty("number", 1) == 123);
            }
            THEN("we set 456 to item 5") {
                properties.setIntegerProperty("number", 5, 456);
                REQUIRE(properties.getIntegerProperty("number", 5) == 456);
            }
            THEN("we set -789 to item 3") {
                properties.setIntegerProperty("number", 3, 789);
                REQUIRE(properties.getIntegerProperty("number", 3) == 789);
            }
        }
    }
}