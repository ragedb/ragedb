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
#include <sstream>
#include <Graph.h>

SCENARIO( "Node can be created", "[node]" ) {
    GIVEN("Given Nothing") {
        ragedb::Node nothing;
        ragedb::Node empty(1, "User", "Helene");
        std::map<std::string, ragedb::property_type_t>  properties;
        properties.try_emplace("name", std::string("max"));
        properties.try_emplace("age", int64_t(42));
        std::vector<bool> valid {true, true, false, true};
        properties.try_emplace("valid", valid[0]);
        properties.try_emplace("weight", 239.5);
        properties.try_emplace("sane", valid);
        std::vector<int64_t> hairs;
        hairs.emplace_back(0);
        hairs.emplace_back(5000);
        properties.try_emplace("hairs", hairs);
        std::vector<double> shoe_sizes;
        shoe_sizes.emplace_back(10);
        shoe_sizes.emplace_back(10.5);
        properties.try_emplace("shoe_sizes", shoe_sizes);
        std::vector<std::string> nicknames;
        nicknames.emplace_back("maxie");
        nicknames.emplace_back("doogie");
        properties.emplace("nicknames", nicknames);

        sol::state lua;

        ragedb::Node with_properties(2, "User", "Max", properties);

        WHEN("things are requested from a nothing node") {
            THEN("we get the right things back") {
                REQUIRE(nothing.getId() == 0);
                REQUIRE(nothing.getType().empty());
                REQUIRE(nothing.getKey().empty());
                REQUIRE(nothing.getProperties().empty());
                REQUIRE(nothing.getPropertiesLua(lua.lua_state()).empty());
            }
        }
        WHEN("things are requested from an empty node") {
            THEN("we get the right things back") {
                REQUIRE(empty.getId() == 1);
                REQUIRE(empty.getType() == "User");
                REQUIRE(empty.getKey() == "Helene");
                REQUIRE(empty.getProperties().empty());
                REQUIRE(empty.getPropertiesLua(lua.lua_state()).empty());
            }
        }
        WHEN("things are requested from a node with properties") {
            THEN("we get the right things back") {
                REQUIRE(with_properties.getId() == 2);
                REQUIRE(with_properties.getType() == "User");
                REQUIRE(with_properties.getKey() == "Max");
                REQUIRE(!with_properties.getProperties().empty());
                REQUIRE(!with_properties.getPropertiesLua(lua.lua_state()).empty());
                REQUIRE(get<std::string>(with_properties.getProperty("name")) == "max");
                REQUIRE(get<int64_t>(with_properties.getProperty("age")) == 42);
                REQUIRE(get<double>(with_properties.getProperty("weight")) == 239.5);
                REQUIRE(get<bool>(with_properties.getProperty("valid")));
                REQUIRE(get<std::vector<std::string>>(with_properties.getProperty("nicknames")) == nicknames);
                REQUIRE(with_properties.getProperty("not_there").index() == 0);
            }
        }
        WHEN("we print a nothing node") {
            std::stringstream out;
            out << nothing;
            THEN("we get the correct output") {
                REQUIRE(out.str() == "{ \"id\": 0, \"type\": \"\", \"key\": \"\", \"properties\": {  } }");
            }
        }
        WHEN("we print an empty node") {
            std::stringstream out;
            out << empty;
            THEN("we get the correct output") {
                REQUIRE(out.str() == "{ \"id\": 1, \"type\": \"User\", \"key\": \"Helene\", \"properties\": {  } }");
            }
        }
        WHEN("we print a node with properties") {
            std::stringstream out;
            out << with_properties;
            THEN("we get the correct output") {
                REQUIRE(out.str() == "{ \"id\": 2, \"type\": \"User\", \"key\": \"Max\", \"properties\": { \"age\": 42, \"hairs\": [0, 5000], \"name\": \"max\", \"nicknames\": [\"maxie\", \"doogie\"], \"sane\": [true, true, false, true], \"shoe_sizes\": [10, 10.5], \"valid\": true, \"weight\": 239.5 } }");
            }
        }
    }
}