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

SCENARIO( "Link can be created", "[link]" ) {
    GIVEN("Nothing") {
        ragedb::Link link(1,2);

        WHEN("a node id is requested") {
            THEN("we get right id back") {
                REQUIRE(link.node_id == 1);
                REQUIRE(link.rel_id == 2);
            }
        }
        WHEN("we print a link") {
            std::stringstream out;
            out << link;
            THEN("we get the correct output") {
                REQUIRE(out.str() == "{ \"node_id\": 1, \"rel_id\": 2 }");
            }
        }
    }
}