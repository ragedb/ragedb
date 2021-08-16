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

SCENARIO( "Graph can be created", "[graph]" ) {
    GIVEN("A Test Graph") {
        ragedb::Graph graph = ragedb::Graph("test");

        WHEN("the Graph is started") {
            THEN("we get it to start") {
                graph.Start().get();
                REQUIRE(graph.GetName() == "test");
                graph.Stop().get();
            }
            THEN("we get it to clear") {
                graph.Start().get();
                graph.Clear();
                REQUIRE(graph.GetName() == "test");
                graph.Stop().get();
            }
        }

    }
}