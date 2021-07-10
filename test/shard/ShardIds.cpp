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

SCENARIO( "Shard can calculate shard ids", "[ids]" ) {
    GIVEN("An empty shard") {
        ragedb::Shard shard(4);

        WHEN("we calculate shard ids on 4 cores") {

            THEN("calculate shard id 0 for an invalid number") {
                REQUIRE(shard.CalculateShardId(99) == 0);
            }
            THEN("calculate shard id 0 for 1024") {
                REQUIRE(shard.CalculateShardId(1024) == 0);
            }
            THEN("calculate shard id 1 for 1025") {
                REQUIRE(shard.CalculateShardId(1025) == 1);
            }
            THEN("calculate shard id 1 for 65537") {
                REQUIRE(shard.CalculateShardId(65537) == 1);
            }
            THEN("calculate shard id 2 for 65538") {
                REQUIRE(shard.CalculateShardId(65538) == 2);
            }
            THEN("calculate shard id 3 for 65539") {
                REQUIRE(shard.CalculateShardId(65539) == 3);
            }

            THEN("calculate shard ids for type and key") {
                REQUIRE(shard.CalculateShardId("User", "maxdemarzi") == 0);
                REQUIRE(shard.CalculateShardId("User", "helene") == 1);
                REQUIRE(shard.CalculateShardId("User", "alejandro") == 2);
                REQUIRE(shard.CalculateShardId("User", "tyler") == 1);
                REQUIRE(shard.CalculateShardId("User", "ronnie") == 1);
                REQUIRE(shard.CalculateShardId("User", "penny") == 0);
                REQUIRE(shard.CalculateShardId("User", "someone") == 2);
                REQUIRE(shard.CalculateShardId("User", "else") == 1);
                REQUIRE(shard.CalculateShardId("User", "maxdemarzi1") == 3);
            }
        }
    }
}