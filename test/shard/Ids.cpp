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

SCENARIO( "Shard can handle internal and external id conversions", "[ids]" ) {
    GIVEN("An empty shard") {
        ragedb::Shard shard(4);

        WHEN("we convert external ids to internal ones") {
            THEN("we convert 524296 to 1") {
                REQUIRE(shard.externalToInternal(524296) == 1);
            }

            THEN("we convert 524296 to 1") {
                REQUIRE(shard.externalToInternal(524297) == 1);
            }

            THEN("we convert 524296 to 1") {
                REQUIRE(shard.externalToInternal(524298) == 1);
            }

            THEN("we convert 524296 to 1") {
                REQUIRE(shard.externalToInternal(524299) == 1);
            }

            THEN("we convert 1048584 to 2") {
                REQUIRE(shard.externalToInternal(1048584) == 2);
            }
        }

        WHEN("we convert external ids to type ids") {
            THEN("we convert 524296 to 1") {
                REQUIRE(shard.externalToTypeId(524296) == 1);
            }

            THEN("we convert 524297 to 1") {
                REQUIRE(shard.externalToTypeId(524297) == 1);
            }

            THEN("we convert 524298 to 1") {
                REQUIRE(shard.externalToTypeId(524298) == 1);
            }

            THEN("we convert 524299 to 1") {
                REQUIRE(shard.externalToTypeId(524299) == 1);
            }

            THEN("we convert 1572880 to 2") {
                REQUIRE(shard.externalToTypeId(1572880) == 2);
            }
        }

        WHEN("we convert internal ids to external ones for shard_id:0") {
            THEN("we convert 1, 1 to 524296") {
                REQUIRE(shard.internalToExternal(1, 1) == 524296);
            }

            THEN("we convert 1, 2 to 1048584") {
                REQUIRE(shard.internalToExternal(1, 2) == 1048584);
            }

            THEN("we convert 2, 3 to 1572880") {
                REQUIRE(shard.internalToExternal(2, 3) == 1572880);
            }

            THEN("we convert 2, 4 to 2097168") {
                REQUIRE(shard.internalToExternal(2, 4) == 2097168);
            }

            THEN("we convert 3, 5 to 2621464") {
                REQUIRE(shard.internalToExternal(3, 5) == 2621464);
            }
        }
    }
}