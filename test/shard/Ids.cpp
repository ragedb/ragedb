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
            THEN("we convert 67108864 to 1") {
                REQUIRE(shard.externalToInternal(67108864) == 1);
            }

            THEN("we convert 67108865 to 1") {
                REQUIRE(shard.externalToInternal(67108865) == 1);
            }

            THEN("we convert 67108866 to 1") {
                REQUIRE(shard.externalToInternal(67108866) == 1);
            }

            THEN("we convert 67108867 to 1") {
                REQUIRE(shard.externalToInternal(67108867) == 1);
            }

            THEN("we convert 134217728 to 2") {
                REQUIRE(shard.externalToInternal(134217728) == 2);
            }
        }

        WHEN("we convert external ids to type ids") {
            THEN("we convert 67109888 to 1") {
                REQUIRE(shard.externalToTypeId(67109888) == 1);
            }

            THEN("we convert 134218752 to 1") {
                REQUIRE(shard.externalToTypeId(134218752) == 1);
            }

            THEN("we convert 201328640 to 2") {
                REQUIRE(shard.externalToTypeId(201328640) == 2);
            }

            THEN("we convert 268437504 to 2") {
                REQUIRE(shard.externalToTypeId(268437504) == 2);
            }

            THEN("we convert 335547392 to 3") {
                REQUIRE(shard.externalToTypeId(335547392) == 3);
            }
        }

        WHEN("we convert internal ids to external ones for shard_id:0") {
            THEN("we convert 1, 1 to 67109888") {
                REQUIRE(shard.internalToExternal(1, 1) == 67109888);
            }

            THEN("we convert 1, 2 to 134218752") {
                REQUIRE(shard.internalToExternal(1, 2) == 134218752);
            }

            THEN("we convert 2, 3 to 201328640") {
                REQUIRE(shard.internalToExternal(2, 3) == 201328640);
            }

            THEN("we convert 2, 4 to 268437504") {
                REQUIRE(shard.internalToExternal(2, 4) == 268437504);
            }

            THEN("we convert 3, 5 to 335547392") {
                REQUIRE(shard.internalToExternal(3, 5) == 335547392);
            }
        }
    }
}