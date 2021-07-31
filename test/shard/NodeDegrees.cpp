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

SCENARIO( "Shard can handle Node Degrees", "[node]" ) {

    GIVEN( "A shard with an empty node and an existing node with properties" ) {
        ragedb::Shard shard(4);
        shard.NodeTypeInsert("Node", 1);
        shard.NodeTypeInsert("User", 2);
        uint64_t empty = shard.NodeAddEmpty(1,  "empty");
        uint64_t existing = shard.NodeAdd(1,  "existing", R"({ "name":"max" })");

        uint64_t three = shard.NodeAddEmpty(1,  "three");
        uint64_t four = shard.NodeAddEmpty(1,  "four");
        uint64_t five = shard.NodeAddEmpty(1,  "five");
        uint64_t six = shard.NodeAddEmpty(1,  "six");

        REQUIRE( empty == 1024 );
        REQUIRE( existing == 67109888 );
        REQUIRE( three == 134218752 );
        REQUIRE( four == 201327616 );
        REQUIRE( five == 268436480 );
        REQUIRE( six == 335545344 );

        shard.RelationshipTypeInsert("FRIENDS", 1);
        shard.RelationshipTypeInsert("ENEMIES", 2);

        WHEN( "and relationships are added" ) {
            shard.RelationshipAddEmptySameShard(1, four, five);
            shard.RelationshipAddEmptySameShard(2, five, four);

            THEN( "the shard should get the correct degree" ) {
                uint64_t degree = shard.NodeGetDegree("Node","four");
                REQUIRE(2 == degree);
            }
        }

        WHEN( "and relationships are added" ) {
            shard.RelationshipAddEmptySameShard(1, four, five);
            shard.RelationshipAddEmptySameShard(2, five, four);

            THEN( "the shard should get the correct incoming degree" ) {
                uint64_t degree = shard.NodeGetDegree("Node","four", ragedb::IN);
                REQUIRE(1 == degree);
            }
        }

        WHEN( "and relationships are added" ) {
            shard.RelationshipAddEmptySameShard(1, four, five);
            shard.RelationshipAddEmptySameShard(2, five, four);

            THEN( "the shard should get the correct outgoing degree" ) {
                uint64_t degree = shard.NodeGetDegree("Node","four", ragedb::OUT);
                REQUIRE(1 == degree);
            }
        }

        WHEN( "and relationships are added" ) {
            shard.RelationshipAddEmptySameShard(1, four, five);
            shard.RelationshipAddEmptySameShard(2, five, four);

            THEN( "the shard should get the correct BOTH degree" ) {
                uint64_t degree = shard.NodeGetDegree("Node","four", ragedb::BOTH);
                REQUIRE(2 == degree);
            }
        }

        WHEN( "and relationships are added" ) {
            shard.RelationshipAddEmptySameShard(1, four, five);
            shard.RelationshipAddEmptySameShard(2, five, four);

            THEN( "the shard should get the correct IN degree of TYPE" ) {
                uint64_t degree = shard.NodeGetDegree("Node","four", ragedb::IN, "ENEMIES");
                REQUIRE(1 == degree);
            }
        }

        WHEN( "and relationships are added" ) {
            shard.RelationshipAddEmptySameShard(1, four, five);
            shard.RelationshipAddEmptySameShard(2, five, four);

            THEN( "the shard should get the correct OUT degree of TYPE" ) {
                uint64_t degree = shard.NodeGetDegree("Node","four", ragedb::OUT, "ENEMIES");
                REQUIRE(0 == degree);
            }
        }

        WHEN( "and relationships are added" ) {
            shard.RelationshipAddEmptySameShard(1, four, five);
            shard.RelationshipAddEmptySameShard(2, five, four);

            THEN( "the shard should get the correct BOTH degree of TYPE" ) {
                uint64_t degree = shard.NodeGetDegree("Node","four", ragedb::BOTH, "ENEMIES");
                REQUIRE(1 == degree);
            }
        }

        WHEN( "and relationships are added" ) {
            shard.RelationshipAddEmptySameShard(1, four, five);
            shard.RelationshipAddEmptySameShard(2, five, four);

            THEN( "the shard should get the correct IN degree of Multiple TYPE" ) {
                uint64_t degree = shard.NodeGetDegree("Node","four", ragedb::IN, std::vector<std::string>{"FRIENDS", "ENEMIES"});
                REQUIRE(1 == degree);
            }
        }

        WHEN( "and relationships are added" ) {
            shard.RelationshipAddEmptySameShard(1, four, five);
            shard.RelationshipAddEmptySameShard(2, five, four);

            THEN( "the shard should get the correct OUT degree of Multiple TYPE" ) {
                uint64_t degree = shard.NodeGetDegree("Node","four", ragedb::OUT, std::vector<std::string>{"FRIENDS", "ENEMIES"});
                REQUIRE(1 == degree);
            }
        }

        WHEN( "and relationships are added" ) {
            shard.RelationshipAddEmptySameShard(1, four, five);
            shard.RelationshipAddEmptySameShard(2, five, four);

            THEN( "the shard should get the correct BOTH degree of Multiple TYPE" ) {
                uint64_t degree = shard.NodeGetDegree("Node","four", ragedb::BOTH, std::vector<std::string>{"FRIENDS", "ENEMIES"});
                REQUIRE(2 == degree);
            }
        }

        WHEN( "an relationships are added" ) {
            shard.RelationshipAddEmptySameShard(1, four, five);
            shard.RelationshipAddEmptySameShard(2, five, four);

            THEN( "the shard should get the correct degree" ) {
                uint64_t degree = shard.NodeGetDegree(four);
                REQUIRE(2 == degree);
            }
        }

        WHEN( "and relationships are added" ) {
            shard.RelationshipAddEmptySameShard(1, four, five);
            shard.RelationshipAddEmptySameShard(2, five, four);

            THEN( "the shard should get the correct incoming degree" ) {
                uint64_t degree = shard.NodeGetDegree(four, ragedb::IN);
                REQUIRE(1 == degree);
            }
        }

        WHEN( "and relationships are added" ) {
            shard.RelationshipAddEmptySameShard(1, four, five);
            shard.RelationshipAddEmptySameShard(2, five, four);

            THEN( "the shard should get the correct outgoing degree" ) {
                uint64_t degree = shard.NodeGetDegree(four, ragedb::OUT);
                REQUIRE(1 == degree);
            }
        }

        WHEN( "and relationships are added" ) {
            shard.RelationshipAddEmptySameShard(1, four, five);
            shard.RelationshipAddEmptySameShard(2, five, four);

            THEN( "the shard should get the correct BOTH degree" ) {
                uint64_t degree = shard.NodeGetDegree(four, ragedb::BOTH);
                REQUIRE(2 == degree);
            }
        }

        WHEN( "and relationships are added" ) {
            shard.RelationshipAddEmptySameShard(1, four, five);
            shard.RelationshipAddEmptySameShard(2, five, four);

            THEN( "the shard should get the correct IN degree of TYPE" ) {
                uint64_t degree = shard.NodeGetDegree(four, ragedb::IN, "ENEMIES");
                REQUIRE(1 == degree);
            }
        }

        WHEN( "and relationships are added" ) {
            shard.RelationshipAddEmptySameShard(1, four, five);
            shard.RelationshipAddEmptySameShard(2, five, four);

            THEN( "the shard should get the correct OUT degree of TYPE" ) {
                uint64_t degree = shard.NodeGetDegree(four, ragedb::OUT, "ENEMIES");
                REQUIRE(0 == degree);
            }
        }

        WHEN( "and relationships are added" ) {
            shard.RelationshipAddEmptySameShard(1, four, five);
            shard.RelationshipAddEmptySameShard(2, five, four);

            THEN( "the shard should get the correct BOTH degree of TYPE" ) {
                uint64_t degree = shard.NodeGetDegree(four, ragedb::BOTH, "ENEMIES");
                REQUIRE(1 == degree);
            }
        }

        WHEN( "and relationships are added" ) {
            shard.RelationshipAddEmptySameShard(1, four, five);
            shard.RelationshipAddEmptySameShard(2, five, four);

            THEN( "the shard should get the correct IN degree of Multiple TYPE" ) {
                uint64_t degree = shard.NodeGetDegree(four, ragedb::IN, std::vector<std::string>{"FRIENDS", "ENEMIES"});
                REQUIRE(1 == degree);
            }
        }

        WHEN( "and relationships are added" ) {
            shard.RelationshipAddEmptySameShard(1, four, five);
            shard.RelationshipAddEmptySameShard(2, five, four);

            THEN( "the shard should get the correct OUT degree of Multiple TYPE" ) {
                uint64_t degree = shard.NodeGetDegree(four, ragedb::OUT, std::vector<std::string>{"FRIENDS", "ENEMIES"});
                REQUIRE(1 == degree);
            }
        }

        WHEN( "and relationships are added" ) {
            shard.RelationshipAddEmptySameShard(1, four, five);
            shard.RelationshipAddEmptySameShard(2, five, four);

            THEN( "the shard should get the correct BOTH degree of Multiple TYPE" ) {
                uint64_t degree = shard.NodeGetDegree(four, ragedb::BOTH, std::vector<std::string>{"FRIENDS", "ENEMIES"});
                REQUIRE(2 == degree);
            }
        }
    }
}