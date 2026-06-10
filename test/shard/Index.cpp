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

SCENARIO( "Shard can handle Property Indexes", "[index]" ) {
    GIVEN("A shard with node types and property types") {
        ragedb::Shard shard(1);
        shard.NodeTypeInsert("Person", 1);
        shard.NodePropertyTypeAdd(1, "name", 4); // string
        shard.NodePropertyTypeAdd(1, "age", 2);  // integer
        shard.NodePropertyTypeAdd(1, "score", 3); // double
        shard.NodePropertyTypeAdd(1, "active", 1); // boolean

        WHEN("We create property indexes") {
            bool created_name = shard.NodeIndexCreate(1, "name");
            bool created_age = shard.NodeIndexCreate(1, "age");
            bool created_score = shard.NodeIndexCreate(1, "score");
            bool created_active = shard.NodeIndexCreate(1, "active");
            REQUIRE(created_name);
            REQUIRE(created_age);
            REQUIRE(created_score);
            REQUIRE(created_active);

            // Re-creation should return false
            bool recreate_name = shard.NodeIndexCreate(1, "name");
            REQUIRE_FALSE(recreate_name);

            AND_WHEN("We add nodes") {
                uint64_t node1 = shard.NodeAdd(1, "one", R"({ "name":"max", "age":30, "score":95.5, "active":true })");
                uint64_t node2 = shard.NodeAdd(1, "two", R"({ "name":"alex", "age":25, "score":85.0, "active":false })");
                uint64_t node3 = shard.NodeAdd(1, "three", R"({ "name":"max", "age":35, "score":75.2, "active":true })");

                THEN("We lookup values using index lookup directly") {
                    // Verify Lua index listing methods
                    auto node_indexes_lua = shard.NodeIndexesGetViaLua();
                    auto node_indexes_map = node_indexes_lua.value();
                    REQUIRE(node_indexes_map.size() == 1);
                    REQUIRE(node_indexes_map["Person"].size() == 4);

                    auto name_indexes_lua = shard.NodeIndexesGetByTypeViaLua("Person");
                    auto name_indexes_vec = name_indexes_lua.value();
                    REQUIRE(name_indexes_vec.size() == 4);
                    REQUIRE(std::find(name_indexes_vec.begin(), name_indexes_vec.end(), "name") != name_indexes_vec.end());

                    auto empty_indexes_lua = shard.NodeIndexesGetByTypeViaLua("NonExistent");
                    REQUIRE(empty_indexes_lua.value().empty());

                    // String Lookups
                    auto res_max = shard.NodeIndexLookup(1, "name", ragedb::Operation::EQ, std::string("max"));
                    REQUIRE(res_max.size() == 2);
                    REQUIRE(std::find(res_max.begin(), res_max.end(), node1) != res_max.end());
                    REQUIRE(std::find(res_max.begin(), res_max.end(), node3) != res_max.end());

                    auto res_alex = shard.NodeIndexLookup(1, "name", ragedb::Operation::EQ, std::string("alex"));
                    REQUIRE(res_alex.size() == 1);
                    REQUIRE(res_alex[0] == node2);

                    auto res_name_neq = shard.NodeIndexLookup(1, "name", ragedb::Operation::NEQ, std::string("max"));
                    REQUIRE(res_name_neq.size() == 1);
                    REQUIRE(res_name_neq[0] == node2);

                    auto res_name_lt = shard.NodeIndexLookup(1, "name", ragedb::Operation::LT, std::string("max"));
                    REQUIRE(res_name_lt.size() == 1);
                    REQUIRE(res_name_lt[0] == node2);

                    auto res_name_lte = shard.NodeIndexLookup(1, "name", ragedb::Operation::LTE, std::string("max"));
                    REQUIRE(res_name_lte.size() == 3);

                    auto res_name_gt = shard.NodeIndexLookup(1, "name", ragedb::Operation::GT, std::string("alex"));
                    REQUIRE(res_name_gt.size() == 2);

                    auto res_name_gte = shard.NodeIndexLookup(1, "name", ragedb::Operation::GTE, std::string("max"));
                    REQUIRE(res_name_gte.size() == 2);

                    // Integer Lookups
                    auto res_age_eq = shard.NodeIndexLookup(1, "age", ragedb::Operation::EQ, int64_t(30));
                    REQUIRE(res_age_eq.size() == 1);
                    REQUIRE(res_age_eq[0] == node1);

                    auto res_age_neq = shard.NodeIndexLookup(1, "age", ragedb::Operation::NEQ, int64_t(30));
                    REQUIRE(res_age_neq.size() == 2);

                    auto res_age_gt = shard.NodeIndexLookup(1, "age", ragedb::Operation::GT, int64_t(25));
                    REQUIRE(res_age_gt.size() == 2);

                    auto res_age_gte = shard.NodeIndexLookup(1, "age", ragedb::Operation::GTE, int64_t(25));
                    REQUIRE(res_age_gte.size() == 3);

                    auto res_age_lt = shard.NodeIndexLookup(1, "age", ragedb::Operation::LT, int64_t(35));
                    REQUIRE(res_age_lt.size() == 2);

                    auto res_age_lte = shard.NodeIndexLookup(1, "age", ragedb::Operation::LTE, int64_t(35));
                    REQUIRE(res_age_lte.size() == 3);

                    // Double Lookups
                    auto res_score_eq = shard.NodeIndexLookup(1, "score", ragedb::Operation::EQ, 95.5);
                    REQUIRE(res_score_eq.size() == 1);
                    REQUIRE(res_score_eq[0] == node1);

                    auto res_score_neq = shard.NodeIndexLookup(1, "score", ragedb::Operation::NEQ, 95.5);
                    REQUIRE(res_score_neq.size() == 2);

                    auto res_score_gt = shard.NodeIndexLookup(1, "score", ragedb::Operation::GT, 80.0);
                    REQUIRE(res_score_gt.size() == 2);

                    auto res_score_lt = shard.NodeIndexLookup(1, "score", ragedb::Operation::LT, 80.0);
                    REQUIRE(res_score_lt.size() == 1);
                    REQUIRE(res_score_lt[0] == node3);

                    // Boolean Lookups
                    auto res_active_true = shard.NodeIndexLookup(1, "active", ragedb::Operation::EQ, true);
                    REQUIRE(res_active_true.size() == 2);
                    REQUIRE(std::find(res_active_true.begin(), res_active_true.end(), node1) != res_active_true.end());
                    REQUIRE(std::find(res_active_true.begin(), res_active_true.end(), node3) != res_active_true.end());

                    auto res_active_false = shard.NodeIndexLookup(1, "active", ragedb::Operation::EQ, false);
                    REQUIRE(res_active_false.size() == 1);
                    REQUIRE(res_active_false[0] == node2);

                    auto res_active_neq = shard.NodeIndexLookup(1, "active", ragedb::Operation::NEQ, true);
                    REQUIRE(res_active_neq.size() == 1);
                    REQUIRE(res_active_neq[0] == node2);

                    auto res_active_lt = shard.NodeIndexLookup(1, "active", ragedb::Operation::LT, true);
                    REQUIRE(res_active_lt.size() == 1);
                    REQUIRE(res_active_lt[0] == node2);

                    auto res_active_lte = shard.NodeIndexLookup(1, "active", ragedb::Operation::LTE, true);
                    REQUIRE(res_active_lte.size() == 3);

                    auto res_active_gt = shard.NodeIndexLookup(1, "active", ragedb::Operation::GT, false);
                    REQUIRE(res_active_gt.size() == 2);

                    auto res_active_gte = shard.NodeIndexLookup(1, "active", ragedb::Operation::GTE, true);
                    REQUIRE(res_active_gte.size() == 2);
                }

                THEN("FindNodeIds uses indexes and returns correctly") {
                    auto ids = shard.FindNodeIds("Person", "name", ragedb::Operation::EQ, std::string("max"), 0, 10);
                    REQUIRE(ids.size() == 2);

                    auto ids_active = shard.FindNodeIds("Person", "active", ragedb::Operation::EQ, true, 0, 10);
                    REQUIRE(ids_active.size() == 2);

                    auto ids_score = shard.FindNodeIds("Person", "score", ragedb::Operation::GT, 80.0, 0, 10);
                    REQUIRE(ids_score.size() == 2);
                }

                AND_WHEN("We update a node's properties") {
                    shard.NodeSetProperty(node1, "name", std::string("maxim"));
                    shard.NodeSetProperty(node1, "age", int64_t(31));
                    shard.NodeSetProperty(node1, "score", 99.9);
                    shard.NodeSetProperty(node1, "active", false);

                    THEN("The index is updated correctly") {
                        auto res_max = shard.NodeIndexLookup(1, "name", ragedb::Operation::EQ, std::string("max"));
                        REQUIRE(res_max.size() == 1); // Only node3 is left
                        REQUIRE(res_max[0] == node3);

                        auto res_maxim = shard.NodeIndexLookup(1, "name", ragedb::Operation::EQ, std::string("maxim"));
                        REQUIRE(res_maxim.size() == 1);
                        REQUIRE(res_maxim[0] == node1);

                        auto res_age_30 = shard.NodeIndexLookup(1, "age", ragedb::Operation::EQ, int64_t(30));
                        REQUIRE(res_age_30.empty());

                        auto res_age_31 = shard.NodeIndexLookup(1, "age", ragedb::Operation::EQ, int64_t(31));
                        REQUIRE(res_age_31.size() == 1);
                        REQUIRE(res_age_31[0] == node1);

                        auto res_score_95 = shard.NodeIndexLookup(1, "score", ragedb::Operation::EQ, 95.5);
                        REQUIRE(res_score_95.empty());

                        auto res_score_99 = shard.NodeIndexLookup(1, "score", ragedb::Operation::EQ, 99.9);
                        REQUIRE(res_score_99.size() == 1);
                        REQUIRE(res_score_99[0] == node1);

                        auto res_active_true = shard.NodeIndexLookup(1, "active", ragedb::Operation::EQ, true);
                        REQUIRE(res_active_true.size() == 1); // node3

                        auto res_active_false = shard.NodeIndexLookup(1, "active", ragedb::Operation::EQ, false);
                        REQUIRE(res_active_false.size() == 2); // node2 and node1
                    }
                }

                AND_WHEN("We delete properties") {
                    shard.NodeDeleteProperty(node1, "name");
                    shard.NodeDeleteProperty(node1, "age");
                    shard.NodeDeleteProperty(node1, "score");
                    shard.NodeDeleteProperty(node1, "active");

                    THEN("The index entries are removed") {
                        auto res_max = shard.NodeIndexLookup(1, "name", ragedb::Operation::EQ, std::string("max"));
                        REQUIRE(res_max.size() == 1); // Only node3

                        auto res_maxim = shard.NodeIndexLookup(1, "name", ragedb::Operation::EQ, std::string("maxim"));
                        REQUIRE(res_maxim.empty());

                        auto res_age = shard.NodeIndexLookup(1, "age", ragedb::Operation::EQ, int64_t(30));
                        REQUIRE(res_age.empty());

                        auto res_score = shard.NodeIndexLookup(1, "score", ragedb::Operation::EQ, 95.5);
                        REQUIRE(res_score.empty());

                        auto res_active = shard.NodeIndexLookup(1, "active", ragedb::Operation::EQ, true);
                        REQUIRE(res_active.size() == 1); // node3
                    }
                }

                AND_WHEN("We delete the node") {
                    shard.NodeRemove(node1);

                    THEN("All index entries for the node are removed") {
                        auto res_max = shard.NodeIndexLookup(1, "name", ragedb::Operation::EQ, std::string("max"));
                        REQUIRE(res_max.size() == 1); // Only node3

                        auto res_age_30 = shard.NodeIndexLookup(1, "age", ragedb::Operation::EQ, int64_t(30));
                        REQUIRE(res_age_30.empty());

                        auto res_score = shard.NodeIndexLookup(1, "score", ragedb::Operation::EQ, 95.5);
                        REQUIRE(res_score.empty());

                        auto res_active = shard.NodeIndexLookup(1, "active", ragedb::Operation::EQ, true);
                        REQUIRE(res_active.size() == 1); // node3
                    }
                }

                AND_WHEN("We delete the indexes") {
                    bool del_name = shard.NodeIndexDelete(1, "name");
                    bool del_age = shard.NodeIndexDelete(1, "age");
                    bool del_score = shard.NodeIndexDelete(1, "score");
                    bool del_active = shard.NodeIndexDelete(1, "active");
                    REQUIRE(del_name);
                    REQUIRE(del_age);
                    REQUIRE(del_score);
                    REQUIRE(del_active);

                    THEN("Lookups are empty / not indexed") {
                        auto res_max = shard.NodeIndexLookup(1, "name", ragedb::Operation::EQ, std::string("max"));
                        REQUIRE(res_max.empty());

                        auto res_age = shard.NodeIndexLookup(1, "age", ragedb::Operation::EQ, int64_t(35));
                        REQUIRE(res_age.empty());
                    }
                }
            }
        }
    }
}

SCENARIO( "Shard can handle Relationship Property Indexes", "[index]" ) {
    GIVEN("A shard with node and relationship types and properties") {
        ragedb::Shard shard(1);
        shard.NodeTypeInsert("Person", 1);
        shard.RelationshipTypeInsert("Knows", 1);
        shard.RelationshipPropertyTypeAdd(1, "comment", 4); // string
        shard.RelationshipPropertyTypeAdd(1, "since", 2); // integer
        shard.RelationshipPropertyTypeAdd(1, "weight", 3); // double
        shard.RelationshipPropertyTypeAdd(1, "active", 1); // boolean

        WHEN("We create property indexes") {
            bool created_comment = shard.RelationshipIndexCreate(1, "comment");
            bool created_since = shard.RelationshipIndexCreate(1, "since");
            bool created_weight = shard.RelationshipIndexCreate(1, "weight");
            bool created_active = shard.RelationshipIndexCreate(1, "active");
            REQUIRE(created_comment);
            REQUIRE(created_since);
            REQUIRE(created_weight);
            REQUIRE(created_active);

            // Re-creation should return false
            bool recreate_since = shard.RelationshipIndexCreate(1, "since");
            REQUIRE_FALSE(recreate_since);

            AND_WHEN("We add nodes and relationships") {
                shard.NodeAddEmpty(1, "one");
                shard.NodeAddEmpty(1, "two");

                uint64_t rel1 = shard.RelationshipAddSameShard(1, "Person", "one", "Person", "two", R"({ "comment":"good", "since":2020, "weight":1.5, "active":true })");
                uint64_t rel2 = shard.RelationshipAddSameShard(1, "Person", "two", "Person", "one", R"({ "comment":"bad", "since":2018, "weight":2.5, "active":false })");
                uint64_t rel3 = shard.RelationshipAddSameShard(1, "Person", "one", "Person", "two", R"({ "comment":"good", "since":2020, "weight":3.5, "active":true })");

                THEN("We lookup values using index lookup directly") {
                    // Verify Lua index listing methods
                    auto rel_indexes_lua = shard.RelationshipIndexesGetViaLua();
                    auto rel_indexes_map = rel_indexes_lua.value();
                    REQUIRE(rel_indexes_map.size() == 1);
                    REQUIRE(rel_indexes_map["Knows"].size() == 4);

                    auto since_indexes_lua = shard.RelationshipIndexesGetByTypeViaLua("Knows");
                    auto since_indexes_vec = since_indexes_lua.value();
                    REQUIRE(since_indexes_vec.size() == 4);
                    REQUIRE(std::find(since_indexes_vec.begin(), since_indexes_vec.end(), "since") != since_indexes_vec.end());

                    auto empty_rel_indexes_lua = shard.RelationshipIndexesGetByTypeViaLua("NonExistent");
                    REQUIRE(empty_rel_indexes_lua.value().empty());

                    // String Lookups
                    auto res_good = shard.RelationshipIndexLookup(1, "comment", ragedb::Operation::EQ, std::string("good"));
                    REQUIRE(res_good.size() == 2);
                    REQUIRE(std::find(res_good.begin(), res_good.end(), rel1) != res_good.end());
                    REQUIRE(std::find(res_good.begin(), res_good.end(), rel3) != res_good.end());

                    // Integer Lookups
                    auto res_2020 = shard.RelationshipIndexLookup(1, "since", ragedb::Operation::EQ, int64_t(2020));
                    REQUIRE(res_2020.size() == 2);
                    REQUIRE(std::find(res_2020.begin(), res_2020.end(), rel1) != res_2020.end());
                    REQUIRE(std::find(res_2020.begin(), res_2020.end(), rel3) != res_2020.end());

                    auto res_2018 = shard.RelationshipIndexLookup(1, "since", ragedb::Operation::EQ, int64_t(2018));
                    REQUIRE(res_2018.size() == 1);
                    REQUIRE(res_2018[0] == rel2);

                    // Double Lookups
                    auto res_weight_gt = shard.RelationshipIndexLookup(1, "weight", ragedb::Operation::GT, 2.0);
                    REQUIRE(res_weight_gt.size() == 2);

                    auto res_weight_lt = shard.RelationshipIndexLookup(1, "weight", ragedb::Operation::LT, 3.0);
                    REQUIRE(res_weight_lt.size() == 2);

                    // Boolean Lookups
                    auto res_active_true = shard.RelationshipIndexLookup(1, "active", ragedb::Operation::EQ, true);
                    REQUIRE(res_active_true.size() == 2);

                    auto res_active_false = shard.RelationshipIndexLookup(1, "active", ragedb::Operation::EQ, false);
                    REQUIRE(res_active_false.size() == 1);
                    REQUIRE(res_active_false[0] == rel2);
                }

                THEN("FindRelationshipIds uses indexes and returns correctly") {
                    auto ids = shard.FindRelationshipIds("Knows", "since", ragedb::Operation::EQ, int64_t(2020), 0, 10);
                    REQUIRE(ids.size() == 2);

                    auto ids_active = shard.FindRelationshipIds("Knows", "active", ragedb::Operation::EQ, true, 0, 10);
                    REQUIRE(ids_active.size() == 2);
                }

                AND_WHEN("We update a relationship's properties") {
                    shard.RelationshipSetProperty(rel1, "comment", std::string("excellent"));
                    shard.RelationshipSetProperty(rel1, "since", int64_t(2021));
                    shard.RelationshipSetProperty(rel1, "weight", 4.5);
                    shard.RelationshipSetProperty(rel1, "active", false);

                    THEN("The index is updated correctly") {
                        auto res_good = shard.RelationshipIndexLookup(1, "comment", ragedb::Operation::EQ, std::string("good"));
                        REQUIRE(res_good.size() == 1); // Only rel3 is left

                        auto res_exc = shard.RelationshipIndexLookup(1, "comment", ragedb::Operation::EQ, std::string("excellent"));
                        REQUIRE(res_exc.size() == 1);
                        REQUIRE(res_exc[0] == rel1);

                        auto res_2020 = shard.RelationshipIndexLookup(1, "since", ragedb::Operation::EQ, int64_t(2020));
                        REQUIRE(res_2020.size() == 1); // Only rel3 is left
                        REQUIRE(res_2020[0] == rel3);

                        auto res_2021 = shard.RelationshipIndexLookup(1, "since", ragedb::Operation::EQ, int64_t(2021));
                        REQUIRE(res_2021.size() == 1);
                        REQUIRE(res_2021[0] == rel1);

                        auto res_active_true = shard.RelationshipIndexLookup(1, "active", ragedb::Operation::EQ, true);
                        REQUIRE(res_active_true.size() == 1); // Only rel3
                    }
                }

                AND_WHEN("We delete a property") {
                    shard.RelationshipDeleteProperty(rel1, "comment");
                    shard.RelationshipDeleteProperty(rel1, "since");
                    shard.RelationshipDeleteProperty(rel1, "weight");
                    shard.RelationshipDeleteProperty(rel1, "active");

                    THEN("The index entries are removed") {
                        auto res_good = shard.RelationshipIndexLookup(1, "comment", ragedb::Operation::EQ, std::string("good"));
                        REQUIRE(res_good.size() == 1); // Only rel3

                        auto res_exc = shard.RelationshipIndexLookup(1, "comment", ragedb::Operation::EQ, std::string("excellent"));
                        REQUIRE(res_exc.empty());

                        auto res_2020 = shard.RelationshipIndexLookup(1, "since", ragedb::Operation::EQ, int64_t(2020));
                        REQUIRE(res_2020.size() == 1); // Only rel3

                        auto res_2021 = shard.RelationshipIndexLookup(1, "since", ragedb::Operation::EQ, int64_t(2021));
                        REQUIRE(res_2021.empty());
                    }
                }

                AND_WHEN("We delete the relationship") {
                    std::pair<uint16_t, uint64_t> rel_type_incoming_node_id = shard.RelationshipRemoveGetIncoming(rel1);
                    shard.RelationshipRemoveIncoming(rel_type_incoming_node_id.first, rel1, rel_type_incoming_node_id.second);

                    THEN("All index entries for the relationship are removed") {
                        auto res_good = shard.RelationshipIndexLookup(1, "comment", ragedb::Operation::EQ, std::string("good"));
                        REQUIRE(res_good.size() == 1); // Only rel3

                        auto res_2020 = shard.RelationshipIndexLookup(1, "since", ragedb::Operation::EQ, int64_t(2020));
                        REQUIRE(res_2020.size() == 1); // Only rel3

                        auto res_2021 = shard.RelationshipIndexLookup(1, "since", ragedb::Operation::EQ, int64_t(2021));
                        REQUIRE(res_2021.empty());
                    }
                }

                AND_WHEN("We delete the index") {
                    bool deleted = shard.RelationshipIndexDelete(1, "since");
                    REQUIRE(deleted);

                    THEN("Lookups are empty / not indexed") {
                        auto res_2020 = shard.RelationshipIndexLookup(1, "since", ragedb::Operation::EQ, int64_t(2020));
                        REQUIRE(res_2020.empty());
                    }
                }
            }
        }
    }
}

