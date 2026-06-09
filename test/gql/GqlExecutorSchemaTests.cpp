/*
 * Copyright RageDB Contributors. All Rights Reserved.
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
#include "../../src/gql/GqlParser.h"
#include "../../src/gql/GqlOptimizer.h"
#include "../../src/gql/GqlExecutor.h"

using namespace ragedb;
using namespace ragedb::gql;

TEST_CASE("GQL Execution Schema DDL Tests", "[gql_executor_schema]") {
    auto graph = Graph("gql_test");
    graph.Start().get();
    graph.Clear();

    SECTION("CREATE NODE TYPE and retrieve it") {
        std::string query = "CREATE NODE TYPE User";
        std::string res = GqlExecutor::execute(graph, GqlParser::parse(query)).get();
        REQUIRE(res == "{\"status\": \"created\", \"node_type\": \"User\"}");

        auto node_types = graph.shard.local().NodeTypesGetPeered();
        REQUIRE(node_types.find("User") != node_types.end());
    }

    SECTION("CREATE NODE TYPE with properties and retrieve them") {
        std::string query = "CREATE NODE TYPE Customer (name STRING, age INTEGER, active BOOLEAN, balance DOUBLE, tags STRING_LIST)";
        std::string res = GqlExecutor::execute(graph, GqlParser::parse(query)).get();
        REQUIRE(res == "{\"status\": \"created\", \"node_type\": \"Customer\"}");

        auto props = graph.shard.local().NodeTypeGet("Customer");
        REQUIRE(props["name"] == "string");
        REQUIRE(props["age"] == "integer");
        REQUIRE(props["active"] == "boolean");
        REQUIRE(props["balance"] == "double");
        REQUIRE(props["tags"] == "string_list");
    }

    SECTION("ALTER NODE TYPE ADD property") {
        graph.shard.local().NodeTypeInsertPeered("User").get();

        std::string query = "ALTER NODE TYPE User ADD email STRING";
        std::string res = GqlExecutor::execute(graph, GqlParser::parse(query)).get();
        REQUIRE(res == "{\"status\": \"altered\", \"node_type\": \"User\", \"added\": \"email\"}");

        auto props = graph.shard.local().NodeTypeGet("User");
        REQUIRE(props["email"] == "string");
    }

    SECTION("ALTER NODE TYPE DROP property") {
        graph.shard.local().NodeTypeInsertPeered("User").get();
        graph.shard.local().NodePropertyTypeAddPeered("User", "email", "string").get();

        std::string query = "ALTER NODE TYPE User DROP email";
        std::string res = GqlExecutor::execute(graph, GqlParser::parse(query)).get();
        REQUIRE(res == "{\"status\": \"altered\", \"node_type\": \"User\", \"dropped\": \"email\"}");

        auto props = graph.shard.local().NodeTypeGet("User");
        REQUIRE(props.find("email") == props.end());
    }

    SECTION("DROP NODE TYPE") {
        graph.shard.local().NodeTypeInsertPeered("User").get();

        std::string query = "DROP NODE TYPE User";
        std::string res = GqlExecutor::execute(graph, GqlParser::parse(query)).get();
        REQUIRE(res == "{\"status\": \"deleted\", \"node_type\": \"User\"}");

        auto node_types = graph.shard.local().NodeTypesGetPeered();
        REQUIRE(node_types.find("User") == node_types.end());
    }

    SECTION("CREATE RELATIONSHIP TYPE and retrieve it") {
        std::string query = "CREATE RELATIONSHIP TYPE FRIEND_OF";
        std::string res = GqlExecutor::execute(graph, GqlParser::parse(query)).get();
        REQUIRE(res == "{\"status\": \"created\", \"relationship_type\": \"FRIEND_OF\"}");

        auto rel_types = graph.shard.local().RelationshipTypesGetPeered();
        REQUIRE(rel_types.find("FRIEND_OF") != rel_types.end());
    }

    SECTION("CREATE RELATIONSHIP TYPE with properties") {
        std::string query = "CREATE RELATIONSHIP TYPE LIKES (since STRING, rating DOUBLE)";
        std::string res = GqlExecutor::execute(graph, GqlParser::parse(query)).get();
        REQUIRE(res == "{\"status\": \"created\", \"relationship_type\": \"LIKES\"}");

        auto props = graph.shard.local().RelationshipTypeGet("LIKES");
        REQUIRE(props["since"] == "string");
        REQUIRE(props["rating"] == "double");
    }

    SECTION("ALTER RELATIONSHIP TYPE ADD property") {
        graph.shard.local().RelationshipTypeInsertPeered("FRIEND_OF").get();

        std::string query = "ALTER RELATIONSHIP TYPE FRIEND_OF ADD years INTEGER";
        std::string res = GqlExecutor::execute(graph, GqlParser::parse(query)).get();
        REQUIRE(res == "{\"status\": \"altered\", \"relationship_type\": \"FRIEND_OF\", \"added\": \"years\"}");

        auto props = graph.shard.local().RelationshipTypeGet("FRIEND_OF");
        REQUIRE(props["years"] == "integer");
    }

    SECTION("ALTER RELATIONSHIP TYPE DROP property") {
        graph.shard.local().RelationshipTypeInsertPeered("FRIEND_OF").get();
        graph.shard.local().RelationshipPropertyTypeAddPeered("FRIEND_OF", "years", "integer").get();

        std::string query = "ALTER RELATIONSHIP TYPE FRIEND_OF DROP years";
        std::string res = GqlExecutor::execute(graph, GqlParser::parse(query)).get();
        REQUIRE(res == "{\"status\": \"altered\", \"relationship_type\": \"FRIEND_OF\", \"dropped\": \"years\"}");

        auto props = graph.shard.local().RelationshipTypeGet("FRIEND_OF");
        REQUIRE(props.find("years") == props.end());
    }

    SECTION("DROP RELATIONSHIP TYPE") {
        graph.shard.local().RelationshipTypeInsertPeered("FRIEND_OF").get();

        std::string query = "DROP RELATIONSHIP TYPE FRIEND_OF";
        std::string res = GqlExecutor::execute(graph, GqlParser::parse(query)).get();
        REQUIRE(res == "{\"status\": \"deleted\", \"relationship_type\": \"FRIEND_OF\"}");

        auto rel_types = graph.shard.local().RelationshipTypesGetPeered();
        REQUIRE(rel_types.find("FRIEND_OF") == rel_types.end());
    }

    SECTION("DDL Validation Errors") {
        SECTION("Create existing node type throws exception") {
            graph.shard.local().NodeTypeInsertPeered("User").get();
            std::string query = "CREATE NODE TYPE User";
            REQUIRE_THROWS_AS(GqlExecutor::execute(graph, GqlParser::parse(query)).get(), std::runtime_error);
        }

        SECTION("Drop non-existing node type throws exception") {
            std::string query = "DROP NODE TYPE NonExistent";
            REQUIRE_THROWS_AS(GqlExecutor::execute(graph, GqlParser::parse(query)).get(), std::runtime_error);
        }

        SECTION("Alter non-existing node type throws exception") {
            std::string query = "ALTER NODE TYPE NonExistent ADD prop STRING";
            REQUIRE_THROWS_AS(GqlExecutor::execute(graph, GqlParser::parse(query)).get(), std::runtime_error);
        }

        SECTION("Alter node type add existing property throws exception") {
            graph.shard.local().NodeTypeInsertPeered("User").get();
            graph.shard.local().NodePropertyTypeAddPeered("User", "name", "string").get();
            std::string query = "ALTER NODE TYPE User ADD name STRING";
            REQUIRE_THROWS_AS(GqlExecutor::execute(graph, GqlParser::parse(query)).get(), std::runtime_error);
        }

        SECTION("Alter node type drop non-existing property throws exception") {
            graph.shard.local().NodeTypeInsertPeered("User").get();
            std::string query = "ALTER NODE TYPE User DROP nonexistent";
            REQUIRE_THROWS_AS(GqlExecutor::execute(graph, GqlParser::parse(query)).get(), std::runtime_error);
        }
    }

    graph.Stop().get();
}
