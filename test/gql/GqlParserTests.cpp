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
#include "../../src/gql/GqlParser.h"

using namespace ragedb;
using namespace ragedb::gql;

TEST_CASE("GQL Parser builds AST", "[gql_parser]") {
    std::string query = "MATCH (p:Person) RETURN p.name";
    auto q = GqlParser::parse(query);

    REQUIRE(q.matches.size() == 1);
    REQUIRE(q.matches[0].pattern.nodes.size() == 1);
    REQUIRE(q.matches[0].pattern.nodes[0].variable == "p");
    REQUIRE(q.matches[0].pattern.nodes[0].label == "Person");
    REQUIRE(q.returns.size() == 1);
}

TEST_CASE("GQL Parser supports IS keyword for label specification", "[gql_parser]") {
    std::string query = "MATCH (p IS Person)-[e IS ACTED_IN]->(m IS Movie) RETURN p.name";
    auto q = GqlParser::parse(query);

    REQUIRE(q.matches.size() == 1);
    REQUIRE(q.matches[0].pattern.nodes.size() == 2);
    REQUIRE(q.matches[0].pattern.nodes[0].variable == "p");
    REQUIRE(q.matches[0].pattern.nodes[0].label == "Person");
    REQUIRE(q.matches[0].pattern.edges.size() == 1);
    REQUIRE(q.matches[0].pattern.edges[0].variable == "e");
    REQUIRE(q.matches[0].pattern.edges[0].label == "ACTED_IN");
    REQUIRE(q.matches[0].pattern.nodes[1].variable == "m");
    REQUIRE(q.matches[0].pattern.nodes[1].label == "Movie");
    REQUIRE(q.returns.size() == 1);
}

TEST_CASE("GQL Parser parses write statements", "[gql_parser]") {
    SECTION("INSERT statement") {
        std::string query = "INSERT (p:Person {name: 'Charlie', age: 25, key: 'charlie'})";
        auto q = GqlParser::parse(query);

        REQUIRE(q.writes.size() == 1);
        REQUIRE(q.writes[0].type == WriteOp::Type::INSERT);
        REQUIRE(q.writes[0].insert_pattern.nodes.size() == 1);
        REQUIRE(q.writes[0].insert_pattern.nodes[0].variable == "p");
        REQUIRE(q.writes[0].insert_pattern.nodes[0].label == "Person");
        REQUIRE(q.writes[0].insert_pattern.nodes[0].properties.count("name") == 1);
    }

    SECTION("SET statement") {
        std::string query = "MATCH (p:Person) SET p.age = 30";
        auto q = GqlParser::parse(query);

        REQUIRE(q.writes.size() == 1);
        REQUIRE(q.writes[0].type == WriteOp::Type::SET);
        REQUIRE(q.writes[0].set_var == "p");
        REQUIRE(q.writes[0].set_prop == "age");
        REQUIRE(q.writes[0].set_expr != nullptr);
    }

    SECTION("REMOVE statement") {
        std::string query = "MATCH (p:Person) REMOVE p.age";
        auto q = GqlParser::parse(query);

        REQUIRE(q.writes.size() == 1);
        REQUIRE(q.writes[0].type == WriteOp::Type::REMOVE);
        REQUIRE(q.writes[0].remove_var == "p");
        REQUIRE(q.writes[0].remove_prop == "age");
    }

    SECTION("DELETE statement") {
        std::string query = "MATCH (p:Person) DELETE p";
        auto q = GqlParser::parse(query);

        REQUIRE(q.writes.size() == 1);
        REQUIRE(q.writes[0].type == WriteOp::Type::DELETE_OP);
        REQUIRE(q.writes[0].delete_var == "p");
        REQUIRE(q.writes[0].detach == false);
    }

    SECTION("DETACH DELETE statement") {
        std::string query = "MATCH (p:Person) DETACH DELETE p";
        auto q = GqlParser::parse(query);

        REQUIRE(q.writes.size() == 1);
        REQUIRE(q.writes[0].type == WriteOp::Type::DELETE_OP);
        REQUIRE(q.writes[0].delete_var == "p");
        REQUIRE(q.writes[0].detach == true);
    }
}
