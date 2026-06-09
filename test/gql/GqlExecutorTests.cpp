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

TEST_CASE("GQL Execution integration test", "[gql_executor]") {
    auto graph = Graph("gql_test");
    graph.Start().get();
    graph.Clear();
    
    // Setup schemas
    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "age", "integer").get();

    // Create test nodes
    uint64_t id1 = graph.shard.local().NodeAddPeered("Person", "alice", "{\"name\": \"Alice\", \"age\": 30}").get();
    uint64_t id2 = graph.shard.local().NodeAddPeered("Person", "bob", "{\"name\": \"Bob\", \"age\": 35}").get();

    REQUIRE(id1 > 0);
    REQUIRE(id2 > 0);

    SECTION("Simple match return properties") {
        std::string query_str = "MATCH (p:Person) RETURN p.name, p.age";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        
        std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
        
        REQUIRE(results_json.find("Alice") != std::string::npos);
        REQUIRE(results_json.find("Bob") != std::string::npos);
        REQUIRE(results_json.find("30") != std::string::npos);
        REQUIRE(results_json.find("35") != std::string::npos);
    }

    SECTION("Filtered match return properties") {
        std::string query_str = "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name, p.age";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        
        std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
        
        REQUIRE(results_json.find("Alice") != std::string::npos);
        REQUIRE(results_json.find("Bob") == std::string::npos);
    }

    SECTION("INSERT node and relationship") {
        try {
            graph.shard.local().NodeTypeInsertPeered("Movie").get();
            graph.shard.local().NodePropertyTypeAddPeered("Movie", "title", "string").get();
            graph.shard.local().RelationshipTypeInsertPeered("ACTED_IN").get();

            std::string query_insert_node = "INSERT (m:Movie {title: 'Inception', key: 'inception'})";
            GqlExecutor::execute(graph, GqlParser::parse(query_insert_node)).get();

            std::string check_query = "MATCH (m:Movie) WHERE m.title = 'Inception' RETURN m.title";
            std::string check_res = GqlExecutor::execute(graph, GqlParser::parse(check_query)).get();
            REQUIRE(check_res.find("Inception") != std::string::npos);

            std::string query_insert_rel = "MATCH (p:Person) MATCH (m:Movie) WHERE p.name = 'Alice' AND m.title = 'Inception' INSERT (p)-[r:ACTED_IN]->(m)";
            GqlExecutor::execute(graph, GqlParser::parse(query_insert_rel)).get();

            std::string check_rel = "MATCH (p:Person)-[:ACTED_IN]->(m:Movie) RETURN p.name, m.title";
            std::string check_rel_res = GqlExecutor::execute(graph, GqlParser::parse(check_rel)).get();
            REQUIRE(check_rel_res.find("Alice") != std::string::npos);
            REQUIRE(check_rel_res.find("Inception") != std::string::npos);
        } catch (const std::exception& e) {
            std::cerr << "!!! EXCEPTION CAUGHT: " << e.what() << std::endl;
            try { graph.Stop().get(); } catch (...) {}
            throw;
        }
    }

    SECTION("SET property") {
        std::string query_set = "MATCH (p:Person) WHERE p.name = 'Alice' SET p.age = 45";
        GqlExecutor::execute(graph, GqlParser::parse(query_set)).get();

        std::string check_query = "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.age";
        std::string check_res = GqlExecutor::execute(graph, GqlParser::parse(check_query)).get();
        REQUIRE(check_res.find("45") != std::string::npos);
    }

    SECTION("REMOVE property") {
        std::string query_remove = "MATCH (p:Person) WHERE p.name = 'Alice' REMOVE p.age";
        GqlExecutor::execute(graph, GqlParser::parse(query_remove)).get();

        std::string check_query = "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.age";
        std::string check_res = GqlExecutor::execute(graph, GqlParser::parse(check_query)).get();
        REQUIRE(check_res.find("null") != std::string::npos);
    }

    SECTION("DELETE node") {
        std::string query_delete = "MATCH (p:Person) WHERE p.name = 'Bob' DELETE p";
        GqlExecutor::execute(graph, GqlParser::parse(query_delete)).get();

        std::string check_query = "MATCH (p:Person) WHERE p.name = 'Bob' RETURN p.name";
        std::string check_res = GqlExecutor::execute(graph, GqlParser::parse(check_query)).get();
        REQUIRE(check_res == "[]");
    }

    graph.Stop().get();
}
