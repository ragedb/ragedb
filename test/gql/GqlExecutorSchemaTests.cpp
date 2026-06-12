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

struct GraphStopGuard {
    Graph& g;
    bool stopped = false;
    explicit GraphStopGuard(Graph& graph) : g(graph) {}
    ~GraphStopGuard() {
        if (!stopped) {
            try {
                g.Stop().get();
            } catch (...) {}
            stopped = true;
        }
    }
    void stop() {
        if (!stopped) {
            g.Stop().get();
            stopped = true;
        }
    }
};

TEST_CASE("GQL Execution Schema DDL Tests", "[gql_executor_schema]") {
    auto graph = Graph("gql_test");
    graph.Start().get();
    graph.Clear();
    GraphStopGuard guard(graph);

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

    SECTION("Property Index Management") {
        // Setup schema
        graph.shard.local().NodeTypeInsertPeered("User").get();
        graph.shard.local().NodePropertyTypeAddPeered("User", "name", "string").get();
        graph.shard.local().RelationshipTypeInsertPeered("FRIEND_OF").get();
        graph.shard.local().RelationshipPropertyTypeAddPeered("FRIEND_OF", "since", "integer").get();

        // 1. Create index
        std::string query_create_node = "CREATE INDEX User.name";
        std::string res_create_node = GqlExecutor::execute(graph, GqlParser::parse(query_create_node)).get();
        REQUIRE(res_create_node == "{\"status\": \"created\", \"index\": \"User.name\"}");

        std::string query_create_rel = "CREATE INDEX FRIEND_OF.since";
        std::string res_create_rel = GqlExecutor::execute(graph, GqlParser::parse(query_create_rel)).get();
        REQUIRE(res_create_rel == "{\"status\": \"created\", \"index\": \"FRIEND_OF.since\"}");

        // 2. Show indexes
        std::string query_show = "SHOW INDEXES";
        std::string res_show = GqlExecutor::execute(graph, GqlParser::parse(query_show)).get();
        REQUIRE(res_show.find("\"label\": \"User\"") != std::string::npos);
        REQUIRE(res_show.find("\"property\": \"name\"") != std::string::npos);
        REQUIRE(res_show.find("\"label\": \"FRIEND_OF\"") != std::string::npos);
        REQUIRE(res_show.find("\"property\": \"since\"") != std::string::npos);

        std::string query_show_filter = "SHOW INDEXES ON User";
        std::string res_show_filter = GqlExecutor::execute(graph, GqlParser::parse(query_show_filter)).get();
        REQUIRE(res_show_filter == "[{\"type\": \"Node\", \"label\": \"User\", \"property\": \"name\"}]");

        // 3. Drop index
        std::string query_drop = "DROP INDEX User.name";
        std::string res_drop = GqlExecutor::execute(graph, GqlParser::parse(query_drop)).get();
        REQUIRE(res_drop == "{\"status\": \"dropped\", \"index\": \"User.name\"}");

        std::string res_show_after = GqlExecutor::execute(graph, GqlParser::parse(query_show)).get();
        REQUIRE(res_show_after.find("\"label\": \"User\"") == std::string::npos);
        REQUIRE(res_show_after.find("\"label\": \"FRIEND_OF\"") != std::string::npos);

        // 4. Drop node type (which should automatically delete its associated index)
        std::string query_drop_type = "DROP NODE TYPE User";
        GqlExecutor::execute(graph, GqlParser::parse(query_drop_type)).get();

        std::string res_show_final = GqlExecutor::execute(graph, GqlParser::parse(query_show)).get();
        REQUIRE(res_show_final.find("\"label\": \"User\"") == std::string::npos);
    }

    SECTION("Full-Text Search Index Management") {
        // Setup schema
        graph.shard.local().NodeTypeInsertPeered("Product").get();
        graph.shard.local().NodePropertyTypeAddPeered("Product", "description", "string").get();

        // 1. Create fulltext index
        std::string query_create = "CREATE FULLTEXT INDEX Product.description";
        std::string res_create = GqlExecutor::execute(graph, GqlParser::parse(query_create)).get();
        REQUIRE(res_create == "{\"status\": \"created\", \"index\": \"Product.description\", \"kind\": \"fulltext\"}");

        // 2. Show indexes
        std::string query_show = "SHOW INDEXES ON Product";
        std::string res_show = GqlExecutor::execute(graph, GqlParser::parse(query_show)).get();
        REQUIRE(res_show == "[{\"type\": \"Node\", \"label\": \"Product\", \"property\": \"description\", \"kind\": \"fulltext\"}]");

        // 3. Drop index
        std::string query_drop = "DROP INDEX Product.description";
        std::string res_drop = GqlExecutor::execute(graph, GqlParser::parse(query_drop)).get();
        REQUIRE(res_drop == "{\"status\": \"dropped\", \"index\": \"Product.description\"}");

        std::string res_show_after = GqlExecutor::execute(graph, GqlParser::parse(query_show)).get();
        REQUIRE(res_show_after == "[]");
    }
    
    SECTION("GQL Seek Operators & Path Reordering Optimizer Tests") {
        // Setup schema
        graph.shard.local().NodeTypeInsertPeered("Person").get();
        graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
        graph.shard.local().NodePropertyTypeAddPeered("Person", "age", "integer").get();
        graph.shard.local().RelationshipTypeInsertPeered("KNOWS").get();
        graph.shard.local().RelationshipPropertyTypeAddPeered("KNOWS", "since", "integer").get();

        // Register indexes
        graph.shard.local().NodeIndexCreatePeered("Person", "name").get();
        graph.shard.local().RelationshipIndexCreatePeered("KNOWS", "since").get();

        // 1. Check EXPLAIN for NodeIndexSeek
        {
            std::string query = "EXPLAIN MATCH (p:Person {name: 'Alice'}) RETURN p";
            std::string res = GqlExecutor::execute(graph, query).get();
            REQUIRE(res.find("NodeIndexSeek") != std::string::npos);
            REQUIRE(res.find("Seek (p:Person(name)) via index") != std::string::npos);
        }

        // 2. Check EXPLAIN for RelationshipIndexSeek
        {
            std::string query = "EXPLAIN MATCH (p1)-[e:KNOWS {since: 2020}]->(p2) RETURN e";
            std::string res = GqlExecutor::execute(graph, query).get();
            REQUIRE(res.find("RelationshipIndexSeek") != std::string::npos);
            REQUIRE(res.find("Seek [e:KNOWS(since)] via index") != std::string::npos);
        }

        // 3. Check EXPLAIN for Path Reordering (Node Index Seek at end of pattern)
        {
            std::string query = "EXPLAIN MATCH (p1)-[e:KNOWS]->(p2:Person {name: 'Bob'}) RETURN p1";
            std::string res = GqlExecutor::execute(graph, query).get();
            REQUIRE(res.find("NodeIndexSeek") != std::string::npos);
            REQUIRE(res.find("Seek (p2:Person(name)) via index") != std::string::npos);
        }

        // 4. Check EXPLAIN for Path Reordering (Edge Index Seek at end of pattern)
        {
            std::string query = "EXPLAIN MATCH (p1)-[e1]->(p2)-[e2:KNOWS {since: 2020}]->(p3) RETURN p1";
            std::string res = GqlExecutor::execute(graph, query).get();
            REQUIRE(res.find("RelationshipIndexSeek") != std::string::npos);
            REQUIRE(res.find("Seek [e2:KNOWS(since)] via index") != std::string::npos);
        }

        // 5. End-to-end execution verification with real data
        GqlExecutor::execute(graph, "INSERT (a:Person {name: 'Alice', age: 30, key: 'alice'})").get();
        GqlExecutor::execute(graph, "INSERT (b:Person {name: 'Bob', age: 35, key: 'bob'})").get();
        GqlExecutor::execute(graph, "MATCH (a:Person) MATCH (b:Person) WHERE a.name = 'Alice' AND b.name = 'Bob' INSERT (a)-[:KNOWS {since: 2020}]->(b)").get();

        // Query using NodeIndexSeek
        {
            std::string query = "MATCH (p:Person {name: 'Alice'}) RETURN p.age";
            std::string res = GqlExecutor::execute(graph, query).get();
            REQUIRE(res.find("\"p.age\": 30") != std::string::npos);
        }

        // Query using RelationshipIndexSeek
        {
            std::string query = "MATCH (p1)-[e:KNOWS {since: 2020}]->(p2) RETURN p1.name, p2.name";
            std::string res = GqlExecutor::execute(graph, query).get();
            REQUIRE(res.find("\"p1.name\": \"Alice\"") != std::string::npos);
            REQUIRE(res.find("\"p2.name\": \"Bob\"") != std::string::npos);
        }

        // Query with path reordering (Bob name is indexed, start at Bob, traverse backward)
        {
            std::string query = "MATCH (p1)-[e:KNOWS]->(p2:Person {name: 'Bob'}) RETURN p1.name";
            std::string res = GqlExecutor::execute(graph, query).get();
            REQUIRE(res.find("\"p1.name\": \"Alice\"") != std::string::npos);
        }
    }

    guard.stop();
}
