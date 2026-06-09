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

TEST_CASE("GQL Execution Aggregation and Set Tests", "[gql_executor_aggregation]") {
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

    SECTION("Aggregations: Global COUNT, SUM, AVG, MIN, MAX") {
        std::string query = "MATCH (p:Person) RETURN count(*), sum(p.age), avg(p.age), min(p.age), max(p.age)";
        std::string res = GqlExecutor::execute(graph, GqlParser::parse(query)).get();

        // Expect: [{"count(*)": 2, "sum(p.age)": 65, "avg(p.age)": 32.500000, "min(p.age)": 30, "max(p.age)": 35}]
        REQUIRE(res.find("\"count(*)\": 2") != std::string::npos);
        REQUIRE(res.find("\"sum(p.age)\": 65") != std::string::npos);
        REQUIRE(res.find("\"avg(p.age)\": 32.5") != std::string::npos);
        REQUIRE(res.find("\"min(p.age)\": 30") != std::string::npos);
        REQUIRE(res.find("\"max(p.age)\": 35") != std::string::npos);
    }

    SECTION("Aggregations: Grouping by property and sorting") {
        // Insert Charlie, who is also 30, so we have two people aged 30 and one aged 35
        uint64_t id3 = graph.shard.local().NodeAddPeered("Person", "charlie", "{\"name\": \"Charlie\", \"age\": 30}").get();
        REQUIRE(id3 > 0);

        SECTION("Group by age and sort by age descending") {
            std::string query = "MATCH (p:Person) RETURN p.age, count(*), sum(p.age) ORDER BY p.age DESC";
            std::string res = GqlExecutor::execute(graph, GqlParser::parse(query)).get();

            // Expected order:
            // 1. age 35, count 1, sum 35
            // 2. age 30, count 2, sum 60
            size_t pos_35 = res.find("\"p.age\": 35");
            size_t pos_30 = res.find("\"p.age\": 30");

            REQUIRE(pos_35 != std::string::npos);
            REQUIRE(pos_30 != std::string::npos);
            REQUIRE(pos_35 < pos_30); // 35 must come before 30 due to ORDER BY p.age DESC
            REQUIRE(res.find("\"count(*)\": 1") != std::string::npos);
            REQUIRE(res.find("\"count(*)\": 2") != std::string::npos);
        }

        SECTION("Group by age and sort by count(*) ascending") {
            std::string query = "MATCH (p:Person) RETURN p.age, count(*) ORDER BY count(*) ASC";
            std::string res = GqlExecutor::execute(graph, GqlParser::parse(query)).get();

            // Expected order:
            // 1. age 35, count 1
            // 2. age 30, count 2
            size_t pos_35 = res.find("\"p.age\": 35");
            size_t pos_30 = res.find("\"p.age\": 30");

            REQUIRE(pos_35 != std::string::npos);
            REQUIRE(pos_30 != std::string::npos);
            REQUIRE(pos_35 < pos_30); // 35 has count 1, 30 has count 2, so 35 comes first
        }
    }

    SECTION("Aggregations: Empty input defaults") {
        SECTION("Aggregates only returns a single default row") {
            std::string query = "MATCH (p:Person) WHERE p.name = 'DoesNotExist' RETURN count(*), sum(p.age)";
            std::string res = GqlExecutor::execute(graph, GqlParser::parse(query)).get();

            // Expect: [{"count(*)": 0, "sum(p.age)": null}]
            REQUIRE(res.find("\"count(*)\": 0") != std::string::npos);
            REQUIRE(res.find("\"sum(p.age)\": null") != std::string::npos);
        }

        SECTION("Aggregates with grouping keys returns empty result") {
            std::string query = "MATCH (p:Person) WHERE p.name = 'DoesNotExist' RETURN p.age, count(*)";
            std::string res = GqlExecutor::execute(graph, GqlParser::parse(query)).get();

            REQUIRE(res == "[]");
        }
    }

    SECTION("Set Operations: UNION and UNION ALL") {
        SECTION("UNION distinct") {
            std::string query = "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name UNION MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name";
            std::string res = GqlExecutor::execute(graph, GqlParser::parse(query)).get();

            // Result should only contain Alice once
            REQUIRE(res == "[{\"p.name\": \"Alice\"}]");
        }

        SECTION("UNION ALL preserves duplicates") {
            std::string query = "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name UNION ALL MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name";
            std::string res = GqlExecutor::execute(graph, GqlParser::parse(query)).get();

            // Result should contain Alice twice
            REQUIRE(res == "[{\"p.name\": \"Alice\"}, {\"p.name\": \"Alice\"}]");
        }

        SECTION("UNION of different subqueries") {
            std::string query = "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name UNION MATCH (p:Person) WHERE p.name = 'Bob' RETURN p.name";
            std::string res = GqlExecutor::execute(graph, GqlParser::parse(query)).get();

            REQUIRE(res.find("Alice") != std::string::npos);
            REQUIRE(res.find("Bob") != std::string::npos);
        }
    }

    SECTION("Set Operations: INTERSECT") {
        SECTION("INTERSECT of matching results") {
            std::string query = "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name INTERSECT MATCH (p:Person) RETURN p.name";
            std::string res = GqlExecutor::execute(graph, GqlParser::parse(query)).get();

            REQUIRE(res == "[{\"p.name\": \"Alice\"}]");
        }

        SECTION("INTERSECT of non-matching results") {
            std::string query = "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name INTERSECT MATCH (p:Person) WHERE p.name = 'Bob' RETURN p.name";
            std::string res = GqlExecutor::execute(graph, GqlParser::parse(query)).get();

            REQUIRE(res == "[]");
        }
    }

    SECTION("Set Operations: Top-level ORDER BY and LIMIT") {
        std::string query = "MATCH (p:Person) RETURN p.name UNION MATCH (p:Person) RETURN p.name ORDER BY p.name DESC LIMIT 1";
        std::string res = GqlExecutor::execute(graph, GqlParser::parse(query)).get();

        REQUIRE(res == "[{\"p.name\": \"Bob\"}]");
    }

    SECTION("Set Operations: Column Mismatch Error") {
        std::string query = "MATCH (p:Person) RETURN p.name UNION MATCH (p:Person) RETURN p.name, p.age";
        REQUIRE_THROWS_AS(GqlExecutor::execute(graph, GqlParser::parse(query)).get(), std::runtime_error);
    }

    graph.Stop().get();
}
