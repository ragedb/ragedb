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

    SECTION("Aggregations: STDDEV_POP and STDDEV_SAMP") {
        // Ages 30 and 35, mean 32.5. Squared deviations sum to 12.5.
        // Population: sqrt(12.5 / 2) = 2.5. Sample: sqrt(12.5 / 1) = 3.5355...
        std::string res = GqlExecutor::execute(graph, GqlParser::parse(
            "MATCH (p:Person) RETURN stddev_pop(p.age) AS sp, stddev_samp(p.age) AS ss")).get();
        INFO("result: " << res);
        REQUIRE(res.find("\"sp\": 2.5") != std::string::npos);
        REQUIRE(res.find("\"ss\": 3.53") != std::string::npos);
    }

    SECTION("Aggregations: STDDEV_SAMP of a single value is NULL") {
        std::string res = GqlExecutor::execute(graph, GqlParser::parse(
            "MATCH (p:Person) FILTER p.name = 'Alice' RETURN stddev_samp(p.age) AS ss, stddev_pop(p.age) AS sp")).get();
        INFO("result: " << res);
        // n = 1: sample stddev is undefined (n-1 = 0) -> NULL; population stddev is 0.
        REQUIRE(res.find("\"ss\": null") != std::string::npos);
        REQUIRE(res.find("\"sp\": 0") != std::string::npos);
    }

    SECTION("Aggregations: PERCENTILE_CONT and PERCENTILE_DISC") {
        // Ages 30 and 35. CONT interpolates: median = 30 + (35-30)*0.5 = 32.5. DISC returns an actual
        // value: the lower of the two at fraction 0.5. The endpoints are the min and max.
        std::string res = GqlExecutor::execute(graph, GqlParser::parse(
            "MATCH (p:Person) RETURN percentile_cont(p.age, 0.5) AS pc, percentile_disc(p.age, 0.5) AS pd")).get();
        INFO("result: " << res);
        REQUIRE(res.find("\"pc\": 32.5") != std::string::npos);
        REQUIRE(res.find("\"pd\": 30") != std::string::npos);

        std::string ends = GqlExecutor::execute(graph, GqlParser::parse(
            "MATCH (p:Person) RETURN percentile_cont(p.age, 0.0) AS lo, percentile_cont(p.age, 1.0) AS hi")).get();
        INFO("ends: " << ends);
        REQUIRE(ends.find("\"lo\": 30") != std::string::npos);
        REQUIRE(ends.find("\"hi\": 35") != std::string::npos);
    }

    SECTION("Aggregations: explicit GROUP BY a bound variable") {
        uint64_t id3 = graph.shard.local().NodeAddPeered("Person", "charlie", "{\"name\": \"Charlie\", \"age\": 30}").get();
        REQUIRE(id3 > 0);
        // Two people aged 30, one aged 35. GROUP BY the LET-bound age variable collapses rows by value.
        std::string res = GqlExecutor::execute(graph, GqlParser::parse(
            "MATCH (p:Person) LET a = p.age RETURN a AS ag, count(*) AS c GROUP BY a ORDER BY a")).get();
        INFO("group by: " << res);
        REQUIRE(res.find("\"ag\": 30") != std::string::npos);
        REQUIRE(res.find("\"ag\": 35") != std::string::npos);
        REQUIRE(res.find("\"c\": 2") != std::string::npos);   // two aged 30
        REQUIRE(res.find("\"c\": 1") != std::string::npos);   // one aged 35
        // 30 sorts before 35.
        REQUIRE(res.find("\"ag\": 30") < res.find("\"ag\": 35"));
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

    SECTION("Set Operations: EXCEPT") {
        SECTION("EXCEPT removes the right side's rows") {
            // All people except Alice -> Bob (the graph has Alice and Bob).
            std::string query = "MATCH (p:Person) RETURN p.name EXCEPT MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name";
            std::string res = GqlExecutor::execute(graph, GqlParser::parse(query)).get();
            REQUIRE(res == "[{\"p.name\": \"Bob\"}]");
        }

        SECTION("EXCEPT of a superset is empty") {
            std::string query = "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name EXCEPT MATCH (p:Person) RETURN p.name";
            std::string res = GqlExecutor::execute(graph, GqlParser::parse(query)).get();
            REQUIRE(res == "[]");
        }

        SECTION("EXCEPT is distinct: duplicate left rows collapse") {
            // UNION ALL duplicates Alice, then EXCEPT Bob leaves a single distinct Alice.
            std::string query =
                "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name "
                "UNION ALL MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name "
                "EXCEPT MATCH (p:Person) WHERE p.name = 'Bob' RETURN p.name";
            std::string res = GqlExecutor::execute(graph, GqlParser::parse(query)).get();
            REQUIRE(res == "[{\"p.name\": \"Alice\"}]");
        }
    }

    SECTION("Set Operations: explicit DISTINCT quantifier parses") {
        // UNION DISTINCT and INTERSECT DISTINCT mean the same as the bare forms (DISTINCT is the default).
        std::string u = GqlExecutor::execute(graph, GqlParser::parse(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name "
            "UNION DISTINCT MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name")).get();
        REQUIRE(u == "[{\"p.name\": \"Alice\"}]");

        std::string i = GqlExecutor::execute(graph, GqlParser::parse(
            "MATCH (p:Person) RETURN p.name "
            "INTERSECT DISTINCT MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name")).get();
        REQUIRE(i == "[{\"p.name\": \"Alice\"}]");
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

// A mixed integer/float SUM must keep the integer subtotal exact. A single float64 running total rounds
// once a large integer enters it, so a following float is lost and cancelling integers zero the result.
TEST_CASE("SUM keeps an exact integer subtotal across a mixed int/float aggregate", "[gql_executor_aggregation]") {
    auto graph = Graph("gql_test");
    graph.Start().get();
    graph.Clear();

    graph.shard.local().NodeTypeInsertPeered("Num").get();
    graph.shard.local().NodePropertyTypeAddPeered("Num", "i", "integer").get();
    graph.shard.local().NodePropertyTypeAddPeered("Num", "f", "double").get();

    // 2^60 and -2^60 cancel exactly as integers; the 1.5 is the only real contribution. A float64 running
    // total rounds 2^60 + 1.5 back to 2^60, then subtracts to 0 and drops the 1.5.
    graph.shard.local().NodeAddPeered("Num", "big",  "{\"i\": 1152921504606846976, \"f\": 0.0}").get();
    graph.shard.local().NodeAddPeered("Num", "frac", "{\"i\": 0, \"f\": 1.5}").get();
    graph.shard.local().NodeAddPeered("Num", "nbig", "{\"i\": -1152921504606846976, \"f\": 0.0}").get();

    // CASE routes each row to its integer or its float value, so the aggregate sees a genuine int/float mix.
    std::string query = "MATCH (n:Num) RETURN sum(CASE WHEN n.f > 0 THEN n.f ELSE n.i END) AS s";
    std::string res = GqlExecutor::execute(graph, GqlParser::parse(query)).get();

    REQUIRE(res.find("\"s\": 1.500000") != std::string::npos);
    REQUIRE(res.find("\"s\": 0.000000") == std::string::npos);

    graph.Stop().get();
}
