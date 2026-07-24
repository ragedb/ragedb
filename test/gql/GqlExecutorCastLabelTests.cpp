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

/// Stops the graph even when a test throws. Without it, an escaping exception unwinds past Stop() and the
/// sharded<Shard> destructor aborts, which replaces the real error message with a bare SIGILL.
struct CastGraphStopGuard {
    Graph& g;
    bool stopped = false;
    explicit CastGraphStopGuard(Graph& graph) : g(graph) {}
    ~CastGraphStopGuard() {
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

/*
 * ISO GQL CAST(x AS T) and `x IS [NOT] LABELED <labelExpr>` -- both required by the
 * bi/spb/finbench query sets. A value with no representation in the target type casts to NULL rather
 * than to a truncated prefix, and IS LABELED reuses the pattern label grammar so AND/OR/NOT compose.
 *
 * Data: two Person nodes and one Company node, with a string-valued `code` and an integer `rank`.
 */
static void populate_cast_graph(Graph& graph) {
    graph.Clear();

    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "code", "string").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "rank", "integer").get();
    graph.shard.local().NodeTypeInsertPeered("Company").get();
    graph.shard.local().NodePropertyTypeAddPeered("Company", "name", "string").get();

    graph.shard.local().NodeAddPeered("Person", "alice",
        "{\"name\": \"Alice\", \"code\": \"42\", \"rank\": 7}").get();
    graph.shard.local().NodeAddPeered("Person", "bob",
        "{\"name\": \"Bob\", \"code\": \"abc\", \"rank\": 3}").get();
    graph.shard.local().NodeAddPeered("Company", "acme", "{\"name\": \"Acme\"}").get();
}

TEST_CASE("ISO GQL CAST converts values and yields NULL when it cannot", "[gql_executor_cast]") {
    auto graph = Graph("gql_cast_test");
    graph.Start().get();
    CastGraphStopGuard guard(graph);
    populate_cast_graph(graph);

    auto run = [&graph](const std::string& query_str) {
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        return GqlExecutor::execute(graph, std::move(query)).get();
    };

    SECTION("a numeric string casts to INTEGER") {
        std::string res = run("MATCH (p:Person) FILTER p.name = 'Alice' RETURN CAST(p.code AS INTEGER) AS c");
        INFO("result: " << res);
        REQUIRE(res.find("\"c\": 42") != std::string::npos);
    }

    SECTION("a non-numeric string casts to NULL, not to a truncated prefix") {
        std::string res = run("MATCH (p:Person) FILTER p.name = 'Bob' RETURN CAST(p.code AS INTEGER) AS c");
        INFO("result: " << res);
        REQUIRE(res.find("null") != std::string::npos);
        REQUIRE(res.find("\"c\": 0") == std::string::npos);
    }

    SECTION("an integer casts to STRING") {
        std::string res = run("MATCH (p:Person) FILTER p.name = 'Alice' RETURN CAST(p.rank AS STRING) AS r");
        INFO("result: " << res);
        REQUIRE(res.find("\"r\": \"7\"") != std::string::npos);
    }

    SECTION("an integer casts to FLOAT") {
        std::string res = run("MATCH (p:Person) FILTER p.name = 'Alice' RETURN CAST(p.rank AS FLOAT) AS r");
        INFO("result: " << res);
        REQUIRE(res.find("\"r\": 7") != std::string::npos);
    }

    SECTION("a cast value is usable in a predicate") {
        std::string res = run("MATCH (p:Person) FILTER CAST(p.code AS INTEGER) = 42 RETURN p.name AS n");
        INFO("result: " << res);
        REQUIRE(res.find("Alice") != std::string::npos);
        REQUIRE(res.find("Bob") == std::string::npos);
    }

    guard.stop();
}

TEST_CASE("ISO GQL IS LABELED tests an entity's label", "[gql_executor_cast]") {
    auto graph = Graph("gql_labeled_test");
    graph.Start().get();
    CastGraphStopGuard guard(graph);
    populate_cast_graph(graph);

    auto run = [&graph](const std::string& query_str) {
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        return GqlExecutor::execute(graph, std::move(query)).get();
    };

    SECTION("IS LABELED keeps only the matching label") {
        std::string res = run("MATCH (n) FILTER n IS LABELED Person RETURN n.name AS name");
        INFO("result: " << res);
        REQUIRE(res.find("Alice") != std::string::npos);
        REQUIRE(res.find("Bob") != std::string::npos);
        REQUIRE(res.find("Acme") == std::string::npos);
    }

    SECTION("IS NOT LABELED excludes it") {
        std::string res = run("MATCH (n) FILTER n IS NOT LABELED Person RETURN n.name AS name");
        INFO("result: " << res);
        REQUIRE(res.find("Acme") != std::string::npos);
        REQUIRE(res.find("Alice") == std::string::npos);
    }

    SECTION("the label side composes with OR, as it does in a pattern") {
        std::string res = run("MATCH (n) FILTER n IS LABELED Person | Company RETURN n.name AS name");
        INFO("result: " << res);
        REQUIRE(res.find("Alice") != std::string::npos);
        REQUIRE(res.find("Acme") != std::string::npos);
    }

    SECTION("IS LABELED is projectable as a boolean, not only usable as a filter") {
        std::string res = run("MATCH (n) FILTER n.name = 'Acme' RETURN n IS LABELED Person AS isPerson");
        INFO("result: " << res);
        REQUIRE(res.find("false") != std::string::npos);
    }

    SECTION("a trailing OR belongs to the enclosing predicate, not to the label") {
        // The label grammar accepts `OR` as a label operator inside a pattern, where a delimiter ends it.
        // In an expression it must not swallow the boolean OR, or this reads as `IS LABELED (Person OR n)`.
        std::string res = run("MATCH (n) FILTER n IS LABELED Company OR n.rank > 5 RETURN n.name AS name");
        INFO("result: " << res);
        REQUIRE(res.find("Acme") != std::string::npos);   // labelled Company
        REQUIRE(res.find("Alice") != std::string::npos);  // rank 7 > 5
        REQUIRE(res.find("Bob") == std::string::npos);    // Person, rank 3
    }

    guard.stop();
}
