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

/// Stops the graph even when a test throws. Without it an escaping exception unwinds past Stop() and the
/// sharded<Shard> destructor aborts, replacing the real error message with a bare SIGILL.
struct ForGraphStopGuard {
    Graph& g;
    bool stopped = false;
    explicit ForGraphStopGuard(Graph& graph) : g(graph) {}
    ~ForGraphStopGuard() {
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
 * ISO GQL `FOR x IN <list>` -- the standard's UNWIND. Unlike LET, which adds a column to each
 * row, FOR MULTIPLIES the rows: one per element. It expands the working table before LET/FILTER/RETURN
 * see it, and it is a row source in its own right, so a pattern-less `FOR x IN [1,2,3] RETURN x` is a
 * complete query.
 *
 * Also covers the list literal `[a, b, c]`, which had no expression form at all before this.
 */
static void populate_for_graph(Graph& graph) {
    graph.Clear();

    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().RelationshipTypeInsertPeered("KNOWS").get();

    uint64_t alice = graph.shard.local().NodeAddPeered("Person", "alice", "{\"name\": \"Alice\"}").get();
    uint64_t bob = graph.shard.local().NodeAddPeered("Person", "bob", "{\"name\": \"Bob\"}").get();
    uint64_t carol = graph.shard.local().NodeAddPeered("Person", "carol", "{\"name\": \"Carol\"}").get();
    graph.shard.local().RelationshipAddPeered("KNOWS", alice, bob, "{}").get();
    graph.shard.local().RelationshipAddPeered("KNOWS", alice, carol, "{}").get();
}

TEST_CASE("ISO GQL FOR expands a list into rows", "[gql_executor_for]") {
    auto graph = Graph("gql_for_test");
    graph.Start().get();
    ForGraphStopGuard guard(graph);
    populate_for_graph(graph);

    auto run = [&graph](const std::string& query_str) {
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        return GqlExecutor::execute(graph, std::move(query)).get();
    };

    SECTION("FOR over a list literal is a complete query with no pattern at all") {
        std::string res = run("FOR x IN [1, 2, 3] RETURN x");
        INFO("result: " << res);
        REQUIRE(res.find("\"x\": 1") != std::string::npos);
        REQUIRE(res.find("\"x\": 2") != std::string::npos);
        REQUIRE(res.find("\"x\": 3") != std::string::npos);
    }

    SECTION("FOR multiplies the matched rows, one per element") {
        // 3 people x 2 elements = 6 rows, so the count is the product, not either factor.
        std::string res = run("MATCH (p:Person) FOR x IN [1, 2] RETURN count(*) AS n");
        INFO("result: " << res);
        REQUIRE(res.find("\"n\": 6") != std::string::npos);
    }

    SECTION("the element variable is usable downstream, in FILTER and RETURN") {
        std::string res = run("MATCH (p:Person) FOR x IN [1, 2] FILTER x = 2 RETURN p.name AS name, x");
        INFO("result: " << res);
        REQUIRE(res.find("\"x\": 2") != std::string::npos);
        REQUIRE(res.find("\"x\": 1") == std::string::npos);
        REQUIRE(res.find("Alice") != std::string::npos);
    }

    SECTION("a LET may read the FOR element, since FOR expands first") {
        std::string res = run("FOR x IN [10, 20] LET doubled = x + x RETURN doubled ORDER BY doubled");
        INFO("result: " << res);
        REQUIRE(res.find("\"doubled\": 20") != std::string::npos);
        REQUIRE(res.find("\"doubled\": 40") != std::string::npos);
    }

    SECTION("an empty list expands to no rows") {
        REQUIRE(run("FOR x IN [] RETURN x") == "[]");
    }

    SECTION("FOR expands a collected list, not only a literal") {
        // collect_list gathers the neighbours; FOR then unwinds them back into rows.
        std::string res = run(
            "MATCH (a:Person)-[:KNOWS]->(f:Person) FILTER a.name = 'Alice' "
            "RETURN collect_list(f.name) AS friends "
            "NEXT FOR name IN friends RETURN name ORDER BY name");
        INFO("result: " << res);
        REQUIRE(res.find("Bob") != std::string::npos);
        REQUIRE(res.find("Carol") != std::string::npos);
    }

    SECTION("FOR over a non-list expands to no rows rather than failing") {
        REQUIRE(run("MATCH (p:Person) FOR x IN p.name RETURN x") == "[]");
    }

    guard.stop();
}
