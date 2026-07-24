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
#include "../../src/gql/executor/PathTraverser.h"

using namespace ragedb;
using namespace ragedb::gql;

/*
 * WITH-pipeline correctness: continuation segments carry piped rows, so any planner
 * that answers from indexes or re-runs the pattern from scratch must be skipped for them, and an
 * empty segment must still flow into a later ungrouped aggregate (which yields count = 0).
 *
 * Data: Alice/Bob/Carol form a KNOWS triangle; Dave/Erin/Frank form a second, disjoint triangle.
 * Two Comment nodes exist for the count fast-path checks.
 */
static void populate_with_pipeline_graph(Graph& graph) {
    graph.Clear();
    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().NodeTypeInsertPeered("Comment").get();
    graph.shard.local().NodePropertyTypeAddPeered("Comment", "score", "integer").get();
    graph.shard.local().RelationshipTypeInsertPeered("KNOWS").get();

    auto add_person = [&](const std::string& key, const std::string& name) {
        return graph.shard.local().NodeAddPeered("Person", key, "{\"name\": \"" + name + "\"}").get();
    };
    uint64_t alice = add_person("alice", "Alice");
    uint64_t bob = add_person("bob", "Bob");
    uint64_t carol = add_person("carol", "Carol");
    uint64_t dave = add_person("dave", "Dave");
    uint64_t erin = add_person("erin", "Erin");
    uint64_t frank = add_person("frank", "Frank");

    auto knows = [&](uint64_t from, uint64_t to) {
        graph.shard.local().RelationshipAddPeered("KNOWS", from, to, "{}").get();
    };
    knows(alice, bob); knows(bob, carol); knows(carol, alice);
    knows(dave, erin); knows(erin, frank); knows(frank, dave);

    graph.shard.local().NodeAddPeered("Comment", "c1", "{\"score\": 1}").get();
    graph.shard.local().NodeAddPeered("Comment", "c2", "{\"score\": 2}").get();
}

TEST_CASE("NEXT continuation segments must consume piped rows", "[gql_executor_with]") {
    auto graph = Graph("gql_with_piped_rows_test");
    graph.Start().get();
    populate_with_pipeline_graph(graph);

    SECTION("count fast path must not fire on a continuation segment: empty pipe means count 0") {
        std::string res = GqlExecutor::execute(graph,
            std::string("MATCH (p:Person {name: 'Nobody'}) RETURN p NEXT MATCH (c:Comment) RETURN count(c) AS n")).get();
        INFO("result: " << res);
        REQUIRE(res.find("\"n\": 0") != std::string::npos);
        REQUIRE(res.find("\"n\": 2") == std::string::npos);
    }

    SECTION("count after a non-empty pipe is per-row (cartesian), not the global label count") {
        // 6 piped persons x 2 comments = 12, not 2.
        std::string res = GqlExecutor::execute(graph,
            std::string("MATCH (p:Person) RETURN p NEXT MATCH (c:Comment) RETURN count(c) AS n")).get();
        INFO("result: " << res);
        REQUIRE(res.find("\"n\": 12") != std::string::npos);
    }

    SECTION("cyclic pattern after NEXT is anchored to the piped binding, not every cycle") {
        std::string res = GqlExecutor::execute(graph,
            std::string("MATCH (a:Person {name: 'Alice'}) RETURN a NEXT "
                        "MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c)-[:KNOWS]->(a) "
                        "RETURN b.name AS bn, c.name AS cn")).get();
        INFO("result: " << res);
        REQUIRE(res.find("\"bn\": \"Bob\", \"cn\": \"Carol\"") != std::string::npos);
        // The disjoint Dave/Erin/Frank triangle must not appear.
        REQUIRE(res.find("Dave") == std::string::npos);
        REQUIRE(res.find("Erin") == std::string::npos);
        REQUIRE(res.find("Frank") == std::string::npos);
    }

    SECTION("an empty segment still reaches a later ungrouped aggregate: one row with count 0") {
        std::string res = GqlExecutor::execute(graph,
            std::string("MATCH (a:Person {name: 'Nobody'}) RETURN a NEXT "
                        "MATCH (a)-[:KNOWS]->(b) RETURN count(b) AS n NEXT RETURN n")).get();
        INFO("result: " << res);
        REQUIRE(res.find("\"n\": 0") != std::string::npos);
    }

    SECTION("edge-expansion count after NEXT is anchored to the piped node") {
        // Alice has exactly one outgoing KNOWS edge (to Bob); the count->degree-sum rewrite must
        // not fire on the continuation part (its anchor is piped in, never degree-populated).
        std::string res = GqlExecutor::execute(graph,
            std::string("MATCH (a:Person {name: 'Alice'}) RETURN a NEXT "
                        "MATCH (a)-[:KNOWS]->(b) RETURN count(b) AS n")).get();
        INFO("result: " << res);
        REQUIRE(res.find("\"n\": 1") != std::string::npos);
    }

    graph.Stop().get();
}

TEST_CASE("streamed top-K and group folds match materialised results", "[gql_executor_with][stream]") {
    auto graph = Graph("gql_streaming_fold_test");
    graph.Start().get();
    graph.Clear();
    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().NodeTypeInsertPeered("Comment").get();
    graph.shard.local().NodePropertyTypeAddPeered("Comment", "score", "integer").get();
    graph.shard.local().RelationshipTypeInsertPeered("KNOWS").get();
    graph.shard.local().RelationshipTypeInsertPeered("HAS_CREATOR").get();

    uint64_t alice = graph.shard.local().NodeAddPeered("Person", "alice", "{\"name\": \"Alice\"}").get();
    uint64_t bob = graph.shard.local().NodeAddPeered("Person", "bob", "{\"name\": \"Bob\"}").get();
    uint64_t carol = graph.shard.local().NodeAddPeered("Person", "carol", "{\"name\": \"Carol\"}").get();
    uint64_t dave = graph.shard.local().NodeAddPeered("Person", "dave", "{\"name\": \"Dave\"}").get();
    graph.shard.local().RelationshipAddPeered("KNOWS", alice, bob, "{}").get();
    graph.shard.local().RelationshipAddPeered("KNOWS", alice, carol, "{}").get();
    graph.shard.local().RelationshipAddPeered("KNOWS", dave, bob, "{}").get();

    auto comment = [&](const std::string& key, int64_t score, uint64_t creator) {
        uint64_t c = graph.shard.local().NodeAddPeered("Comment", key,
            "{\"score\": " + std::to_string(score) + "}").get();
        graph.shard.local().RelationshipAddPeered("HAS_CREATOR", c, creator, "{}").get();
    };
    // Bob: scores 1, 5, 1 (three comments, two distinct scores); Carol: 2, 4.
    comment("b1", 1, bob); comment("b2", 5, bob); comment("b3", 1, bob);
    comment("c1", 2, carol); comment("c2", 4, carol);

    const size_t saved = gql_stream_chunk_size;
    gql_stream_chunk_size = 2;  // force multi-chunk streaming on this tiny graph

    SECTION("IC2 shape: RETURN ... ORDER BY ... LIMIT NEXT over a two-hop expansion (top-K)") {
        std::string res = GqlExecutor::execute(graph,
            std::string("MATCH (a:Person {name: 'Alice'})-[:KNOWS]-(f:Person)<-[:HAS_CREATOR]-(m:Comment) "
                        "WHERE m.score > 1 RETURN m ORDER BY m.score DESC LIMIT 2 NEXT RETURN m.score AS s")).get();
        INFO("result: " << res);
        // Friend comments with score > 1: {5, 2, 4}; top 2 descending = 5, 4.
        REQUIRE(res.find("[{\"s\": 5}, {\"s\": 4}]") != std::string::npos);
    }

    SECTION("RETURN DISTINCT then expand then count (the crash shape, group fold)") {
        std::string res = GqlExecutor::execute(graph,
            std::string("MATCH (a:Person {name: 'Alice'})-[:KNOWS]-(f:Person) RETURN DISTINCT f "
                        "NEXT MATCH (f)<-[:HAS_CREATOR]-(m:Comment) RETURN count(m) AS n")).get();
        INFO("result: " << res);
        REQUIRE(res.find("\"n\": 5") != std::string::npos);
    }

    SECTION("grouped fold over a two-hop expansion with DISTINCT aggregate and ORDER BY") {
        std::string res = GqlExecutor::execute(graph,
            std::string("MATCH (a:Person {name: 'Alice'})-[:KNOWS]-(f:Person)<-[:HAS_CREATOR]-(m:Comment) "
                        "RETURN f.name AS fn, count(m) AS n, count(DISTINCT m.score) AS d "
                        "ORDER BY count(m) DESC")).get();
        INFO("result: " << res);
        REQUIRE(res.find("\"fn\": \"Bob\", \"n\": 3, \"d\": 2") != std::string::npos);
        REQUIRE(res.find("\"fn\": \"Carol\", \"n\": 2, \"d\": 2") != std::string::npos);
        REQUIRE(res.find("Bob") < res.find("Carol"));
    }

    SECTION("streamed results equal materialised results (no LIMIT control)") {
        // Without a LIMIT the top-K gate does not fire, so this exercises the ordinary path on
        // the same data; the LIMIT 3 variant must be its prefix.
        std::string full = GqlExecutor::execute(graph,
            std::string("MATCH (a:Person {name: 'Alice'})-[:KNOWS]-(f:Person)<-[:HAS_CREATOR]-(m:Comment) "
                        "RETURN m ORDER BY m.score DESC NEXT RETURN m.score AS s")).get();
        std::string streamed = GqlExecutor::execute(graph,
            std::string("MATCH (a:Person {name: 'Alice'})-[:KNOWS]-(f:Person)<-[:HAS_CREATOR]-(m:Comment) "
                        "RETURN m ORDER BY m.score DESC LIMIT 3 NEXT RETURN m.score AS s")).get();
        INFO("full: " << full << " streamed: " << streamed);
        REQUIRE(full.find("[{\"s\": 5}, {\"s\": 4}, {\"s\": 2}") != std::string::npos);
        REQUIRE(streamed.find("[{\"s\": 5}, {\"s\": 4}, {\"s\": 2}]") != std::string::npos);
    }

    gql_stream_chunk_size = saved;
    graph.Stop().get();
}

TEST_CASE("ORDER BY over RETURN aliases executes correctly", "[gql_executor_with]") {
    auto graph = Graph("gql_orderby_alias_test");
    graph.Start().get();
    graph.Clear();
    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().NodeTypeInsertPeered("Comment").get();
    graph.shard.local().RelationshipTypeInsertPeered("HAS_CREATOR").get();

    uint64_t alice = graph.shard.local().NodeAddPeered("Person", "alice", "{\"name\": \"Alice\"}").get();
    uint64_t bob = graph.shard.local().NodeAddPeered("Person", "bob", "{\"name\": \"Bob\"}").get();
    uint64_t carol = graph.shard.local().NodeAddPeered("Person", "carol", "{\"name\": \"Carol\"}").get();
    auto comment = [&](const std::string& key, uint64_t creator) {
        uint64_t c = graph.shard.local().NodeAddPeered("Comment", key, "{}").get();
        graph.shard.local().RelationshipAddPeered("HAS_CREATOR", c, creator, "{}").get();
    };
    comment("c1", alice); comment("c2", alice); comment("c3", alice);
    comment("c4", bob);
    comment("c5", carol); comment("c6", carol);

    SECTION("sort by projected aggregate alias, then by projected name alias") {
        std::string res = GqlExecutor::execute(graph,
            std::string("MATCH (p:Person)<-[:HAS_CREATOR]-(m:Comment) "
                        "RETURN p.name AS name, count(DISTINCT m) AS cnt "
                        "ORDER BY cnt DESC, name ASC LIMIT 2")).get();
        INFO("result: " << res);
        REQUIRE(res.find("\"name\": \"Alice\", \"cnt\": 3") != std::string::npos);
        REQUIRE(res.find("\"name\": \"Carol\", \"cnt\": 2") != std::string::npos);
        REQUIRE(res.find("Bob") == std::string::npos);
        REQUIRE(res.find("Alice") < res.find("Carol"));
    }

    SECTION("alias sort keys work in an intermediate NEXT segment") {
        std::string res = GqlExecutor::execute(graph,
            std::string("MATCH (p:Person)<-[:HAS_CREATOR]-(m:Comment) "
                        "RETURN p, count(m) AS cnt ORDER BY cnt DESC LIMIT 1 "
                        "NEXT RETURN p.name AS n, cnt")).get();
        INFO("result: " << res);
        REQUIRE(res.find("\"n\": \"Alice\", \"cnt\": 3") != std::string::npos);
    }

    graph.Stop().get();
}

TEST_CASE("standalone ORDER BY/LIMIT before RETURN pages the working table", "[gql_executor_with]") {
    auto graph = Graph("gql_standalone_order_page_test");
    graph.Start().get();
    graph.Clear();
    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "likes", "integer").get();

    auto person = [&](const std::string& key, const std::string& name, int64_t likes) {
        return graph.shard.local().NodeAddPeered("Person", key,
            "{\"name\": \"" + name + "\", \"likes\": " + std::to_string(likes) + "}").get();
    };
    uint64_t alice = person("alice", "Alice", 30);
    uint64_t bob = person("bob", "Bob", 20);
    uint64_t carol = person("carol", "Carol", 10);
    // Alice knows Bob then Carol, so the non-start variable's id order is the opposite of the order
    // the DESC test below asks for.
    graph.shard.local().RelationshipTypeInsertPeered("KNOWS").get();
    graph.shard.local().RelationshipAddPeered("KNOWS", alice, bob, "{}").get();
    graph.shard.local().RelationshipAddPeered("KNOWS", alice, carol, "{}").get();

    SECTION("ordering and paging apply to the matched rows, and the RETURN re-projects them") {
        std::string res = GqlExecutor::execute(graph,
            std::string("MATCH (p:Person) ORDER BY p.likes DESC LIMIT 2 RETURN p.name AS n")).get();
        INFO("result: " << res);
        REQUIRE(res.find("[{\"n\": \"Alice\"}, {\"n\": \"Bob\"}]") != std::string::npos);
    }

    SECTION("the sort key may be a LET binding that the RETURN never projects") {
        std::string res = GqlExecutor::execute(graph,
            std::string("MATCH (p:Person) LET score = p.likes * 2 "
                        "ORDER BY score ASC LIMIT 2 RETURN p.name AS n")).get();
        INFO("result: " << res);
        REQUIRE(res.find("[{\"n\": \"Carol\"}, {\"n\": \"Bob\"}]") != std::string::npos);
    }

    SECTION("a page before an aggregating RETURN bounds the rows fed to the aggregate") {
        // LIMIT 2 pages the three matched people, so the count is of the page, not of the whole
        // label -- and not a limit on the single aggregate row.
        std::string res = GqlExecutor::execute(graph,
            std::string("MATCH (p:Person) ORDER BY p.likes DESC LIMIT 2 RETURN count(p) AS c")).get();
        INFO("result: " << res);
        REQUIRE(res.find("\"c\": 2") != std::string::npos);
    }

    SECTION("ordering by a bare variable that is not the start node is not traversal order") {
        // The start-node scan can answer ORDER BY <start node> by scanning in id order, but a bare
        // variable bound anywhere else says nothing about traversal order. Claiming otherwise silently
        // replaced the sort with "start-node id ascending", discarding the key and the direction.
        std::string asc = GqlExecutor::execute(graph,
            std::string("MATCH (p:Person) LET score = p.likes * 2 "
                        "RETURN p.name AS n ORDER BY score ASC")).get();
        INFO("asc: " << asc);
        REQUIRE(asc.find("[{\"n\": \"Carol\"}, {\"n\": \"Bob\"}, {\"n\": \"Alice\"}]") != std::string::npos);

        std::string desc = GqlExecutor::execute(graph,
            std::string("MATCH (p:Person) LET score = p.likes * 2 "
                        "RETURN p.name AS n ORDER BY score DESC")).get();
        INFO("desc: " << desc);
        REQUIRE(desc.find("[{\"n\": \"Alice\"}, {\"n\": \"Bob\"}, {\"n\": \"Carol\"}]") != std::string::npos);
    }

    SECTION("ordering by a later pattern variable is not the start node's id order") {
        // `f` is bound by the second node of the pattern, so its id order says nothing about the scan
        // order of `p`. Alice knows Bob (id 2) and Carol (id 3): DESC must put Carol first.
        std::string res = GqlExecutor::execute(graph,
            std::string("MATCH (p:Person {name: 'Alice'})-[:KNOWS]->(f:Person) "
                        "RETURN f.name AS n ORDER BY f DESC")).get();
        INFO("result: " << res);
        REQUIRE(res.find("[{\"n\": \"Carol\"}, {\"n\": \"Bob\"}]") != std::string::npos);
    }

    SECTION("ordering by a column piped into a segment that has its own MATCH") {
        // The sort key is a column carried in from the previous segment, not this segment's start
        // node, so the traversal order cannot stand in for it.
        std::string res = GqlExecutor::execute(graph,
            std::string("MATCH (p:Person) RETURN p.name AS pn, p.likes AS pl "
                        "NEXT MATCH (q:Person {name: 'Alice'}) "
                        "RETURN pn ORDER BY pl ASC")).get();
        INFO("result: " << res);
        REQUIRE(res.find("[{\"pn\": \"Carol\"}, {\"pn\": \"Bob\"}, {\"pn\": \"Alice\"}]") != std::string::npos);
    }

    SECTION("a NEXT segment of only primitive query statements forwards its working table") {
        // The middle segment binds a value and hands its rows on without a result statement of its
        // own; the total must survive into the final segment's projection.
        std::string res = GqlExecutor::execute(graph,
            std::string("MATCH (p:Person) RETURN count(p) AS totalInt "
                        "NEXT LET total = CAST(totalInt AS FLOAT) "
                        "NEXT RETURN total")).get();
        INFO("result: " << res);
        REQUIRE(res.find("\"total\": 3") != std::string::npos);
    }

    SECTION("a FILTER-only NEXT segment forwards only the surviving rows") {
        std::string res = GqlExecutor::execute(graph,
            std::string("MATCH (p:Person) RETURN p.name AS n, p.likes AS likes "
                        "NEXT FILTER likes > 15 "
                        "NEXT RETURN n ORDER BY n ASC")).get();
        INFO("result: " << res);
        REQUIRE(res.find("[{\"n\": \"Alice\"}, {\"n\": \"Bob\"}]") != std::string::npos);
    }

    graph.Stop().get();
}

TEST_CASE("count over an empty expansion is 0 even when rewritten to a degree sum", "[gql_executor_with]") {
    auto graph = Graph("gql_count_to_sum_empty_test");
    graph.Start().get();
    graph.Clear();
    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().RelationshipTypeInsertPeered("KNOWS").get();

    // No Person nodes at all: the degree-sum rewrite fires, but the result must stay
    // count-shaped (0), not sum-shaped (null).
    std::string res = GqlExecutor::execute(graph,
        std::string("MATCH (a:Person)-[:KNOWS]->(b) RETURN count(b) AS n")).get();
    INFO("result: " << res);
    REQUIRE(res.find("\"n\": 0") != std::string::npos);

    graph.Stop().get();
}

TEST_CASE("streaming edge aggregate rejects unsupported shapes", "[gql_executor_with]") {
    auto graph = Graph("gql_streaming_guard_test");
    graph.Start().get();
    graph.Clear();
    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().RelationshipTypeInsertPeered("KNOWS").get();
    uint64_t a = graph.shard.local().NodeAddPeered("Person", "alice", "{\"name\": \"Alice\"}").get();
    uint64_t b = graph.shard.local().NodeAddPeered("Person", "bob", "{\"name\": \"Bob\"}").get();
    // One self-loop (Alice) and two ordinary edges.
    graph.shard.local().RelationshipAddPeered("KNOWS", a, a, "{}").get();
    graph.shard.local().RelationshipAddPeered("KNOWS", a, b, "{}").get();
    graph.shard.local().RelationshipAddPeered("KNOWS", b, a, "{}").get();

    SECTION("self-loop pattern counts only self-loops, not every edge") {
        std::string res = GqlExecutor::execute(graph,
            std::string("MATCH (x:Person)-[:KNOWS]->(x) RETURN count(x) AS n")).get();
        INFO("result: " << res);
        REQUIRE(res.find("\"n\": 1") != std::string::npos);
        REQUIRE(res.find("\"n\": 3") == std::string::npos);
    }

    graph.Stop().get();
}

TEST_CASE("GQL expression forms execute (CASE/length/zoned_datetime/COUNT{}/collect_list/IN)",
          "[gql_executor_with]") {
    auto graph = Graph("gql_expr_forms_test");
    graph.Start().get();
    graph.Clear();
    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "created", "integer").get();
    graph.shard.local().RelationshipTypeInsertPeered("KNOWS").get();
    // created is epoch MILLISECONDS: Alice 2011-06-01 (1306886400000), Bob 2010-06-01 (1275350400000).
    uint64_t alice = graph.shard.local().NodeAddPeered("Person", "alice", "{\"name\": \"Alice\", \"created\": 1306886400000}").get();
    uint64_t bob   = graph.shard.local().NodeAddPeered("Person", "bob",   "{\"name\": \"Bob\", \"created\": 1275350400000}").get();
    uint64_t carol = graph.shard.local().NodeAddPeered("Person", "carol", "{\"name\": \"Carol\", \"created\": 1306886400000}").get();
    graph.shard.local().RelationshipAddPeered("KNOWS", alice, bob, "{}").get();
    graph.shard.local().RelationshipAddPeered("KNOWS", bob, carol, "{}").get();
    graph.shard.local().RelationshipAddPeered("KNOWS", alice, carol, "{}").get();

    SECTION("sum(CASE WHEN ... THEN 1 ELSE 0 END)") {
        std::string res = GqlExecutor::execute(graph,
            std::string("MATCH (p:Person) RETURN sum(CASE WHEN p.name = 'Alice' THEN 1 ELSE 0 END) AS n")).get();
        INFO("result: " << res);
        REQUIRE(res.find("\"n\": 1") != std::string::npos);
    }

    SECTION("COUNT { } subquery-count of a friend expansion") {
        std::string res = GqlExecutor::execute(graph,
            std::string("MATCH (p:Person {name: 'Alice'}) RETURN COUNT { (p)-[:KNOWS]->(f:Person) } AS c")).get();
        INFO("result: " << res);
        REQUIRE(res.find("\"c\": 2") != std::string::npos);  // Alice knows Bob and Carol
    }

    SECTION("length(path) over a shortest path is the hop count") {
        std::string res = GqlExecutor::execute(graph,
            std::string("MATCH p = ANY SHORTEST (a)-[:KNOWS]-{1,3}(b) "
                        "WHERE a.name = 'Alice' AND b.name = 'Carol' RETURN length(p) AS d")).get();
        INFO("result: " << res);
        REQUIRE(res.find("\"d\": 1") != std::string::npos);  // Alice-[:KNOWS]->Carol is direct
    }

    SECTION("zoned_datetime() compares in epoch milliseconds (unit check)") {
        // created >= 2011-01-01 (1293840000000 ms). Alice/Carol are 2011-06 (match); Bob is 2010-06 (no).
        // If zoned_datetime returned SECONDS, all three would match -- so this discriminates the unit.
        std::string res = GqlExecutor::execute(graph,
            std::string("MATCH (p:Person) WHERE p.created >= zoned_datetime('2011-01-01') "
                        "RETURN p.name AS n ORDER BY n")).get();
        INFO("result: " << res);
        REQUIRE(res.find("Alice") != std::string::npos);
        REQUIRE(res.find("Carol") != std::string::npos);
        REQUIRE(res.find("Bob") == std::string::npos);
    }

    SECTION("collect_list then NOT (x IN before) membership across NEXT") {
        std::string res = GqlExecutor::execute(graph,
            std::string("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(f:Person) "
                        "RETURN collect_list(f) AS before "
                        "NEXT "
                        "MATCH (x:Person) WHERE NOT (x IN before) RETURN x.name AS n")).get();
        INFO("result: " << res);
        REQUIRE(res.find("Alice") != std::string::npos);  // Alice is not among her own friends
        REQUIRE(res.find("Bob") == std::string::npos);
        REQUIRE(res.find("Carol") == std::string::npos);
    }

    graph.Stop().get();
}

TEST_CASE("multi-match streamed group fold matches the batch result", "[gql_executor_with]") {
    auto graph = Graph("gql_multimatch_stream_test");
    graph.Start().get();
    graph.Clear();
    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "id", "integer").get();
    graph.shard.local().NodeTypeInsertPeered("Forum").get();
    graph.shard.local().NodePropertyTypeAddPeered("Forum", "id", "integer").get();
    graph.shard.local().NodeTypeInsertPeered("Post").get();
    graph.shard.local().NodePropertyTypeAddPeered("Post", "id", "integer").get();
    graph.shard.local().RelationshipTypeInsertPeered("KNOWS").get();
    graph.shard.local().RelationshipTypeInsertPeered("HAS_MEMBER").get();
    graph.shard.local().RelationshipTypeInsertPeered("CONTAINER_OF").get();
    graph.shard.local().RelationshipTypeInsertPeered("HAS_CREATOR").get();
    uint64_t p1 = graph.shard.local().NodeAddPeered("Person", "1", "{\"id\": 1}").get();
    uint64_t f1 = graph.shard.local().NodeAddPeered("Person", "2", "{\"id\": 2}").get();
    graph.shard.local().RelationshipAddPeered("KNOWS", p1, f1, "{}").get();
    // forum10: 3 distinct posts by f1; forum20: 1 post by f1. Both have f1 as member.
    uint64_t forum10 = graph.shard.local().NodeAddPeered("Forum", "10", "{\"id\": 10}").get();
    uint64_t forum20 = graph.shard.local().NodeAddPeered("Forum", "20", "{\"id\": 20}").get();
    graph.shard.local().RelationshipAddPeered("HAS_MEMBER", forum10, f1, "{}").get();
    graph.shard.local().RelationshipAddPeered("HAS_MEMBER", forum20, f1, "{}").get();
    for (int i = 100; i < 103; ++i) {  // forum10 posts 100,101,102
        uint64_t post = graph.shard.local().NodeAddPeered("Post", std::to_string(i), "{\"id\": " + std::to_string(i) + "}").get();
        graph.shard.local().RelationshipAddPeered("CONTAINER_OF", forum10, post, "{}").get();
        graph.shard.local().RelationshipAddPeered("HAS_CREATOR", post, f1, "{}").get();
    }
    uint64_t post200 = graph.shard.local().NodeAddPeered("Post", "200", "{\"id\": 200}").get();  // forum20 post
    graph.shard.local().RelationshipAddPeered("CONTAINER_OF", forum20, post200, "{}").get();
    graph.shard.local().RelationshipAddPeered("HAS_CREATOR", post200, f1, "{}").get();

    const size_t saved = gql_stream_chunk_size;
    gql_stream_chunk_size = 1;  // force multi-chunk streaming through the 2-match chain

    // Piped frontier -> two-MATCH expansion -> grouped count(DISTINCT) with ORDER BY (IC5 shape).
    std::string res = GqlExecutor::execute(graph,
        std::string("MATCH (p:Person {id: 1})-[:KNOWS]-{1,2}(f:Person) WHERE f.id <> 1 "
                    "RETURN DISTINCT f "
                    "NEXT "
                    "MATCH (forum:Forum)-[:HAS_MEMBER]->(f) "
                    "MATCH (forum)-[:CONTAINER_OF]->(post:Post)-[:HAS_CREATOR]->(f) "
                    "RETURN forum.id AS forumId, count(DISTINCT post) AS postCount "
                    "ORDER BY postCount DESC, forumId ASC")).get();
    INFO("result: " << res);
    gql_stream_chunk_size = saved;

    REQUIRE(res.find("\"forumId\": 10, \"postCount\": 3") != std::string::npos);
    REQUIRE(res.find("\"forumId\": 20, \"postCount\": 1") != std::string::npos);
    REQUIRE(res.find("10") < res.find("20"));  // ordered by postCount DESC

    graph.Stop().get();
}

TEST_CASE("GQL IC5 shape executes without crashing", "[gql_executor_with]") {
    auto graph = Graph("gql_ic5_shape_test");
    graph.Start().get();
    graph.Clear();
    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "id", "integer").get();
    graph.shard.local().NodeTypeInsertPeered("Forum").get();
    graph.shard.local().NodePropertyTypeAddPeered("Forum", "id", "integer").get();
    graph.shard.local().NodeTypeInsertPeered("Post").get();
    graph.shard.local().NodePropertyTypeAddPeered("Post", "id", "integer").get();
    graph.shard.local().RelationshipTypeInsertPeered("KNOWS").get();
    graph.shard.local().RelationshipTypeInsertPeered("HAS_MEMBER").get();
    graph.shard.local().RelationshipTypeInsertPeered("CONTAINER_OF").get();
    graph.shard.local().RelationshipTypeInsertPeered("HAS_CREATOR").get();
    uint64_t p1 = graph.shard.local().NodeAddPeered("Person", "1", "{\"id\": 1}").get();
    uint64_t f1 = graph.shard.local().NodeAddPeered("Person", "2", "{\"id\": 2}").get();
    uint64_t f2 = graph.shard.local().NodeAddPeered("Person", "3", "{\"id\": 3}").get();
    uint64_t forum = graph.shard.local().NodeAddPeered("Forum", "10", "{\"id\": 10}").get();
    uint64_t post1 = graph.shard.local().NodeAddPeered("Post", "100", "{\"id\": 100}").get();
    uint64_t post2 = graph.shard.local().NodeAddPeered("Post", "101", "{\"id\": 101}").get();
    graph.shard.local().RelationshipAddPeered("KNOWS", p1, f1, "{}").get();
    graph.shard.local().RelationshipAddPeered("KNOWS", f1, f2, "{}").get();
    graph.shard.local().RelationshipAddPeered("HAS_MEMBER", forum, f1, "{}").get();
    graph.shard.local().RelationshipAddPeered("CONTAINER_OF", forum, post1, "{}").get();
    graph.shard.local().RelationshipAddPeered("CONTAINER_OF", forum, post2, "{}").get();
    graph.shard.local().RelationshipAddPeered("HAS_CREATOR", post1, f1, "{}").get();
    graph.shard.local().RelationshipAddPeered("HAS_CREATOR", post2, f1, "{}").get();

    SECTION("RETURN DISTINCT f NEXT (frontier only)") {
        std::string res = GqlExecutor::execute(graph,
            std::string("MATCH (p:Person {id: 1})-[:KNOWS]-{1,2}(f:Person) WHERE f.id <> 1 "
                        "RETURN DISTINCT f NEXT RETURN f.id AS fid ORDER BY fid")).get();
        INFO("result: " << res);
        REQUIRE(res.find("\"fid\": 2") != std::string::npos);
    }
    SECTION("full IC5 shape") {
        std::string res = GqlExecutor::execute(graph,
            std::string("MATCH (p:Person {id: 1})-[:KNOWS]-{1,2}(f:Person) WHERE f.id <> 1 "
                        "RETURN DISTINCT f "
                        "NEXT "
                        "MATCH (forum:Forum)-[:HAS_MEMBER]->(f) "
                        "MATCH (forum)-[:CONTAINER_OF]->(post:Post)-[:HAS_CREATOR]->(f) "
                        "RETURN forum.id AS forumId, count(DISTINCT post) AS postCount "
                        "ORDER BY postCount DESC, forumId ASC LIMIT 5")).get();
        INFO("result: " << res);
        REQUIRE(res.find("\"forumId\": 10, \"postCount\": 2") != std::string::npos);
    }

    graph.Stop().get();
}

TEST_CASE("NEXT ORDER BY LIMIT pushes a bounded top-K into the producing segment", "[gql_executor_with]") {
    auto graph = Graph("gql_topk_pushdown_test");
    graph.Start().get();
    graph.Clear();
    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "id", "integer").get();
    graph.shard.local().NodeTypeInsertPeered("Post").get();
    graph.shard.local().NodePropertyTypeAddPeered("Post", "id", "integer").get();
    graph.shard.local().NodePropertyTypeAddPeered("Post", "creationDate", "integer").get();
    graph.shard.local().RelationshipTypeInsertPeered("HAS_CREATOR").get();
    uint64_t p = graph.shard.local().NodeAddPeered("Person", "1", "{\"id\": 1}").get();
    // Five posts with distinct creationDates; the top-2 by creationDate DESC are ids 105 and 104.
    for (int i = 0; i < 5; ++i) {
        int id = 101 + i;
        int cd = 1000 + i * 10;  // 1000,1010,1020,1030,1040
        uint64_t post = graph.shard.local().NodeAddPeered("Post", std::to_string(id),
            "{\"id\": " + std::to_string(id) + ", \"creationDate\": " + std::to_string(cd) + "}").get();
        graph.shard.local().RelationshipAddPeered("HAS_CREATOR", post, p, "{}").get();
    }

    const size_t saved = gql_stream_chunk_size;
    gql_stream_chunk_size = 1;  // force the producing segment's traversal to stream in chunks

    std::string res = GqlExecutor::execute(graph,
        std::string("MATCH (p:Person {id: 1})<-[:HAS_CREATOR]-(m:Post) "
                    "RETURN m.creationDate AS ms, m.id AS mid "
                    "NEXT "
                    "ORDER BY ms DESC, mid ASC LIMIT 2 "
                    "RETURN mid, ms")).get();
    INFO("result: " << res);
    gql_stream_chunk_size = saved;

    // Only the top 2 by creationDate DESC survive: post 105 (1040) then post 104 (1030).
    REQUIRE(res.find("\"mid\": 105, \"ms\": 1040") != std::string::npos);
    REQUIRE(res.find("\"mid\": 104, \"ms\": 1030") != std::string::npos);
    REQUIRE(res.find("\"mid\": 103") == std::string::npos);
    REQUIRE(res.find("\"mid\": 102") == std::string::npos);
    REQUIRE(res.find("\"mid\": 101") == std::string::npos);
    REQUIRE(res.find("105") < res.find("104"));  // DESC order preserved

    graph.Stop().get();
}

TEST_CASE("cost-based reorder runs the cheap match first", "[gql_executor_with]") {
    auto graph = Graph("gql_reorder_test");
    graph.Start().get();
    graph.Clear();
    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "id", "integer").get();
    graph.shard.local().NodeTypeInsertPeered("Forum").get();
    graph.shard.local().NodePropertyTypeAddPeered("Forum", "id", "integer").get();
    graph.shard.local().NodeTypeInsertPeered("Post").get();
    graph.shard.local().NodePropertyTypeAddPeered("Post", "id", "integer").get();
    graph.shard.local().RelationshipTypeInsertPeered("KNOWS").get();
    graph.shard.local().RelationshipTypeInsertPeered("HAS_MEMBER").get();
    graph.shard.local().RelationshipTypeInsertPeered("CONTAINER_OF").get();
    graph.shard.local().RelationshipTypeInsertPeered("HAS_CREATOR").get();
    uint64_t p1 = graph.shard.local().NodeAddPeered("Person", "1", "{\"id\": 1}").get();
    uint64_t p2 = graph.shard.local().NodeAddPeered("Person", "2", "{\"id\": 2}").get();
    graph.shard.local().RelationshipAddPeered("KNOWS", p1, p2, "{}").get();
    // Skew so that driving forum-first (HAS_MEMBER 5 x CONTAINER_OF 3) is far dearer than post-first
    // (HAS_CREATOR ~1 x CONTAINER_OF-back 1): 5 forums each with p2 as member and 3 posts; p2 created 2.
    int post_key = 100;
    for (int i = 0; i < 5; ++i) {
        uint64_t forum = graph.shard.local().NodeAddPeered("Forum", std::to_string(10 + i), "{\"id\": " + std::to_string(10 + i) + "}").get();
        graph.shard.local().RelationshipAddPeered("HAS_MEMBER", forum, p2, "{}").get();
        for (int j = 0; j < 3; ++j) {
            uint64_t post = graph.shard.local().NodeAddPeered("Post", std::to_string(post_key), "{\"id\": " + std::to_string(post_key) + "}").get();
            graph.shard.local().RelationshipAddPeered("CONTAINER_OF", forum, post, "{}").get();
            if (i == 0 && j < 2) graph.shard.local().RelationshipAddPeered("HAS_CREATOR", post, p2, "{}").get();
            ++post_key;
        }
    }

    auto q = GqlParser::parse(
        "MATCH (p:Person {id: 1})-[:KNOWS]-(f:Person) RETURN DISTINCT f "
        "NEXT "
        "MATCH (forum:Forum)-[:HAS_MEMBER]->(f) "
        "MATCH (forum)-[:CONTAINER_OF]->(post:Post)-[:HAS_CREATOR]->(f) "
        "RETURN forum.id AS forumId, count(DISTINCT post) AS postCount");
    GqlOptimizer::optimize(graph, q);

    // The final segment's MATCHes are reordered so the cheap post expansion (3-node) runs first and the
    // membership match (2-node) becomes a trailing verification.
    REQUIRE(q.matches.size() == 2);
    REQUIRE(q.matches[0].pattern.nodes.size() == 3);
    REQUIRE(q.matches[1].pattern.nodes.size() == 2);

    graph.Stop().get();
}

/*
 * A property map value is a general value expression in ISO GQL, not only a literal, so a binding
 * carried in from an earlier segment is legal there. It used to be rejected outright ("Expected literal
 * value for property map"), which forced the FILTER spelling of the same predicate.
 */
TEST_CASE("property map accepts a value expression, not only a literal", "[gql_executor_with]") {
    auto graph = Graph("gql_property_map_expr_test");
    graph.Start().get();
    populate_with_pipeline_graph(graph);

    auto run = [&graph](const std::string& query_str) {
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        return GqlExecutor::execute(graph, std::move(query)).get();
    };

    SECTION("a NEXT-piped binding resolves inside the property map") {
        std::string res = run(
            "MATCH (p:Person) FILTER p.name = 'Alice' RETURN p.name AS n "
            "NEXT MATCH (q:Person {name: n}) RETURN q.name AS qn");
        INFO("result: " << res);
        REQUIRE(res.find("\"qn\": \"Alice\"") != std::string::npos);
        REQUIRE(res.find("Bob") == std::string::npos);
    }

    SECTION("the property map and FILTER spellings of the same predicate agree") {
        std::string mapped = run(
            "MATCH (p:Person) FILTER p.name = 'Alice' RETURN p.name AS n "
            "NEXT MATCH (q:Person {name: n}) RETURN q.name AS qn");
        std::string filtered = run(
            "MATCH (p:Person) FILTER p.name = 'Alice' RETURN p.name AS n "
            "NEXT MATCH (q:Person) FILTER q.name = n RETURN q.name AS qn");
        REQUIRE(mapped == filtered);
    }

    SECTION("a piped binding constrains an expansion, not just a bare node") {
        // Alice's KNOWS triangle: Bob and Carol, and none of the disjoint Dave/Erin/Frank triangle.
        std::string res = run(
            "MATCH (p:Person) FILTER p.name = 'Alice' RETURN p.name AS n "
            "NEXT MATCH (a:Person {name: n})-[:KNOWS]-(f:Person) RETURN count(f) AS friends");
        INFO("result: " << res);
        REQUIRE(res.find("\"friends\": 2") != std::string::npos);
    }

    SECTION("a literal property map still resolves") {
        std::string res = run("MATCH (p:Person {name: 'Bob'}) RETURN p.name AS n");
        REQUIRE(res.find("\"n\": \"Bob\"") != std::string::npos);
        REQUIRE(res.find("Alice") == std::string::npos);
    }

    graph.Stop().get();
}
