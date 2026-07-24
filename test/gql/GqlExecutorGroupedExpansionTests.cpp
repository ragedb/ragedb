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
 * Grouped aggregation over a multi-hop expansion--the shape that OOMs at scale:
 *   MATCH (c:Comment)-[:HAS_CREATOR]->(p:Person) RETURN p.name, count(c) ...
 * The executor flattens the whole (comment, person) expansion before grouping, so at SF1 scale
 * (~1.7M comments) it exhausts memory. These tests pin the CORRECT small-graph behaviour so a
 * streaming/factorized aggregate rewrite can be proven not to change results.
 *
 * Data: three people, six comments--Alice authored 3, Bob 2, Carol 1; comment scores are
 * Alice {1,2,3}, Bob {4,5}, Carol {10}.
 */
static void populate_expansion_graph(Graph& graph) {
    graph.Clear();

    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().NodeTypeInsertPeered("Comment").get();
    graph.shard.local().NodePropertyTypeAddPeered("Comment", "score", "integer").get();
    graph.shard.local().RelationshipTypeInsertPeered("HAS_CREATOR").get();

    uint64_t alice = graph.shard.local().NodeAddPeered("Person", "alice", "{\"name\": \"Alice\"}").get();
    uint64_t bob = graph.shard.local().NodeAddPeered("Person", "bob", "{\"name\": \"Bob\"}").get();
    uint64_t carol = graph.shard.local().NodeAddPeered("Person", "carol", "{\"name\": \"Carol\"}").get();

    auto add_comment = [&](const std::string& key, int64_t score, uint64_t creator) {
        uint64_t c = graph.shard.local().NodeAddPeered("Comment", key, "{\"score\": " + std::to_string(score) + "}").get();
        graph.shard.local().RelationshipAddPeered("HAS_CREATOR", c, creator, "{}").get();
    };
    add_comment("a1", 1, alice);
    add_comment("a2", 2, alice);
    add_comment("a3", 3, alice);
    add_comment("b1", 4, bob);
    add_comment("b2", 5, bob);
    add_comment("c1", 10, carol);
}

TEST_CASE("GQL grouped aggregation over expansion: count grouped by creator", "[gql_executor_aggregation][grouped_expansion]") {
    auto graph = Graph("gql_grouped_expansion_test");
    graph.Start().get();
    populate_expansion_graph(graph);

    SECTION("count(c) grouped by person, ordered by count descending") {
        std::string query = "MATCH (c:Comment)-[:HAS_CREATOR]->(p:Person) RETURN p.name AS name, count(c) AS n ORDER BY count(c) DESC";
        std::string res = GqlExecutor::execute(graph, GqlParser::parse(query)).get();

        // Every group present with the right count.
        REQUIRE(res.find("\"name\": \"Alice\", \"n\": 3") != std::string::npos);
        REQUIRE(res.find("\"name\": \"Bob\", \"n\": 2") != std::string::npos);
        REQUIRE(res.find("\"name\": \"Carol\", \"n\": 1") != std::string::npos);

        // Ordered by count DESC: Alice(3) before Bob(2) before Carol(1).
        REQUIRE(res.find("Alice") < res.find("Bob"));
        REQUIRE(res.find("Bob") < res.find("Carol"));
    }

    SECTION("count(*) grouped by person") {
        std::string query = "MATCH (c:Comment)-[:HAS_CREATOR]->(p:Person) RETURN p.name AS name, count(*) AS n ORDER BY p.name ASC";
        std::string res = GqlExecutor::execute(graph, GqlParser::parse(query)).get();
        REQUIRE(res.find("\"name\": \"Alice\", \"n\": 3") != std::string::npos);
        REQUIRE(res.find("\"name\": \"Bob\", \"n\": 2") != std::string::npos);
        REQUIRE(res.find("\"name\": \"Carol\", \"n\": 1") != std::string::npos);
    }

    SECTION("top-k: ORDER BY count(c) DESC LIMIT 2 drops the smallest group") {
        std::string query = "MATCH (c:Comment)-[:HAS_CREATOR]->(p:Person) RETURN p.name AS name, count(c) AS n ORDER BY count(c) DESC LIMIT 2";
        std::string res = GqlExecutor::execute(graph, GqlParser::parse(query)).get();
        REQUIRE(res.find("Alice") != std::string::npos);
        REQUIRE(res.find("Bob") != std::string::npos);
        REQUIRE(res.find("Carol") == std::string::npos);
    }

    graph.Stop().get();
}

TEST_CASE("GQL grouped aggregation over expansion: sum/min/max of an expansion property", "[gql_executor_aggregation][grouped_expansion]") {
    auto graph = Graph("gql_grouped_expansion_test");
    graph.Start().get();
    populate_expansion_graph(graph);

    std::string query = "MATCH (c:Comment)-[:HAS_CREATOR]->(p:Person) RETURN p.name AS name, sum(c.score) AS s, min(c.score) AS mn, max(c.score) AS mx ORDER BY p.name ASC";
    std::string res = GqlExecutor::execute(graph, GqlParser::parse(query)).get();

    // Alice scores {1,2,3}: sum 6, min 1, max 3.
    REQUIRE(res.find("\"name\": \"Alice\", \"s\": 6, \"mn\": 1, \"mx\": 3") != std::string::npos);
    // Bob scores {4,5}: sum 9, min 4, max 5.
    REQUIRE(res.find("\"name\": \"Bob\", \"s\": 9, \"mn\": 4, \"mx\": 5") != std::string::npos);
    // Carol score {10}: sum 10, min 10, max 10.
    REQUIRE(res.find("\"name\": \"Carol\", \"s\": 10, \"mn\": 10, \"mx\": 10") != std::string::npos);

    graph.Stop().get();
}

TEST_CASE("GQL grouped aggregation over expansion: ungrouped count/sum over the whole expansion", "[gql_executor_aggregation][grouped_expansion]") {
    auto graph = Graph("gql_grouped_expansion_test");
    graph.Start().get();
    populate_expansion_graph(graph);

    SECTION("ungrouped count(c) counts every expansion row") {
        std::string query = "MATCH (c:Comment)-[:HAS_CREATOR]->(p:Person) RETURN count(c) AS n";
        std::string res = GqlExecutor::execute(graph, GqlParser::parse(query)).get();
        REQUIRE(res.find("\"n\": 6") != std::string::npos);
    }

    SECTION("ungrouped sum(c.score) folds every expansion row") {
        std::string query = "MATCH (c:Comment)-[:HAS_CREATOR]->(p:Person) RETURN sum(c.score) AS s";
        std::string res = GqlExecutor::execute(graph, GqlParser::parse(query)).get();
        // 1+2+3+4+5+10 = 25
        REQUIRE(res.find("\"s\": 25") != std::string::npos);
    }

    graph.Stop().get();
}

/*
 * A var-length expansion (the friend-of-friend shape) used to build every path from a start node into
 * one vector before a single row reached the aggregate -- one person's KNOWS{1,3} neighbourhood at SF1
 * is over a million paths, each carrying the nodes and relationships along it, so a bare count(*)
 * exhausted the heap. The expansion now streams its finished paths to the aggregate and
 * expands a node's branches with a bounded number in flight.
 *
 * A six-person ring with one chord, so a 1..3 hop expansion produces many more paths than persons.
 */
static void populate_knows_ring(Graph& graph) {
    graph.Clear();

    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().RelationshipTypeInsertPeered("KNOWS").get();

    std::vector<uint64_t> people;
    for (int i = 0; i < 6; ++i) {
        std::string name = "P" + std::to_string(i);
        people.push_back(graph.shard.local().NodeAddPeered("Person", name, "{\"name\": \"" + name + "\"}").get());
    }
    auto knows = [&](size_t a, size_t b) {
        graph.shard.local().RelationshipAddPeered("KNOWS", people[a], people[b], "{}").get();
    };
    knows(0, 1); knows(1, 2); knows(2, 3); knows(3, 4); knows(4, 5); knows(5, 0);
    knows(0, 3);
}

TEST_CASE("GQL var-length expansion streams into the aggregate", "[gql_executor_aggregation][grouped_expansion]") {
    auto graph = Graph("gql_var_len_stream_test");
    graph.Start().get();
    populate_knows_ring(graph);

    auto run = [&graph](const std::string& query_str) {
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        return GqlExecutor::execute(graph, std::move(query)).get();
    };

    // Same expansion, counted two ways. Binding a path variable keeps the query off the streaming fold,
    // so this pins the streamed count against the materialised one.
    const std::string streamed_q =
        "MATCH (a:Person)-[:KNOWS]-{1,3}(b:Person) FILTER a.name = 'P0' RETURN count(*) AS n";
    const std::string materialised_q =
        "MATCH p = (a:Person)-[:KNOWS]-{1,3}(b:Person) FILTER a.name = 'P0' RETURN count(*) AS n";

    SECTION("streamed count matches the materialised count") {
        REQUIRE(run(streamed_q) == run(materialised_q));
        // The expansion is non-trivial: more paths than there are people.
        REQUIRE(run(streamed_q).find("\"n\": 0") == std::string::npos);
    }

    SECTION("result is invariant under the branch-concurrency and hop-batch bounds") {
        const size_t saved_branch = gql_var_len_branch_concurrency;
        const size_t saved_batch = gql_var_len_hop_batch;

        std::string serial;
        std::string batched;
        try {
            // One branch in flight, one path per batch: the slowest, most fragmented path through the
            // streaming code.
            gql_var_len_branch_concurrency = 1;
            gql_var_len_hop_batch = 1;
            serial = run(streamed_q);

            gql_var_len_branch_concurrency = 16;
            gql_var_len_hop_batch = 1024;
            batched = run(streamed_q);
        } catch (...) {
            gql_var_len_branch_concurrency = saved_branch;
            gql_var_len_hop_batch = saved_batch;
            throw;
        }
        gql_var_len_branch_concurrency = saved_branch;
        gql_var_len_hop_batch = saved_batch;

        REQUIRE(serial == batched);
        REQUIRE(serial == run(materialised_q));
    }

    SECTION("sum over the streamed expansion folds every path") {
        // Every path's endpoint contributes, so the streamed sum must match the materialised one.
        const std::string streamed_sum =
            "MATCH (a:Person)-[:KNOWS]-{1,2}(b:Person) FILTER a.name = 'P0' RETURN count(b) AS n";
        const std::string materialised_sum =
            "MATCH p = (a:Person)-[:KNOWS]-{1,2}(b:Person) FILTER a.name = 'P0' RETURN count(b) AS n";
        REQUIRE(run(streamed_sum) == run(materialised_sum));
    }

    graph.Stop().get();
}
