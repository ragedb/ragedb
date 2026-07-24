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
 * ISO GQL OFFSET: the page is [offset, offset + limit) of the ORDERED result. Every path that
 * prunes rows early -- the streamed top-K heap, the streamed group folds, the sort-with-limit sites and the
 * scan-limit pushdown -- has to retain offset + limit rows rather than just limit, or it throws away
 * exactly the rows the page returns. These tests pin that across the paths.
 *
 * Data: ten people named P0..P9 with rank 0..9, so an ordered projection is trivially predictable, plus a
 * Post per person (rank + 1 of them) to drive the grouped/aggregate paths.
 */
static void populate_paging_graph(Graph& graph) {
    graph.Clear();

    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "rank", "integer").get();
    graph.shard.local().NodeTypeInsertPeered("Post").get();
    graph.shard.local().RelationshipTypeInsertPeered("HAS_CREATOR").get();

    for (int i = 0; i < 10; ++i) {
        std::string name = "P" + std::to_string(i);
        uint64_t p = graph.shard.local().NodeAddPeered(
            "Person", name, "{\"name\": \"" + name + "\", \"rank\": " + std::to_string(i) + "}").get();

        // Person i creates i + 1 posts, so a count grouped by person orders the same way as rank.
        for (int j = 0; j <= i; ++j) {
            uint64_t post = graph.shard.local().NodeAddPeered(
                "Post", name + "_post" + std::to_string(j), "{}").get();
            graph.shard.local().RelationshipAddPeered("HAS_CREATOR", post, p, "{}").get();
        }
    }
}

/*
 * ISO GQL's default path mode is WALK -- "the absence of any filtering" -- so an unqualified quantified
 * pattern may repeat both nodes AND edges. ragedb previously defaulted to TRAIL (no repeated edges), which
 * is Cypher's relationship-uniqueness rule, not GQL's: it silently dropped every path that reuses an edge.
 * A query that wants trail semantics must now say TRAIL.
 *
 * Two people joined by a single undirected edge make the difference unmissable: under WALK, A-B-A is a
 * legal 2-hop walk (it re-crosses the one edge); under TRAIL it is not.
 */
TEST_CASE("the default path mode is WALK, not TRAIL", "[gql_executor_paging]") {
    auto graph = Graph("gql_path_mode_default");
    graph.Start().get();
    graph.Clear();
    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().RelationshipTypeInsertPeered("KNOWS").get();

    uint64_t a = graph.shard.local().NodeAddPeered("Person", "a", "{\"name\": \"A\"}").get();
    uint64_t b = graph.shard.local().NodeAddPeered("Person", "b", "{\"name\": \"B\"}").get();
    graph.shard.local().RelationshipAddPeered("KNOWS", a, b, "{}").get();

    auto run = [&graph](const std::string& q) {
        auto query = GqlParser::parse(q);
        GqlOptimizer::optimize(query);
        return GqlExecutor::execute(graph, std::move(query)).get();
    };

    SECTION("an unqualified quantified pattern walks, so it may re-cross the same edge") {
        // Paths of length 1..2 from A: A-B (1 hop) and A-B-A (2 hops, re-crossing the single edge).
        std::string res = run("MATCH (p:Person)-[:KNOWS]-{1,2}(q:Person) FILTER p.name = 'A' RETURN count(*) AS n");
        INFO("result: " << res);
        REQUIRE(res.find("\"n\": 2") != std::string::npos);
    }

    SECTION("TRAIL still forbids reusing an edge, when the query asks for it") {
        // Only A-B survives: A-B-A would have to re-cross the same edge.
        std::string res = run("MATCH TRAIL (p:Person)-[:KNOWS]-{1,2}(q:Person) FILTER p.name = 'A' RETURN count(*) AS n");
        INFO("result: " << res);
        REQUIRE(res.find("\"n\": 1") != std::string::npos);
    }

    graph.Stop().get();
}

/*
 * An unbounded quantifier ({n,} or *) under WALK has no finite result on cyclic data -- a walk may
 * re-cross an edge -- so the traversal must fail loudly rather than exhaust the heap. Two guards, each
 * applying ONLY to that case: a depth cap for a low-branching cycle, and a path-count budget for an
 * exponential blow-up. A bounded quantifier and a restrictor are already finite and must be unaffected.
 */
TEST_CASE("an unbounded WALK on cyclic data fails loudly instead of diverging", "[gql_executor_paging][gql_walk_unbounded]") {
    auto graph = Graph("gql_walk_unbounded");
    graph.Start().get();
    graph.Clear();
    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().RelationshipTypeInsertPeered("KNOWS").get();

    // A ring: every node knows the next, and the last closes back to the first -- so every node is on a
    // cycle and an unbounded walk never terminates.
    std::vector<uint64_t> ids;
    for (int i = 0; i < 5; ++i) {
        ids.push_back(graph.shard.local().NodeAddPeered("Person", "p" + std::to_string(i),
            "{\"name\": \"P" + std::to_string(i) + "\"}").get());
    }
    for (int i = 0; i < 5; ++i) {
        graph.shard.local().RelationshipAddPeered("KNOWS", ids[i], ids[(i + 1) % 5], "{}").get();
    }

    auto run = [&graph](const std::string& q) {
        auto query = GqlParser::parse(q);
        GqlOptimizer::optimize(query);
        return GqlExecutor::execute(graph, std::move(query)).get();
    };

    const uint64_t saved_depth = gql_var_len_walk_depth_cap;
    const uint64_t saved_budget = gql_var_len_walk_path_budget;

    SECTION("the depth cap stops a low-branching cycle") {
        gql_var_len_walk_depth_cap = 8;
        gql_var_len_walk_path_budget = saved_budget;
        try {
            REQUIRE_THROWS_WITH(
                run("MATCH (a:Person)-[:KNOWS]->{1,}(b:Person) FILTER a.name = 'P0' RETURN count(*) AS n"),
                Catch::Contains("unbounded quantifier"));
        } catch (...) { gql_var_len_walk_depth_cap = saved_depth; gql_var_len_walk_path_budget = saved_budget; throw; }
        gql_var_len_walk_depth_cap = saved_depth;
    }

    SECTION("the path budget stops an exponential blow-up before the depth cap") {
        gql_var_len_walk_depth_cap = saved_depth;   // high, so the budget is what trips
        gql_var_len_walk_path_budget = 50;
        try {
            REQUIRE_THROWS_WITH(
                run("MATCH (a:Person)-[:KNOWS]-{1,}(b:Person) FILTER a.name = 'P0' RETURN count(*) AS n"),
                Catch::Contains("unbounded quantifier"));
        } catch (...) { gql_var_len_walk_depth_cap = saved_depth; gql_var_len_walk_path_budget = saved_budget; throw; }
        gql_var_len_walk_path_budget = saved_budget;
    }

    SECTION("a bounded quantifier and a restrictor are unaffected by the guards") {
        // Both guards shrunk to tiny values: a bounded {1,3} and an unbounded TRAIL must still complete,
        // because neither is the divergent case the guards target.
        gql_var_len_walk_depth_cap = 3;
        gql_var_len_walk_path_budget = 5;
        try {
            REQUIRE_NOTHROW(run("MATCH (a:Person)-[:KNOWS]->{1,3}(b:Person) FILTER a.name = 'P0' RETURN count(*) AS n"));
            REQUIRE_NOTHROW(run("MATCH TRAIL (a:Person)-[:KNOWS]->{1,}(b:Person) FILTER a.name = 'P0' RETURN count(*) AS n"));
        } catch (...) { gql_var_len_walk_depth_cap = saved_depth; gql_var_len_walk_path_budget = saved_budget; throw; }
        gql_var_len_walk_depth_cap = saved_depth;
        gql_var_len_walk_path_budget = saved_budget;
    }

    gql_var_len_walk_depth_cap = saved_depth;
    gql_var_len_walk_path_budget = saved_budget;
    graph.Stop().get();
}

TEST_CASE("ISO GQL OFFSET pages the ordered result", "[gql_executor_paging]") {
    auto graph = Graph("gql_paging_test");
    graph.Start().get();
    populate_paging_graph(graph);

    auto run = [&graph](const std::string& query_str) {
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        return GqlExecutor::execute(graph, std::move(query)).get();
    };

    SECTION("OFFSET skips the first rows of the ordered result") {
        std::string res = run("MATCH (p:Person) RETURN p.name AS n ORDER BY p.rank ASC OFFSET 3 LIMIT 2");
        INFO("result: " << res);
        // Ranks 0,1,2 are skipped; the page is P3, P4.
        REQUIRE(res.find("P3") != std::string::npos);
        REQUIRE(res.find("P4") != std::string::npos);
        REQUIRE(res.find("P2") == std::string::npos);
        REQUIRE(res.find("P5") == std::string::npos);
    }

    SECTION("LIMIT before OFFSET parses to the same page") {
        REQUIRE(run("MATCH (p:Person) RETURN p.name AS n ORDER BY p.rank ASC LIMIT 2 OFFSET 3") ==
                run("MATCH (p:Person) RETURN p.name AS n ORDER BY p.rank ASC OFFSET 3 LIMIT 2"));
    }

    SECTION("OFFSET without LIMIT returns the rest of the ordered result") {
        std::string res = run("MATCH (p:Person) RETURN p.name AS n ORDER BY p.rank ASC OFFSET 8");
        INFO("result: " << res);
        REQUIRE(res.find("P8") != std::string::npos);
        REQUIRE(res.find("P9") != std::string::npos);
        REQUIRE(res.find("P7") == std::string::npos);
    }

    SECTION("an OFFSET past the end returns no rows") {
        REQUIRE(run("MATCH (p:Person) RETURN p.name AS n ORDER BY p.rank ASC OFFSET 50 LIMIT 5") == "[]");
    }

    SECTION("consecutive pages tile the ordered result without gaps or repeats") {
        // The union of OFFSET 0 LIMIT 5 and OFFSET 5 LIMIT 5 must be every person, each once.
        std::string first = run("MATCH (p:Person) RETURN p.name AS n ORDER BY p.rank ASC OFFSET 0 LIMIT 5");
        std::string second = run("MATCH (p:Person) RETURN p.name AS n ORDER BY p.rank ASC OFFSET 5 LIMIT 5");
        INFO("first: " << first << "  second: " << second);
        for (int i = 0; i < 5; ++i) {
            std::string name = "P" + std::to_string(i);
            REQUIRE(first.find("\"" + name + "\"") != std::string::npos);
            REQUIRE(second.find("\"" + name + "\"") == std::string::npos);
        }
        for (int i = 5; i < 10; ++i) {
            std::string name = "P" + std::to_string(i);
            REQUIRE(second.find("\"" + name + "\"") != std::string::npos);
            REQUIRE(first.find("\"" + name + "\"") == std::string::npos);
        }
    }

    SECTION("OFFSET pages a grouped aggregate, not just a plain projection") {
        // Person i has i + 1 posts, so DESC by count puts P9 (10) first; skipping 2 lands on P7 (8).
        std::string res = run(
            "MATCH (post:Post)-[:HAS_CREATOR]->(p:Person) "
            "RETURN p.name AS n, count(post) AS c ORDER BY count(post) DESC OFFSET 2 LIMIT 1");
        INFO("result: " << res);
        REQUIRE(res.find("\"n\": \"P7\"") != std::string::npos);
        REQUIRE(res.find("\"c\": 8") != std::string::npos);
        REQUIRE(res.find("P9") == std::string::npos);
        REQUIRE(res.find("P8") == std::string::npos);
    }

    SECTION("a paged query returns the same window the unpaged result would have yielded") {
        // The scan-limit pushdown and the top-K heap both prune early; if either kept only `limit` rows
        // instead of offset + limit, this page would come back short or wrong.
        std::string paged = run("MATCH (p:Person) RETURN p.name AS n ORDER BY p.rank ASC OFFSET 6 LIMIT 3");
        INFO("paged: " << paged);
        REQUIRE(paged.find("P6") != std::string::npos);
        REQUIRE(paged.find("P7") != std::string::npos);
        REQUIRE(paged.find("P8") != std::string::npos);
        REQUIRE(paged.find("P9") == std::string::npos);
        REQUIRE(paged.find("P5") == std::string::npos);
    }

    graph.Stop().get();
}
