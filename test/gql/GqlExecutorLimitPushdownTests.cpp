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
 * LIMIT may only bound the physical scan when every remaining predicate runs INSIDE
 * that scan. These cases cover predicates the original residual guard missed: inline property
 * filters on a downstream (non-anchor) node, RETURN DISTINCT, and multi-filter anchor scans.
 * Matching rows are always created AFTER non-matching ones so a pushed-down limit scans only
 * non-matching rows and under-returns.
 */

static size_t count_occurrences(const std::string& haystack, const std::string& needle) {
    size_t count = 0, pos = 0;
    while ((pos = haystack.find(needle, pos)) != std::string::npos) { count++; pos += needle.size(); }
    return count;
}

TEST_CASE("LIMIT pushdown residual gaps", "[gql_executor_limit]") {
    auto graph = Graph("gql_limit_pushdown_residual");
    graph.Start().get();
    graph.Clear();
    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "city", "string").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "age", "integer").get();
    graph.shard.local().RelationshipTypeInsertPeered("KNOWS").get();

    SECTION("inline property filter on the target node must not bound the anchor scan") {
        // 15 sources whose target is NOT named X first, then 5 whose target IS.
        for (int i = 0; i < 15; ++i) {
            uint64_t src = graph.shard.local().NodeAddPeered("Person", "miss_src" + std::to_string(i),
                "{\"name\": \"miss_src" + std::to_string(i) + "\", \"city\": \"A\", \"age\": 20}").get();
            uint64_t tgt = graph.shard.local().NodeAddPeered("Person", "miss_tgt" + std::to_string(i),
                "{\"name\": \"NotX\", \"city\": \"A\", \"age\": 20}").get();
            graph.shard.local().RelationshipAddPeered("KNOWS", src, tgt, "{}").get();
        }
        for (int i = 0; i < 5; ++i) {
            uint64_t src = graph.shard.local().NodeAddPeered("Person", "hit_src" + std::to_string(i),
                "{\"name\": \"hit_src" + std::to_string(i) + "\", \"city\": \"A\", \"age\": 20}").get();
            uint64_t tgt = graph.shard.local().NodeAddPeered("Person", "hit_tgt" + std::to_string(i),
                "{\"name\": \"X\", \"city\": \"A\", \"age\": 20}").get();
            graph.shard.local().RelationshipAddPeered("KNOWS", src, tgt, "{}").get();
        }

        std::string res = GqlExecutor::execute(graph,
            std::string("MATCH (a:Person)-[:KNOWS]->(b:Person {name: 'X'}) RETURN a.name AS an LIMIT 5")).get();
        INFO("result: " << res);
        REQUIRE(count_occurrences(res, "hit_src") == 5);
    }

    SECTION("RETURN DISTINCT with LIMIT must dedup over the full scan") {
        for (int i = 0; i < 10; ++i) {
            graph.shard.local().NodeAddPeered("Person", "pa" + std::to_string(i),
                "{\"name\": \"pa" + std::to_string(i) + "\", \"city\": \"A\", \"age\": 20}").get();
        }
        graph.shard.local().NodeAddPeered("Person", "pb", "{\"name\": \"pb\", \"city\": \"B\", \"age\": 20}").get();
        graph.shard.local().NodeAddPeered("Person", "pc", "{\"name\": \"pc\", \"city\": \"C\", \"age\": 20}").get();

        std::string res = GqlExecutor::execute(graph,
            std::string("MATCH (n:Person) RETURN DISTINCT n.city AS city LIMIT 3")).get();
        INFO("result: " << res);
        REQUIRE(count_occurrences(res, "\"city\":") == 3);
    }

    SECTION("multi-filter anchor scan must not truncate per-filter lists before intersecting") {
        for (int i = 0; i < 10; ++i) {
            graph.shard.local().NodeAddPeered("Person", "m30a" + std::to_string(i),
                "{\"name\": \"m30a" + std::to_string(i) + "\", \"city\": \"A\", \"age\": 30}").get();
        }
        for (int i = 0; i < 10; ++i) {
            graph.shard.local().NodeAddPeered("Person", "m40b" + std::to_string(i),
                "{\"name\": \"m40b" + std::to_string(i) + "\", \"city\": \"B\", \"age\": 40}").get();
        }
        graph.shard.local().NodeAddPeered("Person", "target",
            "{\"name\": \"target\", \"city\": \"B\", \"age\": 30}").get();

        std::string res = GqlExecutor::execute(graph,
            std::string("MATCH (n:Person {age: 30, city: 'B'}) RETURN n.name AS nn LIMIT 1")).get();
        INFO("result: " << res);
        REQUIRE(res.find("target") != std::string::npos);
    }

    graph.Stop().get();
}

TEST_CASE("chunked start-node scan matches one-shot scan results", "[gql_executor_limit]") {
    auto graph = Graph("gql_chunked_scan");
    graph.Start().get();
    graph.Clear();
    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().RelationshipTypeInsertPeered("KNOWS").get();

    // 12 persons; only the even-indexed ones point at p0, so odd anchors yield no match and a
    // LIMIT-bounded anchor scan would under-return without scanning past them.
    std::vector<uint64_t> ids;
    for (int i = 0; i < 12; ++i) {
        ids.push_back(graph.shard.local().NodeAddPeered("Person", "p" + std::to_string(i),
            "{\"name\": \"p" + std::to_string(i) + "\"}").get());
    }
    for (int i = 2; i < 12; i += 2) {
        graph.shard.local().RelationshipAddPeered("KNOWS", ids[i], ids[0], "{}").get();
    }

    auto count_rows = [](const std::string& r) {
        size_t c = 0, pos = 0;
        while ((pos = r.find("\"an\":", pos)) != std::string::npos) { c++; pos += 5; }
        return c;
    };

    const size_t saved_chunk = gql_scan_chunk_size;

    SECTION("full result set is identical across chunk boundaries") {
        std::string full = GqlExecutor::execute(graph,
            std::string("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS an")).get();
        gql_scan_chunk_size = 4;
        std::string chunked = GqlExecutor::execute(graph,
            std::string("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS an")).get();
        gql_scan_chunk_size = saved_chunk;
        INFO("full: " << full << " chunked: " << chunked);
        REQUIRE(count_rows(chunked) == 5);
        REQUIRE(chunked == full);
    }

    SECTION("LIMIT stops the paged scan once satisfied without under-returning") {
        gql_scan_chunk_size = 3;
        std::string res = GqlExecutor::execute(graph,
            std::string("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS an LIMIT 4")).get();
        gql_scan_chunk_size = saved_chunk;
        INFO("result: " << res);
        REQUIRE(count_rows(res) == 4);
    }

    graph.Stop().get();
}

/*
 * A pattern with NO edges -- a plain label scan -- was excluded from both the paged scan and the streamed
 * folds, so any aggregate or top-K over a bare label held the whole label (every node WITH its properties)
 * in one vector before folding. At SF1 a date-filtered count of Posts died on std::bad_alloc that way. The
 * scan now pages and the fold consumes each page, so these must be correct AND independent of the page
 * size.
 */
TEST_CASE("aggregates and top-K over a bare label scan page instead of materialising", "[gql_executor_limit]") {
    auto graph = Graph("gql_bare_scan");
    graph.Start().get();
    graph.Clear();
    graph.shard.local().NodeTypeInsertPeered("Post").get();
    graph.shard.local().NodePropertyTypeAddPeered("Post", "score", "integer").get();

    // Scores 0..19, so an aggregate and a top-K both have predictable answers.
    for (int i = 0; i < 20; ++i) {
        graph.shard.local().NodeAddPeered("Post", "post" + std::to_string(i),
            "{\"score\": " + std::to_string(i) + "}").get();
    }

    const size_t saved_chunk = gql_scan_chunk_size;

    auto run = [&graph](const std::string& q) {
        auto query = GqlParser::parse(q);
        GqlOptimizer::optimize(query);
        return GqlExecutor::execute(graph, std::move(query)).get();
    };

    SECTION("a residual-filtered count over a bare label is right at any page size") {
        // The filter has a non-literal right side, so it is NOT pushed into the scan and stays a residual
        // predicate -- exactly the SF1 shape (m.creationDate >= zoned_datetime(...)) that OOMed. Scores
        // 15..19 qualify.
        const std::string q = "MATCH (m:Post) FILTER m.score >= 10 + 5 RETURN count(m) AS n";

        gql_scan_chunk_size = 3;
        std::string paged = run(q);
        gql_scan_chunk_size = saved_chunk;
        std::string one_shot = run(q);

        INFO("paged: " << paged << "  one_shot: " << one_shot);
        REQUIRE(paged == one_shot);
        REQUIRE(paged.find("\"n\": 5") != std::string::npos);
    }

    SECTION("an ungrouped aggregate over a bare label folds every scanned node") {
        const std::string q = "MATCH (m:Post) RETURN max(m.score) AS hi, min(m.score) AS lo, count(m) AS n";
        gql_scan_chunk_size = 4;
        std::string res = run(q);
        gql_scan_chunk_size = saved_chunk;
        INFO("result: " << res);
        REQUIRE(res.find("\"hi\": 19") != std::string::npos);
        REQUIRE(res.find("\"lo\": 0") != std::string::npos);
        REQUIRE(res.find("\"n\": 20") != std::string::npos);
    }

    SECTION("ORDER BY + LIMIT over a bare label keeps the true top-K across page boundaries") {
        const std::string q = "MATCH (m:Post) RETURN m.score AS s ORDER BY m.score DESC LIMIT 3";
        gql_scan_chunk_size = 3;
        std::string paged = run(q);
        gql_scan_chunk_size = saved_chunk;
        std::string one_shot = run(q);

        INFO("paged: " << paged << "  one_shot: " << one_shot);
        REQUIRE(paged == one_shot);
        // The winners are the last page's rows, so a heap that only saw one page would get this wrong.
        REQUIRE(paged.find("\"s\": 19") != std::string::npos);
        REQUIRE(paged.find("\"s\": 18") != std::string::npos);
        REQUIRE(paged.find("\"s\": 17") != std::string::npos);
        REQUIRE(paged.find("\"s\": 16") == std::string::npos);
    }

    SECTION("a grouped aggregate over a bare label is right at any page size") {
        const std::string q = "MATCH (m:Post) FILTER m.score < 4 RETURN m.score AS s, count(m) AS n ORDER BY m.score ASC";
        gql_scan_chunk_size = 2;
        std::string paged = run(q);
        gql_scan_chunk_size = saved_chunk;
        std::string one_shot = run(q);
        INFO("paged: " << paged << "  one_shot: " << one_shot);
        REQUIRE(paged == one_shot);
        REQUIRE(paged.find("\"s\": 0, \"n\": 1") != std::string::npos);
        REQUIRE(paged.find("\"s\": 3, \"n\": 1") != std::string::npos);
    }

    gql_scan_chunk_size = saved_chunk;
    graph.Stop().get();
}

/*
 * A LIMIT bounds the RESULT rows. An aggregate folds the matched rows into far fewer result rows, so the
 * LIMIT must not be pushed into the scan -- doing so truncates the rows the aggregate folds over and
 * answers with the limit instead of the aggregate. At SF1 `RETURN count(f) ... LIMIT 1` returned 1 for a
 * person with 848 friends. The executor's own scan-limit gate refused this; the pushdown pass
 * did not.
 */
TEST_CASE("LIMIT is not pushed into the scan when the query aggregates", "[gql_executor_limit]") {
    auto graph = Graph("gql_limit_aggregate");
    graph.Start().get();
    graph.Clear();
    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().RelationshipTypeInsertPeered("KNOWS").get();

    // One hub with 6 friends, so any scan truncation shows up as a wrong count.
    uint64_t hub = graph.shard.local().NodeAddPeered("Person", "hub", "{\"name\": \"Hub\"}").get();
    for (int i = 0; i < 6; ++i) {
        uint64_t f = graph.shard.local().NodeAddPeered("Person", "f" + std::to_string(i),
            "{\"name\": \"F" + std::to_string(i) + "\"}").get();
        graph.shard.local().RelationshipAddPeered("KNOWS", hub, f, "{}").get();
    }

    auto run = [&graph](const std::string& q) {
        auto query = GqlParser::parse(q);
        GqlOptimizer::optimize(query);
        return GqlExecutor::execute(graph, std::move(query)).get();
    };

    SECTION("count with a LIMIT still counts every matched row") {
        std::string truth = run("MATCH (p:Person {name: 'Hub'})-[:KNOWS]->(f:Person) RETURN count(f) AS n");
        std::string limited = run("MATCH (p:Person {name: 'Hub'})-[:KNOWS]->(f:Person) RETURN count(f) AS n LIMIT 1");
        INFO("truth: " << truth << "  limited: " << limited);
        REQUIRE(truth.find("\"n\": 6") != std::string::npos);
        // The LIMIT bounds the single aggregate result row, so the count is unchanged.
        REQUIRE(limited == truth);
    }

    SECTION("collect_list with a LIMIT still gathers every matched row") {
        // The SF1 shape: collect_list came back holding a single element because the scan had been
        // truncated to the LIMIT before the accumulator ever saw the rest.
        std::string res = run("MATCH (p:Person {name: 'Hub'})-[:KNOWS]->(f:Person) "
                              "RETURN collect_list(f.name) AS names LIMIT 1");
        INFO("result: " << res);
        for (int i = 0; i < 6; ++i) {
            REQUIRE(res.find("\"F" + std::to_string(i) + "\"") != std::string::npos);
        }
    }

    SECTION("a non-aggregating LIMIT is still pushed down and bounds the rows") {
        std::string res = run("MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN f.name AS n LIMIT 2");
        INFO("result: " << res);
        size_t rows = 0, pos = 0;
        while ((pos = res.find("\"n\":", pos)) != std::string::npos) { rows++; pos += 4; }
        REQUIRE(rows == 2);
    }

    graph.Stop().get();
}
