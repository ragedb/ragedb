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

TEST_CASE("GQL Execution Correlated Subquery Tests", "[gql_executor_subquery]") {
    auto graph = Graph("gql_test_subquery");
    graph.Start().get();
    graph.Clear();
    GraphStopGuard guard(graph);
    
    // Setup schemas
    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().RelationshipTypeInsertPeered("FRIEND").get();

    // Create test nodes
    uint64_t id1 = graph.shard.local().NodeAddPeered("Person", "alice", "{\"name\": \"Alice\"}").get();
    uint64_t id2 = graph.shard.local().NodeAddPeered("Person", "bob", "{\"name\": \"Bob\"}").get();
    uint64_t id3 = graph.shard.local().NodeAddPeered("Person", "charlie", "{\"name\": \"Charlie\"}").get();

    // Alice is friends with Bob. Charlie has no friends.
    graph.shard.local().RelationshipAddPeered("FRIEND", id1, id2, "{}").get();

    SECTION("Correlated EXISTS subquery") {
        std::string query_str = "MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:FRIEND]->(f) } RETURN p.name";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        std::string results = GqlExecutor::execute(graph, std::move(query)).get();
        REQUIRE(results.find("Alice") != std::string::npos);
        REQUIRE(results.find("Bob") == std::string::npos);
        REQUIRE(results.find("Charlie") == std::string::npos);
    }

    SECTION("Correlated EXISTS subquery with filter") {
        std::string query_str = "MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:FRIEND]->(f) WHERE f.name = 'Bob' } RETURN p.name";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        std::string results = GqlExecutor::execute(graph, std::move(query)).get();
        REQUIRE(results.find("Alice") != std::string::npos);
        REQUIRE(results.find("Bob") == std::string::npos);
        REQUIRE(results.find("Charlie") == std::string::npos);
    }

    SECTION("Correlated NOT EXISTS subquery") {
        std::string query_str = "MATCH (p:Person) WHERE NOT EXISTS { MATCH (p)-[:FRIEND]->(f) } RETURN p.name";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        std::string results = GqlExecutor::execute(graph, std::move(query)).get();
        REQUIRE(results.find("Alice") == std::string::npos);
        REQUIRE(results.find("Bob") != std::string::npos);
        REQUIRE(results.find("Charlie") != std::string::npos);
    }

    guard.stop();
}

// A COUNT{} subquery whose far node carries a label/property filter cannot be answered by the far-node-blind
// degree rewrite -- it must count actual matching pattern rows. Here a person created 2 Posts and 3 Comments;
// COUNT { (p)<-[:HAS_CREATOR]-(:Post) } must be 2, not 5 (all HAS_CREATOR edges). Also covers COUNT{} in a
// LET (which the degree rewrite never populated -> was null) and the bare unconstrained fast path.
TEST_CASE("GQL COUNT subquery applies far-node filter and works in LET", "[gql_executor_count_subquery]") {
    auto graph = Graph("gql_test_count_subquery");
    graph.Start().get();
    graph.Clear();
    GraphStopGuard guard(graph);

    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "id", "integer").get();
    graph.shard.local().NodeTypeInsertPeered("Post").get();
    graph.shard.local().NodeTypeInsertPeered("Comment").get();
    graph.shard.local().RelationshipTypeInsertPeered("HAS_CREATOR").get();

    uint64_t p = graph.shard.local().NodeAddPeered("Person", "p1", "{\"id\": 1}").get();
    for (int i = 0; i < 2; ++i) {
        uint64_t post = graph.shard.local().NodeAddPeered("Post", "post" + std::to_string(i), "{}").get();
        graph.shard.local().RelationshipAddPeered("HAS_CREATOR", post, p, "{}").get();
    }
    for (int i = 0; i < 3; ++i) {
        uint64_t c = graph.shard.local().NodeAddPeered("Comment", "c" + std::to_string(i), "{}").get();
        graph.shard.local().RelationshipAddPeered("HAS_CREATOR", c, p, "{}").get();
    }

    auto run = [&graph](const std::string& q) {
        auto query = GqlParser::parse(q);
        GqlOptimizer::optimize(query);
        return GqlExecutor::execute(graph, std::move(query)).get();
    };

    SECTION("far-node label filter is applied (2 Posts, not 5 HAS_CREATOR edges)") {
        std::string r = run("MATCH (p:Person {id: 1}) RETURN COUNT { (p)<-[:HAS_CREATOR]-(:Post) } AS c");
        INFO("posts count: " << r);
        REQUIRE(r.find("\"c\": 2") != std::string::npos);
    }

    SECTION("COUNT{} bound in a LET is evaluated, not null") {
        std::string r = run("MATCH (p:Person {id: 1}) LET total = COUNT { (p)<-[:HAS_CREATOR]-(:Post) } RETURN total");
        INFO("let total: " << r);
        REQUIRE(r.find("\"total\": 2") != std::string::npos);
        REQUIRE(r.find("null") == std::string::npos);
    }

    SECTION("bare unconstrained COUNT{} still counts all edges (degree fast path)") {
        std::string r = run("MATCH (p:Person {id: 1}) RETURN COUNT { (p)<-[:HAS_CREATOR]-() } AS c");
        INFO("all edges: " << r);
        REQUIRE(r.find("\"c\": 5") != std::string::npos);
    }

    SECTION("EXISTS inside a projection CASE is precomputed, not silently false (spb q9 shape)") {
        // An EXISTS as a value (inside CASE/aggregate) is not reached by the WHERE-only semi-join rewrite;
        // it must be precomputed. Before that, both branches evaluated the EXISTS as false.
        std::string r = run(
            "MATCH (p:Person {id: 1}) "
            "RETURN CASE WHEN EXISTS { (p)<-[:HAS_CREATOR]-(:Post) } THEN 1 ELSE 0 END AS created, "
            "       CASE WHEN EXISTS { (p)-[:HAS_CREATOR]->(:Post) } THEN 1 ELSE 0 END AS authoredBy");
        INFO("projection exists: " << r);
        REQUIRE(r.find("\"created\": 1") != std::string::npos);     // p has incoming HAS_CREATOR from Posts
        REQUIRE(r.find("\"authoredBy\": 0") != std::string::npos);  // no outgoing HAS_CREATOR from p
    }

    guard.stop();
}

// A COUNT{} whose own WHERE nests an EXISTS is a correlated subquery the far-node-blind degree rewrite
// cannot touch and the base COUNT{} precompute skipped. The precompute now recurses: it traverses the
// COUNT pattern anchored on the outer row (carrying p and foaf), then for each sub-row resolves the nested
// EXISTS before filtering. This is the IC10 "common" shape -- count a friend-of-a-friend's posts whose tag
// is also one the person is interested in. foaf created 3 posts (tags t1, t2, t1); the person is interested
// only in t1, so common = 2.
TEST_CASE("GQL COUNT{} with a nested EXISTS in its WHERE (IC10 common)", "[gql_executor_count_subquery]") {
    auto graph = Graph("gql_test_common_subquery");
    graph.Start().get();
    graph.Clear();
    GraphStopGuard guard(graph);

    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "id", "integer").get();
    graph.shard.local().NodeTypeInsertPeered("Post").get();
    graph.shard.local().NodeTypeInsertPeered("Tag").get();
    graph.shard.local().NodePropertyTypeAddPeered("Tag", "id", "integer").get();
    graph.shard.local().RelationshipTypeInsertPeered("HAS_CREATOR").get();
    graph.shard.local().RelationshipTypeInsertPeered("HAS_TAG").get();
    graph.shard.local().RelationshipTypeInsertPeered("HAS_INTEREST").get();

    uint64_t p = graph.shard.local().NodeAddPeered("Person", "p1", "{\"id\": 1}").get();
    uint64_t foaf = graph.shard.local().NodeAddPeered("Person", "p2", "{\"id\": 2}").get();
    uint64_t t1 = graph.shard.local().NodeAddPeered("Tag", "t1", "{\"id\": 1}").get();
    uint64_t t2 = graph.shard.local().NodeAddPeered("Tag", "t2", "{\"id\": 2}").get();

    // The person is interested only in t1.
    graph.shard.local().RelationshipAddPeered("HAS_INTEREST", p, t1, "{}").get();

    // foaf created three posts, tagged t1, t2, t1 respectively.
    uint64_t tags[3] = {t1, t2, t1};
    for (int i = 0; i < 3; ++i) {
        uint64_t post = graph.shard.local().NodeAddPeered("Post", "post" + std::to_string(i), "{}").get();
        graph.shard.local().RelationshipAddPeered("HAS_CREATOR", post, foaf, "{}").get();
        graph.shard.local().RelationshipAddPeered("HAS_TAG", post, tags[i], "{}").get();
    }

    auto run = [&graph](const std::string& q) {
        auto query = GqlParser::parse(q);
        GqlOptimizer::optimize(query);
        return GqlExecutor::execute(graph, std::move(query)).get();
    };

    SECTION("posts whose tag the person shares (2 of 3)") {
        std::string r = run(
            "MATCH (p:Person {id: 1}) MATCH (foaf:Person {id: 2}) "
            "RETURN COUNT { (foaf)<-[:HAS_CREATOR]-(post:Post) "
            "               WHERE EXISTS { (post)-[:HAS_TAG]->(:Tag)<-[:HAS_INTEREST]-(p) } } AS common");
        INFO("common: " << r);
        REQUIRE(r.find("\"common\": 2") != std::string::npos);
    }

    guard.stop();
}

TEST_CASE("a positive EXISTS filter is a semi-join rather than a row multiplier", "[gql_executor_count_subquery]") {
    // A positive EXISTS in a WHERE is unnested into an OPTIONAL MATCH, which yields one row per
    // subquery match. Left as-is that multiplies the outer rows, so a work matching the subquery
    // twice would be counted twice -- the SPB a5 shape. The rows are deduplicated on the outer
    // variables after the filter; this pins that, since nothing else covers the many-valued case.
    auto graph = Graph("gql_test_exists_semijoin");
    graph.Start().get();
    graph.Clear();
    GraphStopGuard guard(graph);

    graph.shard.local().NodeTypeInsertPeered("CreativeWork").get();
    graph.shard.local().NodeTypeInsertPeered("Thing").get();
    graph.shard.local().NodePropertyTypeAddPeered("Thing", "uri", "string").get();
    graph.shard.local().NodeTypeInsertPeered("Category").get();
    graph.shard.local().NodePropertyTypeAddPeered("Category", "uri", "string").get();
    graph.shard.local().RelationshipTypeInsertPeered("about").get();
    graph.shard.local().RelationshipTypeInsertPeered("category").get();

    uint64_t thing = graph.shard.local().NodeAddPeered("Thing", "t1", "{\"uri\": \"thing/1\"}").get();
    uint64_t company = graph.shard.local().NodeAddPeered("Category", "company", "{\"uri\": \"cat/Company\"}").get();
    uint64_t event = graph.shard.local().NodeAddPeered("Category", "event", "{\"uri\": \"cat/Event\"}").get();
    uint64_t other = graph.shard.local().NodeAddPeered("Category", "other", "{\"uri\": \"cat/Other\"}").get();

    // w1 matches the EXISTS twice (both categories), w2 once, w3 not at all.
    uint64_t w1 = graph.shard.local().NodeAddPeered("CreativeWork", "w1", "{}").get();
    graph.shard.local().RelationshipAddPeered("category", w1, company, "{}").get();
    graph.shard.local().RelationshipAddPeered("category", w1, event, "{}").get();
    uint64_t w2 = graph.shard.local().NodeAddPeered("CreativeWork", "w2", "{}").get();
    graph.shard.local().RelationshipAddPeered("category", w2, company, "{}").get();
    uint64_t w3 = graph.shard.local().NodeAddPeered("CreativeWork", "w3", "{}").get();
    graph.shard.local().RelationshipAddPeered("category", w3, other, "{}").get();
    for (uint64_t w : {w1, w2, w3}) {
        graph.shard.local().RelationshipAddPeered("about", w, thing, "{}").get();
    }

    auto run = [&graph](const std::string& q) {
        auto query = GqlParser::parse(q);
        GqlOptimizer::optimize(query);
        return GqlExecutor::execute(graph, std::move(query)).get();
    };

    SECTION("the outer pattern alone sees every work (data control)") {
        std::string r = run("MATCH (w:CreativeWork)-[:about]->(e:Thing) RETURN count(w) AS n");
        INFO("control: " << r);
        REQUIRE(r.find("\"n\": 3") != std::string::npos);
    }

    SECTION("EXISTS with a pushable equality predicate") {
        // A literal comparison becomes a scan filter on the lifted pattern, so it does not depend on
        // the subquery's properties surviving projection pruning.
        std::string r = run(
            "MATCH (w:CreativeWork)-[:about]->(e:Thing) "
            "WHERE EXISTS { MATCH (w)-[:category]->(c) WHERE c.uri = 'cat/Company' } "
            "RETURN count(w) AS n");
        INFO("pushable: " << r);
        REQUIRE(r.find("\"n\": 2") != std::string::npos);   // w1 and w2
    }

    SECTION("EXISTS with an unpushable NOT predicate") {
        // Not an OR: isolates "the predicate cannot become a scan filter" from anything OR-specific.
        std::string r = run(
            "MATCH (w:CreativeWork)-[:about]->(e:Thing) "
            "WHERE EXISTS { MATCH (w)-[:category]->(c) WHERE NOT (c.uri = 'cat/Other') } "
            "RETURN count(w) AS n");
        INFO("unpushable NOT: " << r);
        REQUIRE(r.find("\"n\": 2") != std::string::npos);   // w1 and w2
    }

    SECTION("a work matching the EXISTS twice is counted once (SPB a5 shape)") {
        std::string r = run(
            "MATCH (w:CreativeWork)-[:about]->(e:Thing) "
            "WHERE EXISTS { MATCH (w)-[:category]->(c) "
            "               WHERE c.uri = 'cat/Company' OR c.uri = 'cat/Event' } "
            "RETURN e.uri AS k, count(*) AS n");
        INFO("counts: " << r);
        REQUIRE(r.find("\"n\": 2") != std::string::npos);   // w1 and w2, not 3
    }

    SECTION("the same filter without an aggregate yields one row per work rather than per category") {
        std::string r = run(
            "MATCH (w:CreativeWork)-[:about]->(e:Thing) "
            "WHERE EXISTS { MATCH (w)-[:category]->(c) "
            "               WHERE c.uri = 'cat/Company' OR c.uri = 'cat/Event' } "
            "RETURN count(w) AS n");
        INFO("rows: " << r);
        REQUIRE(r.find("\"n\": 2") != std::string::npos);
    }

    guard.stop();
}
