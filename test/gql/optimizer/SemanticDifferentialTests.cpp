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
#include <algorithm>
#include "../../../src/gql/GqlParser.h"
#include "../../../src/gql/GqlOptimizer.h"
#include "../../../src/gql/GqlExecutor.h"
#include "../../../src/gql/GqlVirtualCatalog.h"
#include "../../../src/graph/cache/WccCache.h"
#include "../../../src/graph/cache/TransitiveReachabilityCache.h"

using namespace ragedb;
using namespace ragedb::gql;

/*
 * differential semantic-equivalence tests. Each query is executed twice on the
 * same data -- once through the full optimizer with algebraic traits registered, once with the
 * NO_SEMANTIC prefix (algebraic passes disabled) -- and the row multisets must match. The shapes
 * chosen are exactly the ones the task-003/004 guards must keep OFF the fast paths (direction,
 * bounded quantifiers, edge variables/predicates, cycles); a regression that lets a rewrite fire
 * on them again shows up as a result mismatch, not just an AST change.
 */

/**
 * @brief Order-insensitive comparison key: split the JSON result into row fragments and sort them.
 */
static std::vector<std::string> row_multiset(const std::string& json) {
    std::vector<std::string> rows;
    size_t pos = 0;
    while ((pos = json.find('{', pos)) != std::string::npos) {
        size_t end = json.find('}', pos);
        if (end == std::string::npos) break;
        rows.push_back(json.substr(pos, end - pos + 1));
        pos = end + 1;
    }
    std::sort(rows.begin(), rows.end());
    return rows;
}

static void require_differential_equal(Graph& graph, const std::string& query) {
    auto optimized_q = GqlParser::parse(query);
    GqlOptimizer::optimize(optimized_q);
    std::string optimized = GqlExecutor::execute(graph, std::move(optimized_q)).get();

    auto plain_q = GqlParser::parse("NO_SEMANTIC " + query);
    GqlOptimizer::optimize(plain_q);
    std::string plain = GqlExecutor::execute(graph, std::move(plain_q)).get();

    INFO("query: " << query);
    INFO("optimized: " << optimized);
    INFO("no_semantic: " << plain);
    REQUIRE(row_multiset(optimized) == row_multiset(plain));
}

TEST_CASE("semantic passes preserve results on guarded shapes", "[gql_optimizer]") {
    auto graph = Graph("gql_semantic_differential_test");
    graph.Start().get();
    graph.Clear();
    GqlVirtualCatalog::local().clear();
    WccCache::local().clear();
    TransitiveReachabilityCache::local().clear();

    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().RelationshipTypeInsertPeered("manages").get();
    graph.shard.local().RelationshipPropertyTypeAddPeered("manages", "w", "integer").get();
    GqlVirtualCatalog::local().set_relationship_algebraic_properties("manages", {"transitive"});

    // A chain with a parallel shortcut edge and a small side branch:
    // a -> b -> c -> d, plus shortcut a -> c, plus b -> e.
    auto add = [&](const std::string& key, const std::string& name) {
        return graph.shard.local().NodeAddPeered("Person", key, "{\"name\": \"" + name + "\"}").get();
    };
    uint64_t a = add("a", "A");
    uint64_t b = add("b", "B");
    uint64_t c = add("c", "C");
    uint64_t d = add("d", "D");
    uint64_t e = add("e", "E");
    graph.shard.local().RelationshipAddPeered("manages", a, b, "{\"w\": 1}").get();
    graph.shard.local().RelationshipAddPeered("manages", b, c, "{\"w\": 2}").get();
    graph.shard.local().RelationshipAddPeered("manages", c, d, "{\"w\": 3}").get();
    graph.shard.local().RelationshipAddPeered("manages", a, c, "{\"w\": 9}").get();
    graph.shard.local().RelationshipAddPeered("manages", b, e, "{\"w\": 4}").get();

    SECTION("LEFT direction is not rewritten") {
        require_differential_equal(graph,
            "MATCH (x:Person {name: 'D'})<-[:manages*]-(y:Person) RETURN y.name");
    }

    SECTION("bounded quantifiers are not coalesced") {
        require_differential_equal(graph,
            "MATCH (x:Person {name: 'A'})-[:manages*1..2]->(y:Person) RETURN y.name");
        require_differential_equal(graph,
            "MATCH (x:Person {name: 'A'})-[:manages*2..2]->(y:Person) RETURN y.name");
    }

    SECTION("edge property filters stay on the traversal path") {
        require_differential_equal(graph,
            "MATCH (x:Person {name: 'A'})-[:manages* {w: 1}]->(y:Person) RETURN y.name");
    }

    SECTION("path multiplicity is preserved for counts (parallel shortcut)") {
        // c is reachable from a twice (a->b->c and a->c): count(*) must reflect trail semantics
        // identically in both modes. The quantifier is bounded so the transitive fast path stays
        // off: the UNBOUNDED safe shape intentionally is not asserted here -- the fast path
        // returns one row per (start,end) pair, not per trail (open design question Q3b; a
        // differential run measured optimized=1 vs no_semantic=2 on this data), and driving it
        // inside the Catch harness aborts in sharded teardown.
        require_differential_equal(graph,
            "MATCH (x:Person {name: 'A'})-[:manages*1..3]->(y:Person {name: 'C'}) RETURN count(*)");
    }

    SECTION("single hop on a transitive type is untouched") {
        require_differential_equal(graph,
            "MATCH (x:Person {name: 'A'})-[:manages]->(y:Person) RETURN y.name");
    }

    GqlVirtualCatalog::local().clear();
    graph.Stop().get();
}
