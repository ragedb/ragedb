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

// The disjoint-concept pruner answers a query with nothing at all: proving a variable-length traversal
// impossible sets no_op, and the executor returns an empty result without touching the graph. A false
// positive is therefore indistinguishable from "no matches" -- the most expensive kind of wrong answer
// to notice. The pass had only a timing benchmark, so what follows pins both directions, with the
// emphasis on everything that must NOT be pruned.

#include <catch2/catch.hpp>
#include "../../../src/gql/GqlParser.h"
#include "../../../src/gql/GqlOptimizer.h"
#include "../../../src/gql/GqlVirtualCatalog.h"

using namespace ragedb;
using namespace ragedb::gql;

namespace {
GqlQuery optimized(const std::string& text) {
    auto query = GqlParser::parse(text);
    GqlOptimizer::optimize(query);
    return query;
}
}  // namespace

TEST_CASE("a variable-length path between disjoint labels is unsatisfiable", "[gql_optimizer]") {
    GqlVirtualCatalog::local().clear();
    GqlVirtualCatalog::local().add_disjoint_labels("Person", "Product");

    auto q = optimized("MATCH (p:Person)-[:KNOWS*1..3]->(x:Product) RETURN p");
    REQUIRE(q.no_op);

    GqlVirtualCatalog::local().clear();
}

TEST_CASE("the disjoint pruner leaves satisfiable queries alone", "[gql_optimizer]") {
    SECTION("labels that are not declared disjoint") {
        GqlVirtualCatalog::local().clear();
        GqlVirtualCatalog::local().add_disjoint_labels("Person", "Product");
        auto q = optimized("MATCH (p:Person)-[:KNOWS*1..3]->(f:Person) RETURN p");
        REQUIRE_FALSE(q.no_op);
        GqlVirtualCatalog::local().clear();
    }

    SECTION("an empty disjointness registry prunes nothing") {
        GqlVirtualCatalog::local().clear();
        auto q = optimized("MATCH (p:Person)-[:KNOWS*1..3]->(x:Product) RETURN p");
        REQUIRE_FALSE(q.no_op);
    }

    SECTION("an unlabeled endpoint cannot be proven disjoint") {
        GqlVirtualCatalog::local().clear();
        GqlVirtualCatalog::local().add_disjoint_labels("Person", "Product");
        auto q = optimized("MATCH (p:Person)-[:KNOWS*1..3]->(x) RETURN p");
        REQUIRE_FALSE(q.no_op);
        GqlVirtualCatalog::local().clear();
    }

    SECTION("NO_SEMANTIC opts the query out of semantic pruning entirely") {
        GqlVirtualCatalog::local().clear();
        GqlVirtualCatalog::local().add_disjoint_labels("Person", "Product");
        auto q = optimized("NO_SEMANTIC MATCH (p:Person)-[:KNOWS*1..3]->(x:Product) RETURN p");
        REQUIRE_FALSE(q.no_op);
        GqlVirtualCatalog::local().clear();
    }
}

TEST_CASE("the disjoint pruner only judges variable-length traversals", "[gql_optimizer]") {
    // A single fixed hop between disjoint labels is equally impossible, but proving that is another
    // pass's job -- this one inspects quantified edges only. Pinned so the boundary is deliberate:
    // widening it later should be a decision, not a surprise.
    GqlVirtualCatalog::local().clear();
    GqlVirtualCatalog::local().add_disjoint_labels("Person", "Product");

    auto q = optimized("MATCH (p:Person)-[:KNOWS]->(x:Product) RETURN p");
    REQUIRE_FALSE(q.no_op);

    GqlVirtualCatalog::local().clear();
}
