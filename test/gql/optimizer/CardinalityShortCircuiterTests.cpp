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
#include "../../../src/gql/GqlParser.h"
#include "../../../src/gql/GqlOptimizer.h"
#include "../../../src/gql/GqlVirtualCatalog.h"

using namespace ragedb;
using namespace ragedb::gql;

TEST_CASE("GQL Optimizer Phase 5: Cardinality-Constrained Traversal Short-Circuiting", "[gql_optimizer][semantic]") {
    GqlVirtualCatalog::local().clear();

    // Register cardinality constraint: Person has FRIEND out-degree <= 1
    GqlVirtualCatalog::local().add_constraint(
        "PersonFriendMaxCard",
        "MATCH (p:Person)-[:FRIEND]->(f1) MATCH (p)-[:FRIEND]->(f2) WHERE f1 != f2 RETURN p"
    );

    SECTION("Matches constraint out-degree <= 1") {
        std::string query_str = "MATCH (p:Person)-[r:FRIEND]->(f:Person) RETURN p, f";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);

        REQUIRE(query.matches[0].pattern.edges[0].max_cardinality_limit.has_value());
        REQUIRE(*query.matches[0].pattern.edges[0].max_cardinality_limit == 1);
    }

    SECTION("Does not match constraint due to direction") {
        std::string query_str = "MATCH (p:Person)<-[r:FRIEND]-(f:Person) RETURN p, f";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);

        REQUIRE_FALSE(query.matches[0].pattern.edges[0].max_cardinality_limit.has_value());
    }

    SECTION("Does not match constraint due to relationship type") {
        std::string query_str = "MATCH (p:Person)-[r:KNOWS]->(f:Person) RETURN p, f";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);

        REQUIRE_FALSE(query.matches[0].pattern.edges[0].max_cardinality_limit.has_value());
    }

    GqlVirtualCatalog::local().clear();
}
