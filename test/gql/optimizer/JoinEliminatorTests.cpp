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

TEST_CASE("GQL Optimizer Phase 2: Join Elimination", "[gql_optimizer][semantic]") {
    GqlVirtualCatalog::local().clear();
    // Register mandatory relationship constraint: every Shipment must have a SHIPPED_FROM Location
    GqlVirtualCatalog::local().add_constraint("MandatoryShippedFrom", "MATCH (s:Shipment) WHERE NOT EXISTS { MATCH (s)-[:SHIPPED_FROM]->(l:Location) } RETURN s");

    std::string query_str = "MATCH (s:Shipment)-[:SHIPPED_FROM]->(l:Location) RETURN s";
    auto query = GqlParser::parse(query_str);
    GqlOptimizer::optimize(query);

    // The join should be eliminated: the pattern should contain only the start node and no edges
    REQUIRE(query.matches[0].pattern.nodes.size() == 1);
    REQUIRE(query.matches[0].pattern.nodes[0].variable == "s");
    REQUIRE(query.matches[0].pattern.edges.empty());

    GqlVirtualCatalog::local().clear();
}
