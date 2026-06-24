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

TEST_CASE("GQL Optimizer Phase 8: Transitive DAG Reachability Short-Circuiting", "[gql_optimizer][semantic]") {
    GqlVirtualCatalog::local().clear();
    
    // Register hierarchy/taxonomy constraints:
    // SciFi_Books -> Books
    // SpaceOpera -> SciFi_Books
    GqlVirtualCatalog::local().add_constraint(
        "SciFiToBooks",
        "MATCH (c:Category {name: 'SciFi_Books'})-[:SUBCLASS_OF]->(p:Category {name: 'Books'}) RETURN c"
    );
    GqlVirtualCatalog::local().add_constraint(
        "SpaceOperaToSciFi",
        "MATCH (c:Category {name: 'SpaceOpera'})-[:SUBCLASS_OF]->(p:Category {name: 'SciFi_Books'}) RETURN c"
    );
    // Gardening_Tools -> Tools
    GqlVirtualCatalog::local().add_constraint(
        "GardeningToTools",
        "MATCH (c:Category {name: 'Gardening_Tools'})-[:SUBCLASS_OF]->(p:Category {name: 'Tools'}) RETURN c"
    );

    SECTION("Valid transitive reachability query") {
        // SpaceOpera -> SciFi_Books -> Books exists (length 2)
        std::string query_str = "MATCH (c1:Category)-[:SUBCLASS_OF*]->(c2:Category) WHERE c1.name = 'SpaceOpera' AND c2.name = 'Books' RETURN count(*)";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        REQUIRE_FALSE(query.no_op == true);
    }

    SECTION("Unreachable path between disjoint taxonomy trees") {
        // SpaceOpera to Gardening_Tools has no path -> contradiction
        std::string query_str = "MATCH (c1:Category)-[:SUBCLASS_OF*]->(c2:Category) WHERE c1.name = 'SpaceOpera' AND c2.name = 'Gardening_Tools' RETURN count(*)";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        REQUIRE(query.no_op == true);
    }

    SECTION("Unreachable because of hops limits") {
        // SpaceOpera -> Books is length 2. If we specify max_hops = 1, it should be unreachable -> contradiction
        std::string query_str = "MATCH (c1:Category)-[:SUBCLASS_OF*1..1]->(c2:Category) WHERE c1.name = 'SpaceOpera' AND c2.name = 'Books' RETURN count(*)";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        REQUIRE(query.no_op == true);
    }

    SECTION("Valid query under exact hops constraints") {
        // SpaceOpera -> Books is length 2. If we specify min_hops=1, max_hops=3, it should be allowed
        std::string query_str = "MATCH (c1:Category)-[:SUBCLASS_OF*1..3]->(c2:Category) WHERE c1.name = 'SpaceOpera' AND c2.name = 'Books' RETURN count(*)";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        REQUIRE_FALSE(query.no_op == true);
    }

    SECTION("Valid direct 1-hop reachability query") {
        // SciFi_Books -> Books has direct subclass edge
        std::string query_str = "MATCH (c1:Category)-[:SUBCLASS_OF]->(c2:Category) WHERE c1.name = 'SciFi_Books' AND c2.name = 'Books' RETURN count(*)";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        REQUIRE_FALSE(query.no_op == true);
    }

    SECTION("Invalid direct 1-hop reachability query") {
        // SpaceOpera -> Books has path of length 2 but not length 1 -> contradiction for direct edge
        std::string query_str = "MATCH (c1:Category)-[:SUBCLASS_OF]->(c2:Category) WHERE c1.name = 'SpaceOpera' AND c2.name = 'Books' RETURN count(*)";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        REQUIRE(query.no_op == true);
    }

    GqlVirtualCatalog::local().clear();
}
