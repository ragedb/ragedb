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

TEST_CASE("GQL Optimizer Phase 7: Composite Attribute Domain Constraint Reasoning", "[gql_optimizer][semantic]") {
    GqlVirtualCatalog::local().clear();
    // Register composite constraint: p.age < 18 OR p.status = 'minor' OR p.is_student = true is impossible
    GqlVirtualCatalog::local().add_constraint(
        "CompositePersonConstraint",
        "MATCH (p:Person) WHERE p.age < 18 OR p.status = 'minor' OR p.is_student = true RETURN p"
    );

    SECTION("Satisfiable query matching consistent attributes") {
        // age >= 18 AND status != 'minor' AND is_student != true is consistent
        std::string query_str = "MATCH (x:Person) WHERE x.age >= 18 AND x.status = 'adult' AND x.is_student = false RETURN x.name";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        REQUIRE_FALSE(query.no_op == true);
    }

    SECTION("Unsatisfiable query matching subset of impossible age range") {
        // x.age < 18 contradicts the constraint guarantee (which implies age >= 18)
        std::string query_str = "MATCH (x:Person) WHERE x.age = 15 RETURN x.name";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        REQUIRE(query.no_op == true);
    }

    SECTION("Unsatisfiable query matching subset of impossible string domain") {
        // x.status = 'minor' contradicts the constraint guarantee (which implies status != 'minor')
        std::string query_str = "MATCH (x:Person) WHERE x.status = 'minor' RETURN x.name";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        REQUIRE(query.no_op == true);
    }

    SECTION("Unsatisfiable query matching subset of impossible boolean domain") {
        // x.is_student = true contradicts the constraint guarantee (which implies is_student != true)
        std::string query_str = "MATCH (x:Person) WHERE x.is_student = true RETURN x.name";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        REQUIRE(query.no_op == true);
    }

    SECTION("Satisfiable query with overlapping but consistent ranges") {
        std::string query_str = "MATCH (x:Person) WHERE x.age > 20 RETURN x.name";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        REQUIRE_FALSE(query.no_op == true);
    }

    GqlVirtualCatalog::local().clear();
}
