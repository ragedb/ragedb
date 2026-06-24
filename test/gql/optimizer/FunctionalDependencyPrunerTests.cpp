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

TEST_CASE("GQL Optimizer Phase 9: Functional Dependencies & Attribute-Correlation Rewriting", "[gql_optimizer][functional_dependency]") {
    GqlVirtualCatalog::local().clear();

    SECTION("Case 1: Standard rewrite from count(b.state_name) to count(*)") {
        // Register functional dependency constraint: zip_code -> state_name
        GqlVirtualCatalog::local().add_constraint(
            "CityZipToState",
            "MATCH (u:CityNode) MATCH (v:CityNode) WHERE u.zip_code = v.zip_code AND u.state_name != v.state_name RETURN u"
        );

        std::string query_str = "MATCH (b:CityNode) RETURN b.zip_code, count(b.state_name)";
        auto query = GqlParser::parse(query_str);
        
        // Before optimization, aggregate expression is count(b.state_name)
        auto* before_agg = static_cast<const AggregateExpr*>(query.returns[1].expr.get());
        REQUIRE(before_agg->fn_kind == AggregateKind::COUNT);
        REQUIRE(before_agg->expr != nullptr);

        GqlOptimizer::optimize(query);

        // After optimization, count(b.state_name) should be count(*) (expr is nullptr)
        auto* after_agg = static_cast<const AggregateExpr*>(query.returns[1].expr.get());
        REQUIRE(after_agg->fn_kind == AggregateKind::COUNT);
        REQUIRE(after_agg->expr == nullptr);

        GqlVirtualCatalog::local().clear();
    }

    SECTION("Case 2: Bypass if determining property is not in grouping keys") {
        GqlVirtualCatalog::local().add_constraint(
            "CityZipToState",
            "MATCH (u:CityNode) MATCH (v:CityNode) WHERE u.zip_code = v.zip_code AND u.state_name != v.state_name RETURN u"
        );

        // Group by other_prop instead of zip_code
        std::string query_str = "MATCH (b:CityNode) RETURN b.other_prop, count(b.state_name)";
        auto query = GqlParser::parse(query_str);

        GqlOptimizer::optimize(query);

        // Should NOT be rewritten, expr is still present
        auto* after_agg = static_cast<const AggregateExpr*>(query.returns[1].expr.get());
        REQUIRE(after_agg->expr != nullptr);

        GqlVirtualCatalog::local().clear();
    }

    SECTION("Case 3: Bypass if node label does not match constraint") {
        GqlVirtualCatalog::local().add_constraint(
            "CityZipToState",
            "MATCH (u:CityNode) MATCH (v:CityNode) WHERE u.zip_code = v.zip_code AND u.state_name != v.state_name RETURN u"
        );

        // Label is OtherNode instead of CityNode
        std::string query_str = "MATCH (b:OtherNode) RETURN b.zip_code, count(b.state_name)";
        auto query = GqlParser::parse(query_str);

        GqlOptimizer::optimize(query);

        // Should NOT be rewritten
        auto* after_agg = static_cast<const AggregateExpr*>(query.returns[1].expr.get());
        REQUIRE(after_agg->expr != nullptr);

        GqlVirtualCatalog::local().clear();
    }

    SECTION("Case 4: Supports functional dependency query with comma-separated MATCH") {
        GqlVirtualCatalog::local().add_constraint(
            "CityZipToStateComma",
            "MATCH (u:CityNode), (v:CityNode) WHERE u.zip_code = v.zip_code AND u.state_name != v.state_name RETURN u"
        );

        std::string query_str = "MATCH (b:CityNode) RETURN b.zip_code, count(b.state_name)";
        auto query = GqlParser::parse(query_str);

        GqlOptimizer::optimize(query);

        // Should be rewritten
        auto* after_agg = static_cast<const AggregateExpr*>(query.returns[1].expr.get());
        REQUIRE(after_agg->expr == nullptr);

        GqlVirtualCatalog::local().clear();
    }
}
