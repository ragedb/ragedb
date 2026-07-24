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

/*
 * optimizer passes that rewrite COUNT shapes (count -> count(*), count -> degree sum,
 * count -> algebraic path count) change row multiplicity, which is exactly what DISTINCT
 * aggregates observe. Each pass must leave DISTINCT aggregates untouched. Every section pairs a
 * positive control (the plain-count rewrite still fires) with the DISTINCT bail-out.
 */

TEST_CASE("FunctionalDependencyPruner keeps count(DISTINCT b.y)", "[gql_optimizer]") {
    GqlVirtualCatalog::local().clear();
    GqlVirtualCatalog::local().add_constraint(
        "CityZipToState",
        "MATCH (u:CityNode) MATCH (v:CityNode) WHERE u.zip_code = v.zip_code AND u.state_name != v.state_name RETURN u"
    );

    SECTION("positive control: plain count(b.state_name) still becomes count(*)") {
        auto query = GqlParser::parse("MATCH (b:CityNode) RETURN b.zip_code, count(b.state_name)");
        GqlOptimizer::optimize(query);
        auto* agg = static_cast<const AggregateExpr*>(query.returns[1].expr.get());
        REQUIRE(agg->fn_kind == AggregateKind::COUNT);
        REQUIRE(agg->expr == nullptr);
    }

    SECTION("count(DISTINCT b.state_name) keeps its expression and flag") {
        auto query = GqlParser::parse("MATCH (b:CityNode) RETURN b.zip_code, count(DISTINCT b.state_name)");
        GqlOptimizer::optimize(query);
        auto* agg = static_cast<const AggregateExpr*>(query.returns[1].expr.get());
        REQUIRE(agg->fn_kind == AggregateKind::COUNT);
        REQUIRE(agg->distinct);
        REQUIRE(agg->expr != nullptr);
    }

    GqlVirtualCatalog::local().clear();
}

TEST_CASE("degree-sum rewrite skips DISTINCT counts", "[gql_optimizer]") {
    GqlVirtualCatalog::local().clear();

    SECTION("positive control: count(b) over one hop becomes a degree sum") {
        auto query = GqlParser::parse("MATCH (a:Person)-[:KNOWS]->(b) RETURN count(b)");
        GqlOptimizer::optimize(query);
        auto* agg = static_cast<const AggregateExpr*>(query.returns[0].expr.get());
        REQUIRE(agg->fn_kind == AggregateKind::SUM);
        REQUIRE(query.matches[0].pattern.edges.empty());
    }

    SECTION("count(DISTINCT b) keeps the COUNT and the full pattern") {
        auto query = GqlParser::parse("MATCH (a:Person)-[:KNOWS]->(b) RETURN count(DISTINCT b)");
        GqlOptimizer::optimize(query);
        REQUIRE(query.returns[0].expr->kind == ExpressionKind::AGGREGATION);
        auto* agg = static_cast<const AggregateExpr*>(query.returns[0].expr.get());
        REQUIRE(agg->fn_kind == AggregateKind::COUNT);
        REQUIRE(agg->distinct);
        REQUIRE(query.matches[0].pattern.edges.size() == 1);
        REQUIRE(query.matches[0].pattern.nodes.size() == 2);
    }

    GqlVirtualCatalog::local().clear();
}

TEST_CASE("algebraic path-count rewrite skips DISTINCT counts", "[gql_optimizer]") {
    GqlVirtualCatalog::local().clear();

    SECTION("positive control: count(b) over a two-hop chain becomes a path count") {
        auto query = GqlParser::parse("MATCH (a:Person)-[:KNOWS]->()-[:KNOWS]->(b) RETURN count(b)");
        GqlOptimizer::optimize(query);
        REQUIRE(query.matches[0].algebraic_path_count);
        REQUIRE(query.matches[0].pattern.nodes.size() == 1);
    }

    SECTION("count(DISTINCT b) keeps the COUNT and the full pattern") {
        auto query = GqlParser::parse("MATCH (a:Person)-[:KNOWS]->()-[:KNOWS]->(b) RETURN count(DISTINCT b)");
        GqlOptimizer::optimize(query);
        REQUIRE_FALSE(query.matches[0].algebraic_path_count);
        REQUIRE(query.matches[0].pattern.nodes.size() == 3);
        REQUIRE(query.returns[0].expr->kind == ExpressionKind::AGGREGATION);
        auto* agg = static_cast<const AggregateExpr*>(query.returns[0].expr.get());
        REQUIRE(agg->fn_kind == AggregateKind::COUNT);
        REQUIRE(agg->distinct);
    }

    GqlVirtualCatalog::local().clear();
}
