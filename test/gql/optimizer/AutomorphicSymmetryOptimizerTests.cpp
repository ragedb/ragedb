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

using namespace ragedb;
using namespace ragedb::gql;

TEST_CASE("GQL Optimizer Phase 10: Automorphic Graph Symmetry Deduplication", "[gql_optimizer][symmetry]") {
    SECTION("Case 1: Standard homogeneous directed triangle cycle count query (single match)") {
        std::string query_str = "MATCH (a:Person)-[:FRIEND]->(b:Person)-[:FRIEND]->(c:Person)-[:FRIEND]->(a) RETURN count(*)";
        auto query = GqlParser::parse(query_str);

        REQUIRE(query.count_multiplication_factor == 1);
        REQUIRE(query.where_expr == nullptr);

        GqlOptimizer::optimize(query);

        REQUIRE(query.count_multiplication_factor == 3);
        REQUIRE(query.where_expr == nullptr);
        
        const auto& node_b = query.matches[0].pattern.nodes[1];
        REQUIRE(node_b.where_expr != nullptr);
        REQUIRE(node_b.where_expr->kind == ExpressionKind::BINARY_OP);
        auto* lt_b = static_cast<BinaryOpExpr*>(node_b.where_expr.get());
        REQUIRE(lt_b->op == BinaryOpKind::LT);
        
        const auto& node_c = query.matches[0].pattern.nodes[2];
        REQUIRE(node_c.where_expr != nullptr);
        REQUIRE(node_c.where_expr->kind == ExpressionKind::BINARY_OP);
        auto* lt_c = static_cast<BinaryOpExpr*>(node_c.where_expr.get());
        REQUIRE(lt_c->op == BinaryOpKind::LT);
    }

    SECTION("Case 2: Comma-separated homogeneous directed triangle cycle count query") {
        std::string query_str = "MATCH (a:Person)-[:FRIEND]->(b:Person), (b)-[:FRIEND]->(c:Person), (c)-[:FRIEND]->(a) RETURN count(*)";
        auto query = GqlParser::parse(query_str);

        REQUIRE(query.count_multiplication_factor == 1);
        REQUIRE(query.where_expr == nullptr);

        GqlOptimizer::optimize(query);

        REQUIRE(query.count_multiplication_factor == 3);
        REQUIRE(query.where_expr == nullptr);
        REQUIRE(query.matches[0].pattern.nodes[1].where_expr != nullptr);
        REQUIRE(query.matches[1].pattern.nodes[1].where_expr != nullptr);
    }

    SECTION("Case 3: Bypass if it is not a count query") {
        std::string query_str = "MATCH (a:Person)-[:FRIEND]->(b:Person)-[:FRIEND]->(c:Person)-[:FRIEND]->(a) RETURN a, b, c";
        auto query = GqlParser::parse(query_str);

        GqlOptimizer::optimize(query);

        REQUIRE(query.count_multiplication_factor == 1);
        REQUIRE(query.where_expr == nullptr);
    }

    SECTION("Case 4: Bypass if labels are not homogeneous") {
        std::string query_str = "MATCH (a:Person)-[:FRIEND]->(b:Animal)-[:FRIEND]->(c:Person)-[:FRIEND]->(a) RETURN count(*)";
        auto query = GqlParser::parse(query_str);

        GqlOptimizer::optimize(query);

        REQUIRE(query.count_multiplication_factor == 1);
        REQUIRE(query.where_expr == nullptr);
    }

    SECTION("Case 5: Bypass if there are pre-existing where clause filters") {
        std::string query_str = "MATCH (a:Person)-[:FRIEND]->(b:Person)-[:FRIEND]->(c:Person)-[:FRIEND]->(a) WHERE a.name = 'Alice' RETURN count(*)";
        auto query = GqlParser::parse(query_str);

        GqlOptimizer::optimize(query);

        REQUIRE(query.count_multiplication_factor == 1);
        // Should push down the existing where clause to node 'a' property filters
        REQUIRE(query.where_expr == nullptr);
        REQUIRE(!query.matches[0].pattern.nodes[0].property_filters.empty());
        REQUIRE(query.matches[0].pattern.nodes[0].property_filters[0].property == "name");
        REQUIRE(query.matches[0].pattern.nodes[0].property_filters[0].op == Operation::EQ);
    }
}
