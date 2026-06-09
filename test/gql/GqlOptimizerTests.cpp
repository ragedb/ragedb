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
#include "../../src/gql/GqlParser.h"
#include "../../src/gql/GqlOptimizer.h"

using namespace ragedb;
using namespace ragedb::gql;

TEST_CASE("GQL Optimizer pushdown test", "[gql_optimizer]") {
    std::string query_str = "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name";
    auto query = GqlParser::parse(query_str);

    // Prior to optimization, the node's properties map is empty
    REQUIRE(query.matches[0].pattern.nodes[0].properties.empty());
    REQUIRE(query.where_expr != nullptr);

    GqlOptimizer::optimize(query);

    // After optimization, 'name = Alice' should be pushed down into the node patterns
    auto& properties = query.matches[0].pattern.nodes[0].properties;
    REQUIRE(properties.count("name") == 1);
    REQUIRE(std::get<std::string>(properties.at("name")) == "Alice");

    // The where expression should be simplified and rebuild to nullptr since the only predicate was pushed down
    REQUIRE(query.where_expr == nullptr);
}

TEST_CASE("GQL Optimizer pushdown multiple predicates", "[gql_optimizer]") {
    std::string query_str = "MATCH (p:Person) WHERE p.name = 'Alice' AND p.age = 30 RETURN p.name";
    auto query = GqlParser::parse(query_str);

    GqlOptimizer::optimize(query);

    auto& properties = query.matches[0].pattern.nodes[0].properties;
    REQUIRE(properties.size() == 2);
    REQUIRE(std::get<std::string>(properties.at("name")) == "Alice");
    REQUIRE(std::get<int64_t>(properties.at("age")) == 30);
    REQUIRE(query.where_expr == nullptr);
}

TEST_CASE("GQL Optimizer pushdown edge predicates", "[gql_optimizer]") {
    std::string query_str = "MATCH (p:Person)-[e:ACTED_IN]->(m:Movie) WHERE e.role = 'Hero' RETURN p.name";
    auto query = GqlParser::parse(query_str);

    GqlOptimizer::optimize(query);

    auto& edge_properties = query.matches[0].pattern.edges[0].properties;
    REQUIRE(edge_properties.size() == 1);
    REQUIRE(std::get<std::string>(edge_properties.at("role")) == "Hero");
    REQUIRE(query.where_expr == nullptr);
}

TEST_CASE("GQL Optimizer pushdown mixed predicates", "[gql_optimizer]") {
    // Only 'p.name = Alice' can be pushed down; 'p.age > 20' remains in the where clause.
    std::string query_str = "MATCH (p:Person) WHERE p.name = 'Alice' AND p.age > 20 RETURN p.name";
    auto query = GqlParser::parse(query_str);

    GqlOptimizer::optimize(query);

    auto& properties = query.matches[0].pattern.nodes[0].properties;
    REQUIRE(properties.size() == 1);
    REQUIRE(std::get<std::string>(properties.at("name")) == "Alice");

    // The where expression should NOT be nullptr; it should contain the remaining filter
    REQUIRE(query.where_expr != nullptr);
    REQUIRE(query.where_expr->kind == ExpressionKind::BINARY_OP);
    auto* bin = static_cast<const BinaryOpExpr*>(query.where_expr.get());
    REQUIRE(bin->op == BinaryOpKind::GT);
}

TEST_CASE("GQL Optimizer ignores OR predicates", "[gql_optimizer]") {
    // Since it's an OR, we cannot push either predicate down because the node must match if EITHER matches.
    std::string query_str = "MATCH (p:Person) WHERE p.name = 'Alice' OR p.age = 30 RETURN p.name";
    auto query = GqlParser::parse(query_str);

    GqlOptimizer::optimize(query);

    REQUIRE(query.matches[0].pattern.nodes[0].properties.empty());
    REQUIRE(query.where_expr != nullptr);
    REQUIRE(query.where_expr->kind == ExpressionKind::BINARY_OP);
    auto* bin = static_cast<const BinaryOpExpr*>(query.where_expr.get());
    REQUIRE(bin->op == BinaryOpKind::OR);
}

