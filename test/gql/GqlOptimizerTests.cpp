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

    // Prior to optimization, the node's property filters vector is empty
    REQUIRE(query.matches[0].pattern.nodes[0].property_filters.empty());
    REQUIRE(query.where_expr != nullptr);

    GqlOptimizer::optimize(query);

    // After optimization, 'name = Alice' should be pushed down into the node patterns' property_filters
    auto& filters = query.matches[0].pattern.nodes[0].property_filters;
    REQUIRE(filters.size() == 1);
    REQUIRE(filters[0].property == "name");
    REQUIRE(filters[0].op == Operation::EQ);
    REQUIRE(std::get<std::string>(filters[0].value) == "Alice");

    // The where expression should be simplified and rebuilt to nullptr since the only predicate was pushed down
    REQUIRE(query.where_expr == nullptr);
}

TEST_CASE("GQL Optimizer pushdown multiple predicates", "[gql_optimizer]") {
    std::string query_str = "MATCH (p:Person) WHERE p.name = 'Alice' AND p.age = 30 RETURN p.name";
    auto query = GqlParser::parse(query_str);

    GqlOptimizer::optimize(query);

    auto& filters = query.matches[0].pattern.nodes[0].property_filters;
    REQUIRE(filters.size() == 2);

    bool has_name = false;
    bool has_age = false;
    for (const auto& filter : filters) {
        if (filter.property == "name") {
            has_name = true;
            REQUIRE(filter.op == Operation::EQ);
            REQUIRE(std::get<std::string>(filter.value) == "Alice");
        } else if (filter.property == "age") {
            has_age = true;
            REQUIRE(filter.op == Operation::EQ);
            REQUIRE(std::get<int64_t>(filter.value) == 30);
        }
    }
    REQUIRE(has_name);
    REQUIRE(has_age);
    REQUIRE(query.where_expr == nullptr);
}

TEST_CASE("GQL Optimizer pushdown edge predicates", "[gql_optimizer]") {
    std::string query_str = "MATCH (p:Person)-[e:ACTED_IN]->(m:Movie) WHERE e.role = 'Hero' RETURN p.name";
    auto query = GqlParser::parse(query_str);

    GqlOptimizer::optimize(query);

    auto& edge_filters = query.matches[0].pattern.edges[0].property_filters;
    REQUIRE(edge_filters.size() == 1);
    REQUIRE(edge_filters[0].property == "role");
    REQUIRE(edge_filters[0].op == Operation::EQ);
    REQUIRE(std::get<std::string>(edge_filters[0].value) == "Hero");
    REQUIRE(query.where_expr == nullptr);
}

TEST_CASE("GQL Optimizer pushdown range and inequality predicates", "[gql_optimizer]") {
    // Both 'p.name = Alice' and 'p.age > 20' are pushed down
    std::string query_str = "MATCH (p:Person) WHERE p.name = 'Alice' AND p.age > 20 RETURN p.name";
    auto query = GqlParser::parse(query_str);

    GqlOptimizer::optimize(query);

    auto& filters = query.matches[0].pattern.nodes[0].property_filters;
    REQUIRE(filters.size() == 2);

    bool has_name = false;
    bool has_age = false;
    for (const auto& filter : filters) {
        if (filter.property == "name") {
            has_name = true;
            REQUIRE(filter.op == Operation::EQ);
            REQUIRE(std::get<std::string>(filter.value) == "Alice");
        } else if (filter.property == "age") {
            has_age = true;
            REQUIRE(filter.op == Operation::GT);
            REQUIRE(std::get<int64_t>(filter.value) == 20);
        }
    }
    REQUIRE(has_name);
    REQUIRE(has_age);

    // The where expression should be nullptr since both are pushed down
    REQUIRE(query.where_expr == nullptr);
}

TEST_CASE("GQL Optimizer ignores OR predicates", "[gql_optimizer]") {
    // Since it's an OR, we cannot push either predicate down because the node must match if EITHER matches.
    std::string query_str = "MATCH (p:Person) WHERE p.name = 'Alice' OR p.age = 30 RETURN p.name";
    auto query = GqlParser::parse(query_str);

    GqlOptimizer::optimize(query);

    REQUIRE(query.matches[0].pattern.nodes[0].property_filters.empty());
    REQUIRE(query.where_expr != nullptr);
    REQUIRE(query.where_expr->kind == ExpressionKind::BINARY_OP);
    auto* bin = static_cast<const BinaryOpExpr*>(query.where_expr.get());
    REQUIRE(bin->op == BinaryOpKind::OR);
}
