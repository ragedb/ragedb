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
 
 TEST_CASE("GQL Optimizer redundant match pattern removal", "[gql_optimizer]") {
     std::string query_str = "MATCH (p:Person) MATCH (p:Person) RETURN p.name";
     auto query = GqlParser::parse(query_str);
 
     REQUIRE(query.matches.size() == 2);
 
     GqlOptimizer::optimize(query);
 
     // Redundant MATCH pattern should be pruned
     REQUIRE(query.matches.size() == 1);
     REQUIRE(query.matches[0].pattern.nodes[0].variable == "p");
     REQUIRE_FALSE(query.matches[0].is_optional);
 }
 
 TEST_CASE("GQL Optimizer redundant optional match promotion", "[gql_optimizer]") {
     std::string query_str = "OPTIONAL MATCH (p:Person) MATCH (p:Person) RETURN p.name";
     auto query = GqlParser::parse(query_str);
 
     REQUIRE(query.matches.size() == 2);
     REQUIRE(query.matches[0].is_optional);
     REQUIRE_FALSE(query.matches[1].is_optional);
 
     GqlOptimizer::optimize(query);
 
     // Redundant match should be pruned, and the kept match should be promoted to non-optional
     REQUIRE(query.matches.size() == 1);
     REQUIRE(query.matches[0].pattern.nodes[0].variable == "p");
     REQUIRE_FALSE(query.matches[0].is_optional);
}

TEST_CASE("GQL Optimizer correlated subquery unnesting test", "[gql_optimizer]") {
    std::string query_str = "MATCH (a:Person) WHERE EXISTS { MATCH (a)-[:KNOWS]->(b:Person) WHERE b.name = 'Bob' } RETURN a.name";
    auto query = GqlParser::parse(query_str);

    REQUIRE(query.matches.size() == 1);
    REQUIRE_FALSE(query.has_unnested_subquery);

    GqlOptimizer::optimize(query);

    // The subquery MATCH should be unnested and appended to the main matches list as an optional match
    REQUIRE(query.has_unnested_subquery);
    REQUIRE(query.matches.size() == 2);
    REQUIRE_FALSE(query.matches[0].is_optional);
    REQUIRE(query.matches[1].is_optional);
    
    // The second match pattern should be the subquery pattern: (a)-[:KNOWS]->(b)
    const auto& pattern = query.matches[1].pattern;
    REQUIRE(pattern.nodes.size() == 2);
    REQUIRE(pattern.nodes[0].variable == "a");
    REQUIRE(pattern.nodes[1].variable == "b");
    REQUIRE(pattern.edges.size() == 1);
    
    // The subquery's filter b.name = 'Bob' should be pushed down into the subquery's node b's property filters!
    const auto& b_filters = pattern.nodes[1].property_filters;
    REQUIRE(b_filters.size() == 1);
    REQUIRE(b_filters[0].property == "name");
    REQUIRE(b_filters[0].op == Operation::EQ);
    REQUIRE(std::get<std::string>(b_filters[0].value) == "Bob");

    // The outer variables should be populated
    REQUIRE(query.outer_vars.size() == 1);
    REQUIRE(query.outer_vars.count("a"));
}

#include "../../src/gql/GqlVirtualCatalog.h"

TEST_CASE("GQL Optimizer virtual view expansion", "[gql_optimizer]") {
    GqlVirtualCatalog::local().clear();
    GqlVirtualCatalog::local().add_view("Adult", "MATCH (p:Person) WHERE p.age >= 18 RETURN p");

    std::string query_str = "MATCH (a:Adult) RETURN a.name";
    auto query = GqlParser::parse(query_str);

    GqlOptimizer::optimize(query);

    // Node 'a' should now be Person and have the pushed down age filter
    REQUIRE(query.matches[0].pattern.nodes[0].variable == "a");
    REQUIRE(query.matches[0].pattern.nodes[0].label_expr->name == "Person");
    
    auto& filters = query.matches[0].pattern.nodes[0].property_filters;
    REQUIRE(filters.size() == 1);
    REQUIRE(filters[0].property == "age");
    REQUIRE(filters[0].op == Operation::GTE);
    REQUIRE(std::get<int64_t>(filters[0].value) == 18);
    
    GqlVirtualCatalog::local().clear();
}

