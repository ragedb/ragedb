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

using namespace ragedb::gql;

TEST_CASE("Schema Reachability Pruning (Phase 11)", "[gql_optimizer][phase11]") {
    GqlVirtualCatalog::local().clear();
    GqlVirtualCatalog::local().add_allowed_relationship("Person", "FRIEND", "Person");

    SECTION("Valid schema path is not pruned") {
        std::string query_str = "MATCH (a:Person)-[:FRIEND]->(b:Person) RETURN a";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        REQUIRE_FALSE(query.no_op);
    }

    SECTION("Invalid schema path is pruned") {
        std::string query_str = "MATCH (a:Person)-[:FRIEND]->(b:Category) RETURN a";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        REQUIRE(query.no_op);
    }

    GqlVirtualCatalog::local().clear();
}

TEST_CASE("Optional Match Promotion (Phase 12)", "[gql_optimizer][phase12]") {
    SECTION("Promote optional match due to null-rejecting predicate") {
        std::string query_str = "OPTIONAL MATCH (p:Person)-[:FRIEND]->(f:Person) WHERE f.age > 21 RETURN f";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        REQUIRE(query.matches.size() == 1);
        REQUIRE_FALSE(query.matches[0].is_optional);
    }

    SECTION("Do not promote optional match when predicate is null-friendly") {
        std::string query_str = "OPTIONAL MATCH (p:Person)-[:FRIEND]->(f:Person) WHERE f.age IS NULL RETURN f";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        REQUIRE(query.matches.size() == 1);
        REQUIRE(query.matches[0].is_optional);
    }
}

TEST_CASE("Degree Constraint Pruning (Phase 13)", "[gql_optimizer][phase13]") {
    SECTION("Rewrite size expression to virtual degree property check") {
        std::string query_str = "MATCH (a:Person) WHERE size((a)-[:FRIEND]->()) > 5 RETURN a";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        
        REQUIRE(query.matches.size() == 1);
        const auto& node = query.matches[0].pattern.nodes[0];
        REQUIRE(node.degree_opt_info.size() == 1);
        REQUIRE(node.degree_opt_info[0].property_name == "_deg_a_FRIEND_OUT");
        REQUIRE(node.degree_opt_info[0].direction == ragedb::Direction::OUT);
        
        // The where expression should be rewritten to virtual property and pushed down
        REQUIRE(query.where_expr == nullptr);
        REQUIRE(node.property_filters.size() == 1);
        REQUIRE(node.property_filters[0].property == "_deg_a_FRIEND_OUT");
        REQUIRE(node.property_filters[0].op == ragedb::Operation::GT);
        REQUIRE(std::get<int64_t>(node.property_filters[0].value) == 5);
    }
}

TEST_CASE("Unique Join Elimination (Phase 14)", "[gql_optimizer][phase14]") {
    GqlVirtualCatalog::local().clear();
    // Register unique constraint: Source relates to Target via UNIQUE_REL max 1 out-degree
    // MATCH (s:Source)-[r:UNIQUE_REL]->(t1) MATCH (s)-[:UNIQUE_REL]->(t2) WHERE t1 != t2 RETURN s
    GqlVirtualCatalog::local().add_constraint("UniqueConstraint", 
        "MATCH (s:Source)-[r:UNIQUE_REL]->(t1:Target) MATCH (s)-[:UNIQUE_REL]->(t2:Target) WHERE t1 != t2 RETURN s");

    SECTION("Eliminate unique join in optional match when target node is not referenced") {
        std::string query_str = "OPTIONAL MATCH (a:Source)-[:UNIQUE_REL]->(b:Target) RETURN a";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        
        REQUIRE(query.matches.size() == 1);
        REQUIRE(query.matches[0].pattern.nodes.size() == 1);
        REQUIRE(query.matches[0].pattern.edges.empty());
    }

    GqlVirtualCatalog::local().clear();
}

TEST_CASE("Limit Pushdown (Phase 15)", "[gql_optimizer][phase15]") {
    SECTION("Push down limit in single match query") {
        std::string query_str = "MATCH (a:Person) RETURN a LIMIT 5";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        
        REQUIRE(query.matches.size() == 1);
        REQUIRE(query.matches[0].limit.has_value());
        REQUIRE(*query.matches[0].limit == 5);
    }
}
