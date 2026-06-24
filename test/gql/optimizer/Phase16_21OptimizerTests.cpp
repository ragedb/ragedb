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

TEST_CASE("Transitive Filter Propagation (Phase 16)", "[gql_optimizer][phase16]") {
    SECTION("Propagate constant filters via variable equality") {
        // MATCH (a:Person), (b:Person) WHERE a = b AND a.age = 30 RETURN a
        std::string query_str = "MATCH (a:Person), (b:Person) WHERE a = b AND a.age = 30 RETURN a";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);

        // Since GqlOptimizer pushes filters down at the end, a.age == 30 should be in node a,
        // and the propagated b.age == 30 should be in node b.
        REQUIRE(query.matches.size() == 2);
        
        bool found_a_age = false;
        bool found_b_age = false;
        
        for (const auto& match_stmt : query.matches) {
            for (const auto& node : match_stmt.pattern.nodes) {
                if (node.variable == "a") {
                    for (const auto& f : node.property_filters) {
                        if (f.property == "age" && std::get<int64_t>(f.value) == 30) {
                            found_a_age = true;
                        }
                    }
                } else if (node.variable == "b") {
                    for (const auto& f : node.property_filters) {
                        if (f.property == "age" && std::get<int64_t>(f.value) == 30) {
                            found_b_age = true;
                        }
                    }
                }
            }
        }
        
        REQUIRE(found_a_age);
        REQUIRE(found_b_age);
    }
}

TEST_CASE("Relationship-Attribute Contradiction Pruning (Phase 17)", "[gql_optimizer][phase17]") {
    GqlVirtualCatalog::local().clear();

    SECTION("Internal edge attribute contradiction prunes query") {
        // MATCH ()-[r:FRIEND]->() WHERE r.weight > 10 AND r.weight < 5 RETURN r
        std::string query_str = "MATCH ()-[r:FRIEND]->() WHERE r.weight > 10 AND r.weight < 5 RETURN r";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        REQUIRE(query.no_op);
    }

    SECTION("Contradiction against catalog relationship constraints prunes query") {
        // Define a constraint that FRIEND weight must not be negative (< 0)
        GqlVirtualCatalog::local().add_constraint("NegativeWeightFriend",
            "MATCH ()-[r:FRIEND]->() WHERE r.weight < 0 RETURN r");
            
        std::string query_str = "MATCH (a)-[r:FRIEND]->(b) WHERE r.weight = -5 RETURN a";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        REQUIRE(query.no_op);
    }

    GqlVirtualCatalog::local().clear();
}

TEST_CASE("Anti-Semi-Join Promotion (Phase 18)", "[gql_optimizer][phase18]") {
    SECTION("Promote optional match to anti-semi-join when target variable is checked for null") {
        // OPTIONAL MATCH (a)-[r:FRIEND]->(b) WHERE b IS NULL RETURN a
        std::string query_str = "OPTIONAL MATCH (a)-[r:FRIEND]->(b) WHERE b IS NULL RETURN a";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        
        // After optimization, the optional match is promoted to NOT EXISTS.
        // However, the query unnester (at the end of GqlOptimizer::optimize)
        // unnests the EXISTS subquery back into query.matches as an optional match
        // with exists->target_variable set.
        REQUIRE(query.matches.size() == 1);
        REQUIRE(query.where_expr != nullptr);
        
        // Verify it is a UNARY_OP (NOT) containing an EXISTS expression
        REQUIRE(query.where_expr->kind == ExpressionKind::UNARY_OP);
        const auto* not_expr = static_cast<const UnaryOpExpr*>(query.where_expr.get());
        REQUIRE(not_expr->op == UnaryOpKind::NOT);
        REQUIRE(not_expr->expr->kind == ExpressionKind::EXISTS);
        
        // Verify that target_variable is set on ExistsExpr (indicating it was unnested)
        const auto* exists_expr = static_cast<const ExistsExpr*>(not_expr->expr.get());
        REQUIRE(!exists_expr->target_variable.empty());
    }
}

TEST_CASE("Equality Join Elimination (Phase 19)", "[gql_optimizer][phase19]") {
    SECTION("Eliminate redundant self-joins equated in query filters") {
        // MATCH (a)-[:FRIEND]->(b), (a)-[:FRIEND]->(c) WHERE b = c RETURN a, b
        std::string query_str = "MATCH (a)-[:FRIEND]->(b) MATCH (a)-[:FRIEND]->(c) WHERE b = c RETURN a, b";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        
        // The two matches should be merged into a single MATCH (a)-[:FRIEND]->(b), and "c" references replaced with "b".
        // query.matches should contain only 1 match statement now.
        REQUIRE(query.matches.size() == 1);
        REQUIRE(query.matches[0].pattern.nodes.size() == 2);
        
        // And the WHERE clause equality filter should be eliminated (or null since there are no other filters)
        REQUIRE(query.where_expr == nullptr);
    }
}

TEST_CASE("Disjoint Concept Path Pruning (Phase 20)", "[gql_optimizer][phase20]") {
    GqlVirtualCatalog::local().clear();

    SECTION("Direct disjoint label pruning for variable-length paths") {
        GqlVirtualCatalog::local().add_disjoint_labels("Person", "Company");
        
        // MATCH (a:Person)-[:WORKS_FOR*]->(b:Company) RETURN a
        std::string query_str = "MATCH (a:Person)-[:WORKS_FOR*]->(b:Company) RETURN a";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        REQUIRE(query.no_op);
    }

    SECTION("Taxonomy value disjointness pruning") {
        // Define hierarchy constraints:
        // SciFi is a subcategory of Fiction
        GqlVirtualCatalog::local().add_constraint("H1",
            "MATCH (c:Category {name: 'SciFi'})-[:SUB_CATEGORY]->(p:Category {name: 'Fiction'}) RETURN c");
        
        // Register Fiction and Biography as disjoint values
        GqlVirtualCatalog::local().add_disjoint_values("Fiction", "Biography");
        
        // Query: MATCH (a:Category {name: 'SciFi'})-[:SUB_CATEGORY*]->(b:Category {name: 'Biography'}) RETURN a
        // Since SciFi's ancestor is Fiction, and Fiction is disjoint with Biography, the path is impossible.
        std::string query_str = "MATCH (a:Category {name: 'SciFi'})-[:SUB_CATEGORY*]->(b:Category {name: 'Biography'}) RETURN a";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        REQUIRE(query.no_op);
    }

    GqlVirtualCatalog::local().clear();
}

TEST_CASE("Cardinality-Aware Traversal Direction Swap (Phase 21)", "[gql_optimizer][phase21]") {
    SECTION("Swap direction if target node is more selective (unique id lookup vs scan)") {
        // MATCH (a:Person)-[:FRIEND]->(b:Person) WHERE b.id = 1 RETURN a
        std::string query_str = "MATCH (a:Person)-[:FRIEND]->(b:Person) WHERE b.id = 1 RETURN a";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        
        // Since b has a UNIQUE id filter (b.id == 1) and a has SCAN, the traversal direction should be reversed.
        // The pattern should start with b and end with a.
        REQUIRE(query.matches.size() == 1);
        const auto& pattern = query.matches[0].pattern;
        REQUIRE(pattern.nodes.front().variable == "b");
        REQUIRE(pattern.nodes.back().variable == "a");
        REQUIRE(pattern.edges.front().direction == EdgeDirection::LEFT); // Was RIGHT, reversed.
    }
}
