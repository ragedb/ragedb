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

TEST_CASE("GQL Optimizer Phase 22: Symmetric Traversal Simplification", "[gql_optimizer][alglib]") {
    GqlVirtualCatalog::local().clear();
    GqlVirtualCatalog::local().set_relationship_algebraic_properties("knows", {"symmetric"});

    // Query with filter on target node 'b' making it more selective (UNIQUE selectivity)
    std::string query_str = "MATCH (a)-[:knows]->(b) WHERE b.id = 1 RETURN a, b";
    auto query = GqlParser::parse(query_str);
    GqlOptimizer::optimize(query);

    // Direction should swap to start from selective variable 'b'
    const auto& match = query.matches[0];
    REQUIRE(match.pattern.nodes.size() == 2);
    REQUIRE(match.pattern.nodes[0].variable == "b");
    REQUIRE(match.pattern.nodes[1].variable == "a");
    REQUIRE(match.pattern.edges[0].direction == EdgeDirection::LEFT);
}

TEST_CASE("GQL Optimizer Phase 23: Transitive Path Pruning", "[gql_optimizer][alglib]") {
    GqlVirtualCatalog::local().clear();
    GqlVirtualCatalog::local().set_relationship_algebraic_properties("ancestor_of", {"transitive"});

    SECTION("Simplify variable-length paths of transitive relations to single hops") {
        std::string query_str = "MATCH (a)-[:ancestor_of*]->(b) RETURN a, b";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);

        const auto& match = query.matches[0];
        REQUIRE(match.transitive_reachability_lookup == true);
        REQUIRE(match.pattern.edges[0].is_variable_length == false);
    }

    SECTION("Prune redundant shortcut edges in joins") {
        std::string query_str = "MATCH (x)-[:ancestor_of]->(y) MATCH (y)-[:ancestor_of]->(z) MATCH (x)-[:ancestor_of]->(z) RETURN x, y, z";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);

        REQUIRE(query.matches.size() == 2);
        
        bool has_xy = false;
        bool has_yz = false;
        bool has_xz = false;
        
        for (const auto& match : query.matches) {
            std::string src = match.pattern.nodes[0].variable;
            std::string tgt = match.pattern.nodes[1].variable;
            if (src == "x" && tgt == "y") has_xy = true;
            if (src == "y" && tgt == "z") has_yz = true;
            if (src == "x" && tgt == "z") has_xz = true;
        }

        REQUIRE(has_xy == true);
        REQUIRE(has_yz == true);
        REQUIRE(has_xz == false);
    }
}

TEST_CASE("GQL Optimizer Phase 24: Irreflexive Contradiction Pruner", "[gql_optimizer][alglib]") {
    GqlVirtualCatalog::local().clear();
    GqlVirtualCatalog::local().set_relationship_algebraic_properties("parent_of", {"irreflexive"});

    SECTION("Direct self-loop contradiction") {
        std::string query_str = "MATCH (a)-[:parent_of]->(a) RETURN a";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);

        REQUIRE(query.no_op == true);
    }

    SECTION("Indirect equality-filter self-loop contradiction") {
        std::string query_str = "MATCH (a)-[:parent_of]->(b) WHERE a = b RETURN a";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);

        REQUIRE(query.no_op == true);
    }
}

TEST_CASE("GQL Optimizer Phase 25: Antisymmetric Loop Collapse", "[gql_optimizer][alglib]") {
    SECTION("Antisymmetric but not reflexive - should collapse to self-loop") {
        GqlVirtualCatalog::local().clear();
        GqlVirtualCatalog::local().set_relationship_algebraic_properties("part_of", {"antisymmetric"});

        std::string query_str = "MATCH (a)-[:part_of]->(b)-[:part_of]->(a) RETURN a, b";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);

        REQUIRE(query.matches.size() == 1);
        const auto& match = query.matches[0];
        REQUIRE(match.pattern.nodes.size() == 2);
        REQUIRE(match.pattern.nodes[0].variable == "a");
        REQUIRE(match.pattern.nodes[1].variable == "a");
        REQUIRE(match.pattern.edges.size() == 1);
        REQUIRE(match.pattern.edges[0].label_expr->name == "part_of");
    }

    SECTION("Antisymmetric and reflexive - should collapse to single node") {
        GqlVirtualCatalog::local().clear();
        GqlVirtualCatalog::local().set_relationship_algebraic_properties("part_of", {"antisymmetric", "reflexive"});

        std::string query_str = "MATCH (a)-[:part_of]->(b)-[:part_of]->(a) RETURN a, b";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);

        REQUIRE(query.matches.size() == 1);
        const auto& match = query.matches[0];
        REQUIRE(match.pattern.nodes.size() == 1);
        REQUIRE(match.pattern.nodes[0].variable == "a");
        REQUIRE(match.pattern.edges.empty());
    }
}

TEST_CASE("GQL Optimizer Phase 26: Equivalence Class Coalescing", "[gql_optimizer][alglib]") {
    GqlVirtualCatalog::local().clear();
    GqlVirtualCatalog::local().set_relationship_algebraic_properties("same_group", {"reflexive", "symmetric", "transitive"});

    std::string query_str = "MATCH (a)-[:same_group*]->(b) RETURN a, b";
    auto query = GqlParser::parse(query_str);
    GqlOptimizer::optimize(query);

    const auto& match = query.matches[0];
    REQUIRE(match.equivalence_partition_lookup == true);
    REQUIRE(match.pattern.edges[0].is_variable_length == false);
    REQUIRE(match.pattern.edges[0].min_hops == 1);
    REQUIRE(match.pattern.edges[0].max_hops == 1);
}
