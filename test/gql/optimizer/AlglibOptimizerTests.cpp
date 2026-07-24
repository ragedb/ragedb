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

// Phase 21 (DirectionSwapOptimizer) reverses a match so traversal begins at the more selective
// endpoint. It is driven purely by endpoint selectivity and is semantics-preserving for every
// relation -- which is why the old symmetric-only "Phase 22" gate was removed as redundant. These
// cases isolate that trigger: the swap must depend on WHERE-derived selectivity, not on any trait,
// so each asserts the exact reordered pattern and each negative control asserts no reordering.
TEST_CASE("GQL Optimizer Phase 21: Cardinality-Aware Direction Swap", "[gql_optimizer][alglib]") {
    GqlVirtualCatalog::local().clear();

    SECTION("id equality on the end node (UNIQUE) swaps traversal to start there") {
        auto query = GqlParser::parse("MATCH (a)-[:knows]->(b) WHERE b.id = 1 RETURN a, b");
        GqlOptimizer::optimize(query);

        const auto& match = query.matches[0];
        REQUIRE(match.pattern.nodes.size() == 2);
        REQUIRE(match.pattern.nodes[0].variable == "b");
        REQUIRE(match.pattern.nodes[1].variable == "a");
        REQUIRE(match.pattern.edges[0].direction == EdgeDirection::LEFT);
    }

    SECTION("a non-id property filter (INDEXED) on the end node also swaps") {
        auto query = GqlParser::parse("MATCH (a)-[:knows]->(b) WHERE b.age = 5 RETURN a, b");
        GqlOptimizer::optimize(query);

        const auto& match = query.matches[0];
        REQUIRE(match.pattern.nodes[0].variable == "b");
        REQUIRE(match.pattern.edges[0].direction == EdgeDirection::LEFT);
    }

    SECTION("selectivity already at the start node does NOT swap") {
        // 'a' is UNIQUE, 'b' is a SCAN: the end is not more selective, so the pattern is untouched.
        auto query = GqlParser::parse("MATCH (a)-[:knows]->(b) WHERE a.id = 1 RETURN a, b");
        GqlOptimizer::optimize(query);

        const auto& match = query.matches[0];
        REQUIRE(match.pattern.nodes[0].variable == "a");
        REQUIRE(match.pattern.nodes[1].variable == "b");
        REQUIRE(match.pattern.edges[0].direction == EdgeDirection::RIGHT);
    }

    SECTION("no selective filter (both SCAN) does NOT swap") {
        auto query = GqlParser::parse("MATCH (a)-[:knows]->(b) RETURN a, b");
        GqlOptimizer::optimize(query);

        const auto& match = query.matches[0];
        REQUIRE(match.pattern.nodes[0].variable == "a");
        REQUIRE(match.pattern.edges[0].direction == EdgeDirection::RIGHT);
    }

    SECTION("the swap is trait-independent: registering 'symmetric' changes nothing") {
        // The removed Phase 22 gated this reversal on a symmetric trait. Proving the swap is
        // identical with and without the trait is what the old test failed to show.
        GqlVirtualCatalog::local().set_relationship_algebraic_properties("knows", {"symmetric"});
        auto query = GqlParser::parse("MATCH (a)-[:knows]->(b) WHERE b.id = 1 RETURN a, b");
        GqlOptimizer::optimize(query);

        const auto& match = query.matches[0];
        REQUIRE(match.pattern.nodes[0].variable == "b");
        REQUIRE(match.pattern.edges[0].direction == EdgeDirection::LEFT);
    }
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

// Guards: the transitive-reachability rewrite must NOT fire for patterns the fast path
// gets wrong, and shortcut pruning must not drop constrained/bound matches.
TEST_CASE("GQL Optimizer Phase 23: transitive rewrite guards", "[gql_optimizer][alglib]") {
    GqlVirtualCatalog::local().clear();
    GqlVirtualCatalog::local().set_relationship_algebraic_properties("ancestor_of", {"transitive"});

    SECTION("bounded hops *2..2 are not rewritten") {
        auto q = GqlParser::parse("MATCH (a)-[:ancestor_of*2..2]->(b) RETURN a, b");
        GqlOptimizer::optimize(q);
        REQUIRE(q.matches[0].transitive_reachability_lookup == false);
    }
    SECTION("zero-lower-bound *0.. is not rewritten") {
        auto q = GqlParser::parse("MATCH (a)-[:ancestor_of*0..]->(b) RETURN a, b");
        GqlOptimizer::optimize(q);
        REQUIRE(q.matches[0].transitive_reachability_lookup == false);
    }
    SECTION("LEFT direction is not rewritten") {
        auto q = GqlParser::parse("MATCH (a)<-[:ancestor_of*]-(b) RETURN a, b");
        GqlOptimizer::optimize(q);
        REQUIRE(q.matches[0].transitive_reachability_lookup == false);
    }
    SECTION("a bound edge variable is not rewritten") {
        auto q = GqlParser::parse("MATCH (a)-[r:ancestor_of*]->(b) RETURN r");
        GqlOptimizer::optimize(q);
        REQUIRE(q.matches[0].transitive_reachability_lookup == false);
    }
    SECTION("the safe *1.. case is still rewritten") {
        auto q = GqlParser::parse("MATCH (a)-[:ancestor_of*]->(b) RETURN a, b");
        GqlOptimizer::optimize(q);
        REQUIRE(q.matches[0].transitive_reachability_lookup == true);
    }
    SECTION("a shortcut carrying an edge variable is not pruned") {
        auto q = GqlParser::parse("MATCH (x)-[:ancestor_of]->(y) MATCH (y)-[:ancestor_of]->(z) MATCH (x)-[e:ancestor_of]->(z) RETURN x, y, z, e");
        GqlOptimizer::optimize(q);
        REQUIRE(q.matches.size() == 3);
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

// Negative controls: the irreflexive pruner must NOT no_op these--each is satisfiable.
TEST_CASE("GQL Optimizer Phase 24: Irreflexive pruner negative controls", "[gql_optimizer][alglib]") {
    GqlVirtualCatalog::local().clear();
    GqlVirtualCatalog::local().set_relationship_algebraic_properties("parent_of", {"irreflexive"});

    SECTION("Anonymous endpoints are distinct nodes, not a self-loop") {
        auto query = GqlParser::parse("MATCH ()-[:parent_of]->() RETURN count(*)");
        GqlOptimizer::optimize(query);
        REQUIRE(query.no_op == false);
    }

    SECTION("Variable-length edge can form a longer cycle a->b->a (satisfiable)") {
        auto query = GqlParser::parse("MATCH (a)-[:parent_of*2..2]->(a) RETURN a");
        GqlOptimizer::optimize(query);
        REQUIRE(query.no_op == false);
    }

    SECTION("Equality inside a disjunction must not force a contradiction") {
        auto query = GqlParser::parse("MATCH (a)-[:parent_of]->(b) WHERE a = b OR a.x = 1 RETURN a");
        GqlOptimizer::optimize(query);
        REQUIRE(query.no_op == false);
    }

    SECTION("Positive control still fires: a genuine direct self-loop is a contradiction") {
        auto query = GqlParser::parse("MATCH (a)-[:parent_of]->(a) RETURN a");
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

// Guards: the equivalence-partition rewrite must NOT fire for patterns the fast path
// gets wrong (bounded hops, direction, bound edge variable, predicates, path var, shortest).
TEST_CASE("GQL Optimizer Phase 26: equivalence rewrite guards", "[gql_optimizer][alglib]") {
    GqlVirtualCatalog::local().clear();
    GqlVirtualCatalog::local().set_relationship_algebraic_properties("same_group", {"reflexive", "symmetric", "transitive"});

    SECTION("bounded hops *2..3 are not rewritten") {
        auto q = GqlParser::parse("MATCH (a)-[:same_group*2..3]->(b) RETURN a, b");
        GqlOptimizer::optimize(q);
        REQUIRE(q.matches[0].equivalence_partition_lookup == false);
    }
    SECTION("LEFT direction is not rewritten") {
        auto q = GqlParser::parse("MATCH (a)<-[:same_group*]-(b) RETURN a, b");
        GqlOptimizer::optimize(q);
        REQUIRE(q.matches[0].equivalence_partition_lookup == false);
    }
    SECTION("a bound edge variable is not rewritten") {
        auto q = GqlParser::parse("MATCH (a)-[r:same_group*]->(b) RETURN r");
        GqlOptimizer::optimize(q);
        REQUIRE(q.matches[0].equivalence_partition_lookup == false);
    }
    SECTION("the safe *1.. case is still rewritten") {
        auto q = GqlParser::parse("MATCH (a)-[:same_group*]->(b) RETURN a, b");
        GqlOptimizer::optimize(q);
        REQUIRE(q.matches[0].equivalence_partition_lookup == true);
    }
}

// the antisymmetric collapser must preserve labels/constraints/bound edge variables and
// detect the irreflexive contradiction, while still collapsing the unconstrained case.
TEST_CASE("GQL Optimizer Phase 25: collapser preserves constraints", "[gql_optimizer][alglib]") {
    GqlVirtualCatalog::local().clear();

    SECTION("a labeled single-node match is not pruned away") {
        auto q = GqlParser::parse("MATCH (x:Person) MATCH (x)-[:KNOWS]->(y) RETURN x, y");
        GqlOptimizer::optimize(q);
        REQUIRE(q.matches.size() == 2);
    }

    SECTION("labels survive an antisymmetric+reflexive collapse (AND, not dropped)") {
        GqlVirtualCatalog::local().set_relationship_algebraic_properties("po", {"antisymmetric", "reflexive"});
        auto q = GqlParser::parse("MATCH (a:A)-[:po]->(b:B)-[:po]->(a) RETURN a");
        GqlOptimizer::optimize(q);
        REQUIRE(q.matches.size() == 1);
        REQUIRE(q.matches[0].pattern.nodes[0].label_expr != nullptr);
        REQUIRE(q.matches[0].pattern.nodes[0].label_expr->kind == LabelExprKind::AND);
    }

    SECTION("a collapse that would drop a bound edge variable is refused") {
        GqlVirtualCatalog::local().set_relationship_algebraic_properties("po", {"antisymmetric"});
        auto q = GqlParser::parse("MATCH (a)-[r:po]->(b)-[r2:po]->(a) RETURN a, r, r2");
        GqlOptimizer::optimize(q);
        REQUIRE(q.matches[0].pattern.edges.size() == 2);
    }

    SECTION("an antisymmetric + irreflexive 2-cycle is a contradiction (no_op)") {
        GqlVirtualCatalog::local().set_relationship_algebraic_properties("po", {"antisymmetric", "irreflexive"});
        auto q = GqlParser::parse("MATCH (a)-[:po]->(b)-[:po]->(a) RETURN a");
        GqlOptimizer::optimize(q);
        REQUIRE(q.no_op == true);
    }

    SECTION("positive control: an unconstrained antisymmetric 2-cycle still collapses") {
        GqlVirtualCatalog::local().set_relationship_algebraic_properties("po", {"antisymmetric"});
        auto q = GqlParser::parse("MATCH (a)-[:po]->(b)-[:po]->(a) RETURN a, b");
        GqlOptimizer::optimize(q);
        REQUIRE(q.matches.size() == 1);
        REQUIRE(q.matches[0].pattern.edges.size() == 1);
    }
}
