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

TEST_CASE("GQL Optimizer Phase 1: Primitive Constraints & Contradiction Pruning", "[gql_optimizer][semantic]") {
    GqlVirtualCatalog::local().clear();
    // Register constraint: no Person has age < 0 (i.e. Person.age < 0 is impossible)
    GqlVirtualCatalog::local().add_constraint("PositiveAge", "MATCH (p:Person) WHERE p.age < 0 RETURN p");

    SECTION("Contradiction with constraint bounds") {
        std::string query_str = "MATCH (x:Person) WHERE x.age = -5 RETURN x";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        REQUIRE(query.no_op == true);
    }

    SECTION("Contradiction with constraint range bounds") {
        std::string query_str = "MATCH (x:Person) WHERE x.age < -2 RETURN x";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        REQUIRE(query.no_op == true);
    }

    SECTION("Self contradiction within query filters") {
        std::string query_str = "MATCH (x:Person) WHERE x.age > 10 AND x.age < 5 RETURN x";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        REQUIRE(query.no_op == true);
    }

    GqlVirtualCatalog::local().clear();
}

TEST_CASE("GQL Optimizer Phase 2: Join Elimination", "[gql_optimizer][semantic]") {
    GqlVirtualCatalog::local().clear();
    // Register mandatory relationship constraint: every Shipment must have a SHIPPED_FROM Location
    GqlVirtualCatalog::local().add_constraint("MandatoryShippedFrom", "MATCH (s:Shipment) WHERE NOT EXISTS { MATCH (s)-[:SHIPPED_FROM]->(l:Location) } RETURN s");

    std::string query_str = "MATCH (s:Shipment)-[:SHIPPED_FROM]->(l:Location) RETURN s";
    auto query = GqlParser::parse(query_str);
    GqlOptimizer::optimize(query);

    // The join should be eliminated: the pattern should contain only the start node and no edges
    REQUIRE(query.matches[0].pattern.nodes.size() == 1);
    REQUIRE(query.matches[0].pattern.nodes[0].variable == "s");
    REQUIRE(query.matches[0].pattern.edges.empty());

    GqlVirtualCatalog::local().clear();
}

TEST_CASE("GQL Optimizer Phase 3: Multi-Variable Relational Predicate Reasoning", "[gql_optimizer][semantic]") {
    GqlVirtualCatalog::local().clear();

    SECTION("Impossible cycle (strict inequality contradiction)") {
        // a < b AND b <= c AND c < a is impossible (transitivity implies a < a)
        std::string query_str = "MATCH (a:Person), (b:Person), (c:Person) WHERE a.age < b.age AND b.age <= c.age AND c.age < a.age RETURN a";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        REQUIRE(query.no_op == true);
    }

    SECTION("Possible cycle (non-strict cycle)") {
        // a <= b AND b <= c AND c <= a is possible (implies equality)
        std::string query_str = "MATCH (a:Person), (b:Person), (c:Person) WHERE a.age <= b.age AND b.age <= c.age AND c.age <= a.age RETURN a";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        REQUIRE_FALSE(query.no_op == true);
    }
}

TEST_CASE("GQL Optimizer Phase 4: Algebraic Query Rewrites (Aggregation Pushdown)", "[gql_optimizer][semantic]") {
    GqlVirtualCatalog::local().clear();

    std::string query_str = "MATCH (p:Person)-[:FRIEND]->(f:Person) RETURN p.name, sum(p.age * f.age)";
    auto query = GqlParser::parse(query_str);
    GqlOptimizer::optimize(query);

    // The sum(p.age * f.age) aggregation should be rewritten to p.age * sum(f.age)
    REQUIRE(query.returns[1].expr->kind == ExpressionKind::BINARY_OP);
    auto* bin = static_cast<const BinaryOpExpr*>(query.returns[1].expr.get());
    REQUIRE(bin->op == BinaryOpKind::MUL);

    // Left should be p.age
    REQUIRE(bin->left->kind == ExpressionKind::PROPERTY_LOOKUP);
    auto* pl = static_cast<const PropertyLookupExpr*>(bin->left.get());
    REQUIRE(pl->variable == "p");
    REQUIRE(pl->property == "age");

    // Right should be sum(f.age)
    REQUIRE(bin->right->kind == ExpressionKind::AGGREGATION);
    auto* agg = static_cast<const AggregateExpr*>(bin->right.get());
    REQUIRE(agg->fn_kind == AggregateKind::SUM);
    REQUIRE(agg->expr->kind == ExpressionKind::PROPERTY_LOOKUP);
    auto* pl_agg = static_cast<const PropertyLookupExpr*>(agg->expr.get());
    REQUIRE(pl_agg->variable == "f");
    REQUIRE(pl_agg->property == "age");
}

TEST_CASE("GQL Optimizer Phase 5: Skip Semantic Optimization", "[gql_optimizer][semantic]") {
    GqlVirtualCatalog::local().clear();
    GqlVirtualCatalog::local().add_constraint("PositiveAge", "MATCH (p:Person) WHERE p.age < 0 RETURN p");

    SECTION("Using NO_SEMANTIC keyword prefix") {
        std::string query_str = "NO_SEMANTIC MATCH (x:Person) WHERE x.age = -5 RETURN x";
        auto query = GqlParser::parse(query_str);
        REQUIRE(query.skip_semantic == true);
        GqlOptimizer::optimize(query);
        // Bypassed, so no_op should still be false
        REQUIRE(query.no_op == false);
    }

    SECTION("Using /* no_semantic */ comment prefix") {
        std::string query_str = "/* no_semantic */ MATCH (x:Person) WHERE x.age = -5 RETURN x";
        auto query = GqlParser::parse(query_str);
        REQUIRE(query.skip_semantic == true);
        GqlOptimizer::optimize(query);
        // Bypassed, so no_op should still be false
        REQUIRE(query.no_op == false);
    }

    GqlVirtualCatalog::local().clear();
}

TEST_CASE("GQL Optimizer Algebraic Path Count Rewrite", "[gql_optimizer]") {
    GqlVirtualCatalog::local().clear();

    SECTION("Case 1: Variable-length pattern (a)-[:EDGE]->{k}(d)") {
        std::string query_str = "MATCH (a:Person)-[:FRIEND]->{3}(d) RETURN a, count(d)";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);

        // Assert optimizer flags are set
        const auto& match = query.matches[0];
        REQUIRE(match.algebraic_path_count == true);
        REQUIRE(match.path_count_hops == 3);
        REQUIRE(match.path_count_target_var == "d");
        REQUIRE(match.path_count_rel_types == std::vector<std::string>{"FRIEND"});
        REQUIRE(match.path_count_dir == EdgeDirection::RIGHT);

        // Pattern should be truncated to just node 'a'
        REQUIRE(match.pattern.nodes.size() == 1);
        REQUIRE(match.pattern.nodes[0].variable == "a");
        REQUIRE(match.pattern.edges.empty());

        // Return expression should be rewritten to just 'd'
        REQUIRE(query.returns[1].expr->kind == ExpressionKind::VARIABLE);
        REQUIRE(static_cast<const VariableExpr*>(query.returns[1].expr.get())->name == "d");
        REQUIRE(query.returns[1].alias == "count(d)");
    }

    SECTION("Case 2: Hop chain (a)-[:EDGE]->(b)-[:EDGE]->(c)-[:EDGE]->(d)") {
        std::string query_str = "MATCH (a:Person)-[:FRIEND]->(b)-[:FRIEND]->(c)-[:FRIEND]->(d) RETURN a, count(d)";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);

        const auto& match = query.matches[0];
        REQUIRE(match.algebraic_path_count == true);
        REQUIRE(match.path_count_hops == 3);
        REQUIRE(match.path_count_target_var == "d");
        REQUIRE(match.path_count_rel_types == std::vector<std::string>{"FRIEND"});
        REQUIRE(match.path_count_dir == EdgeDirection::RIGHT);

        // Pattern should be truncated to just node 'a'
        REQUIRE(match.pattern.nodes.size() == 1);
        REQUIRE(match.pattern.nodes[0].variable == "a");
        REQUIRE(match.pattern.edges.empty());

        // Return expression should be rewritten to just 'd'
        REQUIRE(query.returns[1].expr->kind == ExpressionKind::VARIABLE);
        REQUIRE(static_cast<const VariableExpr*>(query.returns[1].expr.get())->name == "d");
        REQUIRE(query.returns[1].alias == "count(d)");
    }
}

TEST_CASE("GQL Optimizer Phase 5: Cardinality-Constrained Traversal Short-Circuiting", "[gql_optimizer][semantic]") {
    GqlVirtualCatalog::local().clear();

    // Register cardinality constraint: Person has FRIEND out-degree <= 1
    GqlVirtualCatalog::local().add_constraint(
        "PersonFriendMaxCard",
        "MATCH (p:Person)-[:FRIEND]->(f1) MATCH (p)-[:FRIEND]->(f2) WHERE f1 != f2 RETURN p"
    );

    SECTION("Matches constraint out-degree <= 1") {
        std::string query_str = "MATCH (p:Person)-[r:FRIEND]->(f:Person) RETURN p, f";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);

        REQUIRE(query.matches[0].pattern.edges[0].max_cardinality_limit.has_value());
        REQUIRE(*query.matches[0].pattern.edges[0].max_cardinality_limit == 1);
    }

    SECTION("Does not match constraint due to direction") {
        std::string query_str = "MATCH (p:Person)<-[r:FRIEND]-(f:Person) RETURN p, f";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);

        REQUIRE_FALSE(query.matches[0].pattern.edges[0].max_cardinality_limit.has_value());
    }

    SECTION("Does not match constraint due to relationship type") {
        std::string query_str = "MATCH (p:Person)-[r:KNOWS]->(f:Person) RETURN p, f";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);

        REQUIRE_FALSE(query.matches[0].pattern.edges[0].max_cardinality_limit.has_value());
    }

    GqlVirtualCatalog::local().clear();
}

TEST_CASE("GQL Optimizer Phase 6: Subsumption & Query Containment Pruning", "[gql_optimizer][semantic]") {
    GqlVirtualCatalog::local().clear();

    SECTION("Case 1: Standard implication (b.age > 21 AND c.age > 18) -> prunes second match") {
        std::string query_str = "MATCH (a:Person)-[:FRIEND]->(b:Person), (a)-[:FRIEND]->(c:Person) WHERE b.age > 21 AND c.age > 18 RETURN a.name";
        auto query = GqlParser::parse(query_str);
        
        REQUIRE(query.matches.size() == 2);
        
        GqlOptimizer::optimize(query);
        
        // Second match should be pruned, leaving only 1 match (the stronger one, b)
        REQUIRE(query.matches.size() == 1);
        REQUIRE(query.matches[0].pattern.nodes[1].variable == "b");
        
        // b's filter was pushed down, so where_expr is now nullptr
        REQUIRE(query.where_expr == nullptr);
        
        // Verify filter was pushed down to b
        const auto& filters = query.matches[0].pattern.nodes[1].property_filters;
        REQUIRE(filters.size() == 1);
        REQUIRE(filters[0].property == "age");
        REQUIRE(filters[0].op == Operation::GT);
        REQUIRE(std::get<int64_t>(filters[0].value) == 21);
    }

    SECTION("Case 2: No implication (b.age < 18 AND c.age > 25) -> no pruning") {
        std::string query_str = "MATCH (a:Person)-[:FRIEND]->(b:Person), (a)-[:FRIEND]->(c:Person) WHERE b.age < 18 AND c.age > 25 RETURN a.name";
        auto query = GqlParser::parse(query_str);
        
        REQUIRE(query.matches.size() == 2);
        
        GqlOptimizer::optimize(query);
        
        // No pruning should happen since filters are disjoint
        REQUIRE(query.matches.size() == 2);
    }

    SECTION("Case 2.5: Subsumption when written in reverse order (c.age > 25 implies b.age > 21) -> prunes weaker match b") {
        std::string query_str = "MATCH (a:Person)-[:FRIEND]->(b:Person), (a)-[:FRIEND]->(c:Person) WHERE b.age > 21 AND c.age > 25 RETURN a.name";
        auto query = GqlParser::parse(query_str);
        
        REQUIRE(query.matches.size() == 2);
        
        GqlOptimizer::optimize(query);
        
        // The weaker pattern b should be pruned, leaving only the stronger pattern c
        REQUIRE(query.matches.size() == 1);
        REQUIRE(query.matches[0].pattern.nodes[1].variable == "c");
        
        // c's filter was pushed down
        REQUIRE(query.where_expr == nullptr);
        const auto& filters = query.matches[0].pattern.nodes[1].property_filters;
        REQUIRE(filters.size() == 1);
        REQUIRE(filters[0].property == "age");
        REQUIRE(filters[0].op == Operation::GT);
        REQUIRE(std::get<int64_t>(filters[0].value) == 25);
    }

    SECTION("Case 3: Target variable c is returned -> no pruning") {
        std::string query_str = "MATCH (a:Person)-[:FRIEND]->(b:Person), (a)-[:FRIEND]->(c:Person) WHERE b.age > 21 AND c.age > 18 RETURN a.name, c.name";
        auto query = GqlParser::parse(query_str);
        
        REQUIRE(query.matches.size() == 2);
        
        GqlOptimizer::optimize(query);
        
        // No pruning because c is projected
        REQUIRE(query.matches.size() == 2);
    }

    SECTION("Case 4: Target variables are identical -> handled by redundant join pruning, but let's test empty filters") {
        std::string query_str = "MATCH (a:Person)-[:FRIEND]->(b:Person), (a)-[:FRIEND]->(c:Person) RETURN a.name";
        auto query = GqlParser::parse(query_str);
        
        REQUIRE(query.matches.size() == 2);
        
        GqlOptimizer::optimize(query);
        
        // One should be subsumed by the other because filters are empty (vacuous implication)
        REQUIRE(query.matches.size() == 1);
    }
}

TEST_CASE("GQL Optimizer Phase 7: Composite Attribute Domain Constraint Reasoning", "[gql_optimizer][semantic]") {
    GqlVirtualCatalog::local().clear();
    // Register composite constraint: p.age < 18 OR p.status = 'minor' OR p.is_student = true is impossible
    GqlVirtualCatalog::local().add_constraint(
        "CompositePersonConstraint",
        "MATCH (p:Person) WHERE p.age < 18 OR p.status = 'minor' OR p.is_student = true RETURN p"
    );

    SECTION("Satisfiable query matching consistent attributes") {
        // age >= 18 AND status != 'minor' AND is_student != true is consistent
        std::string query_str = "MATCH (x:Person) WHERE x.age >= 18 AND x.status = 'adult' AND x.is_student = false RETURN x.name";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        REQUIRE_FALSE(query.no_op == true);
    }

    SECTION("Unsatisfiable query matching subset of impossible age range") {
        // x.age < 18 contradicts the constraint guarantee (which implies age >= 18)
        std::string query_str = "MATCH (x:Person) WHERE x.age = 15 RETURN x.name";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        REQUIRE(query.no_op == true);
    }

    SECTION("Unsatisfiable query matching subset of impossible string domain") {
        // x.status = 'minor' contradicts the constraint guarantee (which implies status != 'minor')
        std::string query_str = "MATCH (x:Person) WHERE x.status = 'minor' RETURN x.name";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        REQUIRE(query.no_op == true);
    }

    SECTION("Unsatisfiable query matching subset of impossible boolean domain") {
        // x.is_student = true contradicts the constraint guarantee (which implies is_student != true)
        std::string query_str = "MATCH (x:Person) WHERE x.is_student = true RETURN x.name";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        REQUIRE(query.no_op == true);
    }

    SECTION("Satisfiable query with overlapping but consistent ranges") {
        std::string query_str = "MATCH (x:Person) WHERE x.age > 20 RETURN x.name";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        REQUIRE_FALSE(query.no_op == true);
    }

    GqlVirtualCatalog::local().clear();
}




