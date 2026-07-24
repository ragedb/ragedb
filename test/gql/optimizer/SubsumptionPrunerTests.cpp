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

TEST_CASE("GQL Optimizer Phase 6: Subsumption & Query Containment Pruning", "[gql_optimizer][semantic]") {
    GqlVirtualCatalog::local().clear();

    SECTION("Case 1: Standard implication (b.age > 21 AND c.age > 18) -> prunes second match under DISTINCT") {
        // The two arms bind different targets (b, c), so this is a self-join: erasing one drops its row
        // multiplication and is only sound under set semantics. DISTINCT dedups it.
        std::string query_str = "MATCH (a:Person)-[:FRIEND]->(b:Person), (a)-[:FRIEND]->(c:Person) WHERE b.age > 21 AND c.age > 18 RETURN DISTINCT a.name";
        auto query = GqlParser::parse(query_str);

        REQUIRE(query.matches.size() == 2);

        GqlOptimizer::optimize(query);

        // Second match should be pruned, leaving only 1 match (the stronger one, b)
        REQUIRE(query.matches.size() == 1);
        REQUIRE(query.matches[0].pattern.nodes[0].variable == "b");
        REQUIRE(query.matches[0].pattern.nodes[1].variable == "a");

        // b's filter was pushed down, so where_expr is now nullptr
        REQUIRE(query.where_expr == nullptr);

        // Verify filter was pushed down to b
        const auto& filters = query.matches[0].pattern.nodes[0].property_filters;
        REQUIRE(filters.size() == 1);
        REQUIRE(filters[0].property == "age");
        REQUIRE(filters[0].op == Operation::GT);
        REQUIRE(std::get<int64_t>(filters[0].value) == 21);
    }

    SECTION("a bag-semantic self-join is NOT pruned (multiplicity is observable)") {
        // Same as Case 1 without DISTINCT: erasing the c arm would drop rows, so both matches survive.
        std::string query_str = "MATCH (a:Person)-[:FRIEND]->(b:Person), (a)-[:FRIEND]->(c:Person) WHERE b.age > 21 AND c.age > 18 RETURN a.name";
        auto query = GqlParser::parse(query_str);
        REQUIRE(query.matches.size() == 2);
        GqlOptimizer::optimize(query);
        REQUIRE(query.matches.size() == 2);
    }

    SECTION("a self-join feeding count(*) is NOT pruned (the 093 undercount)") {
        // Erasing the equivalent arm would change count(*) from sum(deg^2) to sum(deg).
        std::string query_str = "MATCH (h:Person)-[:KNOWS]->(f:Person) MATCH (h)-[:KNOWS]->(g:Person) RETURN count(*) AS n";
        auto query = GqlParser::parse(query_str);
        REQUIRE(query.matches.size() == 2);
        GqlOptimizer::optimize(query);
        REQUIRE(query.matches.size() == 2);
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
        std::string query_str = "MATCH (a:Person)-[:FRIEND]->(b:Person), (a)-[:FRIEND]->(c:Person) WHERE b.age > 21 AND c.age > 25 RETURN DISTINCT a.name";
        auto query = GqlParser::parse(query_str);
        
        REQUIRE(query.matches.size() == 2);
        
        GqlOptimizer::optimize(query);
        
        // The weaker pattern b should be pruned, leaving only the stronger pattern c
        REQUIRE(query.matches.size() == 1);
        REQUIRE(query.matches[0].pattern.nodes[0].variable == "c");
        REQUIRE(query.matches[0].pattern.nodes[1].variable == "a");
        
        // c's filter was pushed down
        REQUIRE(query.where_expr == nullptr);
        const auto& filters = query.matches[0].pattern.nodes[0].property_filters;
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

    SECTION("Case 4: empty filters, vacuous implication prunes under DISTINCT") {
        std::string query_str = "MATCH (a:Person)-[:FRIEND]->(b:Person), (a)-[:FRIEND]->(c:Person) RETURN DISTINCT a.name";
        auto query = GqlParser::parse(query_str);

        REQUIRE(query.matches.size() == 2);

        GqlOptimizer::optimize(query);

        // One should be subsumed by the other because filters are empty (vacuous implication)
        REQUIRE(query.matches.size() == 1);
    }

    SECTION("a true-duplicate match (same variables) is pruned even without DISTINCT") {
        // Both arms bind the SAME variable b, so the second is a literal duplicate at multiplicity 1;
        // erasing it cannot change the result, so it is pruned regardless of set semantics. This is the
        // shape a mandatory-join produces after JoinEliminator strips both arms to the same bare node,
        // and limit pushdown relies on the reduction to a single match.
        std::string query_str = "MATCH (a:Person)-[:FRIEND]->(b:Person) MATCH (a:Person)-[:FRIEND]->(b:Person) RETURN a.name";
        auto query = GqlParser::parse(query_str);
        REQUIRE(query.matches.size() == 2);
        GqlOptimizer::optimize(query);
        REQUIRE(query.matches.size() == 1);
    }

    SECTION("count(*) with two subsumable matches does not crash the dead-end walk (SIGSEGV)") {
        // count(*) is an aggregate with a NULL argument; the subsumption dead-end walk pushed that null
        // child and dereferenced it. It only fired when two structurally equivalent matches made the
        // pruner examine the projection -- here the two KNOWS branches around h.
        std::string query_str =
            "MATCH (h:Person)-[:KNOWS]->(f:Person) "
            "MATCH (h)-[:KNOWS]->(g:Person) "
            "RETURN count(*) AS n";
        auto query = GqlParser::parse(query_str);
        REQUIRE(query.matches.size() == 2);
        // Must not segfault. The subsumption pass may or may not prune; the point is it returns.
        GqlOptimizer::optimize(query);
        SUCCEED("optimize returned without crashing");
    }

    SECTION("an aggregate with a null argument is walked safely in ORDER BY too") {
        std::string query_str =
            "MATCH (h:Person)-[:KNOWS]->(f:Person) "
            "MATCH (h)-[:KNOWS]->(g:Person) "
            "RETURN h.name AS name, count(*) AS n ORDER BY count(*) DESC";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        SUCCEED("optimize returned without crashing");
    }
}
