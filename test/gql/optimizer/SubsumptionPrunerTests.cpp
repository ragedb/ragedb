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
