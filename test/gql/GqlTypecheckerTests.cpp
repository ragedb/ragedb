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
#include <Graph.h>
#include "../../src/gql/GqlParser.h"
#include "../../src/gql/GqlOptimizer.h"
#include "../../src/gql/GqlExecutor.h"
#include "../../src/gql/GqlTypechecker.h"

using namespace ragedb;
using namespace ragedb::gql;

TEST_CASE("GQL Static Typechecker Tests", "[gql_typechecker]") {
    auto graph = Graph("gql_typechecker_test");
    graph.Start().get();
    graph.Clear();
    
    // Set up schema
    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "age", "integer").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "weight", "double").get();

    graph.shard.local().NodeTypeInsertPeered("Company").get();
    graph.shard.local().NodePropertyTypeAddPeered("Company", "name", "string").get();

    graph.shard.local().RelationshipTypeInsertPeered("FRIEND_OF").get();
    graph.shard.local().RelationshipPropertyTypeAddPeered("FRIEND_OF", "since", "integer").get();

    SECTION("Valid Query Passes") {
        std::string query_str = "MATCH (p:Person)-[:FRIEND_OF]->(c:Company) WHERE p.age > 21 RETURN p.name, c.name";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        
        REQUIRE_NOTHROW(GqlTypechecker::typecheck(graph, query));
    }

    SECTION("Non-existent Node Type Throws") {
        std::string query_str = "MATCH (p:Animal) RETURN p";
        auto query = GqlParser::parse(query_str);
        
        REQUIRE_THROWS_WITH(GqlTypechecker::typecheck(graph, query), Catch::Contains("NodeType 'Animal' does not exist in schema"));
    }

    SECTION("Non-existent Relationship Type Throws") {
        std::string query_str = "MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN p";
        auto query = GqlParser::parse(query_str);
        
        REQUIRE_THROWS_WITH(GqlTypechecker::typecheck(graph, query), Catch::Contains("RelationshipType 'WORKS_AT' does not exist in schema"));
    }

    SECTION("Non-existent Property Lookup Throws") {
        std::string query_str = "MATCH (p:Person) WHERE p.salary > 50000 RETURN p";
        auto query = GqlParser::parse(query_str);
        
        REQUIRE_THROWS_WITH(GqlTypechecker::typecheck(graph, query), Catch::Contains("Property 'salary' does not exist on any of the matched types"));
    }

    SECTION("Disjoint Variable Conjunction Throws") {
        // Person and Company are distinct types, so p:Person&Company is disjoint
        std::string query_str = "MATCH (p:Person&Company) RETURN p";
        auto query = GqlParser::parse(query_str);
        
        REQUIRE_THROWS_WITH(GqlTypechecker::typecheck(graph, query), Catch::Contains("Label expression reduces to empty set (disjoint conjunction)"));
    }

    SECTION("Conflicting Variable Type (Node as Relationship) Throws") {
        std::string query_str = "MATCH (p:Person)-[p:FRIEND_OF]->(c:Company) RETURN p";
        auto query = GqlParser::parse(query_str);
        
        REQUIRE_THROWS_WITH(GqlTypechecker::typecheck(graph, query), Catch::Contains("Variable 'p' has conflicting types: NODE vs RELATIONSHIP"));
    }

    SECTION("Unbound Variable Reference Throws") {
        std::string query_str = "MATCH (p:Person) RETURN x";
        auto query = GqlParser::parse(query_str);
        
        REQUIRE_THROWS_WITH(GqlTypechecker::typecheck(graph, query), Catch::Contains("Variable 'x' is not bound"));
    }

    SECTION("Incompatible Comparison Throws") {
        std::string query_str = "MATCH (p:Person) WHERE p.name = 123 RETURN p";
        auto query = GqlParser::parse(query_str);
        
        REQUIRE_THROWS_WITH(GqlTypechecker::typecheck(graph, query), Catch::Contains("Incompatible types for comparison: STRING and INTEGER"));
    }

    SECTION("Arithmetic Type Compatibility (Integer Promotion to Double)") {
        // Person.weight is double, p.age is integer. Adding them promotes to double.
        std::string query_str = "MATCH (p:Person) WHERE (p.weight + p.age) > 100.0 RETURN p";
        auto query = GqlParser::parse(query_str);
        
        REQUIRE_NOTHROW(GqlTypechecker::typecheck(graph, query));
    }

    SECTION("Non-numeric Arithmetic Throws") {
        std::string query_str = "MATCH (p:Person) WHERE (p.name + p.age) > 100 RETURN p";
        auto query = GqlParser::parse(query_str);
        
        REQUIRE_THROWS_WITH(GqlTypechecker::typecheck(graph, query), Catch::Contains("Arithmetic operands must be numeric, got STRING and INTEGER"));
    }

    SECTION("Assignment Type Mismatch in SET Throws") {
        std::string query_str = "MATCH (p:Person) SET p.age = 'thirty'";
        auto query = GqlParser::parse(query_str);
        
        REQUIRE_THROWS_WITH(GqlTypechecker::typecheck(graph, query), Catch::Contains("Type mismatch for property 'age': expected INTEGER, got STRING"));
    }

    SECTION("Complex Label Algebra Conjunction and Disjunction (NOT / OR)") {
        // Matches nodes that are Person or Company but NOT Person (reduces to Company)
        std::string query_str = "MATCH (p:(Person|Company)&!Person) RETURN p.name";
        auto query = GqlParser::parse(query_str);
        
        // This is valid, and should evaluate p as Node with label "Company" which has property "name"
        REQUIRE_NOTHROW(GqlTypechecker::typecheck(graph, query));

        // Now lookup property "age" on p (reduces to Company, which has NO "age" property)
        std::string invalid_query_str = "MATCH (p:(Person|Company)&!Person) WHERE p.age > 21 RETURN p";
        auto invalid_query = GqlParser::parse(invalid_query_str);
        REQUIRE_THROWS_WITH(GqlTypechecker::typecheck(graph, invalid_query), Catch::Contains("Property 'age' does not exist on any of the matched types"));
    }

    SECTION("Setting internal key property is rejected") {
        std::string query_str = "MATCH (p:Person) SET p.key = 'new_key'";
        auto query = GqlParser::parse(query_str);
        
        REQUIRE_THROWS_WITH(GqlTypechecker::typecheck(graph, query), Catch::Contains("Cannot set the internal 'key' property"));
    }

    SECTION("Removing internal key property is rejected") {
        std::string query_str = "MATCH (p:Person) REMOVE p.key";
        auto query = GqlParser::parse(query_str);
        
        REQUIRE_THROWS_WITH(GqlTypechecker::typecheck(graph, query), Catch::Contains("Cannot remove the internal 'key' property"));
    }

    SECTION("Creating schema node/relationship type with key property is rejected") {
        std::string query_str1 = "CREATE NODE TYPE Customer (key STRING)";
        auto query1 = GqlParser::parse(query_str1);
        REQUIRE_THROWS_WITH(GqlTypechecker::typecheck(graph, query1), Catch::Contains("Cannot define a property named 'key' on schema types as it is an internal property"));

        std::string query_str2 = "CREATE RELATIONSHIP TYPE ENEMY_OF (key STRING)";
        auto query2 = GqlParser::parse(query_str2);
        REQUIRE_THROWS_WITH(GqlTypechecker::typecheck(graph, query2), Catch::Contains("Cannot define a property named 'key' on schema types as it is an internal property"));
    }

    SECTION("Altering schema node/relationship type to add key property is rejected") {
        std::string query_str1 = "ALTER NODE TYPE Person ADD key STRING";
        auto query1 = GqlParser::parse(query_str1);
        REQUIRE_THROWS_WITH(GqlTypechecker::typecheck(graph, query1), Catch::Contains("Cannot define a property named 'key' on schema types as it is an internal property"));

        std::string query_str2 = "ALTER RELATIONSHIP TYPE FRIEND_OF ADD key STRING";
        auto query2 = GqlParser::parse(query_str2);
        REQUIRE_THROWS_WITH(GqlTypechecker::typecheck(graph, query2), Catch::Contains("Cannot define a property named 'key' on schema types as it is an internal property"));
     }

    SECTION("Typecheck IS NULL and IS NOT NULL") {
        std::string query_str = "MATCH (p:Person) WHERE p.age IS NULL OR p.name IS NOT NULL RETURN p";
        auto query = GqlParser::parse(query_str);
        REQUIRE_NOTHROW(GqlTypechecker::typecheck(graph, query));
    }

    SECTION("Typecheck string concatenation success and failure") {
        {
            std::string query_str = "MATCH (p:Person) WHERE p.name = 'Al' || 'ice' RETURN p";
            auto query = GqlParser::parse(query_str);
            REQUIRE_NOTHROW(GqlTypechecker::typecheck(graph, query));
        }
        {
            std::string query_str = "MATCH (p:Person) WHERE p.name = p.age || 'ice' RETURN p";
            auto query = GqlParser::parse(query_str);
            REQUIRE_THROWS_WITH(GqlTypechecker::typecheck(graph, query), Catch::Contains("Concatenation operands must be STRING, got"));
        }
    }

    SECTION("Typecheck string comparison operators success and failure") {
        {
            std::string query_str = "MATCH (p:Person) WHERE p.name STARTS WITH 'Al' AND p.name ENDS WITH 'ice' OR p.name CONTAINS 'o' RETURN p";
            auto query = GqlParser::parse(query_str);
            REQUIRE_NOTHROW(GqlTypechecker::typecheck(graph, query));
        }
        {
            std::string query_str = "MATCH (p:Person) WHERE p.name STARTS WITH 123 RETURN p";
            auto query = GqlParser::parse(query_str);
            REQUIRE_THROWS_WITH(GqlTypechecker::typecheck(graph, query), Catch::Contains("String comparison operands must be STRING, got"));
        }
    }

    graph.Stop().get();
}

TEST_CASE("Typechecker is linear-query (NEXT) aware", "[gql_typechecker]") {
    auto graph = Graph("gql_typechecker_with_test");
    graph.Start().get();
    graph.Clear();
    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "age", "integer").get();
    graph.shard.local().RelationshipTypeInsertPeered("FRIEND_OF").get();

    SECTION("piped variables are visible in later segments") {
        auto query = GqlParser::parse("MATCH (p:Person) RETURN p NEXT RETURN p.name");
        REQUIRE_NOTHROW(GqlTypechecker::typecheck(graph, query));

        auto staged = GqlParser::parse(
            "MATCH (p:Person) RETURN p NEXT MATCH (p)-[:FRIEND_OF]->(f:Person) RETURN f.name");
        REQUIRE_NOTHROW(GqlTypechecker::typecheck(graph, staged));

        auto aliased = GqlParser::parse("MATCH (p:Person) RETURN p.age AS a NEXT RETURN a");
        REQUIRE_NOTHROW(GqlTypechecker::typecheck(graph, aliased));
    }

    SECTION("an unbound variable after NEXT throws instead of returning nulls") {
        auto query = GqlParser::parse("MATCH (p:Person) RETURN p NEXT RETURN q.name");
        REQUIRE_THROWS_WITH(GqlTypechecker::typecheck(graph, query), Catch::Contains("q"));
    }

    SECTION("a type error inside the first segment is still caught") {
        auto query = GqlParser::parse("MATCH (p:Person) WHERE p.salary > 1 RETURN p NEXT RETURN p.name");
        REQUIRE_THROWS_WITH(GqlTypechecker::typecheck(graph, query),
            Catch::Contains("Property 'salary' does not exist"));
    }

    SECTION("variables not projected before NEXT are out of scope downstream") {
        auto query = GqlParser::parse(
            "MATCH (p:Person)-[:FRIEND_OF]->(r:Person) RETURN p NEXT RETURN r.name");
        REQUIRE_THROWS_WITH(GqlTypechecker::typecheck(graph, query), Catch::Contains("r"));
    }

    SECTION("COUNT { } degree-rewrite virtual property passes typecheck") {
        // The optimizer rewrites the simple-pattern COUNT{} into a synthetic `_deg_p_..._OUT` degree
        // property; the typechecker must accept it (it is populated at execution time, not declared in
        // the schema). Without the fix this throws "Property '_deg_p_FRIEND_OF_OUT' does not exist".
        auto query = GqlParser::parse(
            "MATCH (p:Person) RETURN COUNT { (p)-[:FRIEND_OF]->(f:Person) } AS c");
        GqlOptimizer::optimize(graph, query);
        REQUIRE_NOTHROW(GqlTypechecker::typecheck(graph, query));
    }

    graph.Stop().get();
}
