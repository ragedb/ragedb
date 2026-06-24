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
#include "../../src/gql/GqlVirtualCatalog.h"
#include "../../src/gql/executor/WccCache.h"

using namespace ragedb;
using namespace ragedb::gql;

struct GraphStopGuard {
    Graph& g;
    bool stopped = false;
    explicit GraphStopGuard(Graph& graph) : g(graph) {}
    ~GraphStopGuard() {
        if (!stopped) {
            try {
                g.Stop().get();
            } catch (...) {}
            stopped = true;
        }
    }
    void stop() {
        if (!stopped) {
            g.Stop().get();
            stopped = true;
        }
    }
};

TEST_CASE("GQL Execution Read Tests", "[gql_executor_read]") {
    auto graph = Graph("gql_test");
    graph.Start().get();
    graph.Clear();
    GraphStopGuard guard(graph);
    
    // Setup schemas
    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "age", "integer").get();

    // Create test nodes
    uint64_t id1 = graph.shard.local().NodeAddPeered("Person", "alice", "{\"name\": \"Alice\", \"age\": 30}").get();
    uint64_t id2 = graph.shard.local().NodeAddPeered("Person", "bob", "{\"name\": \"Bob\", \"age\": 35}").get();

    REQUIRE(id1 > 0);
    REQUIRE(id2 > 0);

    SECTION("Simple match return properties") {
        std::string query_str = "MATCH (p:Person) RETURN p.name, p.age";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        
        std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
        
        REQUIRE(results_json.find("Alice") != std::string::npos);
        REQUIRE(results_json.find("Bob") != std::string::npos);
        REQUIRE(results_json.find("30") != std::string::npos);
        REQUIRE(results_json.find("35") != std::string::npos);
    }

    SECTION("Filtered match return properties") {
        std::string query_str = "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name, p.age";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        
        std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
        
        REQUIRE(results_json.find("Alice") != std::string::npos);
        REQUIRE(results_json.find("Bob") == std::string::npos);
    }

    SECTION("Match with LIMIT pushdown") {
        std::string query_str = "MATCH (p:Person) RETURN p.name LIMIT 1";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        
        std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
        
        bool has_alice = results_json.find("Alice") != std::string::npos;
        bool has_bob = results_json.find("Bob") != std::string::npos;
        REQUIRE((has_alice || has_bob));
        REQUIRE_FALSE((has_alice && has_bob));
    }

    SECTION("Match with range and inequality pushdown") {
        {
            std::string query_str = "MATCH (p:Person) WHERE p.age > 32 RETURN p.name";
            auto query = GqlParser::parse(query_str);
            GqlOptimizer::optimize(query);
            std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
            REQUIRE(results_json.find("Bob") != std::string::npos);
            REQUIRE(results_json.find("Alice") == std::string::npos);
        }
        {
            std::string query_str = "MATCH (p:Person) WHERE p.age < 32 RETURN p.name";
            auto query = GqlParser::parse(query_str);
            GqlOptimizer::optimize(query);
            std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
            REQUIRE(results_json.find("Alice") != std::string::npos);
            REQUIRE(results_json.find("Bob") == std::string::npos);
        }
        {
            std::string query_str = "MATCH (p:Person) WHERE p.age != 30 RETURN p.name";
            auto query = GqlParser::parse(query_str);
            GqlOptimizer::optimize(query);
            std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
            REQUIRE(results_json.find("Bob") != std::string::npos);
            REQUIRE(results_json.find("Alice") == std::string::npos);
        }
    }

    SECTION("Top-K push down query") {
        std::string query_str = "MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age DESC LIMIT 1";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
        std::cout << "DEBUG: results_json: " << results_json << std::endl;
        REQUIRE(results_json.find("Bob") != std::string::npos);
        REQUIRE(results_json.find("35") != std::string::npos);
        REQUIRE(results_json.find("Alice") == std::string::npos);
    }

    SECTION("Match with IS NULL and IS NOT NULL") {
        uint64_t id3 = graph.shard.local().NodeAddPeered("Person", "charlie", "{\"name\": \"Charlie\"}").get();
        REQUIRE(id3 > 0);
        {
            std::string query_str = "MATCH (p:Person) WHERE p.age IS NULL RETURN p.name";
            auto query = GqlParser::parse(query_str);
            GqlOptimizer::optimize(query);
            std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
            REQUIRE(results_json.find("Charlie") != std::string::npos);
            REQUIRE(results_json.find("Alice") == std::string::npos);
        }
        {
            std::string query_str = "MATCH (p:Person) WHERE p.age IS NOT NULL RETURN p.name";
            auto query = GqlParser::parse(query_str);
            GqlOptimizer::optimize(query);
            std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
            REQUIRE(results_json.find("Alice") != std::string::npos);
            REQUIRE(results_json.find("Bob") != std::string::npos);
            REQUIRE(results_json.find("Charlie") == std::string::npos);
        }
    }

    SECTION("Match with string comparison operators") {
        uint64_t id3 = graph.shard.local().NodeAddPeered("Person", "charlie", "{\"name\": \"Charlie\"}").get();
        REQUIRE(id3 > 0);
        {
            std::string query_str = "MATCH (p:Person) WHERE p.name STARTS WITH 'Al' RETURN p.name";
            auto query = GqlParser::parse(query_str);
            GqlOptimizer::optimize(query);
            std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
            REQUIRE(results_json.find("Alice") != std::string::npos);
            REQUIRE(results_json.find("Bob") == std::string::npos);
        }
        {
            std::string query_str = "MATCH (p:Person) WHERE p.name ENDS WITH 'ie' RETURN p.name";
            auto query = GqlParser::parse(query_str);
            GqlOptimizer::optimize(query);
            std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
            REQUIRE(results_json.find("Charlie") != std::string::npos);
            REQUIRE(results_json.find("Alice") == std::string::npos);
        }
        {
            std::string query_str = "MATCH (p:Person) WHERE p.name CONTAINS 'o' RETURN p.name";
            auto query = GqlParser::parse(query_str);
            GqlOptimizer::optimize(query);
            std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
            REQUIRE(results_json.find("Bob") != std::string::npos);
            REQUIRE(results_json.find("Alice") == std::string::npos);
        }
    }

    SECTION("Match with string concatenation operator") {
        std::string query_str = "MATCH (p:Person) WHERE p.name = 'Al' || 'ice' RETURN p.name";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
        REQUIRE(results_json.find("Alice") != std::string::npos);
        REQUIRE(results_json.find("Bob") == std::string::npos);
    }

    SECTION("Match with string concatenation operator - Complex") {
        {
            std::string query_str = "MATCH (p:Person) WHERE p.name = 'A' || 'li' || 'ce' RETURN p.name";
            auto query = GqlParser::parse(query_str);
            GqlOptimizer::optimize(query);
            std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
            REQUIRE(results_json.find("Alice") != std::string::npos);
        }
        {
            std::string query_str = "MATCH (p:Person) WHERE p.name = 'Bob' || 'by' RETURN p.name";
            auto query = GqlParser::parse(query_str);
            GqlOptimizer::optimize(query);
            std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
            REQUIRE(results_json.find("Alice") == std::string::npos);
            REQUIRE(results_json.find("Bob") == std::string::npos);
        }
    }

    SECTION("Match with IS NULL and IS NOT NULL - Edge Cases") {
        {
            std::string query_str = "MATCH (p:Person) WHERE NULL IS NULL RETURN p.name";
            auto query = GqlParser::parse(query_str);
            GqlOptimizer::optimize(query);
            std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
            REQUIRE(results_json.find("Alice") != std::string::npos);
            REQUIRE(results_json.find("Bob") != std::string::npos);
        }
        {
            std::string query_str = "MATCH (p:Person) WHERE 1 IS NOT NULL RETURN p.name";
            auto query = GqlParser::parse(query_str);
            GqlOptimizer::optimize(query);
            std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
            REQUIRE(results_json.find("Alice") != std::string::npos);
            REQUIRE(results_json.find("Bob") != std::string::npos);
        }
        {
            std::string query_str = "MATCH (p:Person) WHERE p.name IS NOT NULL AND p.age IS NULL RETURN p.name";
            uint64_t id3 = graph.shard.local().NodeAddPeered("Person", "charlie", "{\"name\": \"Charlie\"}").get();
            REQUIRE(id3 > 0);
            auto query = GqlParser::parse(query_str);
            GqlOptimizer::optimize(query);
            std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
            REQUIRE(results_json.find("Charlie") != std::string::npos);
            REQUIRE(results_json.find("Alice") == std::string::npos);
        }
    }

    SECTION("Match with IS NULL and IS NOT NULL on Relationships") {
        graph.shard.local().RelationshipTypeInsertPeered("KNOWS").get();
        graph.shard.local().RelationshipPropertyTypeAddPeered("KNOWS", "since", "integer").get();
        
        uint64_t r1 = graph.shard.local().RelationshipAddPeered("KNOWS", id1, id2, "{\"since\": 2020}").get();
        uint64_t r2 = graph.shard.local().RelationshipAddPeered("KNOWS", id2, id1, "{}").get();
        REQUIRE(r1 > 0);
        REQUIRE(r2 > 0);
        
        {
            std::string query_str = "MATCH (a)-[r:KNOWS]->(b) WHERE r.since IS NULL RETURN a.name";
            auto query = GqlParser::parse(query_str);
            GqlOptimizer::optimize(query);
            std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
            REQUIRE(results_json.find("Bob") != std::string::npos);
            REQUIRE(results_json.find("Alice") == std::string::npos);
        }
        {
            std::string query_str = "MATCH (a)-[r:KNOWS]->(b) WHERE r.since IS NOT NULL RETURN a.name";
            auto query = GqlParser::parse(query_str);
            GqlOptimizer::optimize(query);
            std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
            REQUIRE(results_json.find("Alice") != std::string::npos);
            REQUIRE(results_json.find("Bob") == std::string::npos);
        }
    }

    SECTION("Match with string comparison operators - Edge Cases") {
        uint64_t id3 = graph.shard.local().NodeAddPeered("Person", "charlie", "{\"name\": \"Charlie\"}").get();
        REQUIRE(id3 > 0);
        {
            // Case sensitivity check for STARTS WITH
            std::string query_str = "MATCH (p:Person) WHERE p.name STARTS WITH 'al' RETURN p.name";
            auto query = GqlParser::parse(query_str);
            GqlOptimizer::optimize(query);
            std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
            REQUIRE(results_json.find("Alice") == std::string::npos);
        }
        {
            // Empty string check for STARTS WITH (should match all strings)
            std::string query_str = "MATCH (p:Person) WHERE p.name STARTS WITH '' RETURN p.name";
            auto query = GqlParser::parse(query_str);
            GqlOptimizer::optimize(query);
            std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
            REQUIRE(results_json.find("Alice") != std::string::npos);
            REQUIRE(results_json.find("Bob") != std::string::npos);
            REQUIRE(results_json.find("Charlie") != std::string::npos);
        }
        {
            // Case sensitivity check for ENDS WITH
            std::string query_str = "MATCH (p:Person) WHERE p.name ENDS WITH 'IE' RETURN p.name";
            auto query = GqlParser::parse(query_str);
            GqlOptimizer::optimize(query);
            std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
            REQUIRE(results_json.find("Charlie") == std::string::npos);
        }
        {
            std::string query_str = "MATCH (p:Person) WHERE p.name CONTAINS 'O' RETURN p.name";
            auto query = GqlParser::parse(query_str);
            GqlOptimizer::optimize(query);
            std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
            REQUIRE(results_json.find("Bob") == std::string::npos);
        }
    }

    SECTION("Equivalence Relation WCC Caching and Lookup Correctness") {
        graph.shard.local().RelationshipTypeInsertPeered("same_group").get();
        GqlVirtualCatalog::local().clear();
        GqlVirtualCatalog::local().set_relationship_algebraic_properties("same_group", {"reflexive", "symmetric", "transitive"});

        WccCache::local().clear();

        uint64_t id3 = graph.shard.local().NodeAddPeered("Person", "charlie", "{\"name\": \"Charlie\"}").get();
        REQUIRE(id3 > 0);

        uint64_t e1 = graph.shard.local().RelationshipAddPeered("same_group", id1, id2, "{}").get();
        uint64_t e2 = graph.shard.local().RelationshipAddPeered("same_group", id2, id3, "{}").get();
        REQUIRE(e1 > 0);
        REQUIRE(e2 > 0);

        {
            std::string query_str = "MATCH (a:Person)-[:same_group*]->(b:Person) WHERE a.name = 'Alice' RETURN b.name";
            auto query = GqlParser::parse(query_str);
            GqlOptimizer::optimize(query);
            
            REQUIRE(query.matches[0].equivalence_partition_lookup == true);

            std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();

            REQUIRE(results_json.find("Alice") != std::string::npos);
            REQUIRE(results_json.find("Bob") != std::string::npos);
            REQUIRE(results_json.find("Charlie") != std::string::npos);
        }

        REQUIRE(WccCache::local().has("same_group") == true);

        uint64_t id4 = graph.shard.local().NodeAddPeered("Person", "david", "{\"name\": \"David\"}").get();
        REQUIRE(id4 > 0);
        uint64_t e3 = graph.shard.local().RelationshipAddPeered("same_group", id3, id4, "{}").get();
        REQUIRE(e3 > 0);

        REQUIRE(WccCache::local().has("same_group") == false);

        {
            std::string query_str = "MATCH (a:Person)-[:same_group*]->(b:Person) WHERE a.name = 'Alice' RETURN b.name";
            auto query = GqlParser::parse(query_str);
            GqlOptimizer::optimize(query);
            std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();

            REQUIRE(results_json.find("Alice") != std::string::npos);
            REQUIRE(results_json.find("Bob") != std::string::npos);
            REQUIRE(results_json.find("Charlie") != std::string::npos);
            REQUIRE(results_json.find("David") != std::string::npos);
        }
    }

    SECTION("Equivalence Relation Case-Insensitivity Verification") {
        graph.shard.local().RelationshipTypeInsertPeered("sameGroup").get();
        GqlVirtualCatalog::local().clear();
        
        // Register in mixed case, query in another mixed case
        GqlVirtualCatalog::local().set_relationship_algebraic_properties("SameGroup", {"reflexive", "symmetric", "transitive"});

        std::string query_str = "MATCH (a:Person)-[:sameGroup*]->(b:Person) WHERE a.name = 'Alice' RETURN b.name";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        
        // The optimizer should normalize names and successfully trigger partition lookup
        REQUIRE(query.matches[0].equivalence_partition_lookup == true);
    }

    guard.stop();
}
