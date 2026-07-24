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
 * See the License for the Schindler-like and limitations under the License.
 */

#include <catch2/catch.hpp>
#include <Graph.h>
#include "../../src/gql/GqlParser.h"
#include "../../src/gql/GqlOptimizer.h"
#include "../../src/gql/GqlExecutor.h"

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

TEST_CASE("GQL Execution EXPLAIN and PROFILE Tests", "[gql_explain_profile]") {
    auto graph = Graph("gql_test_explain_profile");
    graph.Start().get();
    graph.Clear();
    GraphStopGuard guard(graph);
    
    // Setup schemas
    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "age", "integer").get();
    graph.shard.local().RelationshipTypeInsertPeered("FRIEND").get();

    // Create test nodes
    uint64_t id1 = graph.shard.local().NodeAddPeered("Person", "alice", "{\"name\": \"Alice\", \"age\": 30}").get();
    uint64_t id2 = graph.shard.local().NodeAddPeered("Person", "bob", "{\"name\": \"Bob\", \"age\": 35}").get();
    graph.shard.local().RelationshipAddPeered("FRIEND", id1, id2, "{}").get();

    SECTION("EXPLAIN query plan") {
        std::string query_str = "EXPLAIN MATCH (p:Person)-[:FRIEND]->(friend) RETURN p.name, friend.name";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        
        REQUIRE(query.explain);
        REQUIRE_FALSE(query.profile);

        std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();

        // Should return explanation JSON
        REQUIRE(results_json.find("ProduceResults") != std::string::npos);
        REQUIRE(results_json.find("Scan / Traverse") != std::string::npos);
        REQUIRE(results_json.find("Operator") != std::string::npos);
        REQUIRE(results_json.find("Details") != std::string::npos);
        REQUIRE(results_json.find("Outputs") != std::string::npos);
        // Explain should not contain actual rows or time_ms fields in rows
        REQUIRE(results_json.find("Actual Rows") == std::string::npos);
    }

    SECTION("EXPLAIN reports the execution strategy (streaming vs materialising)") {
        // A plain projection with no aggregate / ORDER BY+LIMIT / count fast path materialises.
        std::string mat = GqlExecutor::execute(graph, GqlParser::parse(
            "EXPLAIN MATCH (p:Person)-[:FRIEND]->(friend) RETURN p.name, friend.name")).get();
        INFO("materialising: " << mat);
        REQUIRE(mat.find("\"Execution\":") != std::string::npos);
        REQUIRE(mat.find("Materialising") != std::string::npos);

        // A pure count over a single node label is answered from the count index -- a streaming path.
        std::string cnt = GqlExecutor::execute(graph, GqlParser::parse(
            "EXPLAIN MATCH (p:Person) RETURN count(*)")).get();
        INFO("count: " << cnt);
        REQUIRE(cnt.find("Streaming (node-count)") != std::string::npos);
        // The strategy is not the materialising label for this shape.
        REQUIRE(cnt.find("\"Execution\": \"Materialising\"") == std::string::npos);
    }

    SECTION("EXPLAIN reports estimated per-operator row counts") {
        // Two Person nodes, so a bare scan estimates 2.
        std::string scan = GqlExecutor::execute(graph, GqlParser::parse(
            "EXPLAIN MATCH (p:Person) RETURN p.name")).get();
        INFO("scan: " << scan);
        REQUIRE(scan.find("\"Est. Rows\":") != std::string::npos);
        REQUIRE(scan.find("\"Est. Rows\": 2") != std::string::npos);

        // A global aggregate collapses to a single row.
        std::string agg = GqlExecutor::execute(graph, GqlParser::parse(
            "EXPLAIN MATCH (p:Person) RETURN count(*)")).get();
        INFO("agg: " << agg);
        REQUIRE(agg.find("\"Est. Rows\": 2") != std::string::npos);   // the scan under the aggregate
        REQUIRE(agg.find("\"Est. Rows\": 1") != std::string::npos);   // the aggregate itself

        // An equality filter is highly selective: on two nodes the estimate floors to 1.
        std::string byname = GqlExecutor::execute(graph, GqlParser::parse(
            "EXPLAIN MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name")).get();
        INFO("byname: " << byname);
        REQUIRE(byname.find("\"Est. Rows\": 1") != std::string::npos);
    }

    SECTION("PROFILE query plan") {
        std::string query_str = "PROFILE MATCH (p:Person)-[:FRIEND]->(friend) RETURN p.name, friend.name";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        
        REQUIRE(query.profile);
        REQUIRE_FALSE(query.explain);

        std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();

        // Should return profile JSON including execution stats
        REQUIRE(results_json.find("ProduceResults") != std::string::npos);
        REQUIRE(results_json.find("Scan / Traverse") != std::string::npos);
        REQUIRE(results_json.find("Operator") != std::string::npos);
        REQUIRE(results_json.find("Details") != std::string::npos);
        REQUIRE(results_json.find("Outputs") != std::string::npos);
        REQUIRE(results_json.find("Actual Rows") != std::string::npos);
        REQUIRE(results_json.find("Time (ms)") != std::string::npos);
    }

    SECTION("EXPLAIN schema operation query plan") {
        std::string query_str = "EXPLAIN CREATE NODE TYPE Student (name STRING)";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        
        REQUIRE(query.explain);
        
        std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();
        REQUIRE(results_json.find("SchemaOperation") != std::string::npos);
        REQUIRE(results_json.find("Student") != std::string::npos);
    }

    SECTION("EXPLAIN surfaces the constraint that filters a scan, wherever it lives") {
        // An inline property map is bound to the anchor, so it shows on the Scan node; a FILTER stays a
        // residual predicate on a Filter node. Both used to be invisible -- the map rendered as a bare
        // `(p:Person)` and the Filter node said only "WHERE expression" -- so a constrained scan looked
        // identical to a full one. Both now show the actual constraint.
        std::string map_plan = GqlExecutor::execute(graph, GqlParser::parse(
            "EXPLAIN MATCH (p:Person {age: 30}) RETURN p.name")).get();
        INFO("map plan: " << map_plan);
        REQUIRE(map_plan.find("age = 30") != std::string::npos);

        std::string filter_plan = GqlExecutor::execute(graph, GqlParser::parse(
            "EXPLAIN MATCH (p:Person) FILTER p.age > 30 RETURN p.name")).get();
        INFO("filter plan: " << filter_plan);
        REQUIRE(filter_plan.find("p.age > 30") != std::string::npos);
        // No longer the useless generic label.
        REQUIRE(filter_plan.find("WHERE expression") == std::string::npos);

        // An unconstrained scan shows neither, so it is distinguishable from a constrained one.
        std::string scan_plan = GqlExecutor::execute(graph, GqlParser::parse(
            "EXPLAIN MATCH (p:Person) RETURN p.name")).get();
        INFO("scan plan: " << scan_plan);
        REQUIRE(scan_plan.find("age") == std::string::npos);
    }

    guard.stop();
}
