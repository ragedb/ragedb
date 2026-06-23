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
#include <simdjson.h>
#include "../../src/graph/Join.h"

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

TEST_CASE("GQL Honeycomb WCOJ Execution Tests", "[gql_honeycomb]") {
    auto graph = Graph("gql_test_honeycomb");
    graph.Start().get();
    graph.Clear();
    GraphStopGuard guard(graph);
    
    // Setup schema
    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().RelationshipTypeInsertPeered("FRIEND").get();

    // Create 1,000 Person nodes
    std::vector<uint64_t> person_ids;
    person_ids.reserve(1000);
    for (int i = 0; i < 1000; ++i) {
        std::string name = "Person_" + std::to_string(i);
        std::string props = "{\"name\": \"" + name + "\"}";
        uint64_t id = graph.shard.local().NodeAddPeered("Person", name, props).get();
        person_ids.push_back(id);
    }

    // Connect them to form a cyclic structure.
    // Each node i connects to (i+j)%1000 for j = 1..2
    // Total relationships = 2,000
    for (int i = 0; i < 1000; ++i) {
        for (int j = 1; j <= 2; ++j) {
            int next_idx = (i + j) % 1000;
            graph.shard.local().RelationshipAddPeered("FRIEND", person_ids[i], person_ids[next_idx], "{}").get();
        }
    }

    // Explicitly add back-edges to close exactly 333 triangles: i+2 -> i for i=0,3,6...
    for (int i = 0; i < 997; i += 3) {
        graph.shard.local().RelationshipAddPeered("FRIEND", person_ids[i+2], person_ids[i], "{}").get();
    }

    SECTION("Honeycomb WCOJ execution is triggered and correctly resolves cycles") {
        GqlExecutor::force_enable_honeycomb = true;
        // 1. Profile Query
        std::string query_str = "PROFILE MATCH (a:Person)-[e1:FRIEND]->(b:Person)-[e2:FRIEND]->(c:Person)-[e3:FRIEND]->(a) RETURN a.name, b.name, c.name";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        
        REQUIRE(query.profile);

        std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();

        std::cout << "RESULTS JSON:\n" << results_json << "\n" << std::endl;

        // Verify that the HoneycombJoin operator was chosen by the planner
        REQUIRE(results_json.find("HoneycombJoin") != std::string::npos);

        // Parse the results count using simdjson
        simdjson::dom::parser parser;
        simdjson::dom::element root = parser.parse(results_json);
        
        REQUIRE(root.type() == simdjson::dom::element_type::ARRAY);
        bool found_hc = false;
        for (auto op : root.get_array()) {
            if (op["Operator"].get_string().value() == "HoneycombJoin") {
                REQUIRE(op["Actual Rows"].get_int64().value() == 999);
                found_hc = true;
            }
        }
        REQUIRE(found_hc);

        // 2. Normal Query (without PROFILE)
        std::string query_normal = "MATCH (a:Person)-[e1:FRIEND]->(b:Person)-[e2:FRIEND]->(c:Person)-[e3:FRIEND]->(a) RETURN a.name, b.name, c.name";
        auto q_norm = GqlParser::parse(query_normal);
        GqlOptimizer::optimize(q_norm);

        REQUIRE_FALSE(q_norm.profile);
        REQUIRE_FALSE(q_norm.explain);

        std::string results_normal = GqlExecutor::execute(graph, std::move(q_norm)).get();
        simdjson::dom::element root_norm = parser.parse(results_normal);
        REQUIRE(root_norm.type() == simdjson::dom::element_type::ARRAY);
        REQUIRE(root_norm.get_array().size() == 999);
        
        GqlExecutor::force_enable_honeycomb = false; // Reset toggle
    }

    SECTION("LFTJ WCOJ execution is automatically triggered for smaller queries") {
        // No force flags set. Since total_rels = 2,333 <= 10,000, it should choose LftjJoin
        std::string query_str = "PROFILE MATCH (a:Person)-[e1:FRIEND]->(b:Person)-[e2:FRIEND]->(c:Person)-[e3:FRIEND]->(a) RETURN a.name, b.name, c.name";
        auto query = GqlParser::parse(query_str);
        GqlOptimizer::optimize(query);
        
        REQUIRE(query.profile);

        std::string results_json = GqlExecutor::execute(graph, std::move(query)).get();

        // Verify that the LftjJoin operator was chosen by the planner
        REQUIRE(results_json.find("LftjJoin") != std::string::npos);

        // Parse the results count using simdjson
        simdjson::dom::parser parser;
        simdjson::dom::element root = parser.parse(results_json);
        
        REQUIRE(root.type() == simdjson::dom::element_type::ARRAY);
        bool found_lftj = false;
        for (auto op : root.get_array()) {
            if (op["Operator"].get_string().value() == "LftjJoin") {
                REQUIRE(op["Actual Rows"].get_int64().value() == 999);
                found_lftj = true;
            }
        }
        REQUIRE(found_lftj);
    }
    
    SECTION("Honeycomb WCOJ Performance Benchmarks") {
        // Build adjacency lists for direct LFTJ and standard loop benchmarking
        uint64_t count = graph.shard.local().AllRelationshipsCountPeered("FRIEND").get();
        auto rels = graph.shard.local().AllRelationshipsPeered("FRIEND", 0, count).get();

        std::unordered_map<uint64_t, int> node_id_to_idx;
        for (int i = 0; i < 1000; ++i) {
            node_id_to_idx[person_ids[i]] = i;
        }

        std::vector<std::vector<uint64_t>> adj_out(1000);
        std::vector<std::vector<uint64_t>> adj_in(1000);
        for (const auto& r : rels) {
            int src_idx = node_id_to_idx[r.getStartingNodeId()];
            int dst_idx = node_id_to_idx[r.getEndingNodeId()];
            adj_out[src_idx].push_back(dst_idx);
            adj_in[dst_idx].push_back(src_idx);
        }

        for (int i = 0; i < 1000; ++i) {
            std::sort(adj_out[i].begin(), adj_out[i].end());
            std::sort(adj_in[i].begin(), adj_in[i].end());
        }

        // 1. Honeycomb WCOJ
        GqlExecutor::execute(graph, "CALL CLEAR CACHE").get();
        GqlExecutor::force_disable_honeycomb = false;
        GqlExecutor::force_enable_honeycomb = true;
        auto start_hc = std::chrono::steady_clock::now();
        std::string results_hc = GqlExecutor::execute(graph, "MATCH (a:Person)-[e1:FRIEND]->(b:Person)-[e2:FRIEND]->(c:Person)-[e3:FRIEND]->(a) RETURN a.name, b.name, c.name").get();
        auto end_hc = std::chrono::steady_clock::now();
        double time_hc = std::chrono::duration<double, std::milli>(end_hc - start_hc).count();
        GqlExecutor::force_enable_honeycomb = false; // Reset toggle

        simdjson::dom::parser parser;
        simdjson::dom::element root_hc = parser.parse(results_hc);
        size_t hc_rows = root_hc.get_array().size();

        // 2. Standard Non-WCOJ (Honeycomb and LFTJ disabled)
        GqlExecutor::execute(graph, "CALL CLEAR CACHE").get();
        GqlExecutor::force_disable_honeycomb = true;
        GqlExecutor::force_disable_lftj = true;
        auto start_std = std::chrono::steady_clock::now();
        std::string results_std = GqlExecutor::execute(graph, "MATCH (a:Person)-[e1:FRIEND]->(b:Person)-[e2:FRIEND]->(c:Person)-[e3:FRIEND]->(a) RETURN a.name, b.name, c.name").get();
        auto end_std = std::chrono::steady_clock::now();
        double time_std = std::chrono::duration<double, std::milli>(end_std - start_std).count();
        GqlExecutor::force_disable_honeycomb = false; // Reset toggle
        GqlExecutor::force_disable_lftj = false; // Reset toggle

        simdjson::dom::element root_std = parser.parse(results_std);
        size_t std_rows = root_std.get_array().size();

        // 3. LftjExecutor WCOJ (Reactor-Local)
        GqlExecutor::execute(graph, "CALL CLEAR CACHE").get();
        GqlExecutor::force_enable_lftj = true;
        auto start_lftj_exec = std::chrono::steady_clock::now();
        std::string results_lftj_exec = GqlExecutor::execute(graph, "MATCH (a:Person)-[e1:FRIEND]->(b:Person)-[e2:FRIEND]->(c:Person)-[e3:FRIEND]->(a) RETURN a.name, b.name, c.name").get();
        auto end_lftj_exec = std::chrono::steady_clock::now();
        double time_lftj_exec = std::chrono::duration<double, std::milli>(end_lftj_exec - start_lftj_exec).count();
        GqlExecutor::force_enable_lftj = false; // Reset toggle

        simdjson::dom::element root_lftj_exec = parser.parse(results_lftj_exec);
        size_t lftj_exec_rows = root_lftj_exec.get_array().size();

        // 4. Direct WCOJ Leapfrog Triejoin (using Join.h)
        auto start_lftj = std::chrono::steady_clock::now();
        uint64_t lftj_rows = 0;
        for (int a = 0; a < 1000; ++a) {
            for (uint64_t b : adj_out[a]) {
                std::vector<std::vector<uint64_t>> id_lists = { adj_out[b], adj_in[a] };
                std::vector<uint64_t> intersected = leapfrogJoin(id_lists);
                lftj_rows += intersected.size();
            }
        }
        auto end_lftj = std::chrono::steady_clock::now();
        double time_lftj = std::chrono::duration<double, std::milli>(end_lftj - start_lftj).count();

        std::cout << "\n=========================================\n";
        std::cout << "     WCOJ & JOIN PERFORMANCE BENCHMARKS  \n";
        std::cout << "=========================================\n";
        std::cout << "Honeycomb WCOJ (Parallel):   " << time_hc << " ms (rows: " << hc_rows << ")\n";
        std::cout << "LftjExecutor (Reactor-Local):" << time_lftj_exec << " ms (rows: " << lftj_exec_rows << ")\n";
        std::cout << "Standard Non-WCOJ:           " << time_std << " ms (rows: " << std_rows << ")\n";
        std::cout << "LFTJ direct WCOJ:            " << time_lftj << " ms (rows: " << lftj_rows << ")\n";
        std::cout << "=========================================\n\n";

        REQUIRE(hc_rows == 999);
        REQUIRE(std_rows == 999);
        REQUIRE(lftj_exec_rows == 999);
        REQUIRE(lftj_rows == 999);
    }

    guard.stop();
}
