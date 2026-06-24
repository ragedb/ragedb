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

#ifndef RAGEDB_PERFORMANCEHELPER_H
#define RAGEDB_PERFORMANCEHELPER_H

#include <catch2/catch.hpp>
#include <Graph.h>
#include "../../src/gql/GqlExecutor.h"
#include <chrono>
#include <iostream>

namespace ragedb::gql {

struct PerformanceGraphStopGuard {
    Graph& g;
    bool stopped = false;
    explicit PerformanceGraphStopGuard(Graph& graph) : g(graph) {}
    ~PerformanceGraphStopGuard() {
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

inline void run_bench(Graph& graph, const std::string& name, const std::string& query, const std::string& unopt_query, int iterations = 50) {

    // 1. Cold Semantic (Cache Miss)
    auto start_cold = std::chrono::steady_clock::now();
    for (int i = 0; i < iterations; ++i) {
        GqlExecutor::execute(graph, "CALL CLEAR CACHE").get();
        GqlExecutor::execute(graph, query).get();
    }
    auto end_cold = std::chrono::steady_clock::now();
    double time_cold = std::chrono::duration<double, std::milli>(end_cold - start_cold).count() / iterations;

    // 2. Hot Semantic (Cache Hit)
    GqlExecutor::execute(graph, query).get(); // warm up
    auto start_hot = std::chrono::steady_clock::now();
    for (int i = 0; i < iterations; ++i) {
        GqlExecutor::execute(graph, query).get();
    }
    auto end_hot = std::chrono::steady_clock::now();
    double time_hot = std::chrono::duration<double, std::milli>(end_hot - start_hot).count() / iterations;

    // 3. Unoptimized (Semantic OFF)
    auto start_unopt = std::chrono::steady_clock::now();
    for (int i = 0; i < iterations; ++i) {
        GqlExecutor::execute(graph, unopt_query).get();
    }
    auto end_unopt = std::chrono::steady_clock::now();
    double time_unopt = std::chrono::duration<double, std::milli>(end_unopt - start_unopt).count() / iterations;

    std::cout << "| " << name << " | " 
              << time_cold << " ms | " 
              << time_hot << " ms | " 
              << time_unopt << " ms |\n";
}

} // namespace ragedb::gql

#endif // RAGEDB_PERFORMANCEHELPER_H
