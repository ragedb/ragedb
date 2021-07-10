/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

#include "Graphs.h"
#include "handlers/HealthCheck.h"

bool Graphs::GraphAdd(const std::string& name) {
    auto token_search = graphs.find(name);
    if (token_search != graphs.end()) {
        return false;
    }
//    ragedb::Graph graph = ragedb::Graph(name);
//    Routes r = Routes(graph);
//
//
//
//    graphs.insert(std::make_pair(name, &graph));
//
//    std::pair<ragedb::Graph, Routes> x {graph, routes};
//    std::pair<ragedb::Graph, Routes> pair = std::make_pair(graph, routes);
//
//
//    return graphs.insert({name, graph}).second;
    return false;
}

bool Graphs::GraphRemove(const std::string &name) {
    auto token_search = graphs.find(name);
    if (token_search != graphs.end()) {
        graphs.erase(token_search);
        return true;
    }
    return false;
}

bool Graphs::GraphClear(const std::string &name) {
    auto token_search = graphs.find(name);
    if (token_search != graphs.end()) {
        token_search->second.Clear();
        return true;
    }
    return false;
}

bool Graphs::AddToServer(seastar::http_server_control &server) {
    for (auto && [ name, graph ] : graphs) {
        graph.Start().get();
        std::cout << "Started " << graph.GetName() << " graph \n";
        HealthCheck healthCheck = HealthCheck(graph);
        server.set_routes([&healthCheck](seastar::routes& r) { healthCheck.set_routes(r); }).get();
        std::cout << "Route set\n";
    }

    return true;
}
