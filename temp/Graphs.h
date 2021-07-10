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

#ifndef RAGEDB_GRAPHS_H
#define RAGEDB_GRAPHS_H


#include <Graph.h>
#include <seastar/http/httpd.hh>
#include <seastar/http/function_handlers.hh>

class Graphs {
private:
    std::unordered_map<std::string, ragedb::Graph> graphs;
    std::unordered_map<std::string, Routes> routes;

public:
    bool GraphAdd(const std::string& name);
    bool GraphRemove(const std::string& name);
    bool GraphClear(const std::string& name);
    bool AddToServer(seastar::http_server_control& server);

};


#endif //RAGEDB_GRAPHS_H
