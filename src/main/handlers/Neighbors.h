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

#ifndef RAGEDB_NEIGHBORS_H
#define RAGEDB_NEIGHBORS_H

#include <Graph.h>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/json_path.hh>

class Neighbors {
    class GetNeighborsHandler : public seastar::httpd::handler_base {
    public:
        explicit GetNeighborsHandler(Neighbors& neighbors) : parent(neighbors) {};
    private:
        Neighbors& parent;
        seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::reply> rep) override;
    };

    class GetNeighborsByIdHandler : public seastar::httpd::handler_base {
    public:
        explicit GetNeighborsByIdHandler(Neighbors& neighbors) : parent(neighbors) {};
    private:
        Neighbors& parent;
        seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::reply> rep) override;
    };

    ragedb::Graph& graph;
    GetNeighborsHandler getNeighborsHandler;
    GetNeighborsByIdHandler getNeighborsByIdHandler;

public:
    explicit Neighbors (ragedb::Graph &_graph) : graph(_graph), getNeighborsHandler(*this), getNeighborsByIdHandler(*this) {}
    void set_routes(seastar::routes& routes);
};


#endif //RAGEDB_NEIGHBORS_H
