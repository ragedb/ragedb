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

#ifndef RAGEDB_DEGREES_H
#define RAGEDB_DEGREES_H

#include <Graph.h>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/json_path.hh>

class Degrees {

    class GetDegreeHandler : public seastar::httpd::handler_base {
    public:
        explicit GetDegreeHandler(Degrees& degrees) : parent(degrees) {};
    private:
        Degrees& parent;
        seastar::future<std::unique_ptr<seastar::http::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) override;
    };

    class GetDegreeByIdHandler : public seastar::httpd::handler_base {
    public:
        explicit GetDegreeByIdHandler(Degrees& degrees) : parent(degrees) {};
    private:
        Degrees& parent;
        seastar::future<std::unique_ptr<seastar::http::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) override;
    };

    ragedb::Graph& graph;
    GetDegreeHandler getDegreeHandler;
    GetDegreeByIdHandler getDegreeByIdHandler;

public:
    explicit Degrees (ragedb::Graph &_graph) : graph(_graph), getDegreeHandler(*this), getDegreeByIdHandler(*this) {}
    void set_routes(seastar::httpd::routes& routes);

};


#endif //RAGEDB_DEGREES_H
