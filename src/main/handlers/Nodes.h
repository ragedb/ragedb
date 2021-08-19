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

#ifndef RAGEDB_NODES_H
#define RAGEDB_NODES_H

#include <Graph.h>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/json_path.hh>

using namespace seastar;
using namespace httpd;
using namespace ragedb;

class Nodes {

    class GetNodesHandler : public httpd::handler_base {
    public:
        explicit GetNodesHandler(Nodes& nodes) : parent(nodes) {};
    private:
        Nodes& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

    class GetNodesOfTypeHandler : public httpd::handler_base {
    public:
        explicit GetNodesOfTypeHandler(Nodes& nodes) : parent(nodes) {};
    private:
        Nodes& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

    class GetNodeHandler : public httpd::handler_base {
    public:
        explicit GetNodeHandler(Nodes& nodes) : parent(nodes) {};
    private:
        Nodes& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

    class GetNodeByIdHandler : public httpd::handler_base {
    public:
        explicit GetNodeByIdHandler(Nodes& nodes) : parent(nodes) {};
    private:
        Nodes& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

    class PostNodeHandler : public httpd::handler_base {
    public:
        explicit PostNodeHandler(Nodes& nodes) : parent(nodes) {};
    private:
        Nodes& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

    class DeleteNodeHandler : public httpd::handler_base {
    public:
        explicit DeleteNodeHandler(Nodes& nodes) : parent(nodes) {};
    private:
        Nodes& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

    class DeleteNodeByIdHandler : public httpd::handler_base {
    public:
        explicit DeleteNodeByIdHandler(Nodes& nodes) : parent(nodes) {};
    private:
        Nodes& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

    class FindNodesOfTypeHandler : public httpd::handler_base {
    public:
        explicit FindNodesOfTypeHandler(Nodes& nodes) : parent(nodes) {};
    private:
        Nodes& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };


private:
    Graph& graph;
    GetNodesHandler getNodesHandler;
    GetNodesOfTypeHandler getNodesOfTypeHandler;
    GetNodeHandler getNodeHandler;
    GetNodeByIdHandler getNodeByIdHandler;
    PostNodeHandler postNodeHandler;
    DeleteNodeHandler deleteNodeHandler;
    DeleteNodeByIdHandler deleteNodeByIdHandler;
    FindNodesOfTypeHandler findNodesOfTypeHandler;

public:
    explicit Nodes(Graph &_graph) : graph(_graph), getNodesHandler(*this), getNodesOfTypeHandler(*this),
    getNodeHandler(*this), getNodeByIdHandler(*this), postNodeHandler(*this), deleteNodeHandler(*this),
    deleteNodeByIdHandler(*this), findNodesOfTypeHandler(*this) {}
    void set_routes(routes& routes);
};

#endif //RAGEDB_NODES_H
