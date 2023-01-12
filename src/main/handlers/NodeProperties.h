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

#ifndef RAGEDB_NODEPROPERTIES_H
#define RAGEDB_NODEPROPERTIES_H

#include <Graph.h>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/json_path.hh>

class NodeProperties {
    class GetNodePropertyHandler : public seastar::httpd::handler_base {
    public:
        explicit GetNodePropertyHandler(NodeProperties& nodes) : parent(nodes) {};
    private:
        NodeProperties& parent;
        seastar::future<std::unique_ptr<seastar::http::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) override;
    };

    class GetNodePropertyByIdHandler : public seastar::httpd::handler_base {
    public:
        explicit GetNodePropertyByIdHandler(NodeProperties& nodes) : parent(nodes) {};
    private:
        NodeProperties& parent;
        seastar::future<std::unique_ptr<seastar::http::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) override;
    };

    class PutNodePropertyHandler : public seastar::httpd::handler_base {
    public:
        explicit PutNodePropertyHandler(NodeProperties& nodes) : parent(nodes) {};
    private:
        NodeProperties& parent;
        seastar::future<std::unique_ptr<seastar::http::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) override;
    };

    class PutNodePropertyByIdHandler : public seastar::httpd::handler_base {
    public:
        explicit PutNodePropertyByIdHandler(NodeProperties& nodes) : parent(nodes) {};
    private:
        NodeProperties& parent;
        seastar::future<std::unique_ptr<seastar::http::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) override;
    };

    class DeleteNodePropertyHandler : public seastar::httpd::handler_base {
    public:
        explicit DeleteNodePropertyHandler(NodeProperties& nodes) : parent(nodes) {};
    private:
        NodeProperties& parent;
        seastar::future<std::unique_ptr<seastar::http::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) override;
    };

    class DeleteNodePropertyByIdHandler : public seastar::httpd::handler_base {
    public:
        explicit DeleteNodePropertyByIdHandler(NodeProperties& nodes) : parent(nodes) {};
    private:
        NodeProperties& parent;
        seastar::future<std::unique_ptr<seastar::http::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) override;
    };

    class GetNodePropertiesHandler : public seastar::httpd::handler_base {
    public:
        explicit GetNodePropertiesHandler(NodeProperties& nodes) : parent(nodes) {};
    private:
        NodeProperties& parent;
        seastar::future<std::unique_ptr<seastar::http::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) override;
    };

    class GetNodePropertiesByIdHandler : public seastar::httpd::handler_base {
    public:
        explicit GetNodePropertiesByIdHandler(NodeProperties& nodes) : parent(nodes) {};
    private:
        NodeProperties& parent;
        seastar::future<std::unique_ptr<seastar::http::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) override;
    };

    class PostNodePropertiesHandler : public seastar::httpd::handler_base {
    public:
        explicit PostNodePropertiesHandler(NodeProperties& nodes) : parent(nodes) {};
    private:
        NodeProperties& parent;
        seastar::future<std::unique_ptr<seastar::http::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) override;
    };

    class PostNodePropertiesByIdHandler : public seastar::httpd::handler_base {
    public:
        explicit PostNodePropertiesByIdHandler(NodeProperties& nodes) : parent(nodes) {};
    private:
        NodeProperties& parent;
        seastar::future<std::unique_ptr<seastar::http::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) override;
    };

    class PutNodePropertiesHandler : public seastar::httpd::handler_base {
    public:
        explicit PutNodePropertiesHandler(NodeProperties& nodes) : parent(nodes) {};
    private:
        NodeProperties& parent;
        seastar::future<std::unique_ptr<seastar::http::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) override;
    };

    class PutNodePropertiesByIdHandler : public seastar::httpd::handler_base {
    public:
        explicit PutNodePropertiesByIdHandler(NodeProperties& nodes) : parent(nodes) {};
    private:
        NodeProperties& parent;
        seastar::future<std::unique_ptr<seastar::http::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) override;
    };

    class DeleteNodePropertiesHandler : public seastar::httpd::handler_base {
    public:
        explicit DeleteNodePropertiesHandler(NodeProperties& nodes) : parent(nodes) {};
    private:
        NodeProperties& parent;
        seastar::future<std::unique_ptr<seastar::http::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) override;
    };

    class DeleteNodePropertiesByIdHandler : public seastar::httpd::handler_base {
    public:
        explicit DeleteNodePropertiesByIdHandler(NodeProperties& nodes) : parent(nodes) {};
    private:
        NodeProperties& parent;
        seastar::future<std::unique_ptr<seastar::http::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) override;
    };

    ragedb::Graph& graph;
    GetNodePropertyHandler getNodePropertyHandler;
    GetNodePropertyByIdHandler getNodePropertyByIdHandler;
    PutNodePropertyHandler putNodePropertyHandler;
    PutNodePropertyByIdHandler putNodePropertyByIdHandler;
    DeleteNodePropertyHandler deleteNodePropertyHandler;
    DeleteNodePropertyByIdHandler deleteNodePropertyByIdHandler;

    GetNodePropertiesHandler getNodePropertiesHandler;
    GetNodePropertiesByIdHandler getNodePropertiesByIdHandler;
    PostNodePropertiesHandler postNodePropertiesHandler;
    PutNodePropertiesHandler putNodePropertiesHandler;
    PostNodePropertiesByIdHandler postNodePropertiesByIdHandler;
    PutNodePropertiesByIdHandler putNodePropertiesByIdHandler;
    DeleteNodePropertiesHandler deleteNodePropertiesHandler;
    DeleteNodePropertiesByIdHandler deleteNodePropertiesByIdHandler;

public:
    explicit NodeProperties (ragedb::Graph &_graph) : graph(_graph), getNodePropertyHandler(*this), getNodePropertyByIdHandler(*this),
                                            putNodePropertyHandler(*this), putNodePropertyByIdHandler(*this),
                                            deleteNodePropertyHandler(*this), deleteNodePropertyByIdHandler(*this),
                                            getNodePropertiesHandler(*this), getNodePropertiesByIdHandler(*this),
                                            postNodePropertiesHandler(*this), putNodePropertiesHandler(*this),
                                            postNodePropertiesByIdHandler(*this), putNodePropertiesByIdHandler(*this),
                                            deleteNodePropertiesHandler(*this), deleteNodePropertiesByIdHandler(*this) {}
    void set_routes(seastar::routes& routes);
};


#endif //RAGEDB_NODEPROPERTIES_H
