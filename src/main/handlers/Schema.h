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

#ifndef RAGEDB_SCHEMA_H
#define RAGEDB_SCHEMA_H

#include <Graph.h>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/json_path.hh>

using namespace seastar;
using namespace httpd;
using namespace ragedb;

class Schema {

    class GetNodeTypesHandler : public httpd::handler_base {
    public:
        explicit GetNodeTypesHandler(Schema& schema) : parent(schema) {};
    private:
        Schema& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

    class GetRelationshipTypesHandler : public httpd::handler_base {
    public:
        explicit GetRelationshipTypesHandler(Schema& schema) : parent(schema) {};
    private:
        Schema& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

    class GetNodeTypeHandler : public httpd::handler_base {
    public:
        explicit GetNodeTypeHandler(Schema& schema) : parent(schema) {};
    private:
        Schema& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

    class PostNodeTypeHandler : public httpd::handler_base {
    public:
        explicit PostNodeTypeHandler(Schema& schema) : parent(schema) {};
    private:
        Schema& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

    class DeleteNodeTypeHandler : public httpd::handler_base {
    public:
        explicit DeleteNodeTypeHandler(Schema& schema) : parent(schema) {};
    private:
        Schema& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

    class GetRelationshipTypeHandler : public httpd::handler_base {
    public:
        explicit GetRelationshipTypeHandler(Schema& schema) : parent(schema) {};
    private:
        Schema& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

    class PostRelationshipTypeHandler : public httpd::handler_base {
    public:
        explicit PostRelationshipTypeHandler(Schema& schema) : parent(schema) {};
    private:
        Schema& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

    class DeleteRelationshipTypeHandler : public httpd::handler_base {
    public:
        explicit DeleteRelationshipTypeHandler(Schema& schema) : parent(schema) {};
    private:
        Schema& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

    class GetNodeTypePropertyHandler : public httpd::handler_base {
    public:
        explicit GetNodeTypePropertyHandler(Schema& schema) : parent(schema) {};
    private:
        Schema& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

    class PostNodeTypePropertyHandler : public httpd::handler_base {
    public:
        explicit PostNodeTypePropertyHandler(Schema& schema) : parent(schema) {};
    private:
        Schema& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

    class DeleteNodeTypePropertyHandler : public httpd::handler_base {
    public:
        explicit DeleteNodeTypePropertyHandler(Schema& schema) : parent(schema) {};
    private:
        Schema& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

    class GetRelationshipTypePropertyHandler : public httpd::handler_base {
    public:
        explicit GetRelationshipTypePropertyHandler(Schema& schema) : parent(schema) {};
    private:
        Schema& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

    class PostRelationshipTypePropertyHandler : public httpd::handler_base {
    public:
        explicit PostRelationshipTypePropertyHandler(Schema& schema) : parent(schema) {};
    private:
        Schema& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

    class DeleteRelationshipTypePropertyHandler : public httpd::handler_base {
    public:
        explicit DeleteRelationshipTypePropertyHandler(Schema& schema) : parent(schema) {};
    private:
        Schema& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

private:
    Graph& graph;
    GetNodeTypesHandler getNodeTypesHandler;
    GetRelationshipTypesHandler getRelationshipTypesHandler;
    GetNodeTypeHandler getNodeTypeHandler;
    PostNodeTypeHandler postNodeTypeHandler;
    DeleteNodeTypeHandler deleteNodeTypeHandler;
    GetRelationshipTypeHandler getRelationshipTypeHandler;
    PostRelationshipTypeHandler postRelationshipTypeHandler;
    DeleteRelationshipTypeHandler deleteRelationshipTypeHandler;
    GetNodeTypePropertyHandler getNodeTypePropertyHandler;
    PostNodeTypePropertyHandler postNodeTypePropertyHandler;
    DeleteNodeTypePropertyHandler deleteNodeTypePropertyHandler;
    GetRelationshipTypePropertyHandler getRelationshipTypePropertyHandler;
    PostRelationshipTypePropertyHandler postRelationshipTypePropertyHandler;
    DeleteRelationshipTypePropertyHandler deleteRelationshipTypePropertyHandler;
public:
    explicit Schema(Graph &_graph) : graph(_graph), getNodeTypesHandler(*this), getRelationshipTypesHandler(*this),
                                     getNodeTypeHandler(*this), postNodeTypeHandler(*this), deleteNodeTypeHandler(*this),
                                     getRelationshipTypeHandler(*this), postRelationshipTypeHandler(*this), deleteRelationshipTypeHandler(*this),
                                     getNodeTypePropertyHandler(*this), postNodeTypePropertyHandler(*this), deleteNodeTypePropertyHandler(*this),
                                     getRelationshipTypePropertyHandler(*this), postRelationshipTypePropertyHandler(*this), deleteRelationshipTypePropertyHandler(*this){}
    void set_routes(routes& routes);
};


#endif //RAGEDB_SCHEMA_H
