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

class Schema {

    class ClearGraphHandler : public seastar::httpd::handler_base {
    public:
        explicit ClearGraphHandler(Schema& schema) : parent(schema) {};
    private:
        Schema& parent;
        seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::reply> rep) override;
    };

    class GetNodeTypesHandler : public seastar::httpd::handler_base {
    public:
        explicit GetNodeTypesHandler(Schema& schema) : parent(schema) {};
    private:
        Schema& parent;
        seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::reply> rep) override;
    };

    class GetRelationshipTypesHandler : public seastar::httpd::handler_base {
    public:
        explicit GetRelationshipTypesHandler(Schema& schema) : parent(schema) {};
    private:
        Schema& parent;
        seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::reply> rep) override;
    };

    class GetNodeTypeHandler : public seastar::httpd::handler_base {
    public:
        explicit GetNodeTypeHandler(Schema& schema) : parent(schema) {};
    private:
        Schema& parent;
        seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::reply> rep) override;
    };

    class PostNodeTypeHandler : public seastar::httpd::handler_base {
    public:
        explicit PostNodeTypeHandler(Schema& schema) : parent(schema) {};
    private:
        Schema& parent;
        seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::reply> rep) override;
    };

    class DeleteNodeTypeHandler : public seastar::httpd::handler_base {
    public:
        explicit DeleteNodeTypeHandler(Schema& schema) : parent(schema) {};
    private:
        Schema& parent;
        seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::reply> rep) override;
    };

    class GetRelationshipTypeHandler : public seastar::httpd::handler_base {
    public:
        explicit GetRelationshipTypeHandler(Schema& schema) : parent(schema) {};
    private:
        Schema& parent;
        seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::reply> rep) override;
    };

    class PostRelationshipTypeHandler : public seastar::httpd::handler_base {
    public:
        explicit PostRelationshipTypeHandler(Schema& schema) : parent(schema) {};
    private:
        Schema& parent;
        seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::reply> rep) override;
    };

    class DeleteRelationshipTypeHandler : public seastar::httpd::handler_base {
    public:
        explicit DeleteRelationshipTypeHandler(Schema& schema) : parent(schema) {};
    private:
        Schema& parent;
        seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::reply> rep) override;
    };

    class GetNodeTypePropertyHandler : public seastar::httpd::handler_base {
    public:
        explicit GetNodeTypePropertyHandler(Schema& schema) : parent(schema) {};
    private:
        Schema& parent;
        seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::reply> rep) override;
    };

    class PostNodeTypePropertyHandler : public seastar::httpd::handler_base {
    public:
        explicit PostNodeTypePropertyHandler(Schema& schema) : parent(schema) {};
    private:
        Schema& parent;
        seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::reply> rep) override;
    };

    class DeleteNodeTypePropertyHandler : public seastar::httpd::handler_base {
    public:
        explicit DeleteNodeTypePropertyHandler(Schema& schema) : parent(schema) {};
    private:
        Schema& parent;
        seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::reply> rep) override;
    };

    class GetRelationshipTypePropertyHandler : public seastar::httpd::handler_base {
    public:
        explicit GetRelationshipTypePropertyHandler(Schema& schema) : parent(schema) {};
    private:
        Schema& parent;
        seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::reply> rep) override;
    };

    class PostRelationshipTypePropertyHandler : public seastar::httpd::handler_base {
    public:
        explicit PostRelationshipTypePropertyHandler(Schema& schema) : parent(schema) {};
    private:
        Schema& parent;
        seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::reply> rep) override;
    };

    class DeleteRelationshipTypePropertyHandler : public seastar::httpd::handler_base {
    public:
        explicit DeleteRelationshipTypePropertyHandler(Schema& schema) : parent(schema) {};
    private:
        Schema& parent;
        seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::reply> rep) override;
    };

    ragedb::Graph& graph;
    ClearGraphHandler clearGraphHandler;
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
    explicit Schema (ragedb::Graph &_graph) : graph(_graph), clearGraphHandler(*this),
                                     getNodeTypesHandler(*this), getRelationshipTypesHandler(*this),
                                     getNodeTypeHandler(*this), postNodeTypeHandler(*this), deleteNodeTypeHandler(*this),
                                     getRelationshipTypeHandler(*this), postRelationshipTypeHandler(*this), deleteRelationshipTypeHandler(*this),
                                     getNodeTypePropertyHandler(*this), postNodeTypePropertyHandler(*this), deleteNodeTypePropertyHandler(*this),
                                     getRelationshipTypePropertyHandler(*this), postRelationshipTypePropertyHandler(*this), deleteRelationshipTypePropertyHandler(*this){}
    void set_routes(seastar::routes& routes);
};


#endif //RAGEDB_SCHEMA_H
