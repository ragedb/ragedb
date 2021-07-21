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

#ifndef RAGEDB_RELATIONSHIPS_H
#define RAGEDB_RELATIONSHIPS_H

#include <Graph.h>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/json_path.hh>

using namespace seastar;
using namespace httpd;
using namespace ragedb;

class Relationships {

    class GetRelationshipsHandler : public httpd::handler_base {
    public:
        explicit GetRelationshipsHandler(Relationships& relationships) : parent(relationships) {};
    private:
        Relationships& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

    class GetRelationshipsOfTypeHandler : public httpd::handler_base {
    public:
        explicit GetRelationshipsOfTypeHandler(Relationships& relationships) : parent(relationships) {};
    private:
        Relationships& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

    class GetRelationshipHandler : public httpd::handler_base {
    public:
        explicit GetRelationshipHandler(Relationships& relationships) : parent(relationships) {};
    private:
        Relationships& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

    class PostRelationshipHandler : public httpd::handler_base {
    public:
        explicit PostRelationshipHandler(Relationships& relationships) : parent(relationships) {};
    private:
        Relationships& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

    class PostRelationshipByIdHandler : public httpd::handler_base {
    public:
        explicit PostRelationshipByIdHandler(Relationships& relationships) : parent(relationships) {};
    private:
        Relationships& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

    class DeleteRelationshipHandler : public httpd::handler_base {
    public:
        explicit DeleteRelationshipHandler(Relationships& relationships) : parent(relationships) {};
    private:
        Relationships& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

    class GetNodeRelationshipsHandler : public httpd::handler_base {
    public:
        explicit GetNodeRelationshipsHandler(Relationships& relationships) : parent(relationships) {};
    private:
        Relationships& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

    class GetNodeRelationshipsByIdHandler : public httpd::handler_base {
    public:
        explicit GetNodeRelationshipsByIdHandler(Relationships& relationships) : parent(relationships) {};
    private:
        Relationships& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

private:
    Graph& graph;
    GetRelationshipsHandler getRelationshipsHandler;
    GetRelationshipsOfTypeHandler getRelationshipsOfTypeHandler;
    GetRelationshipHandler getRelationshipHandler;
    PostRelationshipHandler postRelationshipHandler;
    PostRelationshipByIdHandler postRelationshipByIdHandler;
    DeleteRelationshipHandler deleteRelationshipHandler;
    GetNodeRelationshipsHandler getNodeRelationshipsHandler;
    GetNodeRelationshipsByIdHandler getNodeRelationshipsByIdHandler;

public:
    explicit Relationships(Graph &_graph) : graph(_graph), getRelationshipsHandler(*this), getRelationshipsOfTypeHandler(*this),
                                           getRelationshipHandler(*this), postRelationshipHandler(*this), postRelationshipByIdHandler(*this),
                                           deleteRelationshipHandler(*this), getNodeRelationshipsHandler(*this), getNodeRelationshipsByIdHandler(*this) {}
    void set_routes(routes& routes);

};


#endif //RAGEDB_RELATIONSHIPS_H
