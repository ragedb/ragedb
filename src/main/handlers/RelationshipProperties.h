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

#ifndef RAGEDB_RELATIONSHIPPROPERTIES_H
#define RAGEDB_RELATIONSHIPPROPERTIES_H

#include <Graph.h>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/json_path.hh>

using namespace seastar;
using namespace httpd;
using namespace ragedb;

class RelationshipProperties {
    class GetRelationshipPropertyByIdHandler : public httpd::handler_base {
    public:
        explicit GetRelationshipPropertyByIdHandler(RelationshipProperties& Relationships) : parent(Relationships) {};
    private:
        RelationshipProperties& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

    class PutRelationshipPropertyByIdHandler : public httpd::handler_base {
    public:
        explicit PutRelationshipPropertyByIdHandler(RelationshipProperties& Relationships) : parent(Relationships) {};
    private:
        RelationshipProperties& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

    class DeleteRelationshipPropertyByIdHandler : public httpd::handler_base {
    public:
        explicit DeleteRelationshipPropertyByIdHandler(RelationshipProperties& Relationships) : parent(Relationships) {};
    private:
        RelationshipProperties& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

    class GetRelationshipPropertiesByIdHandler : public httpd::handler_base {
    public:
        explicit GetRelationshipPropertiesByIdHandler(RelationshipProperties& Relationships) : parent(Relationships) {};
    private:
        RelationshipProperties& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

    class PostRelationshipPropertiesByIdHandler : public httpd::handler_base {
    public:
        explicit PostRelationshipPropertiesByIdHandler(RelationshipProperties& Relationships) : parent(Relationships) {};
    private:
        RelationshipProperties& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

    class PutRelationshipPropertiesByIdHandler : public httpd::handler_base {
    public:
        explicit PutRelationshipPropertiesByIdHandler(RelationshipProperties& Relationships) : parent(Relationships) {};
    private:
        RelationshipProperties& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

    class DeleteRelationshipPropertiesByIdHandler : public httpd::handler_base {
    public:
        explicit DeleteRelationshipPropertiesByIdHandler(RelationshipProperties& Relationships) : parent(Relationships) {};
    private:
        RelationshipProperties& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

private:
    Graph& graph;
    GetRelationshipPropertyByIdHandler getRelationshipPropertyByIdHandler;
    PutRelationshipPropertyByIdHandler putRelationshipPropertyByIdHandler;
    DeleteRelationshipPropertyByIdHandler deleteRelationshipPropertyByIdHandler;

    GetRelationshipPropertiesByIdHandler getRelationshipPropertiesByIdHandler;
    PostRelationshipPropertiesByIdHandler postRelationshipPropertiesByIdHandler;
    PutRelationshipPropertiesByIdHandler putRelationshipPropertiesByIdHandler;

    DeleteRelationshipPropertiesByIdHandler deleteRelationshipPropertiesByIdHandler;

public:
    explicit RelationshipProperties(Graph &_graph) : graph(_graph), getRelationshipPropertyByIdHandler(*this),
                                                    putRelationshipPropertyByIdHandler(*this),deleteRelationshipPropertyByIdHandler(*this),
                                                    getRelationshipPropertiesByIdHandler(*this), postRelationshipPropertiesByIdHandler(*this),
                                                    putRelationshipPropertiesByIdHandler(*this), deleteRelationshipPropertiesByIdHandler(*this) {}
    void set_routes(routes& routes);
};


#endif //RAGEDB_RELATIONSHIPPROPERTIES_H
