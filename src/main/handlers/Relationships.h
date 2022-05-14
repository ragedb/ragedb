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

class Relationships {

    class GetRelationshipsHandler : public seastar::httpd::handler_base {
    public:
        explicit GetRelationshipsHandler(Relationships& relationships) : parent(relationships) {};
    private:
        Relationships& parent;
        seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::reply> rep) override;
    };

    class GetRelationshipsOfTypeHandler : public seastar::httpd::handler_base {
    public:
        explicit GetRelationshipsOfTypeHandler(Relationships& relationships) : parent(relationships) {};
    private:
        Relationships& parent;
        seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::reply> rep) override;
    };

    class GetRelationshipHandler : public seastar::httpd::handler_base {
    public:
        explicit GetRelationshipHandler(Relationships& relationships) : parent(relationships) {};
    private:
        Relationships& parent;
        seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::reply> rep) override;
    };

    class PostRelationshipHandler : public seastar::httpd::handler_base {
    public:
        explicit PostRelationshipHandler(Relationships& relationships) : parent(relationships) {};
    private:
        Relationships& parent;
        seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::reply> rep) override;
    };

    class PostRelationshipByIdHandler : public seastar::httpd::handler_base {
    public:
        explicit PostRelationshipByIdHandler(Relationships& relationships) : parent(relationships) {};
    private:
        Relationships& parent;
        seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::reply> rep) override;
    };

    class DeleteRelationshipHandler : public seastar::httpd::handler_base {
    public:
        explicit DeleteRelationshipHandler(Relationships& relationships) : parent(relationships) {};
    private:
        Relationships& parent;
        seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::reply> rep) override;
    };

    class GetNodeRelationshipsHandler : public seastar::httpd::handler_base {
    public:
        explicit GetNodeRelationshipsHandler(Relationships& relationships) : parent(relationships) {};
    private:
        Relationships& parent;
        seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::reply> rep) override;
    };

    class GetNodeRelationshipsByIdHandler : public seastar::httpd::handler_base {
    public:
        explicit GetNodeRelationshipsByIdHandler(Relationships& relationships) : parent(relationships) {};
    private:
        Relationships& parent;
        seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::reply> rep) override;
    };

    class FindRelationshipsOfTypeHandler : public seastar::httpd::handler_base {
    public:
        explicit FindRelationshipsOfTypeHandler(Relationships& relationships) : parent(relationships) {};
    private:
        Relationships& parent;
        seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::reply> rep) override;
    };

    ragedb::Graph& graph;
    GetRelationshipsHandler getRelationshipsHandler;
    GetRelationshipsOfTypeHandler getRelationshipsOfTypeHandler;
    GetRelationshipHandler getRelationshipHandler;
    PostRelationshipHandler postRelationshipHandler;
    PostRelationshipByIdHandler postRelationshipByIdHandler;
    DeleteRelationshipHandler deleteRelationshipHandler;
    GetNodeRelationshipsHandler getNodeRelationshipsHandler;
    GetNodeRelationshipsByIdHandler getNodeRelationshipsByIdHandler;
    FindRelationshipsOfTypeHandler findRelationshipsOfTypeHandler;

public:
    explicit Relationships (ragedb::Graph &_graph) : graph(_graph), getRelationshipsHandler(*this), getRelationshipsOfTypeHandler(*this),
                                           getRelationshipHandler(*this), postRelationshipHandler(*this), postRelationshipByIdHandler(*this),
                                           deleteRelationshipHandler(*this), getNodeRelationshipsHandler(*this), getNodeRelationshipsByIdHandler(*this),
                                           findRelationshipsOfTypeHandler(*this) {}
    void set_routes(seastar::routes& routes);

};


#endif //RAGEDB_RELATIONSHIPS_H
