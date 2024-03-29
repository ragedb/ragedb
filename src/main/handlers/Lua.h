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

#ifndef RAGEDB_LUA_H
#define RAGEDB_LUA_H

#include <Graph.h>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/json_path.hh>

class Lua {

    class PostLuaHandler : public seastar::httpd::handler_base {
    public:
        explicit PostLuaHandler(Lua& lua) : parent(lua) {};

    private:
        Lua& parent;
        seastar::future<std::unique_ptr<seastar::http::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) override;
    };

    class PostLuaRWHandler : public seastar::httpd::handler_base {
    public:
      explicit PostLuaRWHandler(Lua& lua) : parent(lua) {};

    private:
      Lua& parent;
      seastar::future<std::unique_ptr<seastar::http::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) override;
    };

    class PostLuaROHandler : public seastar::httpd::handler_base {
    public:
      explicit PostLuaROHandler(Lua& lua) : parent(lua) {};

    private:
      Lua& parent;
      seastar::future<std::unique_ptr<seastar::http::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) override;
    };

    ragedb::Graph& graph;
    PostLuaHandler postLuaHandler;
    PostLuaRWHandler postLuaRWHandler;
    PostLuaROHandler postLuaROHandler;

public:
    explicit Lua (ragedb::Graph &_graph) : graph(_graph), postLuaHandler(*this), postLuaRWHandler(*this), postLuaROHandler(*this) {}
    void set_routes(seastar::httpd::routes& routes);
};


#endif //RAGEDB_LUA_H
