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

using namespace seastar;
using namespace httpd;
using namespace ragedb;

class Lua {

    class PostLuaHandler : public httpd::handler_base {
    public:
        explicit PostLuaHandler(Lua& lua) : parent(lua) {};

    private:
        Lua& parent;
        future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

    class PostLuaRWHandler : public httpd::handler_base {
    public:
      explicit PostLuaRWHandler(Lua& lua) : parent(lua) {};

    private:
      Lua& parent;
      future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

    class PostLuaROHandler : public httpd::handler_base {
    public:
      explicit PostLuaROHandler(Lua& lua) : parent(lua) {};

    private:
      Lua& parent;
      future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
    };

private:
    Graph& graph;
    PostLuaHandler postLuaHandler;
    PostLuaRWHandler postLuaRWHandler;
    PostLuaROHandler postLuaROHandler;

public:
    explicit Lua(Graph &_graph) : graph(_graph), postLuaHandler(*this), postLuaRWHandler(*this), postLuaROHandler(*this) {}
    void set_routes(routes& routes);
};


#endif //RAGEDB_LUA_H
