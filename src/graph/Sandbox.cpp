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

#include <fstream>
#include "Sandbox.h"

namespace ragedb {

  void copyAll(sol::environment &env, const sol::global_table &globals, const std::vector<std::string> &names) {
    for (const auto &name : names) {
      env[name] = globals[name];
    }
  }

  sol::table deepCopy(sol::state &lua, const sol::table &table) {
    sol::table table2(lua, sol::create);
    for (auto pair : table) {
      table2[pair.first] = pair.second;
    }
    return table2;
  }

  void copyTables(sol::environment &env, const sol::global_table &globals, sol::state &lua, const std::vector<std::string> &names) {
    for (const auto &name : names) {
      env[name] = deepCopy(lua, globals[name]);
    }
  }

  void Sandbox::buildEnvironment(Permission permission) {
    env = sol::environment(lua, sol::create);
    env["_G"] = env;

    copyAll(env, lua.globals(), ALLOWED_LUA_FUNCTIONS);
    copyTables(env, lua.globals(), lua, ALLOWED_LUA_LIBRARIES);
    copyAll(env, lua.globals(), ALLOWED_CUSTOM_FUNCTIONS);
    copyAll(env, lua.globals(), ALLOWED_GRAPH_OBJECTS);

    switch (permission) {
      case Permission::ADMIN: {
        copyAll(env, lua.globals(), ALLOWED_GRAPH_ADMIN_FUNCTIONS);
        [[fallthrough]];
      }
      case Permission::WRITE: {
        copyAll(env, lua.globals(), ALLOWED_GRAPH_WRITE_FUNCTIONS);
        [[fallthrough]];
      }
      default: {
        copyAll(env, lua.globals(), ALLOWED_GRAPH_READ_FUNCTIONS);
      }
    }

    copyAll(env, lua.globals(), RESTRICTED_LUA_FUNCTIONS);

    // Individual Functions from Libraries
    sol::table os(lua, sol::create);
    os["clock"] = lua["os"]["clock"];
    os["date"] = lua["os"]["date"];
    os["difftime"] = lua["os"]["difftime"];
    os["time"] = lua["os"]["time"];
    env["os"] = os;

    sol::table io(lua, sol::create);
    io["read"] = lua["io"]["read"];
    io["type"] = lua["io"]["type"];
    env["io"] = io;

#if LUA_VERSION_NUM >= 502
    // Get environment registry index
    lua_rawgeti(lua, LUA_REGISTRYINDEX, env.registry_index());

    // Set the global environment
    lua_rawseti(lua, LUA_REGISTRYINDEX, LUA_RIDX_GLOBALS);
#else
    // Get main thread
    int is_main = lua_pushthread(lua);
    assert(is_main);
    int thread = lua_gettop(lua);

    // Get environment registry index
    lua_rawgeti(lua, LUA_REGISTRYINDEX, env.registry_index());

    // Set the global environment
    if (!lua_setfenv(lua, thread)) {
      // "Security: Unable to set environment of the main Lua thread!");
      throw std::exception();
    };
    lua_pop(lua, 1); // Pop thread
#endif
  }

}