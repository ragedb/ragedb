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
#include <filesystem>
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

  void Sandbox::buildEnvironment() {
    env = sol::environment(lua, sol::create);
    env["_G"] = env;

    copyAll(env, lua.globals(), ALLOWED_LUA_FUNCTIONS);
    copyTables(env, lua.globals(), lua, ALLOWED_LUA_LIBRARIES);
    copyAll(env, lua.globals(), ALLOWED_CUSTOM_FUNCTIONS);
    copyAll(env, lua.globals(), ALLOWED_GRAPH_OBJECTS);
    copyAll(env, lua.globals(), ALLOWED_GRAPH_FUNCTIONS);

    // Allow users to store and load custom Lua
    env.set_function("loadstring", &Sandbox::loadstring, this);
    env.set_function("loadfile", &Sandbox::loadfile, this);
    env.set_function("dofile", &Sandbox::dofile, this);

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

    lua_rawgeti(lua, LUA_REGISTRYINDEX, env.registry_index());
    lua_rawseti(lua, LUA_REGISTRYINDEX, LUA_RIDX_GLOBALS);
  }

  std::tuple<sol::object, sol::object> Sandbox::loadstring(const std::string &str, const std::string &chunkname) {
    if (!str.empty() && str[0] == LUA_SIGNATURE[0]) {
      return std::make_tuple(sol::nil,
        sol::make_object(lua, "Bytecode prohibited by Lua sandbox"));
    }

    sol::load_result result = lua.load(str, chunkname, sol::load_mode::text);
    if (result.valid()) {
      sol::function func = result;
      env.set_on(func);
      return std::make_tuple(func, sol::nil);
    } else {
      return std::make_tuple(
        sol::nil, sol::make_object(lua, ((sol::error)result).what()));
    }
  }

  std::tuple<sol::object, sol::object> Sandbox::loadfile(const std::string &path) {
    if (!checkPath(path, false)) {
      return std::make_tuple(sol::nil,
        sol::make_object(
          lua, "Path is not allowed by the Lua sandbox"));
    }

    std::ifstream t(path);
    std::string str((std::istreambuf_iterator<char>(t)),
      std::istreambuf_iterator<char>());
    return loadstring(str, "@" + path);
  }

  sol::object Sandbox::dofile(const std::string &path) {
    std::tuple<sol::object, sol::object> ret = loadfile(path);
    if (std::get<0>(ret) == sol::nil) {
      throw sol::error(std::get<1>(ret).as<std::string>());
    }

    sol::unsafe_function func = std::get<0>(ret);
    return func();
  }

  bool Sandbox::checkPath(const std::string &filepath, bool write) {
    // TODO: Split between allowed reads and writes
    if (basePath.empty()) {
      return false;
    }

    auto base = std::filesystem::absolute(basePath).lexically_normal();
    auto path = std::filesystem::absolute(filepath).lexically_normal();

    auto [rootEnd, nothing] =
      std::mismatch(base.begin(), base.end(), path.begin());

    return rootEnd == base.end();
  }

}