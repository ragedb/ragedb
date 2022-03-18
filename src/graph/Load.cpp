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

#include <tuple>
#include "Shard.h"

namespace ragedb {

    std::tuple<sol::object, sol::object> Shard::loadstring(const std::string &str, const sol::optional<std::string> &chunkname) {

      if (!str.empty() && str[0] == LUA_SIGNATURE[0]) {
        return std::make_tuple(sol::nil,
          sol::make_object(lua, "Bytecode prohibited by Lua sandbox"));
      }

      sol::load_result result = lua.load(str, chunkname.value(), sol::load_mode::text);
      if (result.valid()) {
        sol::function func = result;
        env.set_on(func);
        return std::make_tuple(func, sol::nil);
      } else {
        return std::make_tuple(
          sol::nil, sol::make_object(lua, ((sol::error)result).what()));
      }
    }

    std::tuple<sol::object, sol::object> Shard::loadfile(const std::string &path) {
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

    sol::object Shard::dofile(const std::string &path) {
      std::tuple<sol::object, sol::object> ret = loadfile(path);
      if (std::get<0>(ret) == sol::nil) {
        throw sol::error(std::get<1>(ret).as<std::string>());
      }

      sol::unsafe_function func = std::get<0>(ret);
      return func();
    }

    bool Shard::checkPath(const std::string &filepath, bool write) {
      for (auto basePath : write ? WRITE_PATHS : READ_PATHS) {
        auto base = std::filesystem::absolute(basePath).lexically_normal();
        auto path = std::filesystem::absolute(filepath).lexically_normal();
        auto [rootEnd, nothing] = std::mismatch(base.begin(), base.end(), path.begin());
        if (rootEnd == base.end()) {
          return true;
        }
      }

      return false;
    }

}