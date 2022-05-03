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

#include "../Shard.h"

namespace ragedb {

  property_type_t Shard::SolObjectToProperty(const sol::object& o) {
    if (o == sol::lua_nil) return std::monostate();
    if (o.is<bool>()) return o.as<bool>();
    if (o.is<int>()) return o.as<int64_t>();
    if (o.is<double>()) return o.as<double>();
    if (o.is<std::string>()) return o.as<std::string>();

    if (o.is<char>()) return std::string(1, o.as<char>());
    if (o.is<unsigned char>()) return std::string(1, o.as<unsigned char >());
    if (o.is<short>()) return o.as<int64_t>();
    if (o.is<unsigned short>()) return o.as<int64_t>();
    if (o.is<unsigned int>()) return o.as<int64_t>();
    if (o.is<long>()) return o.as<int64_t>();
    if (o.is<unsigned long>()) return o.as<int64_t>();
    if (o.is<long long>()) return o.as<int64_t>();
    if (o.is<unsigned long long>()) return o.as<int64_t>();
    if (o.is<float>()) return o.as<double>();

    // TODO: Any missing? What about arrays? Can't I just use this for everything?
    if (o.is<sol::table>()) {
      return o.as<property_type_t>();
    }

    return std::monostate();
  }

  sol::object Shard::PropertyToSolObject(const property_type_t value) {

    switch (value.index()) {
      case 0:
        return sol::lua_nil;
      case 1:
        return sol::make_object(lua.lua_state(), get<bool>(value));
      case 2:
        return sol::make_object(lua.lua_state(), get<int64_t>(value));
      case 3:
        return sol::make_object(lua.lua_state(), get<double>(value));
      case 4:
        return sol::make_object(lua.lua_state(), get<std::string>(value));
      case 5:
        return sol::make_object(lua.lua_state(), sol::as_table(get<std::vector<bool>>(value)));
      case 6:
        return sol::make_object(lua.lua_state(), sol::as_table(get<std::vector<int64_t>>(value)));
      case 7:
        return sol::make_object(lua.lua_state(), sol::as_table(get<std::vector<double>>(value)));
      case 8:
        return sol::make_object(lua.lua_state(), sol::as_table(get<std::vector<std::string>>(value)));
      }
    return sol::lua_nil;
  }

  sol::table Shard::PropertiesToSolObject(const std::map<std::string, property_type_t> properties) {
    sol::table property_map = lua.create_table();
    for (auto [_key, value] : properties) {
      property_map[_key] = PropertyToSolObject(value);
    }
    return property_map;
  }

}