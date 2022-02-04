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

    sol::as_table_t<std::vector<Node>> Shard::NodesGetViaLua(const std::vector<uint64_t> &ids) {
        return sol::as_table(NodesGetPeered(ids).get0());
    }

    sol::as_table_t<std::vector<Node>> Shard::NodesGetByLinksViaLua(const std::vector<Link>& links) {
      return sol::as_table(NodesGetPeered(links).get0());
    }

    sol::as_table_t<std::vector<std::string>> Shard::NodesGetKeyViaLua(const std::vector<uint64_t>& ids) {
      return sol::as_table(NodesGetKeyPeered(ids).get0());
    }

    sol::as_table_t<std::vector<std::string>> Shard::NodesGetKeyByLinksViaLua(const std::vector<Link>& links) {
      return sol::as_table(NodesGetKeyPeered(links).get0());
    }

    sol::as_table_t<std::vector<std::string>> Shard::NodesGetTypeViaLua(const std::vector<uint64_t>& ids) {
      return sol::as_table(NodesGetTypePeered(ids).get0());
    }

    sol::as_table_t<std::vector<std::string>> Shard::NodesGetTypeByLinksViaLua(const std::vector<Link>& links) {
      return sol::as_table(NodesGetTypePeered(links).get0());
    }

    sol::as_table_t<std::vector<uint16_t>> Shard::NodesGetTypeIdViaLua(const std::vector<uint64_t>& ids) {
      return sol::as_table(NodesGetTypeIdPeered(ids).get0());
    }

    sol::as_table_t<std::vector<uint16_t>> Shard::NodesGetTypeIdByLinksViaLua(const std::vector<Link>& links) {
      return sol::as_table(NodesGetTypeIdPeered(links).get0());
    }

    sol::as_table_t<std::vector<sol::object>> Shard::NodesGetPropertyViaLua(const std::vector<uint64_t>& ids, const std::string& property) {
      std::vector<sol::object> properties;
      properties.reserve(ids.size());

      std::vector<property_type_t> values = NodesGetPropertyPeered(ids, property).get0();

      for(auto value : values) {
        properties.emplace_back(NodePropertyToSolObject2(value));
      }
      return sol::as_table(properties);
    }

    sol::as_table_t<std::vector<sol::object>> Shard::NodesGetPropertyByLinksViaLua(const std::vector<Link>& links, const std::string& property) {
      std::vector<sol::object> properties;
      properties.reserve(links.size());

      for(auto value : NodesGetPropertyPeered(links, property).get0()) {
        properties.emplace_back(NodePropertyToSolObject(value));
      }
      return sol::as_table(properties);
    }

    sol::as_table_t<std::vector<sol::object>> Shard::NodesGetPropertiesViaLua(const std::vector<uint64_t>& ids) {
      std::vector<sol::object> properties;
      properties.reserve(ids.size());

      for(auto value : NodesGetPeered(ids).get0()) {
        properties.emplace_back(sol::make_object(lua.lua_state(), value.getPropertiesLua(lua.lua_state())));
      }
      return sol::as_table(properties);
    }

    sol::as_table_t<std::vector<sol::object>> Shard::NodesGetPropertiesByLinksViaLua(const std::vector<Link>& links) {
      std::vector<sol::object> properties;
      properties.reserve(links.size());

      for(auto value : NodesGetPeered(links).get0()) {
        properties.emplace_back(sol::make_object(lua.lua_state(), value.getPropertiesLua(lua.lua_state())));
      }
      return sol::as_table(properties);
    }
}