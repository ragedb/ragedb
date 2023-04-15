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

    sol::table Shard::NodesGetViaLua(std::vector<uint64_t> ids) {
        sol::table nodes = lua.create_table(0, ids.size());
        for (const auto& node : NodesGetPeered(ids).get0()) {
            nodes.set(node.getId(), node);
        }

       return nodes;
    }

    sol::table Shard::NodesGetViaLua(std::vector<Link> links) {
      sol::table nodes = lua.create_table(0, links.size());
      auto results = NodesGetPeered(links).get0();
      for (auto& node : results) {
          nodes.set(node.getId(), node);
      }

      return nodes;
    }

    sol::table Shard::NodesGetKeyViaLua(std::vector<uint64_t> ids) {
        sol::table properties = lua.create_table(0, ids.size());
        for (const auto& [id, value] : NodesGetKeyPeered(ids).get0()) {
            properties.set(id, value);
        }
        return properties;
    }

    sol::table Shard::NodesGetKeyViaLua(std::vector<Link> links) {
        sol::table properties = lua.create_table(0, links.size());
        for (const auto& [id, value] : NodesGetKeyPeered(links).get0()) {
            properties.set(id.node_id, value);
        }
        return properties;
    }

    sol::table Shard::NodesGetTypeViaLua(std::vector<uint64_t> ids) {
        sol::table properties = lua.create_table(0, ids.size());
        for (const auto& [id, value] : NodesGetTypePeered(ids).get0()) {
            properties.set(id, value);
        }
        return properties;
    }

    sol::table Shard::NodesGetTypeViaLua(std::vector<Link> links) {
        sol::table properties = lua.create_table(0, links.size());
        for (const auto& [id, value] : NodesGetTypePeered(links).get0()) {
            properties.set(id.node_id, value);
        }
        return properties;
    }

    //TODO: Deal with vector properties, see PropertiesToSolObject
    sol::table Shard::NodesGetPropertyViaLua(std::vector<uint64_t> ids, const std::string& property) {
        sol::table properties = lua.create_table(0, ids.size());
        for (const auto& [id, value] : NodesGetPropertyPeered(ids, property).get0()) {
            properties.set(id, value);
        }
        return properties;
    }

    sol::table Shard::NodesGetPropertyViaLua(std::vector<Link> links, const std::string& property) {
        sol::table properties = lua.create_table(0, links.size());
        for (const auto& [id, value] : NodesGetPropertyPeered(links, property).get0()) {
            properties.set(id.node_id, value);
        }
        return properties;
    }

    sol::table Shard::NodesGetPropertiesViaLua(std::vector<uint64_t> ids) {
        sol::table properties = lua.create_table(0, ids.size());
        for (const auto& [id, node_properties] : NodesGetPropertiesPeered(ids).get0()) {
            properties.set(id, PropertiesToSolObject(node_properties));
        }
        return properties;
    }

    sol::table Shard::NodesGetPropertiesViaLua(std::vector<Link> links) {
        sol::table properties = lua.create_table(0, links.size());
        for (const auto& [id, node_properties] : NodesGetPropertiesPeered(links).get0()) {
            properties.set(id.node_id, PropertiesToSolObject(node_properties));
        }
        return properties;
    }
}