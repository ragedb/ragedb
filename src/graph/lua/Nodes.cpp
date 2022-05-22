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

    sol::as_table_t<std::vector<Node>> Shard::NodesGetViaLua(std::vector<uint64_t> ids) {
      std::vector<Node> nodes = NodesGetPeered(ids).get0();

      sort(nodes.begin(), nodes.end(), [](const Node& a, const Node& b) {
        return a.getId() < b.getId();
      });

       return sol::as_table(nodes);
    }

    sol::as_table_t<std::vector<Node>> Shard::NodesGetByLinksViaLua(std::vector<Link> links) {
      return sol::as_table(NodesGetPeered(links).get0());
    }

    sol::as_table_t<std::vector<std::string>> Shard::NodesGetKeyViaLua(std::vector<uint64_t> ids) {
      std::vector<std::string> properties;
      for (const auto& [id, value] : NodesGetKeyPeered(ids).get0()) {
        properties.emplace_back(value);
      }
      return sol::as_table(properties);
    }

    sol::as_table_t<std::map<Link, std::string>> Shard::NodesGetKeyByLinksViaLua(std::vector<Link> links) {
      return sol::as_table(NodesGetKeyPeered(links).get0());
    }

    sol::as_table_t<std::vector<std::string>> Shard::NodesGetTypeViaLua(std::vector<uint64_t> ids) {
      std::vector<std::string> properties;
      for (const auto& [id, value] : NodesGetTypePeered(ids).get0()) {
        properties.emplace_back(value);
      }
      return sol::as_table(properties);
    }

    sol::as_table_t<std::map<Link, std::string>> Shard::NodesGetTypeByLinksViaLua(std::vector<Link> links) {
      return sol::as_table(NodesGetTypePeered(links).get0());
    }

    sol::as_table_t<std::vector<uint16_t>> Shard::NodesGetTypeIdViaLua(std::vector<uint64_t> ids) {
      std::vector<uint16_t> properties;
      for (const auto& [id, value] : NodesGetTypeIdPeered(ids).get0()) {
        properties.emplace_back(value);
      }
      return sol::as_table(properties);
    }

    sol::as_table_t<std::map<Link, uint16_t>> Shard::NodesGetTypeIdByLinksViaLua(std::vector<Link> links) {
      return sol::as_table(NodesGetTypeIdPeered(links).get0());
    }

    sol::as_table_t<std::vector<sol::object>> Shard::NodesGetPropertyViaLua(std::vector<uint64_t> ids, const std::string& property) {
      std::vector<sol::object> properties;
      properties.reserve(ids.size());

      for (const auto& [id, value] : NodesGetPropertyPeered(ids, property).get0()) {
        properties.emplace_back(PropertyToSolObject(value));
      }
      return sol::as_table(properties);
    }

    sol::as_table_t<std::map<Link, sol::object>> Shard::NodesGetPropertyByLinksViaLua(std::vector<Link> links, const std::string& property) {
      std::map<Link, sol::object> properties;

      for(const auto& [link, value] : NodesGetPropertyPeered(links, property).get0()) {
        properties[link] = PropertyToSolObject(value);
      }
      return sol::as_table(properties);
    }

    sol::as_table_t<std::vector<sol::object>> Shard::NodesGetPropertiesViaLua(std::vector<uint64_t> ids) {
      std::vector<sol::object> properties;
      properties.reserve(ids.size());

      for(const auto& [id, value]  : NodesGetPropertiesPeered(ids).get0()) {
        properties.emplace_back(PropertiesToSolObject(value));
      }
      return sol::as_table(properties);
    }

    sol::as_table_t<std::map<Link, sol::object>> Shard::NodesGetPropertiesByLinksViaLua(std::vector<Link> links) {
      std::map<Link, sol::object> properties;

      for(const auto& [link, value] : NodesGetPropertiesPeered(links).get0()) {
        properties[link] = PropertiesToSolObject(value);
      }
      return sol::as_table(properties);
    }
}