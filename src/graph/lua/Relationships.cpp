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

    sol::as_table_t<std::vector<Relationship>> Shard::RelationshipsGetViaLua(const std::vector<uint64_t> &ids) {
      std::vector<Relationship> relationships = RelationshipsGetPeered(ids).get0();

      sort(relationships.begin(), relationships.end(), [](const Relationship& a, const Relationship& b) {
        return a.getId() < b.getId();
      });

      return sol::as_table(relationships);
    }

    sol::as_table_t<std::vector<Relationship>> Shard::RelationshipsGetByLinksViaLua(std::vector<Link> links) {
      return sol::as_table(RelationshipsGetPeered(links).get0());
    }

    sol::as_table_t<std::vector<std::string>> Shard::RelationshipsGetTypeViaLua(std::vector<uint64_t> ids) {
      std::vector<std::string> properties;
      for (const auto& [id, value] : RelationshipsGetTypePeered(ids).get0()) {
        properties.emplace_back(value);
      }
      return sol::as_table(properties);
    }

    sol::as_table_t<std::map<Link, std::string>> Shard::RelationshipsGetTypeByLinksViaLua(std::vector<Link> links) {
      return sol::as_table( RelationshipsGetTypePeered(links).get0());
    }

    sol::as_table_t<std::vector<sol::object>> Shard::RelationshipsGetPropertyViaLua(std::vector<uint64_t> ids, const std::string& property) {
      std::vector<sol::object> properties;
      properties.reserve(ids.size());

      for (const auto& [id, value] : RelationshipsGetPropertyPeered(ids, property).get0()) {
        properties.emplace_back(PropertyToSolObject(value));
      }
      return sol::as_table(properties);
    }

    sol::as_table_t<std::map<Link, sol::object>> Shard::RelationshipsGetPropertyByLinksViaLua(std::vector<Link> links, const std::string& property) {
      std::map<Link, sol::object> properties;

      for(const auto& [link, value] : RelationshipsGetPropertyPeered(links, property).get0()) {
        properties[link] = PropertyToSolObject(value);
      }
      return sol::as_table(properties);
    }

    sol::as_table_t<std::vector<sol::object>> Shard::RelationshipsGetPropertiesViaLua(std::vector<uint64_t> ids) {
      std::vector<sol::object> properties;
      properties.reserve(ids.size());

      for(const auto& [id, value] : RelationshipsGetPropertiesPeered(ids).get0()) {
        properties.emplace_back(PropertiesToSolObject(value));
      }
      return sol::as_table(properties);
    }

    sol::as_table_t<std::map<Link, sol::object>> Shard::RelationshipsGetPropertiesByLinksViaLua(std::vector<Link> links) {
      std::map<Link, sol::object> properties;

      for(const auto& [link, value] : RelationshipsGetPropertiesPeered(links).get0()) {
        properties[link] = PropertiesToSolObject(value);
      }
      return sol::as_table(properties);
    }
}