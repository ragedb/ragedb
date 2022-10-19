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

  uint64_t Shard::FilterNodeCountViaLua(std::vector<uint64_t> ids, const std::string& type, const std::string& property, const Operation& operation, const sol::object& object) {
    return FilterNodeCountPeered(ids, type, property, operation, SolObjectToProperty(object)).get0();
  }

  sol::table Shard::FilterNodeIdsViaLua(std::vector<uint64_t> ids, const std::string& type, const std::string& property, const Operation& operation, const  sol::object& object, sol::optional<uint64_t> skip, sol::optional<uint64_t> limit, sol::optional<Sort> sortOrder) {
      sol::table node_ids = lua.create_table(0, 0);
      for (const auto& id : FilterNodeIdsPeered(ids, type, property, operation, SolObjectToProperty(object), skip.value_or(SKIP), limit.value_or(LIMIT), sortOrder.value_or(Sort::NONE)).get0()) {
          node_ids.add(id);
      }
      return node_ids;
  }

  sol::table Shard::FilterNodesViaLua(std::vector<uint64_t> ids, const std::string& type, const std::string& property, const Operation& operation, const  sol::object& object, sol::optional<uint64_t> skip, sol::optional<uint64_t> limit, sol::optional<Sort> sortOrder) {
      sol::table nodes = lua.create_table(0, 0);
      for (const auto& node : FilterNodesPeered(ids, type, property, operation, SolObjectToProperty(object), skip.value_or(SKIP), limit.value_or(LIMIT), sortOrder.value_or(Sort::NONE)).get0()) {
          nodes.add(node);
      }
      return nodes;
  }

  sol::table Shard::FilterNodePropertiesViaLua(std::vector<uint64_t> ids, const std::string& type, const std::string& property, const Operation& operation, const sol::object& object, sol::optional<uint64_t> skip, sol::optional<uint64_t> limit, sol::optional<Sort> sortOrder) {
      sol::table properties = lua.create_table(0, 0);
      for (const auto& node : FilterNodePropertiesPeered(ids, type, property, operation, SolObjectToProperty(object), skip.value_or(SKIP), limit.value_or(LIMIT), sortOrder.value_or(Sort::NONE)).get0() ) {
          sol::table node_properties = lua.create_table(0, 0);
          for (const auto& [_key, value] : node) {
              node_properties.set(_key, value);
          }
          properties.add(node_properties);
      }
      return properties;
  }

  uint64_t Shard::FilterRelationshipCountViaLua(std::vector<uint64_t> ids, const std::string& type, const std::string& property, const Operation& operation, const sol::object& object) {
    return FilterRelationshipCountPeered(ids, type, property, operation, SolObjectToProperty(object)).get0();
  }

  sol::table Shard::FilterRelationshipIdsViaLua(std::vector<uint64_t> ids, const std::string& type, const std::string& property, const Operation& operation, const  sol::object& object, sol::optional<uint64_t> skip, sol::optional<uint64_t> limit) {
      sol::table rel_ids = lua.create_table(0, 0);
      for (const auto& id : FilterRelationshipIdsPeered(ids, type, property, operation, SolObjectToProperty(object), skip.value_or(SKIP), limit.value_or(LIMIT)).get0()) {
          rel_ids.add(id);
      }
      return rel_ids;
  }

  sol::table Shard::FilterRelationshipsViaLua(std::vector<uint64_t> ids, const std::string& type, const std::string& property, const Operation& operation, const  sol::object& object, sol::optional<uint64_t> skip, sol::optional<uint64_t> limit) {
      sol::table rels = lua.create_table(0, 0);
      for (const auto& rel : FilterRelationshipsPeered(ids, type, property, operation, SolObjectToProperty(object), skip.value_or(SKIP), limit.value_or(LIMIT)).get0()) {
          rels.add(rel);
      }
      return rels;
  }

}