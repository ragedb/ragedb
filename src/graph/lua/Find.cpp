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

    uint64_t Shard::FindNodeCountViaLua(const std::string& type, const std::string& property, const Operation& operation, const sol::object& object) {
        return FindNodeCountPeered(type, property, operation, SolObjectToProperty(object)).get0();
    }

    sol::as_table_t<std::vector<uint64_t>> Shard::FindNodeIdsViaLua(const std::string& type, const std::string& property, const Operation& operation, const sol::object& object, sol::optional<uint64_t> skip, sol::optional<uint64_t> limit) {
      return sol::as_table(FindNodeIdsPeered(type, property, operation, SolObjectToProperty(object), skip.value_or(SKIP), limit.value_or(LIMIT)).get0());
    }

    sol::as_table_t<std::vector<Node>> Shard::FindNodesViaLua(const std::string& type, const std::string& property, const Operation& operation, const sol::object& object, sol::optional<uint64_t> skip, sol::optional<uint64_t> limit) {
        return sol::as_table(FindNodesPeered(type, property, operation, SolObjectToProperty(object), skip.value_or(SKIP), limit.value_or(LIMIT)).get0());
    }

    uint64_t Shard::FindRelationshipCountViaLua(const std::string& type, const std::string& property, const Operation& operation, const sol::object& object) {
        return FindRelationshipCountPeered(type, property, operation, SolObjectToProperty(object)).get0();
    }

    sol::as_table_t<std::vector<uint64_t>> Shard::FindRelationshipIdsViaLua(const std::string& type, const std::string& property, const Operation& operation, const sol::object& object, sol::optional<uint64_t> skip, sol::optional<uint64_t> limit) {
        return sol::as_table(FindRelationshipIdsPeered(type, property, operation, SolObjectToProperty(object), skip.value_or(SKIP), limit.value_or(LIMIT)).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::FindRelationshipsViaLua(const std::string& type, const std::string& property, const Operation& operation, const sol::object& object, sol::optional<uint64_t> skip, sol::optional<uint64_t> limit) {
        return sol::as_table(FindRelationshipsPeered(type, property, operation, SolObjectToProperty(object), skip.value_or(SKIP), limit.value_or(LIMIT)).get0());
    }

}