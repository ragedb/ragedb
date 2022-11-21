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

    uint64_t Shard::AllNodesCountViaLua() {
        return AllNodesCountPeered().get0();
    }

    uint64_t Shard::AllNodesCountViaLua(const std::string& type) {
        return AllNodesCountPeered(type).get0();
    }

    sol::table Shard::AllNodesCountsViaLua() {
        sol::table counts = lua.create_table();
        for (const auto& [type, count] : AllNodesCountsPeered().get0()) {
            counts.set(type, count);
        }

        return counts;
    }

    uint64_t Shard::AllRelationshipsCountViaLua() {
        return AllRelationshipsCountPeered().get0();
    }

    uint64_t Shard::AllRelationshipsCountViaLua(const std::string& type) {
        return AllRelationshipsCountPeered(type).get0();
    }

    sol::table Shard::AllRelationshipsCountsViaLua() {
        sol::table counts = lua.create_table();
        for (const auto& [type, count] : AllRelationshipsCountsPeered().get0()) {
            counts.set(type, count);
        }

        return counts;
    }

    sol::as_table_t<std::vector<uint64_t>> Shard::AllNodeIdsViaLua(sol::optional<uint64_t> skip, sol::optional<uint64_t> limit) {
        return sol::as_table(AllNodeIdsPeered(skip.value_or(SKIP), limit.value_or(LIMIT)).get0());
    }

    sol::as_table_t<std::vector<uint64_t>> Shard::AllNodeIdsForTypeViaLua(const std::string& type, sol::optional<uint64_t> skip, sol::optional<uint64_t> limit) {
        return sol::as_table(AllNodeIdsPeered(type, skip.value_or(SKIP), limit.value_or(LIMIT)).get0());
    }

    sol::as_table_t<std::vector<uint64_t>> Shard::AllRelationshipIdsViaLua(sol::optional<uint64_t> skip, sol::optional<uint64_t> limit) {
        return sol::as_table(AllRelationshipIdsPeered(skip.value_or(SKIP), limit.value_or(LIMIT)).get0());
    }

    sol::as_table_t<std::vector<uint64_t>> Shard::AllRelationshipIdsForTypeViaLua(const std::string& rel_type, sol::optional<uint64_t> skip, sol::optional<uint64_t> limit) {
        return sol::as_table(AllRelationshipIdsPeered(rel_type, skip.value_or(SKIP), limit.value_or(LIMIT)).get0());
    }

    sol::as_table_t<std::vector<Node>> Shard::AllNodesViaLua(sol::optional<uint64_t> skip, sol::optional<uint64_t> limit) {
        return sol::as_table(AllNodesPeered(skip.value_or(SKIP), limit.value_or(LIMIT)).get0());
    }

    sol::as_table_t<std::vector<Node>> Shard::AllNodesForTypeViaLua(const std::string& type, sol::optional<uint64_t> skip, sol::optional<uint64_t> limit) {
        return sol::as_table(AllNodesPeered(type, skip.value_or(SKIP), limit.value_or(LIMIT)).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::AllRelationshipsViaLua(sol::optional<uint64_t> skip, sol::optional<uint64_t> limit) {
        return sol::as_table(AllRelationshipsPeered(skip.value_or(SKIP), limit.value_or(LIMIT)).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::AllRelationshipsForTypeViaLua(const std::string& rel_type, sol::optional<uint64_t> skip, sol::optional<uint64_t> limit) {
        return sol::as_table(AllRelationshipsPeered(rel_type, skip.value_or(SKIP), limit.value_or(LIMIT)).get0());
    }

}