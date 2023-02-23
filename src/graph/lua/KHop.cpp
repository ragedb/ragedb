/*
 * Copyright Max De Marzi. All Rights Reserved.
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

    sol::as_table_t<std::vector<uint64_t>> Shard::KHopIdsViaLua(uint64_t id, uint64_t hops) {
        return KHopIdsPeered(id, hops).get0();
    }

    sol::as_table_t<std::vector<uint64_t>> Shard::KHopIdsViaLua(uint64_t id, uint64_t hops, Direction direction) {
       return KHopIdsPeered(id, hops, direction).get0();
    }

    sol::as_table_t<std::vector<uint64_t>> Shard::KHopIdsViaLua(uint64_t id, uint64_t hops, Direction direction, const std::string& rel_type) {
        return KHopIdsPeered(id, hops, direction, rel_type).get0();
    }

    sol::as_table_t<std::vector<uint64_t>> Shard::KHopIdsViaLua(uint64_t id, uint64_t hops, Direction direction, const std::vector<std::string> &rel_types) {
       return KHopIdsPeered(id, hops, direction, rel_types).get0();
    }

    sol::as_table_t<std::vector<uint64_t>> Shard::KHopIdsViaLua(const std::string& type, const std::string& key, uint64_t hops) {
        return KHopIdsPeered(type, key, hops).get0();
    }

    sol::as_table_t<std::vector<uint64_t>> Shard::KHopIdsViaLua(const std::string& type, const std::string& key, uint64_t hops, Direction direction) {
       return KHopIdsPeered(type, key, hops, direction).get0();
    }

    sol::as_table_t<std::vector<uint64_t>> Shard::KHopIdsViaLua(const std::string& type, const std::string& key, uint64_t hops, Direction direction, const std::string& rel_type) {
       return KHopIdsPeered(type, key, hops, direction, rel_type).get0();
    }

    sol::as_table_t<std::vector<uint64_t>> Shard::KHopIdsViaLua(const std::string& type, const std::string& key, uint64_t hops, Direction direction, const std::vector<std::string> &rel_types) {
       return KHopIdsPeered(type, key, hops, direction, rel_types).get0();
    }

    sol::as_table_t<std::vector<Node>> Shard::KHopNodesViaLua(uint64_t id, uint64_t hops) {
        return KHopNodesPeered(id, hops).get0();
    }

    sol::as_table_t<std::vector<Node>> Shard::KHopNodesViaLua(uint64_t id, uint64_t hops, Direction direction) {
        return KHopNodesPeered(id, hops, direction).get0();
    }

    sol::as_table_t<std::vector<Node>> Shard::KHopNodesViaLua(uint64_t id, uint64_t hops, Direction direction, const std::string& rel_type) {
        return KHopNodesPeered(id, hops, direction, rel_type).get0();
    }

    sol::as_table_t<std::vector<Node>> Shard::KHopNodesViaLua(uint64_t id, uint64_t hops, Direction direction, const std::vector<std::string> &rel_types) {
        return KHopNodesPeered(id, hops, direction, rel_types).get0();
    }

    sol::as_table_t<std::vector<Node>> Shard::KHopNodesViaLua(const std::string& type, const std::string& key, uint64_t hops) {
        return KHopNodesPeered(type, key, hops).get0();
    }

    sol::as_table_t<std::vector<Node>> Shard::KHopNodesViaLua(const std::string& type, const std::string& key, uint64_t hops, Direction direction) {
        return KHopNodesPeered(type, key, hops, direction).get0();
    }

    sol::as_table_t<std::vector<Node>> Shard::KHopNodesViaLua(const std::string& type, const std::string& key, uint64_t hops, Direction direction, const std::string& rel_type) {
        return KHopNodesPeered(type, key, hops, direction, rel_type).get0();
    }

    sol::as_table_t<std::vector<Node>> Shard::KHopNodesViaLua(const std::string& type, const std::string& key, uint64_t hops, Direction direction, const std::vector<std::string> &rel_types) {
        return KHopNodesPeered(type, key, hops, direction, rel_types).get0();
    }

    uint64_t Shard::KHopCountViaLua(uint64_t id, uint64_t hops) {
        return KHopCountPeered(id, hops).get0();
    }

    uint64_t Shard::KHopCountViaLua(uint64_t id, uint64_t hops, Direction direction) {
        return KHopCountPeered(id, hops, direction).get0();
    }

    uint64_t Shard::KHopCountViaLua(uint64_t id, uint64_t hops, Direction direction, const std::string& rel_type) {
        return KHopCountPeered(id, hops, direction, rel_type).get0();
    }

    uint64_t Shard::KHopCountViaLua(uint64_t id, uint64_t hops, Direction direction, const std::vector<std::string> &rel_types) {
        return KHopCountPeered(id, hops, direction, rel_types).get0();
    }

    uint64_t Shard::KHopCountViaLua(const std::string& type, const std::string& key, uint64_t hops) {
        return KHopCountPeered(type, key, hops).get0();
    }

    uint64_t Shard::KHopCountViaLua(const std::string& type, const std::string& key, uint64_t hops, Direction direction) {
        return KHopCountPeered(type, key, hops, direction).get0();
    }

    uint64_t Shard::KHopCountViaLua(const std::string& type, const std::string& key, uint64_t hops, Direction direction, const std::string& rel_type) {
        return KHopCountPeered(type, key, hops, direction, rel_type).get0();
    }

    uint64_t Shard::KHopCountViaLua(const std::string& type, const std::string& key, uint64_t hops, Direction direction, const std::vector<std::string> &rel_types) {
        return KHopCountPeered(type, key, hops, direction, rel_types).get0();
    }
}