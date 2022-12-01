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

    sol::table Shard::KHopIdsViaLua(uint64_t id, uint64_t hops) {
        sol::table ids = lua.create_table();
        for (const auto &node_id : KHopIdsPeered(id, hops).get0()) {
            ids.add(node_id);
        }
        return ids;
    }

    sol::table Shard::KHopIdsViaLua(uint64_t id, uint64_t hops, Direction direction) {
        sol::table ids = lua.create_table();
        for (const auto &node_id : KHopIdsPeered(id, hops, direction).get0()) {
            ids.add(node_id);
        }
        return ids;
    }

    sol::table Shard::KHopIdsViaLua(uint64_t id, uint64_t hops, Direction direction, const std::string& rel_type) {
        sol::table ids = lua.create_table();
        for (const auto &node_id : KHopIdsPeered(id, hops, direction, rel_type).get0()) {
            ids.add(node_id);
        }
        return ids;
    }

    sol::table Shard::KHopIdsViaLua(uint64_t id, uint64_t hops, Direction direction, const std::vector<std::string> &rel_types) {
        sol::table ids = lua.create_table();
        for (const auto &node_id : KHopIdsPeered(id, hops, direction, rel_types).get0()) {
            ids.add(node_id);
        }
        return ids;
    }

    sol::table Shard::KHopIdsViaLua(const std::string& type, const std::string& key, uint64_t hops) {
        sol::table ids = lua.create_table();
        for (const auto &node_id : KHopIdsPeered(type, key, hops).get0()) {
            ids.add(node_id);
        }
        return ids;
    }

    sol::table Shard::KHopIdsViaLua(const std::string& type, const std::string& key, uint64_t hops, Direction direction) {
        sol::table ids = lua.create_table();
        for (const auto &node_id : KHopIdsPeered(type, key, hops, direction).get0()) {
            ids.add(node_id);
        }
        return ids;
    }

    sol::table Shard::KHopIdsViaLua(const std::string& type, const std::string& key, uint64_t hops, Direction direction, const std::string& rel_type) {
        sol::table ids = lua.create_table();
        for (const auto &node_id : KHopIdsPeered(type, key, hops, direction, rel_type).get0()) {
            ids.add(node_id);
        }
        return ids;
    }

    sol::table Shard::KHopIdsViaLua(const std::string& type, const std::string& key, uint64_t hops, Direction direction, const std::vector<std::string> &rel_types) {
        sol::table ids = lua.create_table();
        for (const auto &node_id : KHopIdsPeered(type, key, hops, direction, rel_types).get0()) {
            ids.add(node_id);
        }
        return ids;
    }

    sol::table Shard::KHopNodesViaLua(uint64_t id, uint64_t hops) {
        sol::table ids = lua.create_table();
        for (const auto &node_id : KHopNodesPeered(id, hops).get0()) {
            ids.add(node_id);
        }
        return ids;
    }

    sol::table Shard::KHopNodesViaLua(uint64_t id, uint64_t hops, Direction direction) {
        sol::table ids = lua.create_table();
        for (const auto &node_id : KHopNodesPeered(id, hops, direction).get0()) {
            ids.add(node_id);
        }
        return ids;
    }

    sol::table Shard::KHopNodesViaLua(uint64_t id, uint64_t hops, Direction direction, const std::string& rel_type) {
        sol::table ids = lua.create_table();
        for (const auto &node_id : KHopNodesPeered(id, hops, direction, rel_type).get0()) {
            ids.add(node_id);
        }
        return ids;
    }

    sol::table Shard::KHopNodesViaLua(uint64_t id, uint64_t hops, Direction direction, const std::vector<std::string> &rel_types) {
        sol::table ids = lua.create_table();
        for (const auto &node_id : KHopNodesPeered(id, hops, direction, rel_types).get0()) {
            ids.add(node_id);
        }
        return ids;
    }

    sol::table Shard::KHopNodesViaLua(const std::string& type, const std::string& key, uint64_t hops) {
        sol::table ids = lua.create_table();
        for (const auto &node_id : KHopNodesPeered(type, key, hops).get0()) {
            ids.add(node_id);
        }
        return ids;
    }

    sol::table Shard::KHopNodesViaLua(const std::string& type, const std::string& key, uint64_t hops, Direction direction) {
        sol::table ids = lua.create_table();
        for (const auto &node_id : KHopNodesPeered(type, key, hops, direction).get0()) {
            ids.add(node_id);
        }
        return ids;
    }

    sol::table Shard::KHopNodesViaLua(const std::string& type, const std::string& key, uint64_t hops, Direction direction, const std::string& rel_type) {
        sol::table ids = lua.create_table();
        for (const auto &node_id : KHopNodesPeered(type, key, hops, direction, rel_type).get0()) {
            ids.add(node_id);
        }
        return ids;
    }

    sol::table Shard::KHopNodesViaLua(const std::string& type, const std::string& key, uint64_t hops, Direction direction, const std::vector<std::string> &rel_types) {
        sol::table ids = lua.create_table();
        for (const auto &node_id : KHopNodesPeered(type, key, hops, direction, rel_types).get0()) {
            ids.add(node_id);
        }
        return ids;
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