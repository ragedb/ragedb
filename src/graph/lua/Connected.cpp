/*
 * Copyright Max De Marzi. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") {
    return NodeGetConnectedPeered().get0();
}
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

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetConnectedViaLua(const std::string &type, const std::string &key, const std::string &type2, const std::string &key2) {
        return NodeGetConnectedPeered(type, key, type2, key2).get0();
    }
    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetConnectedViaLua(const std::string &type, const std::string &key, const std::string &type2, const std::string &key2, const std::string &rel_type) {
        return NodeGetConnectedPeered(type, key, type2, key2, rel_type).get0();
    }
    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetConnectedViaLua(const std::string &type, const std::string &key, const std::string &type2, const std::string &key2, uint16_t type_id) {
        return NodeGetConnectedPeered(type, key, type2, key2, type_id).get0();
    }
    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetConnectedViaLua(const std::string &type, const std::string &key, const std::string &type2, const std::string &key2, const std::vector<std::string> &rel_types) {
        return NodeGetConnectedPeered(type, key, type2, key2, rel_types).get0();
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetConnectedViaLua(uint64_t id, uint64_t id2) {
        return NodeGetConnectedPeered(id, id2).get0();
    }
    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetConnectedViaLua(uint64_t id, uint64_t id2, const std::string &rel_type) {
        return NodeGetConnectedPeered(id, id2, rel_type).get0();
    }
    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetConnectedViaLua(uint64_t id, uint64_t id2, uint16_t type_id) {
        return NodeGetConnectedPeered(id, id2, type_id).get0();
    }
    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetConnectedViaLua(uint64_t id, uint64_t id2, const std::vector<std::string> &rel_types) {
        return NodeGetConnectedPeered(id, id2, rel_types).get0();
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetConnectedViaLua(const std::string &type, const std::string &key, const std::string &type2, const std::string &key2, Direction direction) {
        return NodeGetConnectedPeered(type, key, type2, key2, direction).get0();
    }
    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetConnectedViaLua(const std::string &type, const std::string &key, const std::string &type2, const std::string &key2, Direction direction, const std::string &rel_type) {
        return NodeGetConnectedPeered(type, key, type2, key2, direction, rel_type).get0();
    }
    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetConnectedViaLua(const std::string &type, const std::string &key, const std::string &type2, const std::string &key2, Direction direction, uint16_t type_id) {
        return NodeGetConnectedPeered(type, key, type2, key2, direction, type_id).get0();
    }
    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetConnectedViaLua(const std::string &type, const std::string &key, const std::string &type2, const std::string &key2, Direction direction, const std::vector<std::string> &rel_types) {
        return NodeGetConnectedPeered(type, key, type2, key2, direction, rel_types).get0();
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetConnectedViaLua(uint64_t id, uint64_t id2, Direction direction) {
        return NodeGetConnectedPeered(id, id2, direction).get0();
    }
    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetConnectedViaLua(uint64_t id, uint64_t id2, Direction direction, const std::string &rel_type) {
        return NodeGetConnectedPeered(id, id2, direction, rel_type).get0();
    }
    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetConnectedViaLua(uint64_t id, uint64_t id2, Direction direction, uint16_t type_id) {
        return NodeGetConnectedPeered(id, id2, direction, type_id).get0();
    }
    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetConnectedViaLua(uint64_t id, uint64_t id2, Direction direction, const std::vector<std::string> &rel_types) {
        return NodeGetConnectedPeered(id, id2, direction, rel_types).get0();
    }
}