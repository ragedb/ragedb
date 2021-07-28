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

    sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsViaLua(const std::string& type, const std::string& key) {
        return sol::as_table(NodeGetNeighborsPeered(type, key).get0());
    }

    sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsForTypeViaLua(const std::string& type, const std::string& key, const std::string& rel_type) {
        return sol::as_table(NodeGetNeighborsPeered(type, key, rel_type).get0());
    }

    sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsForTypeIdViaLua(const std::string& type, const std::string& key, uint16_t type_id) {
        return sol::as_table(NodeGetNeighborsPeered(type, key, type_id).get0());
    }

    sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsForTypesViaLua(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types) {
        return sol::as_table(NodeGetNeighborsPeered(type, key, rel_types).get0());
    }

    sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsByIdViaLua(uint64_t id) {
        return sol::as_table(NodeGetNeighborsPeered(id).get0());
    }

    sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsByIdForTypeViaLua(uint64_t id, const std::string& rel_type) {
        return sol::as_table(NodeGetNeighborsPeered(id, rel_type).get0());
    }

    sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsByIdForTypeIdViaLua(uint64_t id, uint16_t type_id) {
        return sol::as_table(NodeGetNeighborsPeered(id, type_id).get0());
    }

    sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsByIdForTypesViaLua(uint64_t id, const std::vector<std::string> &rel_types) {
        return sol::as_table(NodeGetNeighborsPeered(id, rel_types).get0());
    }

    sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsForDirectionViaLua(const std::string& type, const std::string& key, Direction direction) {
        return sol::as_table(NodeGetNeighborsPeered(type, key, direction).get0());
    }

    sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsForDirectionForTypeViaLua(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type) {
        return sol::as_table(NodeGetNeighborsPeered(type, key, direction, rel_type).get0());
    }

    sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsForDirectionForTypeIdViaLua(const std::string& type, const std::string& key, Direction direction, uint16_t type_id) {
        return sol::as_table(NodeGetNeighborsPeered(type, key, direction, type_id).get0());
    }

    sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsForDirectionForTypesViaLua(const std::string& type, const std::string& key, Direction direction, const std::vector<std::string> &rel_types) {
        return sol::as_table(NodeGetNeighborsPeered(type, key, direction, rel_types).get0());
    }


    sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsByIdForDirectionViaLua(uint64_t id, Direction direction) {
        return sol::as_table(NodeGetNeighborsPeered(id, direction).get0());
    }

    sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsByIdForDirectionForTypeViaLua(uint64_t id, Direction direction, const std::string& rel_type) {
        return sol::as_table(NodeGetNeighborsPeered(id, direction, rel_type).get0());
    }

    sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsByIdForDirectionForTypeIdViaLua(uint64_t id, Direction direction, uint16_t type_id) {
        return sol::as_table(NodeGetNeighborsPeered(id, direction, type_id).get0());
    }

    sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsByIdForDirectionForTypesViaLua(uint64_t id, Direction direction, const std::vector<std::string> &rel_types) {
        return sol::as_table(NodeGetNeighborsPeered(id, direction, rel_types).get0());
    }

}