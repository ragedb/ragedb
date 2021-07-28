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

    sol::as_table_t<std::vector<Link>> Shard::NodeGetRelationshipsIdsViaLua(const std::string& type, const std::string& key) {
        return sol::as_table(NodeGetRelationshipsIDsPeered(type, key).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetRelationshipsIdsForDirectionViaLua(const std::string& type, const std::string& key, Direction direction) {
        return sol::as_table(NodeGetRelationshipsIDsPeered(type, key, direction).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetRelationshipsIdsForDirectionForTypeViaLua(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type) {
        return sol::as_table(NodeGetRelationshipsIDsPeered(type, key, direction, rel_type).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetRelationshipsIdsForDirectionForTypeIdViaLua(const std::string& type, const std::string& key, Direction direction, uint16_t type_id) {
        return sol::as_table(NodeGetRelationshipsIDsPeered(type, key, direction, type_id).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetRelationshipsIdsForDirectionForTypesViaLua(const std::string& type, const std::string& key, Direction direction, const std::vector<std::string> &rel_types) {
        return sol::as_table(NodeGetRelationshipsIDsPeered(type, key, direction, rel_types).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetRelationshipsIdsForTypeViaLua(const std::string& type, const std::string& key, const std::string& rel_type) {
        return sol::as_table(NodeGetRelationshipsIDsPeered(type, key, rel_type).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetRelationshipsIdsForTypeIdViaLua(const std::string& type, const std::string& key, uint16_t type_id) {
        return sol::as_table(NodeGetRelationshipsIDsPeered(type, key, type_id).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetRelationshipsIdsForTypesViaLua(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types) {
        return sol::as_table(NodeGetRelationshipsIDsPeered(type, key, rel_types).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetRelationshipsIdsByIdViaLua(uint64_t id) {
        return sol::as_table(NodeGetRelationshipsIDsPeered(id).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetRelationshipsIdsByIdForDirectionViaLua(uint64_t id, Direction direction) {
        return sol::as_table(NodeGetRelationshipsIDsPeered(id, direction).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetRelationshipsIdsByIdForDirectionForTypeViaLua(uint64_t id, Direction direction, const std::string& rel_type) {
        return sol::as_table(NodeGetRelationshipsIDsPeered(id, direction, rel_type).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetRelationshipsIdsByIdForDirectionForTypeIdViaLua(uint64_t id, Direction direction, uint16_t type_id) {
        return sol::as_table(NodeGetRelationshipsIDsPeered(id, direction, type_id).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetRelationshipsIdsByIdForDirectionForTypesViaLua(uint64_t id, Direction direction, const std::vector<std::string> &rel_types) {
        return sol::as_table(NodeGetRelationshipsIDsPeered(id, direction, rel_types).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetRelationshipsIdsByIdForTypeViaLua(uint64_t id, const std::string& rel_type) {
        return sol::as_table(NodeGetRelationshipsIDsPeered(id, rel_type).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetRelationshipsIdsByIdForTypeIdViaLua(uint64_t id, uint16_t type_id) {
        return sol::as_table(NodeGetRelationshipsIDsPeered(id, type_id).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetRelationshipsIdsByIdForTypesViaLua(uint64_t id, const std::vector<std::string> &rel_types) {
        return sol::as_table(NodeGetRelationshipsIDsPeered(id, rel_types).get0());
    }


    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsViaLua(const std::string& type, const std::string& key) {
        return sol::as_table(NodeGetRelationshipsPeered(type, key).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsForTypeViaLua(const std::string& type, const std::string& key, const std::string& rel_type) {
        return sol::as_table(NodeGetRelationshipsPeered(type, key, rel_type).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsForTypeIdViaLua(const std::string& type, const std::string& key, uint16_t type_id) {
        return sol::as_table(NodeGetRelationshipsPeered(type, key, type_id).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsForTypesViaLua(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types) {
        return sol::as_table(NodeGetRelationshipsPeered(type, key, rel_types).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsByIdViaLua(uint64_t id) {
        return sol::as_table(NodeGetRelationshipsPeered(id).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsByIdForTypeViaLua(uint64_t id, const std::string& rel_type) {
        return sol::as_table(NodeGetRelationshipsPeered(id, rel_type).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsByIdForTypeIdViaLua(uint64_t id, uint16_t type_id) {
        return sol::as_table(NodeGetRelationshipsPeered(id, type_id).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsByIdForTypesViaLua(uint64_t id, const std::vector<std::string> &rel_types) {
        return sol::as_table(NodeGetRelationshipsPeered(id, rel_types).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsForDirectionViaLua(const std::string& type, const std::string& key, Direction direction) {
        return sol::as_table(NodeGetRelationshipsPeered(type, key, direction).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsForDirectionForTypeViaLua(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type) {
        return sol::as_table(NodeGetRelationshipsPeered(type, key, direction, rel_type).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsForDirectionForTypeIdViaLua(const std::string& type, const std::string& key, Direction direction, uint16_t type_id) {
        return sol::as_table(NodeGetRelationshipsPeered(type, key, direction, type_id).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsForDirectionForTypesViaLua(const std::string& type, const std::string& key, Direction direction, const std::vector<std::string> &rel_types) {
        return sol::as_table(NodeGetRelationshipsPeered(type, key, direction, rel_types).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsByIdForDirectionViaLua(uint64_t id, Direction direction) {
        return sol::as_table(NodeGetRelationshipsPeered(id, direction).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsByIdForDirectionForTypeViaLua(uint64_t id, Direction direction, const std::string& rel_type) {
        return sol::as_table(NodeGetRelationshipsPeered(id, direction, rel_type).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsByIdForDirectionForTypeIdViaLua(uint64_t id, Direction direction, uint16_t type_id) {
        return sol::as_table(NodeGetRelationshipsPeered(id, direction, type_id).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsByIdForDirectionForTypesViaLua(uint64_t id, Direction direction, const std::vector<std::string> &rel_types) {
        return sol::as_table(NodeGetRelationshipsPeered(id, direction, rel_types).get0());
    }

}
