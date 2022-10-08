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

    uint16_t Shard::NodeTypesGetCount() const {
      return node_types.getTypeIds().size();
    }

    uint64_t Shard::NodeTypesGetCount(const std::string &type) const {
        uint16_t type_id = node_types.getTypeId(type);
        return node_types.getCount(type_id);
    }

    std::set<std::string> Shard::NodeTypesGet() {
        return node_types.getTypes();
    }

    std::map<std::string, std::string> Shard::NodeTypeGet(const std::string& type) {
        uint16_t type_id = node_types.getTypeId(type);
        return node_types.getProperties(type_id).getPropertyTypes();
    }

    uint16_t Shard::RelationshipTypesGetCount() const {
      return relationship_types.getTypeIds().size();
    }

    uint64_t Shard::RelationshipTypesGetCount(const std::string &type) const {
        uint16_t type_id = relationship_types.getTypeId(type);
        if (relationship_types.ValidTypeId(type_id)) {
            return relationship_types.getCount(type_id);
        }
        // Not a valid Relationship Type
        return 0;
    }

    std::set<std::string> Shard::RelationshipTypesGet() {
        return relationship_types.getTypes();
    }

    std::map<std::string, std::string> Shard::RelationshipTypeGet(const std::string& type) {
        uint16_t type_id = relationship_types.getTypeId(type);
        return relationship_types.getProperties(type_id).getPropertyTypes();
    }

    std::string Shard::NodeTypeGetType(uint16_t type_id) {
        return node_types.getType(type_id);
    }

    uint16_t Shard::NodeTypeGetTypeId(const std::string &type) {
        return node_types.getTypeId(type);
    }

    bool Shard::NodeTypeInsert(const std::string& type, uint16_t type_id) {
        return node_types.addTypeId(type, type_id);
    }

    bool Shard::DeleteNodeType(const std::string& type) {
        return node_types.deleteTypeId(type);
    }

    std::string Shard::RelationshipTypeGetType(uint16_t type_id) {
        return relationship_types.getType(type_id);
    }

    uint16_t Shard::RelationshipTypeGetTypeId(const std::string &type) {
        if ( uint16_t type_id = relationship_types.getTypeId(type);
            relationship_types.ValidTypeId(type_id)) {
            return type_id;
        }
        // Not a valid Relationship Type
        return 0;
    }

    bool Shard::RelationshipTypeInsert(const std::string& type, uint16_t type_id) {
        return relationship_types.addTypeId(type, type_id);
    }

    bool Shard::DeleteRelationshipType(const std::string& type) {
        return relationship_types.deleteTypeId(type);
    }

    uint8_t Shard::NodePropertyTypeAdd(uint16_t type_id, const std::string& key, uint8_t property_type_id) {
        return node_types.getProperties(type_id).setPropertyTypeId(key, property_type_id);
    }

    uint8_t Shard::RelationshipPropertyTypeAdd(uint16_t type_id, const std::string& key, uint8_t property_type_id) {
        return relationship_types.getProperties(type_id).setPropertyTypeId(key, property_type_id);
    }

    std::string Shard::NodePropertyTypeGet(const std::string& type, const std::string& key) {
        uint16_t type_id = node_types.getTypeId(type);
        return node_types.getProperties(type_id).getPropertyType(key);
    }

    std::string Shard::RelationshipPropertyTypeGet(const std::string& type,  const std::string& key) {
        uint16_t type_id = relationship_types.getTypeId(type);
        return relationship_types.getProperties(type_id).getPropertyType(key);
    }

    bool Shard::NodePropertyTypeDelete(uint16_t type_id, const std::string& key) {
        return node_types.deleteTypeProperty(type_id, key);
    }

    bool Shard::RelationshipPropertyTypeDelete(uint16_t type_id, const std::string& key) {
        return relationship_types.deleteTypeProperty(type_id, key);
    }

}