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

    // Relationship Types
    uint16_t Shard::RelationshipTypesGetCountViaLua() const{
        return RelationshipTypesGetCountPeered();
    }

    uint64_t Shard::RelationshipTypesGetCountByTypeViaLua(const std::string& type){
        return RelationshipTypesGetCountPeered(type).get0();
    }

    sol::as_table_t<std::set<std::string>> Shard::RelationshipTypesGetViaLua() {
        return sol::as_table(RelationshipTypesGetPeered());
    }

    sol::as_table_t<std::map<std::string, std::string>> Shard::RelationshipTypeGetViaLua(const std::string& type) {
      return sol::as_table(RelationshipTypeGetPeered(type));
    }

    // Relationship Type
    uint16_t Shard::RelationshipTypeInsertViaLua(const std::string& type) {
        return RelationshipTypeInsertPeered(type).get0();
    }

    // Node Types
    uint16_t Shard::NodeTypesGetCountViaLua() const {
        return NodeTypesGetCountPeered();
    }

    uint64_t Shard::NodeTypesGetCountByTypeViaLua(const std::string& type) {
        return NodeTypesGetCountPeered(type).get0();
    }

    sol::as_table_t<std::set<std::string>> Shard::NodeTypesGetViaLua() {
        return sol::as_table(NodeTypesGetPeered());
    }

    sol::as_table_t<std::map<std::string, std::string>> Shard::NodeTypeGetViaLua(const std::string& type) {
      return sol::as_table(NodeTypeGetPeered(type));
    }

    // Node Type
    uint16_t Shard::NodeTypeInsertViaLua(const std::string& type) {
        return NodeTypeInsertPeered(type).get0();
    }

    // Property Types
    uint8_t Shard::NodePropertyTypeAddViaLua(const std::string& node_type, const std::string& key, const std::string& type) {
        return NodePropertyTypeAddPeered(node_type, key, type).get0();
    }

    uint8_t Shard::RelationshipPropertyTypeAddViaLua(const std::string& relationship_type, const std::string& key, const std::string& type) {
        return RelationshipPropertyTypeAddPeered(relationship_type, key, type).get0();
    }

    bool Shard::NodePropertyTypeDeleteViaLua(const std::string& type, const std::string& key) {
        return NodePropertyTypeDeletePeered(type, key).get0();
    }

    bool Shard::RelationshipPropertyTypeDeleteViaLua(const std::string& type, const std::string& key) {
        return RelationshipPropertyTypeDeletePeered(type, key).get0();
    }

}