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

    uint64_t Shard::RelationshipAddEmptyViaLua(const std::string& rel_type, const std::string& type1, const std::string& key1,
                                               const std::string& type2, const std::string& key2) {
        return RelationshipAddEmptyPeered(rel_type, type1, key1, type2, key2).get0();
    }

    uint64_t Shard::RelationshipAddEmptyByIdsViaLua(const std::string& rel_type, uint64_t id1, uint64_t id2) {
        return RelationshipAddEmptyPeered(rel_type, id1, id2).get0();
    }

    uint64_t Shard::RelationshipAddViaLua(const std::string& rel_type, const std::string& type1, const std::string& key1,
                                          const std::string& type2, const std::string& key2, const std::string& properties) {
        return RelationshipAddPeered(rel_type, type1, key1, type2, key2, properties).get0();
    }

    uint64_t Shard::RelationshipAddByIdsViaLua(const std::string& rel_type, uint64_t id1, uint64_t id2, const std::string& properties) {
        return RelationshipAddPeered(rel_type, id1, id2, properties).get0();
    }

    Relationship Shard::RelationshipGetViaLua(uint64_t id) {
        return RelationshipGetPeered(id).get0();
    }

    bool Shard::RelationshipRemoveViaLua(uint64_t id) {
        return RelationshipRemovePeered(id).get0();
    }

    std::string Shard::RelationshipGetTypeViaLua(uint64_t id) {
        return RelationshipGetTypePeered(id).get0();
    }

    uint64_t Shard::RelationshipGetStartingNodeIdViaLua(uint64_t id) {
        return RelationshipGetStartingNodeIdPeered(id).get0();
    }

    uint64_t Shard::RelationshipGetEndingNodeIdViaLua(uint64_t id) {
        return RelationshipGetEndingNodeIdPeered(id).get0();
    }

    sol::object Shard::RelationshipGetPropertiesViaLua(uint64_t id) {
        Relationship relationship = RelationshipGetPeered(id).get0();
        return sol::make_object(lua.lua_state(), relationship.getPropertiesLua(lua.lua_state()));
    }

    sol::object Shard::RelationshipGetPropertyViaLua(uint64_t id, const std::string& property) {
      property_type_t value = RelationshipGetPropertyPeered(id, property).get0();
      return PropertyToSolObject(value);
    }

    bool Shard::RelationshipSetPropertyViaLua(uint64_t id, const std::string& property, const sol::object& value) {
        if (value == sol::lua_nil) {
            return false;
        }

        if (value.is<std::string>()) {
            return RelationshipSetPropertyPeered(id, property, value.as<std::string>()).get0();
        }
        if (value.is<int64_t>()) {
            return RelationshipSetPropertyPeered(id, property, value.as<int64_t>()).get0();
        }
        if (value.is<double>()) {
            return RelationshipSetPropertyPeered(id, property, value.as<double>()).get0();
        }
        if (value.is<bool>()) {
            return RelationshipSetPropertyPeered(id, property, value.as<bool>()).get0();
        }
        if (value.is<std::vector<std::string>>()) {
          return RelationshipSetPropertyPeered(id, property, value.as<std::vector<std::string>>()).get0();
        }
        if (value.is<std::vector<double>>()) {
          return RelationshipSetPropertyPeered(id, property, value.as<std::vector<double>>()).get0();
        }
        if (value.is<std::vector<int64_t>>()) {
          return RelationshipSetPropertyPeered(id, property, value.as<std::vector<int64_t>>()).get0();
        }
        if (value.is<std::vector<bool>>()) {
          return RelationshipSetPropertyPeered(id, property, value.as<std::vector<bool>>()).get0();
        }
        return false;
    }

    bool Shard::RelationshipSetPropertyFromJsonViaLua(uint64_t id, const std::string& property, const std::string& value) {
        return RelationshipSetPropertyFromJsonPeered(id, property, value).get0();
    }

    bool Shard::RelationshipDeletePropertyViaLua(uint64_t id, const std::string& property) {
        return RelationshipDeletePropertyPeered(id, property).get0();
    }

    bool Shard::RelationshipSetPropertiesFromJsonViaLua(uint64_t id, const std::string &value) {
        return RelationshipSetPropertiesFromJsonPeered(id, value).get0();
    }

    bool Shard::RelationshipResetPropertiesFromJsonViaLua(uint64_t id, const std::string &value) {
        return RelationshipResetPropertiesFromJsonPeered(id, value).get0();
    }

    bool Shard::RelationshipDeletePropertiesViaLua(uint64_t id) {
        return RelationshipDeletePropertiesPeered(id).get0();
    }
}