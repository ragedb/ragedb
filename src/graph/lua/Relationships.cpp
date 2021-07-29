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

    uint64_t Shard::RelationshipAddEmptyByTypeIdByIdsViaLua(uint16_t rel_type_id, uint64_t id1, uint64_t id2) {
        return RelationshipAddEmptyPeered(rel_type_id, id1, id2).get0();
    }

    uint64_t Shard::RelationshipAddEmptyByIdsViaLua(const std::string& rel_type, uint64_t id1, uint64_t id2) {
        return RelationshipAddEmptyPeered(rel_type, id1, id2).get0();
    }

    uint64_t Shard::RelationshipAddViaLua(const std::string& rel_type, const std::string& type1, const std::string& key1,
                                          const std::string& type2, const std::string& key2, const std::string& properties) {
        return RelationshipAddPeered(rel_type, type1, key1, type2, key2, properties).get0();
    }

    uint64_t Shard::RelationshipAddByTypeIdByIdsViaLua(uint16_t rel_type_id, uint64_t id1, uint64_t id2, const std::string& properties) {
        return RelationshipAddPeered(rel_type_id, id1, id2, properties).get0();
    }

    uint64_t Shard::RelationshipAddByIdsViaLua(const std::string& rel_type, uint64_t id1, uint64_t id2, const std::string& properties) {
        return RelationshipAddPeered(rel_type, id1, id2, properties).get0();
    }

    Relationship Shard::RelationshipGetViaLua(uint64_t id) {
        return RelationshipGetPeered(id).get0();
    }

    sol::as_table_t<std::vector<Relationship>> Shard::RelationshipsGetViaLua(const std::vector<uint64_t> &ids) {
        return sol::as_table(RelationshipsGetPeered(ids).get0());
    }

    bool Shard::RelationshipRemoveViaLua(uint64_t id) {
        return RelationshipRemovePeered(id).get0();
    }

    std::string Shard::RelationshipGetTypeViaLua(uint64_t id) {
        return RelationshipGetTypePeered(id).get0();
    }

    uint16_t Shard::RelationshipGetTypeIdViaLua(uint64_t id) {
        return RelationshipGetTypeIdPeered(id).get0();
    }

    uint64_t Shard::RelationshipGetStartingNodeIdViaLua(uint64_t id) {
        return RelationshipGetStartingNodeIdPeered(id).get0();
    }

    uint64_t Shard::RelationshipGetEndingNodeIdViaLua(uint64_t id) {
        return RelationshipGetEndingNodeIdPeered(id).get0();
    }

    sol::object Shard::RelationshipPropertyGetViaLua(uint64_t id, const std::string& property) {
        std::any value = RelationshipPropertyGetPeered(id, property).get0();

        if (!value.has_value()) {
            return sol::lua_nil;
        }

        const auto& value_type = value.type();

        if(value_type == typeid(std::string)) {
            return sol::make_object(lua.lua_state(), std::any_cast<std::string>(value));
        }

        if(value_type == typeid(int64_t)) {
            return sol::make_object(lua.lua_state(), std::any_cast<int64_t>(value));
        }

        if(value_type == typeid(double)) {
            return sol::make_object(lua.lua_state(), std::any_cast<double>(value));
        }

        if(value_type == typeid(bool)) {
            return sol::make_object(lua.lua_state(), std::any_cast<bool>(value));
        }
        
        if(value_type == typeid(std::vector<std::string>)) {
            return sol::make_object(lua.lua_state(), sol::as_table(std::any_cast<std::vector<std::string>>(value)));
        }

        if(value_type == typeid(std::vector<int64_t>)) {
            return sol::make_object(lua.lua_state(), sol::as_table(std::any_cast<std::vector<int64_t>>(value)));
        }

        if(value_type == typeid(std::vector<double>)) {
            return sol::make_object(lua.lua_state(), sol::as_table(std::any_cast<std::vector<double>>(value)));
        }

        if(value_type == typeid(std::vector<bool>)) {
            return sol::make_object(lua.lua_state(), sol::as_table(std::any_cast<std::vector<bool>>(value)));
        }

        if(value_type == typeid(std::map<std::string, std::string>)) {
            return sol::make_object(lua.lua_state(), sol::as_table(std::any_cast<std::map<std::string, std::string>>(value)));
        }

        if(value_type == typeid(std::map<std::string, int64_t>)) {
            return sol::make_object(lua.lua_state(), sol::as_table(std::any_cast<std::map<std::string, int64_t>>(value)));
        }

        if(value_type == typeid(std::map<std::string, double>)) {
            return sol::make_object(lua.lua_state(), sol::as_table(std::any_cast<std::map<std::string, double>>(value)));
        }

        if(value_type == typeid(std::map<std::string, bool>)) {
            return sol::make_object(lua.lua_state(), sol::as_table(std::any_cast<std::map<std::string, bool>>(value)));
        }

        return sol::make_object(lua.lua_state(), sol::lua_nil);
    }

    bool Shard::RelationshipPropertySetViaLua(uint64_t id, const std::string& property, const sol::object& value) {
        if (value == sol::lua_nil) {
            return false;
        }

        if (value.is<std::string>()) {
            return RelationshipPropertySetPeered(id, property, value.as<std::string>()).get0();
        }
        if (value.is<int64_t>()) {
            return RelationshipPropertySetPeered(id, property, value.as<int64_t>()).get0();
        }
        if (value.is<double>()) {
            return RelationshipPropertySetPeered(id, property, value.as<double>()).get0();
        }
        if (value.is<bool>()) {
            return RelationshipPropertySetPeered(id, property, value.as<bool>()).get0();
        }

        return false;
    }

    bool Shard::RelationshipPropertySetFromJsonViaLua(uint64_t id, const std::string& property, const std::string& value) {
        return RelationshipPropertySetFromJsonPeered(id, property, value).get0();
    }

    bool Shard::RelationshipPropertyDeleteViaLua(uint64_t id, const std::string& property) {
        return RelationshipPropertyDeletePeered(id, property).get0();
    }

    bool Shard::RelationshipPropertiesSetFromJsonViaLua(uint64_t id, const std::string &value) {
        return RelationshipPropertiesSetFromJsonPeered(id, value).get0();
    }

    bool Shard::RelationshipPropertiesResetFromJsonViaLua(uint64_t id, const std::string &value) {
        return RelationshipPropertiesResetFromJsonPeered(id, value).get0();
    }

    bool Shard::RelationshipPropertiesDeleteViaLua(uint64_t id) {
        return RelationshipPropertiesDeletePeered(id).get0();
    }
}