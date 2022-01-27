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

    uint64_t Shard::NodeAddEmptyViaLua(const std::string& type, const std::string& key) {
        return NodeAddEmptyPeered(type, key).get0();
    }

    uint64_t Shard::NodeAddViaLua(const std::string& type, const std::string& key, const std::string& properties) {
        return NodeAddPeered(type, key, properties).get0();
    }

    uint64_t Shard::NodeGetIdViaLua(const std::string& type, const std::string& key) {
        return NodeGetIDPeered(type, key).get0();
    }

    Node Shard::NodeGetViaLua(const std::string& type, const std::string& key) {
        return NodeGetPeered(type, key).get0();
    }

    Node Shard::NodeGetByIdViaLua(uint64_t id) {
        return NodeGetPeered(id).get0();
    }

    sol::as_table_t<std::vector<Node>> Shard::NodesGetViaLua(const std::vector<uint64_t> &ids) {
        return sol::as_table(NodesGetPeered(ids).get0());
    }

    bool Shard::NodeRemoveViaLua(const std::string& type, const std::string& key) {
        return NodeRemovePeered(type, key).get0();
    }

    bool Shard::NodeRemoveByIdViaLua(uint64_t id) {
        return NodeRemovePeered(id).get0();
    }

    uint16_t Shard::NodeGetTypeIdViaLua(uint64_t id) {
        return NodeGetTypeIdPeered(id).get0();
    }

    std::string Shard::NodeGetTypeViaLua(uint64_t id) {
        return NodeGetTypePeered(id).get0();
    }

    std::string Shard::NodeGetKeyViaLua(uint64_t id) {
        return NodeGetKeyPeered(id).get0();
    }

    sol::object Shard::NodePropertiesGetViaLua(const std::string& type, const std::string& key) {
        Node node = NodeGetPeered(type, key).get0();
        return sol::make_object(lua.lua_state(), node.getPropertiesLua(lua.lua_state()));
    }

    sol::object Shard::NodePropertiesGetByIdViaLua(uint64_t id) {
        Node node = NodeGetPeered(id).get0();
        return sol::make_object(lua.lua_state(), node.getPropertiesLua(lua.lua_state()));
    }

    sol::object Shard::NodePropertyGetViaLua(const std::string& type, const std::string& key, const std::string& property) {
        std::any value = NodePropertyGetPeered(type, key, property).get0();

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

    sol::object Shard::NodePropertyGetByIdViaLua(uint64_t id, const std::string& property) {
        std::any value = NodePropertyGetPeered(id, property).get0();

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

    bool Shard::NodePropertySetViaLua(const std::string& type, const std::string& key, const std::string& property, const sol::object& value) {
        if (value == sol::lua_nil) {
            return false;
        }

        if (value.is<std::string>()) {
            return NodePropertySetPeered(type, key, property, value.as<std::string>()).get0();
        }
        if (value.is<int64_t>()) {
            return NodePropertySetPeered(type, key, property, value.as<int64_t>()).get0();
        }
        if (value.is<double>()) {
            return NodePropertySetPeered(type, key, property, value.as<double>()).get0();
        }
        if (value.is<bool>()) {
            return NodePropertySetPeered(type, key, property, value.as<bool>()).get0();
        }
        if (value.is<std::vector<std::string>>()) {
          return NodePropertySetPeered(type, key, property, value.as<std::vector<std::string>>()).get0();
        }
        if (value.is<std::vector<double>>()) {
          return NodePropertySetPeered(type, key, property, value.as<std::vector<double>>()).get0();
        }
        if (value.is<std::vector<int64_t>>()) {
          return NodePropertySetPeered(type, key, property, value.as<std::vector<int64_t>>()).get0();
        }
        if (value.is<std::vector<bool>>()) {
          return NodePropertySetPeered(type, key, property, value.as<std::vector<bool>>()).get0();
        }

        return false;
    }

    bool Shard::NodePropertySetByIdViaLua(uint64_t id, const std::string& property, const sol::object& value) {
        if (value == sol::lua_nil) {
            return false;
        }

        if (value.is<std::string>()) {
            return NodePropertySetPeered(id, property, value.as<std::string>()).get0();
        }
        if (value.is<int64_t>()) {
            return NodePropertySetPeered(id, property, value.as<int64_t>()).get0();
        }
        if (value.is<double>()) {
            return NodePropertySetPeered(id, property, value.as<double>()).get0();
        }
        if (value.is<bool>()) {
            return NodePropertySetPeered(id, property, value.as<bool>()).get0();
        }

        return false;
    }

    bool Shard::NodePropertiesSetFromJsonViaLua(const std::string& type, const std::string& key, const std::string& value) {
        return NodePropertiesSetFromJsonPeered(type, key, value).get0();
    }

    bool Shard::NodePropertiesSetFromJsonByIdViaLua(uint64_t id, const std::string& value) {
        return NodePropertiesSetFromJsonPeered(id, value).get0();
    }

    bool Shard::NodePropertiesResetFromJsonViaLua(const std::string& type, const std::string& key, const std::string& value) {
        return NodePropertiesResetFromJsonPeered(type, key, value).get0();
    }

    bool Shard::NodePropertiesResetFromJsonByIdViaLua(uint64_t id, const std::string& value) {
        return NodePropertiesResetFromJsonPeered(id, value).get0();
    }

    bool Shard::NodePropertyDeleteViaLua(const std::string& type, const std::string& key, const std::string& property) {
        return NodePropertyDeletePeered(type, key, property).get0();
    }

    bool Shard::NodePropertyDeleteByIdViaLua(uint64_t id, const std::string& property) {
        return NodePropertyDeletePeered(id, property).get0();
    }

    bool Shard::NodePropertiesDeleteViaLua(const std::string& type, const std::string& key) {
        return NodePropertiesDeletePeered(type, key).get0();
    }

    bool Shard::NodePropertiesDeleteByIdViaLua(uint64_t id) {
        return NodePropertiesDeletePeered(id).get0();
    }

}