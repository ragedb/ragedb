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

    std::vector<uint64_t> Shard::NodeAddManyPeeredViaLua(const std::string &type, const std::vector<std::string>& keys, const std::vector<std::string>& properties) {
      return NodeAddManyPeered(type, keys, properties).get0();
    }

    uint64_t Shard::NodeGetIdViaLua(const std::string& type, const std::string& key) {
        return NodeGetIDPeered(type, key).get0();
    }

    Node Shard::NodeGetViaLua(const std::string& type, const std::string& key) {
        return NodeGetPeered(type, key).get0();
    }

    Node Shard::NodeGetViaLua(uint64_t id) {
        return NodeGetPeered(id).get0();
    }

    bool Shard::NodeRemoveViaLua(const std::string& type, const std::string& key) {
        return NodeRemovePeered(type, key).get0();
    }

    bool Shard::NodeRemoveViaLua(uint64_t id) {
        return NodeRemovePeered(id).get0();
    }

    std::string Shard::NodeGetTypeViaLua(uint64_t id) {
        return NodeGetTypePeered(id).get0();
    }

    std::string Shard::NodeGetKeyViaLua(uint64_t id) {
        return NodeGetKeyPeered(id).get0();
    }

    sol::object Shard::NodeGetPropertiesViaLua(const std::string& type, const std::string& key) {
        Node node = NodeGetPeered(type, key).get0();
        return sol::make_object(lua.lua_state(), node.getPropertiesLua(lua.lua_state()));
    }

    sol::object Shard::NodeGetPropertiesViaLua(uint64_t id) {
        Node node = NodeGetPeered(id).get0();
        return sol::make_object(lua.lua_state(), node.getPropertiesLua(lua.lua_state()));
    }

    sol::object Shard::NodeGetPropertyViaLua(const std::string& type, const std::string& key, const std::string& property) {
        property_type_t value = NodeGetPropertyPeered(type, key, property).get0();
        return PropertyToSolObject(value);
    }

    sol::object Shard::NodeGetPropertyViaLua(uint64_t id, const std::string& property) {
        property_type_t value = NodeGetPropertyPeered(id, property).get0();
        return PropertyToSolObject(value);
    }

    bool Shard::NodeSetPropertyViaLua(const std::string& type, const std::string& key, const std::string& property, const sol::object& value) {
        if (value == sol::lua_nil) {
            return false;
        }

        if (value.is<std::string>()) {
            return NodeSetPropertyPeered(type, key, property, value.as<std::string>()).get0();
        }
        if (value.is<int64_t>()) {
            return NodeSetPropertyPeered(type, key, property, value.as<int64_t>()).get0();
        }
        if (value.is<double>()) {
            return NodeSetPropertyPeered(type, key, property, value.as<double>()).get0();
        }
        if (value.is<bool>()) {
            return NodeSetPropertyPeered(type, key, property, value.as<bool>()).get0();
        }
        if (value.is<std::vector<std::string>>()) {
          return NodeSetPropertyPeered(type, key, property, value.as<std::vector<std::string>>()).get0();
        }
        if (value.is<std::vector<double>>()) {
          return NodeSetPropertyPeered(type, key, property, value.as<std::vector<double>>()).get0();
        }
        if (value.is<std::vector<int64_t>>()) {
          return NodeSetPropertyPeered(type, key, property, value.as<std::vector<int64_t>>()).get0();
        }
        if (value.is<std::vector<bool>>()) {
          return NodeSetPropertyPeered(type, key, property, value.as<std::vector<bool>>()).get0();
        }

        return false;
    }

    bool Shard::NodeSetPropertyViaLua(uint64_t id, const std::string& property, const sol::object& value) {
        if (value == sol::lua_nil) {
            return false;
        }

        if (value.is<std::string>()) {
            return NodeSetPropertyPeered(id, property, value.as<std::string>()).get0();
        }
        if (value.is<int64_t>()) {
            return NodeSetPropertyPeered(id, property, value.as<int64_t>()).get0();
        }
        if (value.is<double>()) {
            return NodeSetPropertyPeered(id, property, value.as<double>()).get0();
        }
        if (value.is<bool>()) {
            return NodeSetPropertyPeered(id, property, value.as<bool>()).get0();
        }

        return false;
    }

    bool Shard::NodeSetPropertyFromJsonViaLua(const std::string& type, const std::string& key, const std::string& property, const std::string& value) {
      return NodeSetPropertyFromJsonPeered(type, key, property, value).get0();
    }

    bool Shard::NodeSetPropertyFromJsonViaLua(uint64_t id, const std::string& property, const std::string& value) {
      return NodeSetPropertyFromJsonPeered(id, property, value).get0();
    }

    bool Shard::NodeSetPropertiesFromJsonViaLua(const std::string& type, const std::string& key, const std::string& value) {
        return NodeSetPropertiesFromJsonPeered(type, key, value).get0();
    }

    bool Shard::NodeSetPropertiesFromJsonViaLua(uint64_t id, const std::string& value) {
        return NodeSetPropertiesFromJsonPeered(id, value).get0();
    }

    bool Shard::NodeResetPropertiesFromJsonViaLua(const std::string& type, const std::string& key, const std::string& value) {
        return NodeResetPropertiesFromJsonPeered(type, key, value).get0();
    }

    bool Shard::NodeResetPropertiesFromJsonViaLua(uint64_t id, const std::string& value) {
        return NodeResetPropertiesFromJsonPeered(id, value).get0();
    }

    bool Shard::NodeDeletePropertyViaLua(const std::string& type, const std::string& key, const std::string& property) {
        return NodeDeletePropertyPeered(type, key, property).get0();
    }

    bool Shard::NodeDeletePropertyViaLua(uint64_t id, const std::string& property) {
        return NodeDeletePropertyPeered(id, property).get0();
    }

    bool Shard::NodeDeletePropertiesViaLua(const std::string& type, const std::string& key) {
        return NodeDeletePropertiesPeered(type, key).get0();
    }

    bool Shard::NodeDeletePropertiesViaLua(uint64_t id) {
        return NodeDeletePropertiesPeered(id).get0();
    }

}