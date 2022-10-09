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

#ifndef RAGEDB_NODE_H
#define RAGEDB_NODE_H

#include <cstdint>
#include <string>
#include <map>
#include <sol/sol.hpp>
#include "PropertyType.h"

namespace ragedb {

    class Node {
    private:
        uint64_t id{};
        std::string type{};
        std::string key{};
        std::map<std::string, property_type_t> properties{};

    public:
        Node();

        Node(uint64_t id, std::string type, std::string key);

        Node(uint64_t id, std::string type, std::string key, std::map<std::string, property_type_t> );

        [[nodiscard]] uint64_t getId() const;

        [[nodiscard]] uint16_t getTypeId() const;

        [[nodiscard]] std::string getType() const;

        [[nodiscard]] std::string getKey() const;

        [[nodiscard]] std::map<std::string, property_type_t> getProperties() const;

        [[nodiscard]] sol::table getPropertiesLua(sol::this_state) const;

        [[nodiscard]] property_type_t getProperty(const std::string& property) const;

//        [[nodiscard]] sol::object getPropertyLua(const std::string& property, sol::this_state) const;

        [[nodiscard]] sol::lua_value getPropertyLua(const std::string& property, sol::this_state) const;

        friend std::ostream& operator<<(std::ostream& os, const Node& node);

    };
}

#endif //RAGEDB_NODE_H
