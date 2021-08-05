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

#include "Node.h"

#include <utility>
#include "Shard.h"

namespace ragedb {

    Node::Node() = default;

    Node::Node(uint64_t id, std::string type, std::string key) : id(id), type(std::move(type)), key(std::move(key)) {}

    Node::Node(uint64_t id, std::string type, std::string key, std::map<std::string, std::any>  properties) : id(id), type(std::move(type)), key(std::move(key)), properties(std::move(properties)) {}

    uint64_t Node::getId() const {
        return id;
    }

    uint16_t Node::getTypeId() const {
        return Shard::externalToTypeId(id);
    }

    std::string Node::getKey() const {
        return key;
    }

    std::string Node::getType() const {
        return type;
    }

    std::map<std::string, std::any> Node::getProperties() const {
        return properties;
    }

    sol::table Node::getPropertiesLua(sol::this_state ts) {
        sol::state_view lua = ts;
        sol::table property_map = lua.create_table();
        for(auto prop : getProperties()) {
            const auto& value_type = prop.second.type();

            if(value_type == typeid(std::string)) {
                property_map[prop.first] = sol::make_object(lua, std::any_cast<std::string>(prop.second));
            }

            if(value_type == typeid(int64_t)) {
                property_map[prop.first] = sol::make_object(lua, std::any_cast<int64_t>(prop.second));
            }

            if(value_type == typeid(double)) {
                property_map[prop.first] = sol::make_object(lua, std::any_cast<double>(prop.second));
            }

            if(value_type == typeid(bool)) {
                property_map[prop.first] = sol::make_object(lua, std::any_cast<bool>(prop.second));
            }

            if(value_type == typeid(std::vector<std::string>)) {
                return sol::make_object(lua, sol::as_table(std::any_cast<std::vector<std::string>>(prop.second)));
            }

            if(value_type == typeid(std::vector<int64_t>)) {
                return sol::make_object(lua, sol::as_table(std::any_cast<std::vector<int64_t>>(prop.second)));
            }

            if(value_type == typeid(std::vector<double>)) {
                return sol::make_object(lua, sol::as_table(std::any_cast<std::vector<double>>(prop.second)));
            }

            if(value_type == typeid(std::vector<bool>)) {
                return sol::make_object(lua, sol::as_table(std::any_cast<std::vector<bool>>(prop.second)));
            }

        }
        return sol::as_table(property_map);
    }

    std::any Node::getProperty(const std::string& property) {
        std::map<std::string, std::any>::iterator it;
        it = properties.find(property);

        if (it != std::end(properties)) {
            return it->second;
        }
        return {};
    }

    std::ostream& operator<<(std::ostream& os, const Node& node) {
        os << "{ \"id\": " << node.id << R"(, "type": ")" << node.type << R"(", "key": )" << "\"" << node.key << "\"" << ", \"properties\": { ";
        bool initial = true;
        for (auto [key, value] : node.properties) {
            if (!initial) {
                os << ", ";
            }
            initial = false;
            os << "\"" << key << "\": ";

            if(value.type() == typeid(std::string)) {
                os << "\"" << std::any_cast<std::string>(value) << "\"";
                continue;
            }
            if(value.type() == typeid(int64_t)) {
                os << std::any_cast<int64_t>(value);
                continue;
            }
            if(value.type() == typeid(double)) {
                os << std::any_cast<double>(value);
                continue;
            }

            // Booleans are stored in std::any as a bit reference so we can't use if(value.type() == typeid(bool)) {
            if(value.type() == typeid(std::_Bit_reference)) {
                if (std::any_cast<std::_Bit_reference>(value) ) {
                    os << "true" ;
                } else {
                    os << "false";
                }
                continue;
            }

            if(value.type() == typeid(std::vector<std::string>)) {
                os << '[';
                bool nested_initial = true;
                for (const auto& item : std::any_cast<std::vector<std::string>>(value)) {
                    if (!nested_initial) {
                        os << ", ";
                    }
                    os << "\"" << item << "\"";
                    nested_initial = false;
                }
                os << ']';
                continue;
            }

            if(value.type() == typeid(std::vector<int64_t>)) {
                os << '[';
                bool nested_initial = true;
                for (const auto& item : std::any_cast<std::vector<int64_t>>(value)) {
                    if (!nested_initial) {
                        os << ", ";
                    }
                    os << item;
                    nested_initial = false;
                }
                os << ']';
                continue;
            }

            if(value.type() == typeid(std::vector<double>)) {
                os << '[';
                bool nested_initial = true;
                for (const auto& item : std::any_cast<std::vector<double>>(value)) {
                    if (!nested_initial) {
                        os << ", ";
                    }
                    os << item;
                    nested_initial = false;
                }
                os << ']';
                continue;
            }

            if(value.type() == typeid(std::vector<bool>)) {
                os << '[';
                bool nested_initial = true;
                for (const auto& item : std::any_cast<std::vector<bool>>(value)) {
                    if (!nested_initial) {
                        os << ", ";
                    }

                    if (item) {
                        os << "true";
                    } else {
                        os << "false";
                    }
                    nested_initial = false;
                }
                os << ']';
                continue;
            }

        }

        os << " } }";

        return os;
    }

}